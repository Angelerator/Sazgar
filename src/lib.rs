extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::CString,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use sysinfo::{
    System, Disks, Networks, Components, 
    CpuRefreshKind, MemoryRefreshKind, ProcessRefreshKind, RefreshKind,
    ProcessStatus,
};

// ============================================================================
// Unit Conversion Helper
// ============================================================================

#[derive(Clone, Copy, Debug)]
enum SizeUnit {
    Bytes,
    KB,   // 1000
    KiB,  // 1024
    MB,   // 1000^2
    MiB,  // 1024^2
    GB,   // 1000^3
    GiB,  // 1024^3
    TB,   // 1000^4
    TiB,  // 1024^4
}

impl SizeUnit {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "BYTES" | "B" | "" => Some(SizeUnit::Bytes),
            "KB" => Some(SizeUnit::KB),
            "KIB" => Some(SizeUnit::KiB),
            "MB" => Some(SizeUnit::MB),
            "MIB" => Some(SizeUnit::MiB),
            "GB" => Some(SizeUnit::GB),
            "GIB" => Some(SizeUnit::GiB),
            "TB" => Some(SizeUnit::TB),
            "TIB" => Some(SizeUnit::TiB),
            _ => None,
        }
    }

    fn divisor(&self) -> f64 {
        match self {
            SizeUnit::Bytes => 1.0,
            SizeUnit::KB => 1_000.0,
            SizeUnit::KiB => 1_024.0,
            SizeUnit::MB => 1_000_000.0,
            SizeUnit::MiB => 1_048_576.0,
            SizeUnit::GB => 1_000_000_000.0,
            SizeUnit::GiB => 1_073_741_824.0,
            SizeUnit::TB => 1_000_000_000_000.0,
            SizeUnit::TiB => 1_099_511_627_776.0,
        }
    }

    fn convert(&self, bytes: u64) -> f64 {
        bytes as f64 / self.divisor()
    }

    fn name(&self) -> &'static str {
        match self {
            SizeUnit::Bytes => "bytes",
            SizeUnit::KB => "KB",
            SizeUnit::KiB => "KiB",
            SizeUnit::MB => "MB",
            SizeUnit::MiB => "MiB",
            SizeUnit::GB => "GB",
            SizeUnit::GiB => "GiB",
            SizeUnit::TB => "TB",
            SizeUnit::TiB => "TiB",
        }
    }
}

/// Check if a mount point should be filtered (virtual filesystem)
fn is_virtual_filesystem(mount_point: &str, fs_type: &str) -> bool {
    let virtual_mount_points = ["/proc", "/sys", "/dev", "/run", "/snap"];
    let virtual_fs_types = ["proc", "sysfs", "devfs", "devtmpfs", "tmpfs", "overlay", "squashfs"];
    
    for vmp in &virtual_mount_points {
        if mount_point.starts_with(vmp) {
            return true;
        }
    }
    
    for vfs in &virtual_fs_types {
        if fs_type.to_lowercase().contains(vfs) {
            return true;
        }
    }
    
    false
}

/// Get system byte order
fn get_byte_order() -> &'static str {
    #[cfg(target_endian = "little")]
    { "Little Endian" }
    #[cfg(target_endian = "big")]
    { "Big Endian" }
}

// ============================================================================
// CPU Table Function - sazgar_cpu()
// Returns information about each CPU core with cache info
// ============================================================================

#[repr(C)]
struct CpuBindData;

#[repr(C)]
struct CpuInitData {
    current_idx: AtomicUsize,
    cpu_count: usize,
    cpu_data: Vec<CpuInfo>,
    byte_order: String,
}

struct CpuInfo {
    core_id: usize,
    name: String,
    usage_percent: f32,
    frequency_mhz: u64,
    brand: String,
    vendor_id: String,
}

struct CpuVTab;

impl VTab for CpuVTab {
    type InitData = CpuInitData;
    type BindData = CpuBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("core_id", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("usage_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("frequency_mhz", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("brand", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("vendor_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("byte_order", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(CpuBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_cpu(CpuRefreshKind::everything())
        );
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_cpu_all();
        
        let cpu_data: Vec<CpuInfo> = sys.cpus().iter().enumerate().map(|(idx, cpu)| {
            CpuInfo {
                core_id: idx,
                name: cpu.name().to_string(),
                usage_percent: cpu.cpu_usage(),
                frequency_mhz: cpu.frequency(),
                brand: cpu.brand().to_string(),
                vendor_id: cpu.vendor_id().to_string(),
            }
        }).collect();
        
        let cpu_count = cpu_data.len();
        
        Ok(CpuInitData {
            current_idx: AtomicUsize::new(0),
            cpu_count,
            cpu_data,
            byte_order: get_byte_order().to_string(),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.cpu_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.cpu_count - current);
        
        for i in 0..batch_size {
            let cpu = &init_data.cpu_data[current + i];
            
            output.flat_vector(0).as_mut_slice::<u64>()[i] = cpu.core_id as u64;
            output.flat_vector(1).insert(i, CString::new(cpu.name.clone())?);
            output.flat_vector(2).as_mut_slice::<f32>()[i] = cpu.usage_percent;
            output.flat_vector(3).as_mut_slice::<u64>()[i] = cpu.frequency_mhz;
            output.flat_vector(4).insert(i, CString::new(cpu.brand.clone())?);
            output.flat_vector(5).insert(i, CString::new(cpu.vendor_id.clone())?);
            output.flat_vector(6).insert(i, CString::new(init_data.byte_order.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Memory Table Function - sazgar_memory()
// Returns memory and swap usage information with unit support
// ============================================================================

#[repr(C)]
struct MemoryBindData {
    unit: SizeUnit,
}

#[repr(C)]
struct MemoryInitData {
    done: AtomicBool,
    unit: SizeUnit,
    total_memory: u64,
    used_memory: u64,
    free_memory: u64,
    available_memory: u64,
    total_swap: u64,
    used_swap: u64,
    free_swap: u64,
}

struct MemoryVTab;

impl VTab for MemoryVTab {
    type InitData = MemoryInitData;
    type BindData = MemoryBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        // Parse unit parameter
        let unit = if bind.get_named_parameter("unit").is_some() {
            let unit_str = bind.get_named_parameter("unit").unwrap().to_string();
            SizeUnit::from_str(&unit_str).unwrap_or(SizeUnit::Bytes)
        } else {
            SizeUnit::Bytes
        };
        
        bind.add_result_column("unit", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("total_memory", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("used_memory", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("free_memory", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("available_memory", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("memory_usage_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("total_swap", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("used_swap", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("free_swap", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("swap_usage_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        Ok(MemoryBindData { unit })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<MemoryBindData>();
        let unit = unsafe { (*bind_data).unit };
        
        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_memory(MemoryRefreshKind::everything())
        );
        sys.refresh_memory();
        
        let total_memory = sys.total_memory();
        let used_memory = sys.used_memory();
        let available_memory = sys.available_memory();
        let free_memory = sys.free_memory();
        let total_swap = sys.total_swap();
        let used_swap = sys.used_swap();
        let free_swap = sys.free_swap();
        
        Ok(MemoryInitData {
            done: AtomicBool::new(false),
            unit,
            total_memory,
            used_memory,
            free_memory,
            available_memory,
            total_swap,
            used_swap,
            free_swap,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        let unit = init_data.unit;
        
        let usage_percent = if init_data.total_memory > 0 {
            (init_data.used_memory as f32 / init_data.total_memory as f32) * 100.0
        } else {
            0.0
        };
        
        let swap_usage_percent = if init_data.total_swap > 0 {
            (init_data.used_swap as f32 / init_data.total_swap as f32) * 100.0
        } else {
            0.0
        };
        
        output.flat_vector(0).insert(0, CString::new(unit.name())?);
        output.flat_vector(1).as_mut_slice::<f64>()[0] = unit.convert(init_data.total_memory);
        output.flat_vector(2).as_mut_slice::<f64>()[0] = unit.convert(init_data.used_memory);
        output.flat_vector(3).as_mut_slice::<f64>()[0] = unit.convert(init_data.free_memory);
        output.flat_vector(4).as_mut_slice::<f64>()[0] = unit.convert(init_data.available_memory);
        output.flat_vector(5).as_mut_slice::<f32>()[0] = usage_percent;
        output.flat_vector(6).as_mut_slice::<f64>()[0] = unit.convert(init_data.total_swap);
        output.flat_vector(7).as_mut_slice::<f64>()[0] = unit.convert(init_data.used_swap);
        output.flat_vector(8).as_mut_slice::<f64>()[0] = unit.convert(init_data.free_swap);
        output.flat_vector(9).as_mut_slice::<f32>()[0] = swap_usage_percent;
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
    
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("unit".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ])
    }
}

// ============================================================================
// OS Table Function - sazgar_os()
// Returns operating system information with process counts
// ============================================================================

#[repr(C)]
struct OsBindData;

#[repr(C)]
struct OsInitData {
    done: AtomicBool,
    os_name: String,
    os_version: String,
    kernel_version: String,
    hostname: String,
    architecture: String,
    distribution_id: String,
    uptime_seconds: u64,
    boot_time: u64,
    process_count: usize,
}

struct OsVTab;

impl VTab for OsVTab {
    type InitData = OsInitData;
    type BindData = OsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("os_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("os_version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("kernel_version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("hostname", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("architecture", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("distribution_id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("uptime_seconds", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("boot_time", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("process_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        Ok(OsBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::everything())
        );
        
        Ok(OsInitData {
            done: AtomicBool::new(false),
            os_name: System::name().unwrap_or_else(|| "Unknown".to_string()),
            os_version: System::os_version().unwrap_or_else(|| "Unknown".to_string()),
            kernel_version: System::kernel_version().unwrap_or_else(|| "Unknown".to_string()),
            hostname: System::host_name().unwrap_or_else(|| "Unknown".to_string()),
            architecture: System::cpu_arch().unwrap_or_else(|| "Unknown".to_string()),
            distribution_id: System::distribution_id(),
            uptime_seconds: System::uptime(),
            boot_time: System::boot_time(),
            process_count: sys.processes().len(),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).insert(0, CString::new(init_data.os_name.clone())?);
        output.flat_vector(1).insert(0, CString::new(init_data.os_version.clone())?);
        output.flat_vector(2).insert(0, CString::new(init_data.kernel_version.clone())?);
        output.flat_vector(3).insert(0, CString::new(init_data.hostname.clone())?);
        output.flat_vector(4).insert(0, CString::new(init_data.architecture.clone())?);
        output.flat_vector(5).insert(0, CString::new(init_data.distribution_id.clone())?);
        output.flat_vector(6).as_mut_slice::<u64>()[0] = init_data.uptime_seconds;
        output.flat_vector(7).as_mut_slice::<u64>()[0] = init_data.boot_time;
        output.flat_vector(8).as_mut_slice::<u64>()[0] = init_data.process_count as u64;
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// System Table Function - sazgar_system()
// Returns combined system overview
// ============================================================================

#[repr(C)]
struct SystemBindData;

#[repr(C)]
struct SystemInitData {
    done: AtomicBool,
    os_name: String,
    os_version: String,
    hostname: String,
    architecture: String,
    cpu_count: u64,
    physical_core_count: u64,
    cpu_brand: String,
    global_cpu_usage: f32,
    total_memory: u64,
    used_memory: u64,
    available_memory: u64,
    memory_usage_percent: f32,
    uptime_seconds: u64,
    process_count: u64,
}

struct SystemVTab;

impl VTab for SystemVTab {
    type InitData = SystemInitData;
    type BindData = SystemBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("os_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("os_version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("hostname", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("architecture", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("cpu_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("physical_core_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("cpu_brand", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("global_cpu_usage_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("total_memory_bytes", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("used_memory_bytes", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("available_memory_bytes", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("memory_usage_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("uptime_seconds", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("process_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        Ok(SystemBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything())
                .with_processes(ProcessRefreshKind::everything())
        );
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_all();
        
        let total_memory = sys.total_memory();
        let used_memory = sys.used_memory();
        let memory_usage_percent = if total_memory > 0 {
            (used_memory as f32 / total_memory as f32) * 100.0
        } else {
            0.0
        };
        
        let cpu_brand = sys.cpus().first()
            .map(|cpu| cpu.brand().to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        
        let global_cpu_usage = sys.global_cpu_usage();
        
        Ok(SystemInitData {
            done: AtomicBool::new(false),
            os_name: System::name().unwrap_or_else(|| "Unknown".to_string()),
            os_version: System::os_version().unwrap_or_else(|| "Unknown".to_string()),
            hostname: System::host_name().unwrap_or_else(|| "Unknown".to_string()),
            architecture: System::cpu_arch().unwrap_or_else(|| "Unknown".to_string()),
            cpu_count: sys.cpus().len() as u64,
            physical_core_count: sys.physical_core_count().unwrap_or(0) as u64,
            cpu_brand,
            global_cpu_usage,
            total_memory,
            used_memory,
            available_memory: sys.available_memory(),
            memory_usage_percent,
            uptime_seconds: System::uptime(),
            process_count: sys.processes().len() as u64,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).insert(0, CString::new(init_data.os_name.clone())?);
        output.flat_vector(1).insert(0, CString::new(init_data.os_version.clone())?);
        output.flat_vector(2).insert(0, CString::new(init_data.hostname.clone())?);
        output.flat_vector(3).insert(0, CString::new(init_data.architecture.clone())?);
        output.flat_vector(4).as_mut_slice::<u64>()[0] = init_data.cpu_count;
        output.flat_vector(5).as_mut_slice::<u64>()[0] = init_data.physical_core_count;
        output.flat_vector(6).insert(0, CString::new(init_data.cpu_brand.clone())?);
        output.flat_vector(7).as_mut_slice::<f32>()[0] = init_data.global_cpu_usage;
        output.flat_vector(8).as_mut_slice::<u64>()[0] = init_data.total_memory;
        output.flat_vector(9).as_mut_slice::<u64>()[0] = init_data.used_memory;
        output.flat_vector(10).as_mut_slice::<u64>()[0] = init_data.available_memory;
        output.flat_vector(11).as_mut_slice::<f32>()[0] = init_data.memory_usage_percent;
        output.flat_vector(12).as_mut_slice::<u64>()[0] = init_data.uptime_seconds;
        output.flat_vector(13).as_mut_slice::<u64>()[0] = init_data.process_count;
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Disks Table Function - sazgar_disks()
// Returns disk information with unit support and virtual FS filtering
// ============================================================================

#[repr(C)]
struct DisksBindData {
    unit: SizeUnit,
}

#[repr(C)]
struct DisksInitData {
    current_idx: AtomicUsize,
    disk_count: usize,
    disk_data: Vec<DiskInfo>,
    unit: SizeUnit,
}

struct DiskInfo {
    name: String,
    mount_point: String,
    file_system: String,
    total_bytes: u64,
    available_bytes: u64,
    is_removable: bool,
    kind: String,
}

struct DisksVTab;

impl VTab for DisksVTab {
    type InitData = DisksInitData;
    type BindData = DisksBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let unit = if bind.get_named_parameter("unit").is_some() {
            let unit_str = bind.get_named_parameter("unit").unwrap().to_string();
            SizeUnit::from_str(&unit_str).unwrap_or(SizeUnit::Bytes)
        } else {
            SizeUnit::Bytes
        };
        
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("mount_point", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("file_system", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("unit", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("total_space", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("available_space", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("used_space", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("usage_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("is_removable", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column("kind", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(DisksBindData { unit })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<DisksBindData>();
        let unit = unsafe { (*bind_data).unit };
        
        let disks = Disks::new_with_refreshed_list();
        
        // Filter out virtual filesystems
        let disk_data: Vec<DiskInfo> = disks.iter()
            .filter(|disk| {
                let mount_point = disk.mount_point().to_string_lossy().to_string();
                let fs_type = disk.file_system().to_string_lossy().to_string();
                !is_virtual_filesystem(&mount_point, &fs_type)
            })
            .map(|disk| {
                DiskInfo {
                    name: disk.name().to_string_lossy().to_string(),
                    mount_point: disk.mount_point().to_string_lossy().to_string(),
                    file_system: disk.file_system().to_string_lossy().to_string(),
                    total_bytes: disk.total_space(),
                    available_bytes: disk.available_space(),
                    is_removable: disk.is_removable(),
                    kind: format!("{:?}", disk.kind()),
                }
            }).collect();
        
        let disk_count = disk_data.len();
        
        Ok(DisksInitData {
            current_idx: AtomicUsize::new(0),
            disk_count,
            disk_data,
            unit,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.disk_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.disk_count - current);
        let unit = init_data.unit;
        
        for i in 0..batch_size {
            let disk = &init_data.disk_data[current + i];
            let used_bytes = disk.total_bytes.saturating_sub(disk.available_bytes);
            let usage_percent = if disk.total_bytes > 0 {
                (used_bytes as f32 / disk.total_bytes as f32) * 100.0
            } else {
                0.0
            };
            
            output.flat_vector(0).insert(i, CString::new(disk.name.clone())?);
            output.flat_vector(1).insert(i, CString::new(disk.mount_point.clone())?);
            output.flat_vector(2).insert(i, CString::new(disk.file_system.clone())?);
            output.flat_vector(3).insert(i, CString::new(unit.name())?);
            output.flat_vector(4).as_mut_slice::<f64>()[i] = unit.convert(disk.total_bytes);
            output.flat_vector(5).as_mut_slice::<f64>()[i] = unit.convert(disk.available_bytes);
            output.flat_vector(6).as_mut_slice::<f64>()[i] = unit.convert(used_bytes);
            output.flat_vector(7).as_mut_slice::<f32>()[i] = usage_percent;
            output.flat_vector(8).as_mut_slice::<bool>()[i] = disk.is_removable;
            output.flat_vector(9).insert(i, CString::new(disk.kind.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
    
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("unit".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ])
    }
}

// ============================================================================
// Network Table Function - sazgar_network()
// Returns network interface information
// ============================================================================

#[repr(C)]
struct NetworkBindData;

#[repr(C)]
struct NetworkInitData {
    current_idx: AtomicUsize,
    network_count: usize,
    network_data: Vec<NetworkInfo>,
}

struct NetworkInfo {
    interface_name: String,
    mac_address: String,
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
    rx_errors: u64,
    tx_errors: u64,
}

struct NetworkVTab;

impl VTab for NetworkVTab {
    type InitData = NetworkInitData;
    type BindData = NetworkBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("interface_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("mac_address", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("rx_bytes", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("tx_bytes", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("rx_packets", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("tx_packets", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("rx_errors", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("tx_errors", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        Ok(NetworkBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let networks = Networks::new_with_refreshed_list();
        
        let network_data: Vec<NetworkInfo> = networks.iter().map(|(name, data)| {
            NetworkInfo {
                interface_name: name.clone(),
                mac_address: data.mac_address().to_string(),
                rx_bytes: data.total_received(),
                tx_bytes: data.total_transmitted(),
                rx_packets: data.total_packets_received(),
                tx_packets: data.total_packets_transmitted(),
                rx_errors: data.total_errors_on_received(),
                tx_errors: data.total_errors_on_transmitted(),
            }
        }).collect();
        
        let network_count = network_data.len();
        
        Ok(NetworkInitData {
            current_idx: AtomicUsize::new(0),
            network_count,
            network_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.network_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.network_count - current);
        
        for i in 0..batch_size {
            let net = &init_data.network_data[current + i];
            
            output.flat_vector(0).insert(i, CString::new(net.interface_name.clone())?);
            output.flat_vector(1).insert(i, CString::new(net.mac_address.clone())?);
            output.flat_vector(2).as_mut_slice::<u64>()[i] = net.rx_bytes;
            output.flat_vector(3).as_mut_slice::<u64>()[i] = net.tx_bytes;
            output.flat_vector(4).as_mut_slice::<u64>()[i] = net.rx_packets;
            output.flat_vector(5).as_mut_slice::<u64>()[i] = net.tx_packets;
            output.flat_vector(6).as_mut_slice::<u64>()[i] = net.rx_errors;
            output.flat_vector(7).as_mut_slice::<u64>()[i] = net.tx_errors;
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Processes Table Function - sazgar_processes()
// Returns running process information
// ============================================================================

#[repr(C)]
struct ProcessesBindData;

#[repr(C)]
struct ProcessesInitData {
    current_idx: AtomicUsize,
    process_count: usize,
    process_data: Vec<ProcessInfo>,
    total_memory: u64,
}

struct ProcessInfo {
    pid: u32,
    name: String,
    exe_path: String,
    status: String,
    cpu_percent: f32,
    memory_bytes: u64,
    start_time: u64,
    run_time: u64,
    user: String,
}

struct ProcessesVTab;

impl VTab for ProcessesVTab {
    type InitData = ProcessesInitData;
    type BindData = ProcessesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("pid", LogicalTypeHandle::from(LogicalTypeId::UInteger));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("exe_path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("cpu_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("memory_bytes", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("memory_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("start_time", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("run_time_seconds", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("user", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(ProcessesBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_processes(ProcessRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything())
                .with_cpu(CpuRefreshKind::everything())
        );
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_all();
        
        let total_memory = sys.total_memory();
        
        let process_data: Vec<ProcessInfo> = sys.processes().iter().map(|(pid, proc)| {
            let status_str = match proc.status() {
                ProcessStatus::Run => "Running",
                ProcessStatus::Sleep => "Sleeping",
                ProcessStatus::Stop => "Stopped",
                ProcessStatus::Zombie => "Zombie",
                ProcessStatus::Idle => "Idle",
                _ => "Unknown",
            };
            
            let user_id = proc.user_id();
            let user_str = user_id
                .map(|uid| uid.to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            
            ProcessInfo {
                pid: pid.as_u32(),
                name: proc.name().to_string_lossy().to_string(),
                exe_path: proc.exe().map(|p| p.to_string_lossy().to_string()).unwrap_or_default(),
                status: status_str.to_string(),
                cpu_percent: proc.cpu_usage(),
                memory_bytes: proc.memory(),
                start_time: proc.start_time(),
                run_time: proc.run_time(),
                user: user_str,
            }
        }).collect();
        
        let process_count = process_data.len();
        
        Ok(ProcessesInitData {
            current_idx: AtomicUsize::new(0),
            process_count,
            process_data,
            total_memory,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.process_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.process_count - current);
        
        for i in 0..batch_size {
            let proc = &init_data.process_data[current + i];
            let memory_percent = if init_data.total_memory > 0 {
                (proc.memory_bytes as f32 / init_data.total_memory as f32) * 100.0
            } else {
                0.0
            };
            
            output.flat_vector(0).as_mut_slice::<u32>()[i] = proc.pid;
            output.flat_vector(1).insert(i, CString::new(proc.name.clone())?);
            output.flat_vector(2).insert(i, CString::new(proc.exe_path.clone())?);
            output.flat_vector(3).insert(i, CString::new(proc.status.clone())?);
            output.flat_vector(4).as_mut_slice::<f32>()[i] = proc.cpu_percent;
            output.flat_vector(5).as_mut_slice::<u64>()[i] = proc.memory_bytes;
            output.flat_vector(6).as_mut_slice::<f32>()[i] = memory_percent;
            output.flat_vector(7).as_mut_slice::<u64>()[i] = proc.start_time;
            output.flat_vector(8).as_mut_slice::<u64>()[i] = proc.run_time;
            output.flat_vector(9).insert(i, CString::new(proc.user.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Load Table Function - sazgar_load()
// Returns system load averages (Unix only, returns 0 on Windows)
// ============================================================================

#[repr(C)]
struct LoadBindData;

#[repr(C)]
struct LoadInitData {
    done: AtomicBool,
    load_1: f64,
    load_5: f64,
    load_15: f64,
}

struct LoadVTab;

impl VTab for LoadVTab {
    type InitData = LoadInitData;
    type BindData = LoadBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("load_1min", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("load_5min", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("load_15min", LogicalTypeHandle::from(LogicalTypeId::Double));
        Ok(LoadBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let load = System::load_average();
        
        Ok(LoadInitData {
            done: AtomicBool::new(false),
            load_1: load.one,
            load_5: load.five,
            load_15: load.fifteen,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).as_mut_slice::<f64>()[0] = init_data.load_1;
        output.flat_vector(1).as_mut_slice::<f64>()[0] = init_data.load_5;
        output.flat_vector(2).as_mut_slice::<f64>()[0] = init_data.load_15;
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Users Table Function - sazgar_users()
// Returns logged-in users information
// ============================================================================

#[repr(C)]
struct UsersBindData;

#[repr(C)]
struct UsersInitData {
    current_idx: AtomicUsize,
    user_count: usize,
    user_data: Vec<UserInfo>,
}

struct UserInfo {
    uid: String,
    gid: String,
    name: String,
}

struct UsersVTab;

impl VTab for UsersVTab {
    type InitData = UsersInitData;
    type BindData = UsersBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("uid", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("gid", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(UsersBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let users = sysinfo::Users::new_with_refreshed_list();
        
        let user_data: Vec<UserInfo> = users.iter().map(|user| {
            UserInfo {
                uid: user.id().to_string(),
                gid: user.group_id().to_string(),
                name: user.name().to_string(),
            }
        }).collect();
        
        let user_count = user_data.len();
        
        Ok(UsersInitData {
            current_idx: AtomicUsize::new(0),
            user_count,
            user_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.user_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.user_count - current);
        
        for i in 0..batch_size {
            let user = &init_data.user_data[current + i];
            
            output.flat_vector(0).insert(i, CString::new(user.uid.clone())?);
            output.flat_vector(1).insert(i, CString::new(user.gid.clone())?);
            output.flat_vector(2).insert(i, CString::new(user.name.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Components Table Function - sazgar_components()
// Returns temperature sensor information
// ============================================================================

#[repr(C)]
struct ComponentsBindData;

#[repr(C)]
struct ComponentsInitData {
    current_idx: AtomicUsize,
    component_count: usize,
    component_data: Vec<ComponentInfo>,
}

struct ComponentInfo {
    label: String,
    temperature: f32,
    max_temperature: f32,
    critical_temperature: Option<f32>,
}

struct ComponentsVTab;

impl VTab for ComponentsVTab {
    type InitData = ComponentsInitData;
    type BindData = ComponentsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("label", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("temperature_celsius", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("max_temperature_celsius", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("critical_temperature_celsius", LogicalTypeHandle::from(LogicalTypeId::Float));
        Ok(ComponentsBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let components = Components::new_with_refreshed_list();
        
        let component_data: Vec<ComponentInfo> = components.iter().map(|comp| {
            ComponentInfo {
                label: comp.label().to_string(),
                temperature: comp.temperature(),
                max_temperature: comp.max(),
                critical_temperature: comp.critical(),
            }
        }).collect();
        
        let component_count = component_data.len();
        
        Ok(ComponentsInitData {
            current_idx: AtomicUsize::new(0),
            component_count,
            component_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.component_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.component_count - current);
        
        for i in 0..batch_size {
            let comp = &init_data.component_data[current + i];
            
            output.flat_vector(0).insert(i, CString::new(comp.label.clone())?);
            output.flat_vector(1).as_mut_slice::<f32>()[i] = comp.temperature;
            output.flat_vector(2).as_mut_slice::<f32>()[i] = comp.max_temperature;
            output.flat_vector(3).as_mut_slice::<f32>()[i] = comp.critical_temperature.unwrap_or(0.0);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Environment Variables Table Function - sazgar_environment()
// Returns environment variables
// ============================================================================

#[repr(C)]
struct EnvironmentBindData {
    filter: Option<String>,
}

struct EnvVar {
    name: String,
    value: String,
}

#[repr(C)]
struct EnvironmentInitData {
    current_idx: AtomicUsize,
    env_count: usize,
    env_data: Vec<EnvVar>,
}

struct EnvironmentVTab;

impl VTab for EnvironmentVTab {
    type InitData = EnvironmentInitData;
    type BindData = EnvironmentBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("value", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        let filter = if bind.get_parameter_count() > 0 {
            let param = bind.get_parameter(0).to_string();
            let cleaned = param.trim_matches('"').to_string();
            if cleaned.is_empty() { None } else { Some(cleaned) }
        } else {
            None
        };
        
        Ok(EnvironmentBindData { filter })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = init.get_bind_data::<EnvironmentBindData>();
        let filter = unsafe { (*bind_data).filter.clone() };
        
        let env_data: Vec<EnvVar> = std::env::vars()
            .filter(|(name, _)| {
                match &filter {
                    Some(f) => name.to_lowercase().contains(&f.to_lowercase()),
                    None => true,
                }
            })
            .map(|(name, value)| EnvVar { name, value })
            .collect();
        
        let env_count = env_data.len();
        
        Ok(EnvironmentInitData {
            current_idx: AtomicUsize::new(0),
            env_count,
            env_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.env_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.env_count - current);
        
        for i in 0..batch_size {
            let env = &init_data.env_data[current + i];
            output.flat_vector(0).insert(i, CString::new(env.name.clone())?);
            output.flat_vector(1).insert(i, CString::new(env.value.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ============================================================================
// Uptime Table Function - sazgar_uptime()
// Returns system uptime in various formats
// ============================================================================

#[repr(C)]
struct UptimeBindData;

#[repr(C)]
struct UptimeInitData {
    done: AtomicBool,
}

struct UptimeVTab;

impl VTab for UptimeVTab {
    type InitData = UptimeInitData;
    type BindData = UptimeBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("uptime_seconds", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("uptime_minutes", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("uptime_hours", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("uptime_days", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("uptime_formatted", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("boot_time_epoch", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        Ok(UptimeBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(UptimeInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        let uptime_secs = System::uptime();
        let uptime_mins = uptime_secs as f64 / 60.0;
        let uptime_hrs = uptime_secs as f64 / 3600.0;
        let uptime_days = uptime_secs as f64 / 86400.0;
        
        let days = uptime_secs / 86400;
        let hours = (uptime_secs % 86400) / 3600;
        let minutes = (uptime_secs % 3600) / 60;
        let seconds = uptime_secs % 60;
        let formatted = format!("{}d {}h {}m {}s", days, hours, minutes, seconds);
        
        let boot_time = System::boot_time();
        
        output.flat_vector(0).as_mut_slice::<i64>()[0] = uptime_secs as i64;
        output.flat_vector(1).as_mut_slice::<f64>()[0] = uptime_mins;
        output.flat_vector(2).as_mut_slice::<f64>()[0] = uptime_hrs;
        output.flat_vector(3).as_mut_slice::<f64>()[0] = uptime_days;
        output.flat_vector(4).insert(0, CString::new(formatted)?);
        output.flat_vector(5).as_mut_slice::<i64>()[0] = boot_time as i64;
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Network Ports Table Function - sazgar_ports()
// Returns open network ports and connections
// ============================================================================

#[repr(C)]
struct PortsBindData {
    protocol_filter: Option<String>,
}

struct PortInfo {
    protocol: String,
    local_address: String,
    local_port: u16,
    remote_address: String,
    remote_port: u16,
    state: String,
    pid: Option<u32>,
    process_name: String,
}

#[repr(C)]
struct PortsInitData {
    current_idx: AtomicUsize,
    port_count: usize,
    port_data: Vec<PortInfo>,
}

struct PortsVTab;

impl VTab for PortsVTab {
    type InitData = PortsInitData;
    type BindData = PortsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("protocol", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("local_address", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("local_port", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("remote_address", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("remote_port", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("state", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("pid", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("process_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        let protocol_filter = if bind.get_parameter_count() > 0 {
            let param = bind.get_parameter(0).to_string();
            let cleaned = param.trim_matches('"').to_uppercase();
            if cleaned.is_empty() { None } else { Some(cleaned) }
        } else {
            None
        };
        
        Ok(PortsBindData { protocol_filter })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        use netstat2::{get_sockets_info, AddressFamilyFlags, ProtocolFlags, ProtocolSocketInfo};
        
        let bind_data = init.get_bind_data::<PortsBindData>();
        let protocol_filter = unsafe { (*bind_data).protocol_filter.clone() };
        
        // Get process info for name lookup
        let sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new())
        );
        
        let af_flags = AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6;
        let proto_flags = ProtocolFlags::TCP | ProtocolFlags::UDP;
        
        let mut port_data: Vec<PortInfo> = Vec::new();
        
        if let Ok(sockets) = get_sockets_info(af_flags, proto_flags) {
            for socket in sockets {
                let (protocol, local_addr, local_port, remote_addr, remote_port, state) = 
                    match &socket.protocol_socket_info {
                        ProtocolSocketInfo::Tcp(tcp) => {
                            if let Some(ref filter) = protocol_filter {
                                if filter != "TCP" { continue; }
                            }
                            (
                                "TCP".to_string(),
                                tcp.local_addr.to_string(),
                                tcp.local_port,
                                tcp.remote_addr.to_string(),
                                tcp.remote_port,
                                format!("{:?}", tcp.state),
                            )
                        }
                        ProtocolSocketInfo::Udp(udp) => {
                            if let Some(ref filter) = protocol_filter {
                                if filter != "UDP" { continue; }
                            }
                            (
                                "UDP".to_string(),
                                udp.local_addr.to_string(),
                                udp.local_port,
                                "".to_string(),
                                0,
                                "".to_string(),
                            )
                        }
                    };
                
                let pids = &socket.associated_pids;
                let pid = pids.first().copied();
                
                let process_name = pid
                    .and_then(|p| sys.process(sysinfo::Pid::from_u32(p)))
                    .map(|proc| proc.name().to_string_lossy().to_string())
                    .unwrap_or_default();
                
                port_data.push(PortInfo {
                    protocol,
                    local_address: local_addr,
                    local_port,
                    remote_address: remote_addr,
                    remote_port,
                    state,
                    pid,
                    process_name,
                });
            }
        }
        
        let port_count = port_data.len();
        
        Ok(PortsInitData {
            current_idx: AtomicUsize::new(0),
            port_count,
            port_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.port_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.port_count - current);
        
        for i in 0..batch_size {
            let port = &init_data.port_data[current + i];
            
            output.flat_vector(0).insert(i, CString::new(port.protocol.clone())?);
            output.flat_vector(1).insert(i, CString::new(port.local_address.clone())?);
            output.flat_vector(2).as_mut_slice::<i32>()[i] = port.local_port as i32;
            output.flat_vector(3).insert(i, CString::new(port.remote_address.clone())?);
            output.flat_vector(4).as_mut_slice::<i32>()[i] = port.remote_port as i32;
            output.flat_vector(5).insert(i, CString::new(port.state.clone())?);
            output.flat_vector(6).as_mut_slice::<i32>()[i] = port.pid.unwrap_or(0) as i32;
            output.flat_vector(7).insert(i, CString::new(port.process_name.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ============================================================================
// GPU Table Function - sazgar_gpu() 
// Returns GPU information (NVIDIA GPUs when feature enabled)
// ============================================================================

#[repr(C)]
struct GpuBindData;

struct GpuInfo {
    index: u32,
    name: String,
    driver_version: String,
    memory_total_mb: u64,
    memory_used_mb: u64,
    memory_free_mb: u64,
    temperature_celsius: Option<u32>,
    power_usage_watts: Option<u32>,
    utilization_gpu_percent: Option<u32>,
    utilization_memory_percent: Option<u32>,
}

#[repr(C)]
struct GpuInitData {
    current_idx: AtomicUsize,
    gpu_count: usize,
    gpu_data: Vec<GpuInfo>,
}

struct GpuVTab;

impl VTab for GpuVTab {
    type InitData = GpuInitData;
    type BindData = GpuBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("index", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("driver_version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("memory_total_mb", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("memory_used_mb", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("memory_free_mb", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("temperature_celsius", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("power_usage_watts", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("utilization_gpu_percent", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("utilization_memory_percent", LogicalTypeHandle::from(LogicalTypeId::Integer));
        Ok(GpuBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        #[allow(unused_mut)]
        let mut gpu_data: Vec<GpuInfo> = Vec::new();
        
        #[cfg(feature = "nvidia")]
        {
            use nvml_wrapper::Nvml;
            
            if let Ok(nvml) = Nvml::init() {
                let driver_version = nvml.sys_driver_version().unwrap_or_else(|_| "unknown".to_string());
                
                if let Ok(device_count) = nvml.device_count() {
                    for idx in 0..device_count {
                        if let Ok(device) = nvml.device_by_index(idx) {
                            let name = device.name().unwrap_or_else(|_| "Unknown GPU".to_string());
                            
                            let (memory_total_mb, memory_used_mb, memory_free_mb) = 
                                if let Ok(mem_info) = device.memory_info() {
                                    (mem_info.total / 1_000_000, mem_info.used / 1_000_000, mem_info.free / 1_000_000)
                                } else {
                                    (0, 0, 0)
                                };
                            
                            let temperature_celsius = device.temperature(nvml_wrapper::enum_wrappers::device::TemperatureSensor::Gpu).ok();
                            
                            let power_usage_watts = device.power_usage().ok().map(|mw| mw / 1000);
                            
                            let (utilization_gpu_percent, utilization_memory_percent) = 
                                if let Ok(util) = device.utilization_rates() {
                                    (Some(util.gpu), Some(util.memory))
                                } else {
                                    (None, None)
                                };
                            
                            gpu_data.push(GpuInfo {
                                index: idx,
                                name,
                                driver_version: driver_version.clone(),
                                memory_total_mb,
                                memory_used_mb,
                                memory_free_mb,
                                temperature_celsius,
                                power_usage_watts,
                                utilization_gpu_percent,
                                utilization_memory_percent,
                            });
                        }
                    }
                }
            }
        }
        
        // If no NVIDIA feature or no GPUs found, return empty
        let gpu_count = gpu_data.len();
        
        Ok(GpuInitData {
            current_idx: AtomicUsize::new(0),
            gpu_count,
            gpu_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.gpu_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.gpu_count - current);
        
        for i in 0..batch_size {
            let gpu = &init_data.gpu_data[current + i];
            
            output.flat_vector(0).as_mut_slice::<i32>()[i] = gpu.index as i32;
            output.flat_vector(1).insert(i, CString::new(gpu.name.clone())?);
            output.flat_vector(2).insert(i, CString::new(gpu.driver_version.clone())?);
            output.flat_vector(3).as_mut_slice::<i64>()[i] = gpu.memory_total_mb as i64;
            output.flat_vector(4).as_mut_slice::<i64>()[i] = gpu.memory_used_mb as i64;
            output.flat_vector(5).as_mut_slice::<i64>()[i] = gpu.memory_free_mb as i64;
            output.flat_vector(6).as_mut_slice::<i32>()[i] = gpu.temperature_celsius.unwrap_or(0) as i32;
            output.flat_vector(7).as_mut_slice::<i32>()[i] = gpu.power_usage_watts.unwrap_or(0) as i32;
            output.flat_vector(8).as_mut_slice::<i32>()[i] = gpu.utilization_gpu_percent.unwrap_or(0) as i32;
            output.flat_vector(9).as_mut_slice::<i32>()[i] = gpu.utilization_memory_percent.unwrap_or(0) as i32;
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Swap Table Function - sazgar_swap()
// Returns swap/virtual memory information
// ============================================================================

#[repr(C)]
struct SwapBindData {
    unit: SizeUnit,
}

#[repr(C)]
struct SwapInitData {
    done: AtomicBool,
    unit: SizeUnit,
}

struct SwapVTab;

impl VTab for SwapVTab {
    type InitData = SwapInitData;
    type BindData = SwapBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("total_swap", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("used_swap", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("free_swap", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("swap_usage_percent", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("unit", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        let unit = if bind.get_parameter_count() > 0 {
            let param = bind.get_parameter(0).to_string();
            let unit_str = param.trim_matches('"');
            SizeUnit::from_str(unit_str).unwrap_or(SizeUnit::Bytes)
        } else {
            SizeUnit::Bytes
        };
        
        Ok(SwapBindData { unit })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = init.get_bind_data::<SwapBindData>();
        let unit = unsafe { (*bind_data).unit };
        
        Ok(SwapInitData {
            done: AtomicBool::new(false),
            unit,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        let mut sys = System::new();
        sys.refresh_memory_specifics(MemoryRefreshKind::new().with_swap());
        
        let total_swap = sys.total_swap();
        let used_swap = sys.used_swap();
        let free_swap = sys.free_swap();
        let usage_percent = if total_swap > 0 {
            (used_swap as f64 / total_swap as f64) * 100.0
        } else {
            0.0
        };
        
        let unit = init_data.unit;
        
        output.flat_vector(0).as_mut_slice::<f64>()[0] = unit.convert(total_swap);
        output.flat_vector(1).as_mut_slice::<f64>()[0] = unit.convert(used_swap);
        output.flat_vector(2).as_mut_slice::<f64>()[0] = unit.convert(free_swap);
        output.flat_vector(3).as_mut_slice::<f64>()[0] = usage_percent;
        output.flat_vector(4).insert(0, CString::new(unit.name())?);
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ============================================================================
// CPU Cores Table Function - sazgar_cpu_cores()
// Returns per-core CPU usage information
// ============================================================================

#[repr(C)]
struct CpuCoresBindData;

struct CpuCoreInfo {
    core_id: usize,
    usage_percent: f32,
    frequency_mhz: u64,
    vendor: String,
    brand: String,
}

#[repr(C)]
struct CpuCoresInitData {
    current_idx: AtomicUsize,
    core_count: usize,
    core_data: Vec<CpuCoreInfo>,
}

struct CpuCoresVTab;

impl VTab for CpuCoresVTab {
    type InitData = CpuCoresInitData;
    type BindData = CpuCoresBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("core_id", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("usage_percent", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("frequency_mhz", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("vendor", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("brand", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(CpuCoresBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let mut sys = System::new();
        sys.refresh_cpu_specifics(CpuRefreshKind::new().with_cpu_usage().with_frequency());
        
        // Need to wait for CPU usage to be calculated
        std::thread::sleep(std::time::Duration::from_millis(200));
        sys.refresh_cpu_specifics(CpuRefreshKind::new().with_cpu_usage().with_frequency());
        
        let core_data: Vec<CpuCoreInfo> = sys.cpus().iter().enumerate().map(|(idx, cpu)| {
            CpuCoreInfo {
                core_id: idx,
                usage_percent: cpu.cpu_usage(),
                frequency_mhz: cpu.frequency(),
                vendor: cpu.vendor_id().to_string(),
                brand: cpu.brand().to_string(),
            }
        }).collect();
        
        let core_count = core_data.len();
        
        Ok(CpuCoresInitData {
            current_idx: AtomicUsize::new(0),
            core_count,
            core_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.core_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.core_count - current);
        
        for i in 0..batch_size {
            let core = &init_data.core_data[current + i];
            
            output.flat_vector(0).as_mut_slice::<i32>()[i] = core.core_id as i32;
            output.flat_vector(1).as_mut_slice::<f32>()[i] = core.usage_percent;
            output.flat_vector(2).as_mut_slice::<i64>()[i] = core.frequency_mhz as i64;
            output.flat_vector(3).insert(i, CString::new(core.vendor.clone())?);
            output.flat_vector(4).insert(i, CString::new(core.brand.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// File Descriptors Table Function - sazgar_fds()
// Returns open file descriptors for processes (Linux/macOS)
// ============================================================================

#[repr(C)]
struct FdsBindData {
    pid_filter: Option<u32>,
}

struct FdInfo {
    pid: u32,
    process_name: String,
    fd_count: usize,
}

#[repr(C)]
struct FdsInitData {
    current_idx: AtomicUsize,
    fd_count: usize,
    fd_data: Vec<FdInfo>,
}

struct FdsVTab;

impl VTab for FdsVTab {
    type InitData = FdsInitData;
    type BindData = FdsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("pid", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("process_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("fd_count", LogicalTypeHandle::from(LogicalTypeId::Integer));
        
        let pid_filter = if bind.get_parameter_count() > 0 {
            let param = bind.get_parameter(0).to_string();
            let cleaned = param.trim_matches('"');
            cleaned.parse::<u32>().ok()
        } else {
            None
        };
        
        Ok(FdsBindData { pid_filter })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = init.get_bind_data::<FdsBindData>();
        let pid_filter = unsafe { (*bind_data).pid_filter };
        
        let sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new())
        );
        
        let fd_data: Vec<FdInfo> = sys.processes()
            .iter()
            .filter(|(pid, _)| {
                match pid_filter {
                    Some(filter) => pid.as_u32() == filter,
                    None => true,
                }
            })
            .map(|(pid, proc)| {
                // Get fd count from /proc/<pid>/fd on Linux
                #[cfg(target_os = "linux")]
                let fd_count = std::fs::read_dir(format!("/proc/{}/fd", pid.as_u32()))
                    .map(|dir| dir.count())
                    .unwrap_or(0);
                
                #[cfg(not(target_os = "linux"))]
                let fd_count = 0usize;
                
                FdInfo {
                    pid: pid.as_u32(),
                    process_name: proc.name().to_string_lossy().to_string(),
                    fd_count,
                }
            })
            .collect();
        
        let count = fd_data.len();
        
        Ok(FdsInitData {
            current_idx: AtomicUsize::new(0),
            fd_count: count,
            fd_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.fd_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.fd_count - current);
        
        for i in 0..batch_size {
            let fd = &init_data.fd_data[current + i];
            
            output.flat_vector(0).as_mut_slice::<i32>()[i] = fd.pid as i32;
            output.flat_vector(1).insert(i, CString::new(fd.process_name.clone())?);
            output.flat_vector(2).as_mut_slice::<i32>()[i] = fd.fd_count as i32;
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Integer)])
    }
}

// ============================================================================
// Docker Containers Table Function - sazgar_docker()
// Returns Docker container information (when Docker is available)
// ============================================================================

#[repr(C)]
struct DockerBindData;

struct DockerContainerInfo {
    id: String,
    name: String,
    image: String,
    status: String,
    state: String,
    created: String,
}

#[repr(C)]
struct DockerInitData {
    current_idx: AtomicUsize,
    container_count: usize,
    container_data: Vec<DockerContainerInfo>,
}

struct DockerVTab;

impl VTab for DockerVTab {
    type InitData = DockerInitData;
    type BindData = DockerBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("id", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("image", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("state", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("created", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(DockerBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let mut container_data: Vec<DockerContainerInfo> = Vec::new();
        
        // Try to get Docker containers using docker CLI
        // This is a simple approach that doesn't require additional dependencies
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if let Ok(output) = std::process::Command::new("docker")
                .args(["ps", "-a", "--format", "{{.ID}}|{{.Names}}|{{.Image}}|{{.Status}}|{{.State}}|{{.CreatedAt}}"])
                .output()
            {
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    for line in stdout.lines() {
                        let parts: Vec<&str> = line.split('|').collect();
                        if parts.len() >= 6 {
                            container_data.push(DockerContainerInfo {
                                id: parts[0].to_string(),
                                name: parts[1].to_string(),
                                image: parts[2].to_string(),
                                status: parts[3].to_string(),
                                state: parts[4].to_string(),
                                created: parts[5].to_string(),
                            });
                        }
                    }
                }
            }
        }
        
        let container_count = container_data.len();
        
        Ok(DockerInitData {
            current_idx: AtomicUsize::new(0),
            container_count,
            container_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.container_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.container_count - current);
        
        for i in 0..batch_size {
            let container = &init_data.container_data[current + i];
            
            output.flat_vector(0).insert(i, CString::new(container.id.clone())?);
            output.flat_vector(1).insert(i, CString::new(container.name.clone())?);
            output.flat_vector(2).insert(i, CString::new(container.image.clone())?);
            output.flat_vector(3).insert(i, CString::new(container.status.clone())?);
            output.flat_vector(4).insert(i, CString::new(container.state.clone())?);
            output.flat_vector(5).insert(i, CString::new(container.created.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Services Table Function - sazgar_services()
// Returns running system services (platform-specific)
// ============================================================================

#[repr(C)]
struct ServicesBindData;

struct ServiceInfo {
    name: String,
    status: String,
    description: String,
}

#[repr(C)]
struct ServicesInitData {
    current_idx: AtomicUsize,
    service_count: usize,
    service_data: Vec<ServiceInfo>,
}

struct ServicesVTab;

impl VTab for ServicesVTab {
    type InitData = ServicesInitData;
    type BindData = ServicesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("description", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(ServicesBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let mut service_data: Vec<ServiceInfo> = Vec::new();
        
        // macOS: Use launchctl
        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = std::process::Command::new("launchctl")
                .args(["list"])
                .output()
            {
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    for line in stdout.lines().skip(1) {  // Skip header
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 3 {
                            service_data.push(ServiceInfo {
                                name: parts[2].to_string(),
                                status: if parts[0] == "-" { "inactive".to_string() } else { "running".to_string() },
                                description: "".to_string(),
                            });
                        }
                    }
                }
            }
        }
        
        // Linux: Use systemctl
        #[cfg(target_os = "linux")]
        {
            if let Ok(output) = std::process::Command::new("systemctl")
                .args(["list-units", "--type=service", "--all", "--no-pager", "--plain"])
                .output()
            {
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    for line in stdout.lines().skip(1) {  // Skip header
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 4 {
                            let name = parts[0].trim_end_matches(".service").to_string();
                            let status = parts[3].to_string();
                            let description = parts[4..].join(" ");
                            service_data.push(ServiceInfo {
                                name,
                                status,
                                description,
                            });
                        }
                    }
                }
            }
        }
        
        let service_count = service_data.len();
        
        Ok(ServicesInitData {
            current_idx: AtomicUsize::new(0),
            service_count,
            service_data,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.service_count {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.service_count - current);
        
        for i in 0..batch_size {
            let service = &init_data.service_data[current + i];
            
            output.flat_vector(0).insert(i, CString::new(service.name.clone())?);
            output.flat_vector(1).insert(i, CString::new(service.status.clone())?);
            output.flat_vector(2).insert(i, CString::new(service.description.clone())?);
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Version Table Function - sazgar_version()
// Returns the extension version
// ============================================================================

#[repr(C)]
struct VersionBindData;

#[repr(C)]
struct VersionInitData {
    done: AtomicBool,
}

struct VersionVTab;

impl VTab for VersionVTab {
    type InitData = VersionInitData;
    type BindData = VersionBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(VersionBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(VersionInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        let version = env!("CARGO_PKG_VERSION");
        output.flat_vector(0).insert(0, CString::new(version)?);
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Extension Entry Point
// ============================================================================

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // Register all table functions
    con.register_table_function::<CpuVTab>("sazgar_cpu")
        .expect("Failed to register sazgar_cpu table function");
    
    con.register_table_function::<MemoryVTab>("sazgar_memory")
        .expect("Failed to register sazgar_memory table function");
    
    con.register_table_function::<OsVTab>("sazgar_os")
        .expect("Failed to register sazgar_os table function");
    
    con.register_table_function::<SystemVTab>("sazgar_system")
        .expect("Failed to register sazgar_system table function");
    
    con.register_table_function::<DisksVTab>("sazgar_disks")
        .expect("Failed to register sazgar_disks table function");
    
    con.register_table_function::<NetworkVTab>("sazgar_network")
        .expect("Failed to register sazgar_network table function");
    
    con.register_table_function::<ProcessesVTab>("sazgar_processes")
        .expect("Failed to register sazgar_processes table function");
    
    con.register_table_function::<LoadVTab>("sazgar_load")
        .expect("Failed to register sazgar_load table function");
    
    con.register_table_function::<UsersVTab>("sazgar_users")
        .expect("Failed to register sazgar_users table function");
    
    con.register_table_function::<ComponentsVTab>("sazgar_components")
        .expect("Failed to register sazgar_components table function");
    
    con.register_table_function::<VersionVTab>("sazgar_version")
        .expect("Failed to register sazgar_version table function");
    
    // New functions in v0.3.0
    con.register_table_function::<EnvironmentVTab>("sazgar_environment")
        .expect("Failed to register sazgar_environment table function");
    
    con.register_table_function::<UptimeVTab>("sazgar_uptime")
        .expect("Failed to register sazgar_uptime table function");
    
    con.register_table_function::<PortsVTab>("sazgar_ports")
        .expect("Failed to register sazgar_ports table function");
    
    con.register_table_function::<GpuVTab>("sazgar_gpu")
        .expect("Failed to register sazgar_gpu table function");
    
    con.register_table_function::<SwapVTab>("sazgar_swap")
        .expect("Failed to register sazgar_swap table function");
    
    con.register_table_function::<CpuCoresVTab>("sazgar_cpu_cores")
        .expect("Failed to register sazgar_cpu_cores table function");
    
    con.register_table_function::<FdsVTab>("sazgar_fds")
        .expect("Failed to register sazgar_fds table function");
    
    con.register_table_function::<DockerVTab>("sazgar_docker")
        .expect("Failed to register sazgar_docker table function");
    
    con.register_table_function::<ServicesVTab>("sazgar_services")
        .expect("Failed to register sazgar_services table function");
    
    Ok(())
}
