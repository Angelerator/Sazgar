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
    
    Ok(())
}
