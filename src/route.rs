//! Sazgar Routing Module
//! 
//! Provides intelligent query routing between local DuckDB and remote backends.
//! 
//! ## Core Functions
//! - `sazgar_resources()` - Current system resources (RAM, CPU, load)
//! - `sazgar_estimate(path)` - Estimate data size for a path
//! - `sazgar_run(query, target)` - Execute query on target backend
//! - `sazgar_backends()` - List registered backends

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    collections::HashMap,
    ffi::CString,
    sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Mutex, OnceLock},
};
use sysinfo::{System, MemoryRefreshKind, CpuRefreshKind, RefreshKind};

// ============================================================================
// Backend Registry (Global State)
// ============================================================================

/// Registered backends storage
static BACKENDS: OnceLock<Mutex<HashMap<String, BackendConfig>>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub name: String,
    pub url: String,
    pub dialect: SqlDialect,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlDialect {
    DuckDB,      // DuckDB, Tavana (DuckDB-based)
    PostgreSQL,  // Standard PostgreSQL
    MySQL,       // MySQL/MariaDB
    BigQuery,    // Google BigQuery
    Snowflake,   // Snowflake
    SQLite,      // SQLite
}

impl SqlDialect {
    fn from_url(url: &str) -> Self {
        if url == "local" || url.starts_with("duckdb://") {
            SqlDialect::DuckDB
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            // Check if it's Tavana (DuckDB behind PostgreSQL wire protocol)
            if url.contains("tavana") {
                SqlDialect::DuckDB
            } else {
                SqlDialect::PostgreSQL
            }
        } else if url.starts_with("mysql://") {
            SqlDialect::MySQL
        } else if url.starts_with("bigquery://") {
            SqlDialect::BigQuery
        } else if url.starts_with("snowflake://") {
            SqlDialect::Snowflake
        } else if url.starts_with("sqlite://") || url.ends_with(".db") || url.ends_with(".sqlite") {
            SqlDialect::SQLite
        } else if url.contains("host=") || url.contains("sslmode=") {
            // PostgreSQL connection string format
            SqlDialect::DuckDB // Assume Tavana/DuckDB behind pg wire
        } else {
            // Default to DuckDB for unknown protocols
            SqlDialect::DuckDB
        }
    }
    
    /// Get the required DuckDB extension for this dialect
    fn required_extension(&self) -> Option<&'static str> {
        match self {
            SqlDialect::DuckDB => None, // No extension needed for local (but postgres ext needed for remote DuckDB like Tavana)
            SqlDialect::PostgreSQL => Some("postgres"),
            SqlDialect::MySQL => Some("mysql"),
            SqlDialect::SQLite => Some("sqlite"),
            SqlDialect::BigQuery => Some("bigquery"),  // Community extension
            SqlDialect::Snowflake => Some("snowflake"), // Community extension
        }
    }
    
    /// Get required extension for remote DuckDB (via PostgreSQL wire)
    fn required_extension_for_remote(&self, url: &str) -> &'static str {
        if url == "local" {
            "none"
        } else {
            "postgres"  // All remote DuckDB connections use postgres extension
        }
    }
    
    /// Get the query function name for this dialect
    fn query_function(&self) -> &'static str {
        match self {
            SqlDialect::DuckDB => "", // Direct execution
            SqlDialect::PostgreSQL => "postgres_query",
            SqlDialect::MySQL => "mysql_query",
            SqlDialect::SQLite => "sqlite_scan",  // SQLite uses scan, not query
            SqlDialect::BigQuery => "bigquery_query",
            SqlDialect::Snowflake => "snowflake_query",
        }
    }
    
    /// Check if this dialect needs ATTACH before query
    fn needs_attach(&self) -> bool {
        matches!(self, SqlDialect::PostgreSQL | SqlDialect::MySQL | SqlDialect::SQLite)
    }
}

fn get_backends() -> &'static Mutex<HashMap<String, BackendConfig>> {
    BACKENDS.get_or_init(|| {
        let mut map = HashMap::new();
        // Register 'local' as the default backend
        map.insert("local".to_string(), BackendConfig {
            name: "local".to_string(),
            url: "local".to_string(),
            dialect: SqlDialect::DuckDB,
        });
        Mutex::new(map)
    })
}

/// Register a named backend
pub fn register_backend(name: &str, url: &str) {
    let dialect = SqlDialect::from_url(url);
    let config = BackendConfig {
        name: name.to_string(),
        url: url.to_string(),
        dialect,
    };
    
    if let Ok(mut backends) = get_backends().lock() {
        backends.insert(name.to_string(), config);
    }
}

/// Get backend by name or URL
pub fn resolve_backend(target: &str) -> BackendConfig {
    // First, try to find by name
    if let Ok(backends) = get_backends().lock() {
        if let Some(config) = backends.get(target) {
            return config.clone();
        }
    }
    
    // If not found, treat target as a URL
    BackendConfig {
        name: target.to_string(),
        url: target.to_string(),
        dialect: SqlDialect::from_url(target),
    }
}

// ============================================================================
// SQL Dialect Translation
// ============================================================================

/// Translate DuckDB SQL to target dialect
pub fn translate_sql(sql: &str, from: &SqlDialect, to: &SqlDialect) -> String {
    if from == to {
        return sql.to_string();
    }
    
    let mut result = sql.to_string();
    
    match to {
        SqlDialect::DuckDB => {
            // No translation needed for DuckDB targets
        }
        SqlDialect::PostgreSQL => {
            // DuckDB → PostgreSQL translations
            // LIMIT with OFFSET: compatible
            // String concat: || works in both
            // ILIKE: works in both
            // Arrays: PostgreSQL uses ARRAY[], DuckDB uses []
            result = result.replace("STRUCT_PACK(", "ROW(");
            result = result.replace("list_value(", "ARRAY[");
            // Note: delta_scan won't work on pure PostgreSQL
        }
        SqlDialect::MySQL => {
            // DuckDB → MySQL translations
            // LIMIT: compatible
            // String concat: MySQL uses CONCAT()
            result = result.replace("||", ", "); // Partial fix for concat
            if result.contains("||") {
                // Wrap in CONCAT - this is simplified
                // Full implementation would need proper parsing
            }
            // ILIKE → LIKE (case sensitivity handled differently)
            result = result.replace("ILIKE", "LIKE");
            // BOOLEAN: MySQL uses TINYINT(1)
            result = result.replace("::boolean", "");
            result = result.replace("::BOOLEAN", "");
        }
        SqlDialect::BigQuery => {
            // DuckDB → BigQuery translations
            // LIMIT: compatible
            // String quotes: BigQuery uses backticks for identifiers
            // Arrays: BigQuery uses []
            // STRUCT: BigQuery uses STRUCT()
            result = result.replace("ILIKE", "LIKE"); // BigQuery is case-insensitive by default
            // Date functions differ significantly
        }
        SqlDialect::Snowflake => {
            // DuckDB → Snowflake translations
            // Most standard SQL works
            // ILIKE: works in Snowflake
            // LIMIT: works
            result = result.replace("STRUCT_PACK(", "OBJECT_CONSTRUCT(");
        }
        SqlDialect::SQLite => {
            // DuckDB → SQLite translations
            // LIMIT: compatible
            // String concat: || works
            // No arrays in SQLite
            // ILIKE → LIKE with COLLATE NOCASE
            result = result.replace("ILIKE", "LIKE");
        }
    }
    
    result
}

// ============================================================================
// Resources Table Function - sazgar_resources()
// ============================================================================

#[repr(C)]
pub struct ResourcesBindData;

#[repr(C)]
pub struct ResourcesInitData {
    done: AtomicBool,
    available_gb: f64,
    total_gb: f64,
    used_gb: f64,
    free_gb: f64,
    cpu_usage: f32,
    cpu_count: u64,
    load_1m: f64,
    load_5m: f64,
    load_15m: f64,
}

pub struct ResourcesVTab;

impl VTab for ResourcesVTab {
    type InitData = ResourcesInitData;
    type BindData = ResourcesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("available_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("total_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("used_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("free_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("cpu_usage", LogicalTypeHandle::from(LogicalTypeId::Float));
        bind.add_result_column("cpu_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("load_1m", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("load_5m", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("load_15m", LogicalTypeHandle::from(LogicalTypeId::Double));
        Ok(ResourcesBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let mut sys = System::new_with_specifics(
            RefreshKind::new()
                .with_memory(MemoryRefreshKind::everything())
                .with_cpu(CpuRefreshKind::everything())
        );
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_all();
        
        let total_memory = sys.total_memory();
        let available_memory = sys.available_memory();
        let used_memory = sys.used_memory();
        let free_memory = sys.free_memory();
        
        let gb = 1_073_741_824.0; // 1 GiB in bytes
        
        let load = System::load_average();
        
        Ok(ResourcesInitData {
            done: AtomicBool::new(false),
            available_gb: available_memory as f64 / gb,
            total_gb: total_memory as f64 / gb,
            used_gb: used_memory as f64 / gb,
            free_gb: free_memory as f64 / gb,
            cpu_usage: sys.global_cpu_usage(),
            cpu_count: sys.cpus().len() as u64,
            load_1m: load.one,
            load_5m: load.five,
            load_15m: load.fifteen,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).as_mut_slice::<f64>()[0] = init_data.available_gb;
        output.flat_vector(1).as_mut_slice::<f64>()[0] = init_data.total_gb;
        output.flat_vector(2).as_mut_slice::<f64>()[0] = init_data.used_gb;
        output.flat_vector(3).as_mut_slice::<f64>()[0] = init_data.free_gb;
        output.flat_vector(4).as_mut_slice::<f32>()[0] = init_data.cpu_usage;
        output.flat_vector(5).as_mut_slice::<u64>()[0] = init_data.cpu_count;
        output.flat_vector(6).as_mut_slice::<f64>()[0] = init_data.load_1m;
        output.flat_vector(7).as_mut_slice::<f64>()[0] = init_data.load_5m;
        output.flat_vector(8).as_mut_slice::<f64>()[0] = init_data.load_15m;
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Estimate Table Function - sazgar_estimate(path1, path2, ...)
// Supports multiple paths (local files, folders, cloud paths)
// ============================================================================

/// Represents a single path estimation result
#[derive(Clone, Debug)]
struct PathEstimate {
    path: String,
    path_type: String,        // "file", "folder", "cloud", "unknown"
    estimated_gb: f64,
    compressed_gb: f64,
    row_count: u64,
    file_count: u64,
    folder_count: u64,
    format: String,
    is_accessible: bool,
}

#[repr(C)]
pub struct EstimateBindData {
    paths: Vec<String>,
}

#[repr(C)]
pub struct EstimateInitData {
    current_idx: AtomicUsize,
    estimates: Vec<PathEstimate>,
}

pub struct EstimateVTab;

impl EstimateVTab {
    /// Recursively calculate folder size
    fn calculate_folder_size_recursive(path: &std::path::Path) -> (u64, u64, u64) {
        let mut total_size = 0u64;
        let mut file_count = 0u64;
        let mut folder_count = 0u64;
        
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        total_size += meta.len();
                        file_count += 1;
                    } else if meta.is_dir() {
                        folder_count += 1;
                        let (sub_size, sub_files, sub_folders) = 
                            Self::calculate_folder_size_recursive(&entry.path());
                        total_size += sub_size;
                        file_count += sub_files;
                        folder_count += sub_folders;
                    }
                }
            }
        }
        
        (total_size, file_count, folder_count)
    }
    
    /// Estimate size for a local file
    fn estimate_local_file(path: &str) -> PathEstimate {
        let gb = 1_073_741_824.0;
        let path_obj = std::path::Path::new(path);
        
        if let Ok(metadata) = std::fs::metadata(path) {
            let size_bytes = metadata.len();
            let size_gb = size_bytes as f64 / gb;
            let format = Self::detect_format(path);
            
            // Estimate uncompressed size based on format
            let (estimated_gb, row_count) = match format.as_str() {
                "parquet" => (size_gb * 5.0, (size_gb * 5.0 * gb / 200.0) as u64),
                "csv" | "json" => (size_gb, (size_gb * gb / 100.0) as u64),
                "gz" | "gzip" => (size_gb * 3.0, 0),
                "zst" | "zstd" => (size_gb * 4.0, 0),
                "snappy" => (size_gb * 2.0, 0),
                _ => (size_gb, 0),
            };
            
            PathEstimate {
                path: path.to_string(),
                path_type: "file".to_string(),
                estimated_gb,
                compressed_gb: size_gb,
                row_count,
                file_count: 1,
                folder_count: 0,
                format,
                is_accessible: true,
            }
        } else {
            PathEstimate {
                path: path.to_string(),
                path_type: "unknown".to_string(),
                estimated_gb: 0.0,
                compressed_gb: 0.0,
                row_count: 0,
                file_count: 0,
                folder_count: 0,
                format: "unknown".to_string(),
                is_accessible: false,
            }
        }
    }
    
    /// Estimate size for a local folder (recursive)
    fn estimate_local_folder(path: &str) -> PathEstimate {
        let gb = 1_073_741_824.0;
        let path_obj = std::path::Path::new(path);
        
        if path_obj.exists() && path_obj.is_dir() {
            let (total_bytes, file_count, folder_count) = 
                Self::calculate_folder_size_recursive(path_obj);
            
            let compressed_gb = total_bytes as f64 / gb;
            
            // Check if it's a Delta table (has _delta_log)
            let delta_log_path = path_obj.join("_delta_log");
            let format = if delta_log_path.exists() {
                "delta".to_string()
            } else {
                // Check for common data formats
                Self::detect_folder_format(path_obj)
            };
            
            // Estimate based on format
            let (estimated_gb, row_count) = match format.as_str() {
                "delta" | "parquet" => (compressed_gb * 4.0, (compressed_gb * 4.0 * gb / 300.0) as u64),
                "csv" => (compressed_gb, (compressed_gb * gb / 100.0) as u64),
                "json" => (compressed_gb, (compressed_gb * gb / 500.0) as u64),
                _ => (compressed_gb, 0),
            };
            
            PathEstimate {
                path: path.to_string(),
                path_type: "folder".to_string(),
                estimated_gb,
                compressed_gb,
                row_count,
                file_count,
                folder_count,
                format,
                is_accessible: true,
            }
        } else {
            PathEstimate {
                path: path.to_string(),
                path_type: "unknown".to_string(),
                estimated_gb: 0.0,
                compressed_gb: 0.0,
                row_count: 0,
                file_count: 0,
                folder_count: 0,
                format: "unknown".to_string(),
                is_accessible: false,
            }
        }
    }
    
    /// Detect format by scanning files in a folder
    fn detect_folder_format(path: &std::path::Path) -> String {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Some(ext) = entry.path().extension() {
                    match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                        "parquet" => return "parquet".to_string(),
                        "csv" => return "csv".to_string(),
                        "json" => return "json".to_string(),
                        "avro" => return "avro".to_string(),
                        _ => continue,
                    }
                }
            }
        }
        "mixed".to_string()
    }
    
    /// Estimate size for a cloud path (S3, Azure, GCS)
    fn estimate_cloud_path(path: &str) -> PathEstimate {
        let format = Self::detect_format(path);
        
        // For cloud paths, we can't get actual size without credentials
        // Return a placeholder indicating it's a cloud path
        PathEstimate {
            path: path.to_string(),
            path_type: "cloud".to_string(),
            estimated_gb: 0.0,  // Unknown - requires Tavana to estimate
            compressed_gb: 0.0,
            row_count: 0,
            file_count: 0,
            folder_count: 0,
            format,
            is_accessible: true,  // Assume accessible, will fail at query time if not
        }
    }
    
    /// Estimate a single path
    fn estimate_path(path: &str) -> PathEstimate {
        let path = path.trim();
        
        // Check if it's a cloud path
        if path.starts_with("s3://") || path.starts_with("az://") || 
           path.starts_with("gs://") || path.starts_with("abfss://") ||
           path.starts_with("https://") {
            return Self::estimate_cloud_path(path);
        }
        
        // Local path
        let path_obj = std::path::Path::new(path);
        if path_obj.exists() {
            if path_obj.is_file() {
                Self::estimate_local_file(path)
            } else if path_obj.is_dir() {
                Self::estimate_local_folder(path)
            } else {
                PathEstimate {
                    path: path.to_string(),
                    path_type: "unknown".to_string(),
                    estimated_gb: 0.0,
                    compressed_gb: 0.0,
                    row_count: 0,
                    file_count: 0,
                    folder_count: 0,
                    format: "unknown".to_string(),
                    is_accessible: false,
                }
            }
        } else {
            // Path doesn't exist locally - might be a cloud path without proper prefix
            PathEstimate {
                path: path.to_string(),
                path_type: "not_found".to_string(),
                estimated_gb: 0.0,
                compressed_gb: 0.0,
                row_count: 0,
                file_count: 0,
                folder_count: 0,
                format: "unknown".to_string(),
                is_accessible: false,
            }
        }
    }
    
    /// Detect format from path/filename
    fn detect_format(path: &str) -> String {
        let path_lower = path.to_lowercase();
        
        // Check file extension first
        if let Some(ext) = std::path::Path::new(path).extension() {
            match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                "parquet" => return "parquet".to_string(),
                "csv" => return "csv".to_string(),
                "json" | "jsonl" | "ndjson" => return "json".to_string(),
                "avro" => return "avro".to_string(),
                "orc" => return "orc".to_string(),
                "gz" | "gzip" => return "gz".to_string(),
                "zst" | "zstd" => return "zstd".to_string(),
                "snappy" => return "snappy".to_string(),
                _ => {}
            }
        }
        
        // Check path patterns
        if path_lower.contains("_delta_log") || path_lower.ends_with('/') {
            "delta".to_string()
        } else if path.starts_with("s3://") || path.starts_with("az://") || path.starts_with("gs://") {
            "delta".to_string()  // Assume Delta for cloud paths (common in data lakes)
        } else {
            "unknown".to_string()
        }
    }
}

impl VTab for EstimateVTab {
    type InitData = EstimateInitData;
    type BindData = EstimateBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        // Get single parameter - can be a single path or comma-separated paths
        let input = if bind.get_parameter_count() > 0 {
            bind.get_parameter(0).to_string().trim_matches('"').to_string()
        } else {
            ".".to_string()
        };
        
        // Parse paths: split by comma, but be careful with URLs containing commas
        let paths: Vec<String> = if input.contains(',') && !input.starts_with("http") {
            input
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            vec![input]
        };
        
        bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("path_type", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("estimated_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("compressed_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("row_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("file_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("folder_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("format", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("is_accessible", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        
        Ok(EstimateBindData { paths })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<EstimateBindData>();
        let paths = unsafe { (*bind_data).paths.clone() };
        
        // Estimate each path
        let estimates: Vec<PathEstimate> = paths
            .iter()
            .map(|p| Self::estimate_path(p))
            .collect();
        
        Ok(EstimateInitData {
            current_idx: AtomicUsize::new(0),
            estimates,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.estimates.len() {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.estimates.len() - current);
        
        for i in 0..batch_size {
            let est = &init_data.estimates[current + i];
            output.flat_vector(0).insert(i, CString::new(est.path.clone())?);
            output.flat_vector(1).insert(i, CString::new(est.path_type.clone())?);
            output.flat_vector(2).as_mut_slice::<f64>()[i] = est.estimated_gb;
            output.flat_vector(3).as_mut_slice::<f64>()[i] = est.compressed_gb;
            output.flat_vector(4).as_mut_slice::<u64>()[i] = est.row_count;
            output.flat_vector(5).as_mut_slice::<u64>()[i] = est.file_count;
            output.flat_vector(6).as_mut_slice::<u64>()[i] = est.folder_count;
            output.flat_vector(7).insert(i, CString::new(est.format.clone())?);
            output.flat_vector(8).as_mut_slice::<bool>()[i] = est.is_accessible;
        }
        
        init_data.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        // Accept variable number of VARCHAR parameters
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ============================================================================
// Backends Table Function - sazgar_backends()
// ============================================================================

#[repr(C)]
pub struct BackendsBindData;

#[repr(C)]
pub struct BackendsInitData {
    current_idx: AtomicUsize,
    backends: Vec<(String, String, String)>, // (name, url, dialect)
}

pub struct BackendsVTab;

impl VTab for BackendsVTab {
    type InitData = BackendsInitData;
    type BindData = BackendsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("url", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(BackendsBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let backends = if let Ok(map) = get_backends().lock() {
            map.iter()
                .map(|(_, config)| {
                    (
                        config.name.clone(),
                        config.url.clone(),
                        format!("{:?}", config.dialect),
                    )
                })
                .collect()
        } else {
            vec![]
        };
        
        Ok(BackendsInitData {
            current_idx: AtomicUsize::new(0),
            backends,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let current = init_data.current_idx.load(Ordering::Relaxed);
        
        if current >= init_data.backends.len() {
            output.set_len(0);
            return Ok(());
        }
        
        let batch_size = std::cmp::min(2048, init_data.backends.len() - current);
        
        for i in 0..batch_size {
            let (name, url, dialect) = &init_data.backends[current + i];
            output.flat_vector(0).insert(i, CString::new(name.clone())?);
            output.flat_vector(1).insert(i, CString::new(url.clone())?);
            output.flat_vector(2).insert(i, CString::new(dialect.clone())?);
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
// Run Table Function - sazgar_run(query, target)
// ============================================================================

#[repr(C)]
pub struct RunBindData {
    query: String,
    target: String,
}

#[repr(C)]
pub struct RunInitData {
    done: AtomicBool,
    executed: bool,
    result_columns: Vec<String>,
    result_types: Vec<String>,
    result_rows: Vec<Vec<String>>,
    target_used: String,
    execution_ms: u64,
    error: Option<String>,
}

pub struct RunVTab;

#[allow(dead_code)]
impl RunVTab {
    /// Execute query locally using DuckDB
    fn execute_local(query: &str) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>), String> {
        // For local execution, we need access to the DuckDB connection
        // This is tricky in the current architecture - the VTab doesn't have direct access
        // to execute arbitrary SQL.
        
        // WORKAROUND: Return a message indicating the query should be executed directly
        // In a full implementation, we'd use DuckDB's internal APIs
        
        // For now, return the query wrapped in a message
        Ok((
            vec!["message".to_string()],
            vec!["VARCHAR".to_string()],
            vec![vec![format!("Execute locally: {}", query)]],
        ))
    }
    
    /// Execute query on a remote PostgreSQL backend (including Tavana)
    fn execute_postgres(url: &str, query: &str, dialect: &SqlDialect) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>), String> {
        // Translate SQL if needed
        let translated_query = translate_sql(query, &SqlDialect::DuckDB, dialect);
        
        // For PostgreSQL backends, we'd use postgres_query()
        // This requires the postgres extension to be loaded
        
        // Return instruction for now
        Ok((
            vec!["backend".to_string(), "query".to_string(), "instruction".to_string()],
            vec!["VARCHAR".to_string(), "VARCHAR".to_string(), "VARCHAR".to_string()],
            vec![vec![
                url.to_string(),
                translated_query.clone(),
                format!("SELECT * FROM postgres_query('{}', '{}')", 
                    url.replace("'", "''"),
                    translated_query.replace("'", "''")
                ),
            ]],
        ))
    }
    
    /// Execute query on MySQL backend
    fn execute_mysql(url: &str, query: &str) -> Result<(Vec<String>, Vec<String>, Vec<Vec<String>>), String> {
        let translated_query = translate_sql(query, &SqlDialect::DuckDB, &SqlDialect::MySQL);
        
        Ok((
            vec!["backend".to_string(), "query".to_string(), "instruction".to_string()],
            vec!["VARCHAR".to_string(), "VARCHAR".to_string(), "VARCHAR".to_string()],
            vec![vec![
                url.to_string(),
                translated_query.clone(),
                format!("SELECT * FROM mysql_query('{}', '{}')", 
                    url.replace("'", "''"),
                    translated_query.replace("'", "''")
                ),
            ]],
        ))
    }
}

impl VTab for RunVTab {
    type InitData = RunInitData;
    type BindData = RunBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let query = if bind.get_parameter_count() > 0 {
            bind.get_parameter(0).to_string().trim_matches('"').to_string()
        } else {
            "".to_string()
        };
        
        let target = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_string().trim_matches('"').to_string()
        } else {
            "local".to_string()
        };
        
        // Comprehensive columns for execution info
        bind.add_result_column("target", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("required_extension", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("original_query", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("translated_query", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("execute_sql", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("full_script", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        Ok(RunBindData { query, target })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<RunBindData>();
        let query = unsafe { (*bind_data).query.clone() };
        let target = unsafe { (*bind_data).target.clone() };
        
        let start = std::time::Instant::now();
        
        // Resolve the target to a backend config
        let backend = resolve_backend(&target);
        
        // Get required extension (special handling for remote DuckDB)
        let required_ext = if matches!(backend.dialect, SqlDialect::DuckDB) && backend.url != "local" {
            "postgres"  // Remote DuckDB (Tavana) needs postgres extension
        } else {
            backend.dialect.required_extension().unwrap_or("none")
        };
        
        // Generate the execution SQL based on backend type
        let (translated_query, execute_sql, dialect_name, full_script) = match backend.dialect {
            SqlDialect::DuckDB => {
                if backend.url == "local" {
                    // Local execution - just return the query
                    (
                        query.clone(),
                        query.clone(),
                        "DuckDB (local)".to_string(),
                        format!("-- Execute directly:\n{}", query)
                    )
                } else {
                    // Remote DuckDB (like Tavana) via postgres extension
                    let escaped_url = backend.url.replace("'", "''");
                    let escaped_query = query.replace("'", "''");
                    let exec_sql = format!("SELECT * FROM postgres_query('remote_db', '{}')", escaped_query);
                    (
                        query.clone(),
                        exec_sql.clone(),
                        "DuckDB (via PostgreSQL wire)".to_string(),
                        format!(
                            "-- Load required extension and configure for Tavana/DuckDB compatibility:\nLOAD postgres;\nSET pg_use_binary_copy = false;\nSET pg_use_text_protocol = true;\n\n-- Attach remote database:\nATTACH '{}' AS remote_db (TYPE postgres);\n\n-- Execute query (data returns to local DuckDB!):\nSELECT * FROM postgres_query('remote_db', '{}');",
                            backend.url,
                            escaped_query
                        )
                    )
                }
            }
            SqlDialect::PostgreSQL => {
                let translated = translate_sql(&query, &SqlDialect::DuckDB, &SqlDialect::PostgreSQL);
                let escaped_url = backend.url.replace("'", "''");
                let escaped_query = translated.replace("'", "''");
                let exec_sql = format!("SELECT * FROM postgres_query('{}', '{}')", escaped_url, escaped_query);
                (
                    translated.clone(),
                    exec_sql.clone(),
                    "PostgreSQL".to_string(),
                    format!(
                        "-- Load required extension:\nLOAD postgres;\n\n-- Execute (returns data to local DuckDB):\n{}",
                        exec_sql
                    )
                )
            }
            SqlDialect::MySQL => {
                let translated = translate_sql(&query, &SqlDialect::DuckDB, &SqlDialect::MySQL);
                let escaped_url = backend.url.replace("'", "''");
                let escaped_query = translated.replace("'", "''");
                let exec_sql = format!("SELECT * FROM mysql_query('{}', '{}')", escaped_url, escaped_query);
                (
                    translated.clone(),
                    exec_sql.clone(),
                    "MySQL".to_string(),
                    format!(
                        "-- Load required extension:\nLOAD mysql;\n\n-- Execute (returns data to local DuckDB):\n{}",
                        exec_sql
                    )
                )
            }
            SqlDialect::SQLite => {
                let translated = translate_sql(&query, &SqlDialect::DuckDB, &SqlDialect::SQLite);
                // SQLite uses ATTACH and direct query
                let exec_sql = format!(
                    "ATTACH '{}' AS sqlite_db (TYPE sqlite);\n{}",
                    backend.url,
                    translated.replace("FROM ", "FROM sqlite_db.")
                );
                (
                    translated.clone(),
                    exec_sql.clone(),
                    "SQLite".to_string(),
                    format!(
                        "-- Load required extension:\nLOAD sqlite;\n\n-- Execute (returns data to local DuckDB):\n{}",
                        exec_sql
                    )
                )
            }
            SqlDialect::BigQuery => {
                let translated = translate_sql(&query, &SqlDialect::DuckDB, &SqlDialect::BigQuery);
                let escaped_url = backend.url.replace("'", "''");
                let escaped_query = translated.replace("'", "''");
                // BigQuery uses the community extension
                let exec_sql = format!(
                    "SELECT * FROM bigquery_execute('{}', '{}')", 
                    escaped_url.replace("bigquery://", ""), 
                    escaped_query
                );
                (
                    translated.clone(),
                    exec_sql.clone(),
                    "BigQuery".to_string(),
                    format!(
                        "-- Install & load community extension:\nINSTALL bigquery FROM community;\nLOAD bigquery;\n\n-- Configure authentication:\n-- SET bigquery_project_id = 'your-project';\n-- SET bigquery_credentials_file = '/path/to/credentials.json';\n\n-- Execute (returns data to local DuckDB):\n{}",
                        exec_sql
                    )
                )
            }
            SqlDialect::Snowflake => {
                let translated = translate_sql(&query, &SqlDialect::DuckDB, &SqlDialect::Snowflake);
                let escaped_query = translated.replace("'", "''");
                // Snowflake uses the community extension
                let exec_sql = format!(
                    "SELECT * FROM snowflake_query('{}')", 
                    escaped_query
                );
                (
                    translated.clone(),
                    exec_sql.clone(),
                    "Snowflake".to_string(),
                    format!(
                        "-- Install & load community extension:\nINSTALL snowflake FROM community;\nLOAD snowflake;\n\n-- Configure authentication:\n-- SET snowflake_account = 'your_account';\n-- SET snowflake_user = 'your_user';\n-- SET snowflake_password = 'your_password';\n\n-- Execute (returns data to local DuckDB):\n{}",
                        exec_sql
                    )
                )
            }
        };
        
        let execution_ms = start.elapsed().as_millis() as u64;
        
        // Store results with all columns
        let result_rows = vec![vec![
            backend.url.clone(),      // target
            dialect_name,             // dialect
            required_ext.to_string(), // required_extension
            query,                    // original_query
            translated_query,         // translated_query
            execute_sql,              // execute_sql
            full_script,              // full_script
        ]];
        
        Ok(RunInitData {
            done: AtomicBool::new(false),
            executed: true,
            result_columns: vec![
                "target".to_string(), 
                "dialect".to_string(), 
                "required_extension".to_string(),
                "original_query".to_string(),
                "translated_query".to_string(),
                "execute_sql".to_string(), 
                "full_script".to_string(),
            ],
            result_types: vec!["VARCHAR".to_string(); 7],
            result_rows,
            target_used: backend.name,
            execution_ms,
            error: None,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        if init_data.result_rows.is_empty() {
            output.set_len(0);
            return Ok(());
        }
        
        // Output the first (and only) row
        let row = &init_data.result_rows[0];
        for (i, value) in row.iter().enumerate() {
            output.flat_vector(i).insert(0, CString::new(value.clone())?);
        }
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // query
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // target
        ])
    }
}

// ============================================================================
// Backend Registration Scalar Function
// ============================================================================

/// Register backend via SQL: CALL sazgar_backend('name', 'url')
/// This is implemented as a table function that returns void
#[repr(C)]
pub struct RegisterBackendBindData {
    name: String,
    url: String,
}

#[repr(C)]
pub struct RegisterBackendInitData {
    done: AtomicBool,
    registered: bool,
    message: String,
}

pub struct RegisterBackendVTab;

impl VTab for RegisterBackendVTab {
    type InitData = RegisterBackendInitData;
    type BindData = RegisterBackendBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let name = if bind.get_parameter_count() > 0 {
            bind.get_parameter(0).to_string().trim_matches('"').to_string()
        } else {
            return Err("Backend name required".into());
        };
        
        let url = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_string().trim_matches('"').to_string()
        } else {
            return Err("Backend URL required".into());
        };
        
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("url", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        Ok(RegisterBackendBindData { name, url })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<RegisterBackendBindData>();
        let name = unsafe { (*bind_data).name.clone() };
        let url = unsafe { (*bind_data).url.clone() };
        
        // Register the backend
        register_backend(&name, &url);
        
        let backend = resolve_backend(&name);
        
        Ok(RegisterBackendInitData {
            done: AtomicBool::new(false),
            registered: true,
            message: format!("Registered backend '{}' -> {} ({:?})", name, url, backend.dialect),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        let name = bind_data.name.clone();
        let url = bind_data.url.clone();
        let backend = resolve_backend(&name);
        
        output.flat_vector(0).insert(0, CString::new("registered")?);
        output.flat_vector(1).insert(0, CString::new(name)?);
        output.flat_vector(2).insert(0, CString::new(url)?);
        output.flat_vector(3).insert(0, CString::new(format!("{:?}", backend.dialect))?);
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // name
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // url
        ])
    }
}

