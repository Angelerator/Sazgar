//! Sazgar Routing Module v0.5.0
//! 
//! Ultra-simplified SQL routing with SQLGlot dialect translation.
//! 
//! ## Requirements
//! - Python 3 with SQLGlot: `pip install sqlglot`
//! 
//! ## Design
//! - Users write SQL in DuckDB dialect ONLY
//! - Sazgar auto-translates to destination dialect via SQLGlot
//! - All sazgar functions can be used in routing conditions
//!
//! ## Example
//! ```sql
//! -- One-line routing with auto translation!
//! SELECT execute_sql FROM sazgar_route(
//!   'SELECT * FROM sales WHERE year = 2024',
//!   'mysql://user:pass@host/db'
//! );
//! ```

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    ffi::CString,
    process::Command,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use sysinfo::{System, MemoryRefreshKind, CpuRefreshKind, RefreshKind};

// ============================================================================
// SQLGlot Integration via Python subprocess
// ============================================================================

/// Translate SQL from DuckDB dialect to target dialect using SQLGlot
pub fn sqlglot_transpile(sql: &str, to_dialect: &str) -> Result<String, String> {
    // No translation needed for DuckDB targets
    let to = to_dialect.to_lowercase();
    if to == "duckdb" || to == "duck" {
        return Ok(sql.to_string());
    }
    
    // Map dialect names to SQLGlot dialect names
    let sqlglot_dialect = match to.as_str() {
        "postgresql" | "postgres" | "pg" => "postgres",
        "mysql" | "mariadb" => "mysql",
        "sqlite" | "sqlite3" => "sqlite",
        "bigquery" | "bq" => "bigquery",
        "snowflake" | "sf" => "snowflake",
        "clickhouse" | "ch" => "clickhouse",
        "oracle" | "plsql" => "oracle",
        "tsql" | "sqlserver" | "mssql" => "tsql",
        "spark" | "databricks" => "spark",
        "hive" => "hive",
        "presto" => "presto",
        "trino" => "trino",
        "athena" => "athena",
        "redshift" => "redshift",
        "teradata" => "teradata",
        "doris" => "doris",
        "starrocks" => "starrocks",
        "drill" => "drill",
        "materialize" => "materialize",
        other => other,
    };
    
    // Escape single quotes for Python
    let escaped_sql = sql.replace('\\', "\\\\").replace('\'', "\\'");
    
    // Python command to run SQLGlot
    let python_code = format!(
        "import sqlglot; print(sqlglot.transpile('{}', read='duckdb', write='{}')[0])",
        escaped_sql, sqlglot_dialect
    );
    
    // Try python3 first, then python
    let result = Command::new("python3")
        .args(["-c", &python_code])
        .output()
        .or_else(|_| Command::new("python").args(["-c", &python_code]).output());
    
    match result {
        Ok(output) => {
            if output.status.success() {
                let translated = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if translated.is_empty() {
                    Ok(sql.to_string()) // Return original if empty result
                } else {
                    Ok(translated)
                }
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if stderr.contains("No module named 'sqlglot'") {
                    Err("SQLGlot not installed. Run: pip install sqlglot".to_string())
                } else {
                    Err(format!("SQLGlot error: {}", stderr.trim()))
                }
            }
        }
        Err(e) => {
            Err(format!("Python not found. Install Python and SQLGlot: pip install sqlglot. Error: {}", e))
        }
    }
}

/// Check if SQLGlot is available
pub fn check_sqlglot() -> Result<String, String> {
    let python_code = "import sqlglot; print(f'SQLGlot {sqlglot.__version__}')";
    
    let result = Command::new("python3")
        .args(["-c", python_code])
        .output()
        .or_else(|_| Command::new("python").args(["-c", python_code]).output());
    
    match result {
        Ok(output) => {
            if output.status.success() {
                Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
            } else {
                Err("SQLGlot not installed. Run: pip install sqlglot".to_string())
            }
        }
        Err(_) => Err("Python not found. Install Python and SQLGlot: pip install sqlglot".to_string()),
    }
}

// ============================================================================
// Connection String Parsing
// ============================================================================

#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub provider_type: String,
    pub dialect: String,
    pub extension: String,
    pub connection: String,
    pub is_local: bool,
}

impl ConnectionInfo {
    pub fn parse(conn_str: &str) -> Self {
        let conn_str = conn_str.trim();
        
        // Local DuckDB
        if conn_str.eq_ignore_ascii_case("local") || conn_str.is_empty() {
            return Self {
                provider_type: "duckdb".to_string(),
                dialect: "duckdb".to_string(),
                extension: "".to_string(),
                connection: "".to_string(),
                is_local: true,
            };
        }
        
        // PostgreSQL URL
        if conn_str.starts_with("postgres://") || conn_str.starts_with("postgresql://") {
            return Self {
                provider_type: "postgres".to_string(),
                dialect: "postgres".to_string(),
                extension: "postgres".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // PostgreSQL connection string (host=... port=...)
        if conn_str.contains("host=") && (conn_str.contains("port=") || conn_str.contains("dbname=")) {
            // Tavana uses DuckDB dialect over PG wire
            let is_tavana = conn_str.to_lowercase().contains("tavana");
            return Self {
                provider_type: "postgres".to_string(),
                dialect: if is_tavana { "duckdb".to_string() } else { "postgres".to_string() },
                extension: "postgres".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // MySQL
        if conn_str.starts_with("mysql://") || conn_str.starts_with("mariadb://") {
            return Self {
                provider_type: "mysql".to_string(),
                dialect: "mysql".to_string(),
                extension: "mysql".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // SQLite
        if conn_str.starts_with("sqlite://") || conn_str.ends_with(".db") || conn_str.ends_with(".sqlite") {
            return Self {
                provider_type: "sqlite".to_string(),
                dialect: "sqlite".to_string(),
                extension: "sqlite".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // JDBC connections - parse dialect from URL
        if conn_str.starts_with("jdbc:") {
            let dialect = if conn_str.contains("postgresql") || conn_str.contains("postgres") {
                "postgres"
            } else if conn_str.contains("mysql") || conn_str.contains("mariadb") {
                "mysql"
            } else if conn_str.contains("oracle") {
                "oracle"
            } else if conn_str.contains("sqlserver") || conn_str.contains("mssql") {
                "tsql"
            } else if conn_str.contains("snowflake") {
                "snowflake"
            } else if conn_str.contains("bigquery") {
                "bigquery"
            } else if conn_str.contains("redshift") {
                "redshift"
            } else if conn_str.contains("clickhouse") {
                "clickhouse"
            } else if conn_str.contains("hive") {
                "hive"
            } else if conn_str.contains("spark") || conn_str.contains("databricks") {
                "spark"
            } else if conn_str.contains("presto") {
                "presto"
            } else if conn_str.contains("trino") {
                "trino"
            } else if conn_str.contains("athena") {
                "athena"
            } else if conn_str.contains("teradata") {
                "teradata"
            } else {
                "postgres" // Default JDBC dialect
            };
            
            return Self {
                provider_type: "jdbc".to_string(),
                dialect: dialect.to_string(),
                extension: "jdbc".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // BigQuery
        if conn_str.starts_with("bigquery://") || conn_str.contains("bigquery") {
            return Self {
                provider_type: "bigquery".to_string(),
                dialect: "bigquery".to_string(),
                extension: "bigquery".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // Snowflake
        if conn_str.starts_with("snowflake://") || conn_str.contains(".snowflakecomputing.com") {
            return Self {
                provider_type: "snowflake".to_string(),
                dialect: "snowflake".to_string(),
                extension: "snowflake".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // ClickHouse
        if conn_str.starts_with("clickhouse://") {
            return Self {
                provider_type: "clickhouse".to_string(),
                dialect: "clickhouse".to_string(),
                extension: "chsql".to_string(),
                connection: conn_str.to_string(),
                is_local: false,
            };
        }
        
        // Default: treat as DuckDB file
        Self {
            provider_type: "duckdb".to_string(),
            dialect: "duckdb".to_string(),
            extension: "".to_string(),
            connection: conn_str.to_string(),
            is_local: conn_str.is_empty(),
        }
    }
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
        
        let gb = 1_073_741_824.0;
        let load = System::load_average();
        
        Ok(ResourcesInitData {
            done: AtomicBool::new(false),
            available_gb: sys.available_memory() as f64 / gb,
            total_gb: sys.total_memory() as f64 / gb,
            used_gb: sys.used_memory() as f64 / gb,
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
        output.flat_vector(3).as_mut_slice::<f32>()[0] = init_data.cpu_usage;
        output.flat_vector(4).as_mut_slice::<u64>()[0] = init_data.cpu_count;
        output.flat_vector(5).as_mut_slice::<f64>()[0] = init_data.load_1m;
        output.flat_vector(6).as_mut_slice::<f64>()[0] = init_data.load_5m;
        output.flat_vector(7).as_mut_slice::<f64>()[0] = init_data.load_15m;
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// Estimate Table Function - sazgar_estimate(paths)
// ============================================================================

#[derive(Clone)]
struct PathEstimate {
    path: String,
    path_type: String,
    estimated_gb: f64,
    compressed_gb: f64,
    file_count: u64,
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
    fn calculate_folder_size(path: &std::path::Path) -> (u64, u64) {
        let mut total_size = 0u64;
        let mut file_count = 0u64;
        
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        total_size += meta.len();
                        file_count += 1;
                    } else if meta.is_dir() {
                        let (s, f) = Self::calculate_folder_size(&entry.path());
                        total_size += s;
                        file_count += f;
                    }
                }
            }
        }
        (total_size, file_count)
    }
    
    fn detect_format(path: &str) -> String {
        if let Some(ext) = std::path::Path::new(path).extension() {
            match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                "parquet" => return "parquet".to_string(),
                "csv" => return "csv".to_string(),
                "json" | "jsonl" => return "json".to_string(),
                "gz" | "gzip" => return "gzip".to_string(),
                _ => {}
            }
        }
        
        let p = std::path::Path::new(path);
        if p.is_dir() && p.join("_delta_log").exists() {
            return "delta".to_string();
        }
        
        if path.starts_with("s3://") || path.starts_with("az://") || path.starts_with("gs://") {
            return "cloud".to_string();
        }
        
        "unknown".to_string()
    }
    
    fn estimate_path(path: &str) -> PathEstimate {
        let path = path.trim();
        let gb = 1_073_741_824.0;
        
        if path.starts_with("s3://") || path.starts_with("az://") || 
           path.starts_with("gs://") || path.starts_with("abfss://") {
            return PathEstimate {
                path: path.to_string(),
                path_type: "cloud".to_string(),
                estimated_gb: 0.0,
                compressed_gb: 0.0,
                file_count: 0,
                format: Self::detect_format(path),
                is_accessible: true,
            };
        }
        
        let p = std::path::Path::new(path);
        
        if p.exists() {
            if p.is_file() {
                let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                let format = Self::detect_format(path);
                let ratio = match format.as_str() {
                    "parquet" | "delta" => 5.0,
                    "gzip" => 3.0,
                    _ => 1.0,
                };
                return PathEstimate {
                    path: path.to_string(),
                    path_type: "file".to_string(),
                    estimated_gb: (size as f64 / gb) * ratio,
                    compressed_gb: size as f64 / gb,
                    file_count: 1,
                    format,
                    is_accessible: true,
                };
            } else if p.is_dir() {
                let (total, count) = Self::calculate_folder_size(p);
                let format = Self::detect_format(path);
                let ratio = match format.as_str() {
                    "parquet" | "delta" => 4.0,
                    _ => 1.0,
                };
                return PathEstimate {
                    path: path.to_string(),
                    path_type: "folder".to_string(),
                    estimated_gb: (total as f64 / gb) * ratio,
                    compressed_gb: total as f64 / gb,
                    file_count: count,
                    format,
                    is_accessible: true,
                };
            }
        }
        
        PathEstimate {
            path: path.to_string(),
            path_type: "not_found".to_string(),
            estimated_gb: 0.0,
            compressed_gb: 0.0,
            file_count: 0,
            format: "unknown".to_string(),
            is_accessible: false,
        }
    }
}

impl VTab for EstimateVTab {
    type InitData = EstimateInitData;
    type BindData = EstimateBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let input = if bind.get_parameter_count() > 0 {
            bind.get_parameter(0).to_string().trim_matches('"').to_string()
        } else {
            ".".to_string()
        };
        
        let paths: Vec<String> = if input.contains(',') && !input.starts_with("http") {
            input.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
        } else {
            vec![input]
        };
        
        bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("path_type", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("estimated_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("compressed_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("file_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("format", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("is_accessible", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        
        Ok(EstimateBindData { paths })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<EstimateBindData>();
        let paths = unsafe { (*bind_data).paths.clone() };
        let estimates: Vec<PathEstimate> = paths.iter().map(|p| Self::estimate_path(p)).collect();
        
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
        
        let batch = std::cmp::min(2048, init_data.estimates.len() - current);
        
        for i in 0..batch {
            let est = &init_data.estimates[current + i];
            output.flat_vector(0).insert(i, CString::new(est.path.clone())?);
            output.flat_vector(1).insert(i, CString::new(est.path_type.clone())?);
            output.flat_vector(2).as_mut_slice::<f64>()[i] = est.estimated_gb;
            output.flat_vector(3).as_mut_slice::<f64>()[i] = est.compressed_gb;
            output.flat_vector(4).as_mut_slice::<u64>()[i] = est.file_count;
            output.flat_vector(5).insert(i, CString::new(est.format.clone())?);
            output.flat_vector(6).as_mut_slice::<bool>()[i] = est.is_accessible;
        }
        
        init_data.current_idx.store(current + batch, Ordering::Relaxed);
        output.set_len(batch);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ============================================================================
// Route Table Function - sazgar_route(query, target)
// 
// The MAIN routing function!
// - User writes DuckDB SQL
// - Sazgar translates to target dialect via SQLGlot
// - Returns ready-to-execute SQL
// ============================================================================

#[repr(C)]
pub struct RouteBindData {
    query: String,
    target: String,
}

#[repr(C)]
pub struct RouteInitData {
    done: AtomicBool,
    target: String,
    dialect: String,
    extension: String,
    original_query: String,
    translated_query: String,
    execute_sql: String,
    setup_sql: String,
    error: String,
}

pub struct RouteVTab;

impl VTab for RouteVTab {
    type InitData = RouteInitData;
    type BindData = RouteBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let query = if bind.get_parameter_count() > 0 {
            bind.get_parameter(0).to_string().trim_matches('"').to_string()
        } else {
            return Err("Query required: sazgar_route(query, target)".into());
        };
        
        let target = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_string().trim_matches('"').to_string()
        } else {
            "local".to_string()
        };
        
        bind.add_result_column("target", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("extension", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("original_query", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("translated_query", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("setup_sql", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("execute_sql", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        Ok(RouteBindData { query, target })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<RouteBindData>();
        let query = unsafe { (*bind_data).query.clone() };
        let target = unsafe { (*bind_data).target.clone() };
        
        let conn_info = ConnectionInfo::parse(&target);
        
        // Translate using SQLGlot
        let (translated, error) = match sqlglot_transpile(&query, &conn_info.dialect) {
            Ok(t) => (t, String::new()),
            Err(e) => (query.clone(), e), // Use original on error
        };
        
        // Generate setup and execute SQL
        let (setup_sql, execute_sql) = if conn_info.is_local {
            ("".to_string(), translated.clone())
        } else {
            let escaped = translated.replace("'", "''");
            
            let setup = if !conn_info.extension.is_empty() {
                format!("LOAD '{}';", conn_info.extension)
            } else {
                "".to_string()
            };
            
            let exec = match conn_info.provider_type.as_str() {
                "postgres" => format!(
                    "SELECT * FROM postgres_query('{}', '{}')",
                    conn_info.connection, escaped
                ),
                "mysql" => format!(
                    "SELECT * FROM mysql_query('{}', '{}')",
                    conn_info.connection, escaped
                ),
                "sqlite" => format!(
                    "ATTACH '{}' AS _remote; {}", 
                    conn_info.connection, translated
                ),
                "jdbc" => format!(
                    "-- JDBC: {}\nSELECT * FROM jdbc_query('{}', '{}')",
                    conn_info.dialect, conn_info.connection, escaped
                ),
                "bigquery" => format!(
                    "SELECT * FROM bigquery_query('{}', '{}')",
                    conn_info.connection, escaped
                ),
                "snowflake" => format!(
                    "SELECT * FROM snowflake_query('{}', '{}')",
                    conn_info.connection, escaped
                ),
                _ => translated.clone(),
            };
            
            (setup, exec)
        };
        
        Ok(RouteInitData {
            done: AtomicBool::new(false),
            target,
            dialect: conn_info.dialect,
            extension: conn_info.extension,
            original_query: query,
            translated_query: translated,
            setup_sql,
            execute_sql,
            error,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).insert(0, CString::new(init_data.target.clone())?);
        output.flat_vector(1).insert(0, CString::new(init_data.dialect.clone())?);
        output.flat_vector(2).insert(0, CString::new(init_data.extension.clone())?);
        output.flat_vector(3).insert(0, CString::new(init_data.original_query.clone())?);
        output.flat_vector(4).insert(0, CString::new(init_data.translated_query.clone())?);
        output.flat_vector(5).insert(0, CString::new(init_data.setup_sql.clone())?);
        output.flat_vector(6).insert(0, CString::new(init_data.execute_sql.clone())?);
        output.flat_vector(7).insert(0, CString::new(init_data.error.clone())?);
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// ============================================================================
// Translate Table Function - sazgar_translate(query, to_dialect)
// ============================================================================

#[repr(C)]
pub struct TranslateBindData {
    query: String,
    to_dialect: String,
}

#[repr(C)]
pub struct TranslateInitData {
    done: AtomicBool,
    from_dialect: String,
    to_dialect: String,
    original: String,
    translated: String,
    error: String,
}

pub struct TranslateVTab;

impl VTab for TranslateVTab {
    type InitData = TranslateInitData;
    type BindData = TranslateBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let query = if bind.get_parameter_count() > 0 {
            bind.get_parameter(0).to_string().trim_matches('"').to_string()
        } else {
            return Err("Query required".into());
        };
        
        let to_dialect = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_string().trim_matches('"').to_string()
        } else {
            "mysql".to_string()
        };
        
        bind.add_result_column("from_dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("to_dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("original", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("translated", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        Ok(TranslateBindData { query, to_dialect })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<TranslateBindData>();
        let query = unsafe { (*bind_data).query.clone() };
        let to_dialect = unsafe { (*bind_data).to_dialect.clone() };
        
        let (translated, error) = match sqlglot_transpile(&query, &to_dialect) {
            Ok(t) => (t, String::new()),
            Err(e) => (query.clone(), e),
        };
        
        Ok(TranslateInitData {
            done: AtomicBool::new(false),
            from_dialect: "duckdb".to_string(),
            to_dialect,
            original: query,
            translated,
            error,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).insert(0, CString::new(init_data.from_dialect.clone())?);
        output.flat_vector(1).insert(0, CString::new(init_data.to_dialect.clone())?);
        output.flat_vector(2).insert(0, CString::new(init_data.original.clone())?);
        output.flat_vector(3).insert(0, CString::new(init_data.translated.clone())?);
        output.flat_vector(4).insert(0, CString::new(init_data.error.clone())?);
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// ============================================================================
// SQLGlot Check Table Function - sazgar_sqlglot()
// ============================================================================

#[repr(C)]
pub struct SqlglotCheckBindData;

#[repr(C)]
pub struct SqlglotCheckInitData {
    done: AtomicBool,
    available: bool,
    version: String,
    error: String,
}

pub struct SqlglotCheckVTab;

impl VTab for SqlglotCheckVTab {
    type InitData = SqlglotCheckInitData;
    type BindData = SqlglotCheckBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("available", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column("version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SqlglotCheckBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let (available, version, error) = match check_sqlglot() {
            Ok(v) => (true, v, String::new()),
            Err(e) => (false, String::new(), e),
        };
        
        Ok(SqlglotCheckInitData {
            done: AtomicBool::new(false),
            available,
            version,
            error,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).as_mut_slice::<bool>()[0] = init_data.available;
        output.flat_vector(1).insert(0, CString::new(init_data.version.clone())?);
        output.flat_vector(2).insert(0, CString::new(init_data.error.clone())?);
        
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

