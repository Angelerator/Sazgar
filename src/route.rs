//! Sazgar Smart Routing Module v0.6.0
//!
//! ONE function for all routing: `sazgar_smart_route(query, fallback, condition)`
//!
//! ## Requirements
//! - Python 3 with SQLGlot: `pip install sqlglot`
//!
//! ## Secure Credentials
//! Register named targets once, use by name forever:
//! ```sql
//! -- Register target (credentials stored in memory, not in queries)
//! SELECT * FROM sazgar_target('tavana', 'host=tavana-dev... password=secret');
//! SELECT * FROM sazgar_target('prod_mysql', 'mysql://user:pass@host/db');
//!
//! -- Then use by name (no credentials in query!)
//! SELECT * FROM sazgar_smart_route(
//!   query := 'SELECT * FROM big_table',
//!   fallback := 'tavana',  -- Just the name!
//!   condition := '(SELECT available_memory FROM sazgar_memory(''GB'')) < 2'
//! );
//! ```
//!
//! ## Direct Connection (for quick testing)
//! ```sql
//! SELECT * FROM sazgar_smart_route(
//!   query := 'SELECT * FROM table',
//!   fallback := 'postgres://user:pass@host/db',
//!   condition := '(SELECT load_1min FROM sazgar_load()) > 5'
//! );
//! ```

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    collections::HashMap,
    ffi::CString,
    process::Command,
    sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, RwLock},
};

// ============================================================================
// Global Target Registry (for secure credential storage)
// ============================================================================

lazy_static::lazy_static! {
    static ref TARGET_REGISTRY: RwLock<HashMap<String, TargetConfig>> = RwLock::new(HashMap::new());
}

#[derive(Clone, Debug)]
pub struct TargetConfig {
    pub name: String,
    pub connection_string: String,
    pub dialect: String,
    pub provider_type: String,
}

impl TargetConfig {
    pub fn new(name: &str, connection_string: &str) -> Self {
        let info = ConnectionInfo::parse(connection_string);
        Self {
            name: name.to_string(),
            connection_string: connection_string.to_string(),
            dialect: info.dialect,
            provider_type: info.provider_type,
        }
    }
}

// ============================================================================
// SQLGlot Integration
// ============================================================================

/// Translate SQL from DuckDB dialect to target dialect using SQLGlot
pub fn sqlglot_transpile(sql: &str, to_dialect: &str) -> Result<String, String> {
    let to = to_dialect.to_lowercase();
    if to == "duckdb" || to == "duck" || to.is_empty() {
        return Ok(sql.to_string());
    }
    
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
        other => other,
    };
    
    let escaped_sql = sql.replace('\\', "\\\\").replace('\'', "\\'");
    let python_code = format!(
        "import sqlglot; print(sqlglot.transpile('{}', read='duckdb', write='{}')[0])",
        escaped_sql, sqlglot_dialect
    );
    
    let result = Command::new("python3")
        .args(["-c", &python_code])
        .output()
        .or_else(|_| Command::new("python").args(["-c", &python_code]).output());
    
    match result {
        Ok(output) => {
            if output.status.success() {
                let translated = String::from_utf8_lossy(&output.stdout).trim().to_string();
                Ok(if translated.is_empty() { sql.to_string() } else { translated })
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if stderr.contains("No module named 'sqlglot'") {
                    // Try auto-install
                    match auto_install_sqlglot() {
                        Ok(_) => {
                            // Retry after install
                            let retry = Command::new("python3")
                                .args(["-c", &python_code])
                                .output()
                                .or_else(|_| Command::new("python").args(["-c", &python_code]).output());
                            match retry {
                                Ok(r) if r.status.success() => {
                                    Ok(String::from_utf8_lossy(&r.stdout).trim().to_string())
                                }
                                _ => Err("SQLGlot installed but translation failed".to_string()),
                            }
                        }
                        Err(e) => Err(format!("SQLGlot not installed: {}. Run: pip install sqlglot", e)),
                    }
                } else {
                    Err(format!("SQLGlot error: {}", stderr.trim()))
                }
            }
        }
        Err(e) => Err(format!("Python not found: {}", e)),
    }
}

fn auto_install_sqlglot() -> Result<String, String> {
    let install = Command::new("python3")
        .args(["-m", "pip", "install", "--user", "--quiet", "sqlglot"])
        .output()
        .or_else(|_| Command::new("pip3").args(["install", "--user", "--quiet", "sqlglot"]).output());
    
    match install {
        Ok(o) if o.status.success() => Ok("SQLGlot installed".to_string()),
        Ok(o) => Err(String::from_utf8_lossy(&o.stderr).to_string()),
        Err(e) => Err(e.to_string()),
    }
}

pub fn check_sqlglot() -> Result<String, String> {
    let result = Command::new("python3")
        .args(["-c", "import sqlglot; print(f'SQLGlot {sqlglot.__version__}')"])
        .output()
        .or_else(|_| Command::new("python")
            .args(["-c", "import sqlglot; print(f'SQLGlot {sqlglot.__version__}')"])
            .output());
    
    match result {
        Ok(o) if o.status.success() => Ok(String::from_utf8_lossy(&o.stdout).trim().to_string()),
        Ok(_) => {
            match auto_install_sqlglot() {
                Ok(msg) => Ok(format!("{} (auto-installed)", msg)),
                Err(e) => Err(format!("SQLGlot not available: {}", e)),
            }
        }
        Err(_) => Err("Python not found".to_string()),
    }
}

// ============================================================================
// Connection String Parsing
// ============================================================================

#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub provider_type: String,
    pub dialect: String,
    pub connection: String,
    pub is_local: bool,
}

impl ConnectionInfo {
    pub fn parse(conn_str: &str) -> Self {
        let conn_str = conn_str.trim();
        
        if conn_str.eq_ignore_ascii_case("local") || conn_str.is_empty() {
            return Self { provider_type: "duckdb".into(), dialect: "duckdb".into(), connection: "".into(), is_local: true };
        }
        
        // PostgreSQL
        if conn_str.starts_with("postgres://") || conn_str.starts_with("postgresql://") 
           || (conn_str.contains("host=") && conn_str.contains("port=")) {
            let is_tavana = conn_str.to_lowercase().contains("tavana");
            return Self {
                provider_type: "postgres".into(),
                dialect: if is_tavana { "duckdb".into() } else { "postgres".into() },
                connection: conn_str.into(),
                is_local: false,
            };
        }
        
        // MySQL
        if conn_str.starts_with("mysql://") || conn_str.starts_with("mariadb://") {
            return Self { provider_type: "mysql".into(), dialect: "mysql".into(), connection: conn_str.into(), is_local: false };
        }
        
        // SQLite
        if conn_str.starts_with("sqlite://") || conn_str.ends_with(".db") || conn_str.ends_with(".sqlite") {
            return Self { provider_type: "sqlite".into(), dialect: "sqlite".into(), connection: conn_str.into(), is_local: false };
        }
        
        // JDBC
        if conn_str.starts_with("jdbc:") {
            let dialect = if conn_str.contains("postgresql") { "postgres" }
                else if conn_str.contains("mysql") { "mysql" }
                else if conn_str.contains("oracle") { "oracle" }
                else if conn_str.contains("sqlserver") { "tsql" }
                else if conn_str.contains("snowflake") { "snowflake" }
                else if conn_str.contains("bigquery") { "bigquery" }
                else { "postgres" };
            return Self { provider_type: "jdbc".into(), dialect: dialect.into(), connection: conn_str.into(), is_local: false };
        }
        
        // BigQuery, Snowflake, ClickHouse
        if conn_str.contains("bigquery") { return Self { provider_type: "bigquery".into(), dialect: "bigquery".into(), connection: conn_str.into(), is_local: false }; }
        if conn_str.contains("snowflake") { return Self { provider_type: "snowflake".into(), dialect: "snowflake".into(), connection: conn_str.into(), is_local: false }; }
        if conn_str.starts_with("clickhouse://") { return Self { provider_type: "clickhouse".into(), dialect: "clickhouse".into(), connection: conn_str.into(), is_local: false }; }
        
        // Default: local DuckDB file
        Self { provider_type: "duckdb".into(), dialect: "duckdb".into(), connection: conn_str.into(), is_local: conn_str.is_empty() }
    }
}

// ============================================================================
// sazgar_target() - Register Named Targets (Secure Credentials)
// ============================================================================

#[repr(C)]
pub struct TargetBindData {
    name: String,
    connection_string: String,
}

#[repr(C)]
pub struct TargetInitData {
    done: AtomicBool,
    name: String,
    dialect: String,
    provider: String,
    status: String,
}

pub struct TargetVTab;

impl VTab for TargetVTab {
    type InitData = TargetInitData;
    type BindData = TargetBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let name = bind.get_parameter(0).to_string().trim_matches('"').to_string();
        let connection_string = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_string().trim_matches('"').to_string()
        } else {
            String::new()
        };
        
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("provider", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        Ok(TargetBindData { name, connection_string })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind = info.get_bind_data::<TargetBindData>();
        let name = unsafe { (*bind).name.clone() };
        let connection_string = unsafe { (*bind).connection_string.clone() };
        
        let (dialect, provider, status) = if connection_string.is_empty() {
            // Lookup mode
            let registry = TARGET_REGISTRY.read().unwrap();
            if let Some(target) = registry.get(&name) {
                (target.dialect.clone(), target.provider_type.clone(), "found".to_string())
            } else {
                ("".to_string(), "".to_string(), "not_found".to_string())
            }
        } else {
            // Register mode
            let target = TargetConfig::new(&name, &connection_string);
            let dialect = target.dialect.clone();
            let provider = target.provider_type.clone();
            TARGET_REGISTRY.write().unwrap().insert(name.clone(), target);
            (dialect, provider, "registered".to_string())
        };
        
        Ok(TargetInitData {
            done: AtomicBool::new(false),
            name,
            dialect,
            provider,
            status,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        if init.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).insert(0, CString::new(init.name.clone())?);
        output.flat_vector(1).insert(0, CString::new(init.dialect.clone())?);
        output.flat_vector(2).insert(0, CString::new(init.provider.clone())?);
        output.flat_vector(3).insert(0, CString::new(init.status.clone())?);
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
// sazgar_targets() - List All Registered Targets
// ============================================================================

#[repr(C)]
pub struct TargetsBindData;

#[repr(C)]
pub struct TargetsInitData {
    current: AtomicUsize,
    targets: Vec<(String, String, String)>,
}

pub struct TargetsVTab;

impl VTab for TargetsVTab {
    type InitData = TargetsInitData;
    type BindData = TargetsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("provider", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(TargetsBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let registry = TARGET_REGISTRY.read().unwrap();
        let targets: Vec<_> = registry.values()
            .map(|t| (t.name.clone(), t.dialect.clone(), t.provider_type.clone()))
            .collect();
        Ok(TargetsInitData { current: AtomicUsize::new(0), targets })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        let current = init.current.load(Ordering::Relaxed);
        if current >= init.targets.len() {
            output.set_len(0);
            return Ok(());
        }
        
        let batch = std::cmp::min(2048, init.targets.len() - current);
        for i in 0..batch {
            let t = &init.targets[current + i];
            output.flat_vector(0).insert(i, CString::new(t.0.clone())?);
            output.flat_vector(1).insert(i, CString::new(t.1.clone())?);
            output.flat_vector(2).insert(i, CString::new(t.2.clone())?);
        }
        init.current.store(current + batch, Ordering::Relaxed);
        output.set_len(batch);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// sazgar_smart_route() - THE ONE ROUTING FUNCTION
// ============================================================================

#[repr(C)]
pub struct SmartRouteBindData {
    query: String,
    fallback: String,
    condition: String,
}

#[repr(C)]
pub struct SmartRouteInitData {
    done: AtomicBool,
    query: String,
    fallback: String,
    condition: String,
    condition_result: bool,
    routed_to: String,
    dialect: String,
    translated_query: String,
    execute_sql: String,
    error: String,
}

pub struct SmartRouteVTab;

impl VTab for SmartRouteVTab {
    type InitData = SmartRouteInitData;
    type BindData = SmartRouteBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let query = bind.get_parameter(0).to_string().trim_matches('"').to_string();
        let fallback = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_string().trim_matches('"').to_string()
        } else {
            "local".to_string()
        };
        let condition = if bind.get_parameter_count() > 2 {
            bind.get_parameter(2).to_string().trim_matches('"').to_string()
        } else {
            "FALSE".to_string() // Default: don't route
        };
        
        bind.add_result_column("query", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("condition", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("condition_result", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column("routed_to", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("translated_query", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("execute_sql", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        Ok(SmartRouteBindData { query, fallback, condition })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind = info.get_bind_data::<SmartRouteBindData>();
        let query = unsafe { (*bind).query.clone() };
        let fallback_input = unsafe { (*bind).fallback.clone() };
        let condition = unsafe { (*bind).condition.clone() };
        
        // Resolve fallback: check if it's a named target
        let (fallback, conn_info) = {
            let registry = TARGET_REGISTRY.read().unwrap();
            if let Some(target) = registry.get(&fallback_input) {
                (target.connection_string.clone(), ConnectionInfo::parse(&target.connection_string))
            } else {
                (fallback_input.clone(), ConnectionInfo::parse(&fallback_input))
            }
        };
        
        // Note: condition_result would be evaluated by DuckDB in actual execution
        // For now, we prepare the routing info. The actual condition evaluation
        // happens when the user wraps this in a CASE or WITH clause.
        let condition_result = false; // Placeholder - actual eval done in SQL
        
        let (translated_query, error) = if conn_info.is_local {
            (query.clone(), String::new())
        } else {
            match sqlglot_transpile(&query, &conn_info.dialect) {
                Ok(t) => (t, String::new()),
                Err(e) => (query.clone(), e),
            }
        };
        
        let routed_to = if conn_info.is_local { "LOCAL".to_string() } else { fallback_input.clone() };
        
        let execute_sql = if conn_info.is_local {
            translated_query.clone()
        } else {
            let escaped = translated_query.replace("'", "''");
            match conn_info.provider_type.as_str() {
                "postgres" => format!("SELECT * FROM postgres_query('{}', '{}')", fallback, escaped),
                "mysql" => format!("SELECT * FROM mysql_query('{}', '{}')", fallback, escaped),
                "sqlite" => format!("ATTACH '{}' AS _remote; {}", fallback, translated_query),
                _ => translated_query.clone(),
            }
        };
        
        Ok(SmartRouteInitData {
            done: AtomicBool::new(false),
            query,
            fallback,
            condition,
            condition_result,
            routed_to,
            dialect: conn_info.dialect,
            translated_query,
            execute_sql,
            error,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        if init.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).insert(0, CString::new(init.query.clone())?);
        output.flat_vector(1).insert(0, CString::new(init.condition.clone())?);
        output.flat_vector(2).as_mut_slice::<bool>()[0] = init.condition_result;
        output.flat_vector(3).insert(0, CString::new(init.routed_to.clone())?);
        output.flat_vector(4).insert(0, CString::new(init.dialect.clone())?);
        output.flat_vector(5).insert(0, CString::new(init.translated_query.clone())?);
        output.flat_vector(6).insert(0, CString::new(init.execute_sql.clone())?);
        output.flat_vector(7).insert(0, CString::new(init.error.clone())?);
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// ============================================================================
// sazgar_translate() - Direct SQL Translation (utility)
// ============================================================================

#[repr(C)]
pub struct TranslateBindData {
    query: String,
    to_dialect: String,
}

#[repr(C)]
pub struct TranslateInitData {
    done: AtomicBool,
    original: String,
    to_dialect: String,
    translated: String,
    error: String,
}

pub struct TranslateVTab;

impl VTab for TranslateVTab {
    type InitData = TranslateInitData;
    type BindData = TranslateBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let query = bind.get_parameter(0).to_string().trim_matches('"').to_string();
        let to_dialect = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_string().trim_matches('"').to_string()
        } else {
            "mysql".to_string()
        };
        
        bind.add_result_column("original", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("to_dialect", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("translated", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(TranslateBindData { query, to_dialect })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind = info.get_bind_data::<TranslateBindData>();
        let query = unsafe { (*bind).query.clone() };
        let to_dialect = unsafe { (*bind).to_dialect.clone() };
        
        let (translated, error) = match sqlglot_transpile(&query, &to_dialect) {
            Ok(t) => (t, String::new()),
            Err(e) => (query.clone(), e),
        };
        
        Ok(TranslateInitData {
            done: AtomicBool::new(false),
            original: query,
            to_dialect,
            translated,
            error,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        if init.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).insert(0, CString::new(init.original.clone())?);
        output.flat_vector(1).insert(0, CString::new(init.to_dialect.clone())?);
        output.flat_vector(2).insert(0, CString::new(init.translated.clone())?);
        output.flat_vector(3).insert(0, CString::new(init.error.clone())?);
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
// sazgar_sqlglot() - Check SQLGlot Status
// ============================================================================

#[repr(C)]
pub struct SqlglotBindData;

#[repr(C)]
pub struct SqlglotInitData {
    done: AtomicBool,
    available: bool,
    version: String,
    error: String,
}

pub struct SqlglotVTab;

impl VTab for SqlglotVTab {
    type InitData = SqlglotInitData;
    type BindData = SqlglotBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("available", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column("version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SqlglotBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let (available, version, error) = match check_sqlglot() {
            Ok(v) => (true, v, String::new()),
            Err(e) => (false, String::new(), e),
        };
        Ok(SqlglotInitData { done: AtomicBool::new(false), available, version, error })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        if init.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }
        
        output.flat_vector(0).as_mut_slice::<bool>()[0] = init.available;
        output.flat_vector(1).insert(0, CString::new(init.version.clone())?);
        output.flat_vector(2).insert(0, CString::new(init.error.clone())?);
        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

// ============================================================================
// sazgar_estimate() - Estimate Data Size (for routing conditions)
// ============================================================================

#[derive(Clone)]
struct PathEstimate {
    path: String,
    path_type: String,
    estimated_gb: f64,
    file_count: u64,
    format: String,
}

#[repr(C)]
pub struct EstimateBindData {
    paths: Vec<String>,
}

#[repr(C)]
pub struct EstimateInitData {
    current: AtomicUsize,
    estimates: Vec<PathEstimate>,
}

pub struct EstimateVTab;

impl EstimateVTab {
    fn folder_size(path: &std::path::Path) -> (u64, u64) {
        let mut size = 0u64;
        let mut count = 0u64;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() { size += meta.len(); count += 1; }
                    else if meta.is_dir() {
                        let (s, c) = Self::folder_size(&entry.path());
                        size += s; count += c;
                    }
                }
            }
        }
        (size, count)
    }
    
    fn detect_format(path: &str) -> String {
        let ext = std::path::Path::new(path).extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();
        match ext.as_str() {
            "parquet" => "parquet",
            "csv" => "csv",
            "json" | "jsonl" => "json",
            _ if path.contains("_delta_log") => "delta",
            _ if path.starts_with("s3://") || path.starts_with("az://") => "cloud",
            _ => "unknown",
        }.to_string()
    }
    
    fn estimate(path: &str) -> PathEstimate {
        let gb = 1_073_741_824.0;
        let p = std::path::Path::new(path);
        
        if !p.exists() {
            return PathEstimate { path: path.into(), path_type: "not_found".into(), estimated_gb: 0.0, file_count: 0, format: "unknown".into() };
        }
        
        if p.is_file() {
            let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            let format = Self::detect_format(path);
            let ratio = if format == "parquet" { 5.0 } else { 1.0 };
            PathEstimate { path: path.into(), path_type: "file".into(), estimated_gb: size as f64 / gb * ratio, file_count: 1, format }
        } else {
            let (size, count) = Self::folder_size(p);
            let format = Self::detect_format(path);
            let ratio = if format == "parquet" || format == "delta" { 4.0 } else { 1.0 };
            PathEstimate { path: path.into(), path_type: "folder".into(), estimated_gb: size as f64 / gb * ratio, file_count: count, format }
        }
    }
}

impl VTab for EstimateVTab {
    type InitData = EstimateInitData;
    type BindData = EstimateBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let input = if bind.get_parameter_count() > 0 {
            bind.get_parameter(0).to_string().trim_matches('"').to_string()
        } else { ".".to_string() };
        
        let paths: Vec<String> = input.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        
        bind.add_result_column("path", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("path_type", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("estimated_gb", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column("file_count", LogicalTypeHandle::from(LogicalTypeId::UBigint));
        bind.add_result_column("format", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(EstimateBindData { paths })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind = info.get_bind_data::<EstimateBindData>();
        let paths = unsafe { (*bind).paths.clone() };
        let estimates: Vec<_> = paths.iter().map(|p| Self::estimate(p)).collect();
        Ok(EstimateInitData { current: AtomicUsize::new(0), estimates })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        let current = init.current.load(Ordering::Relaxed);
        if current >= init.estimates.len() {
            output.set_len(0);
            return Ok(());
        }
        
        let batch = std::cmp::min(2048, init.estimates.len() - current);
        for i in 0..batch {
            let e = &init.estimates[current + i];
            output.flat_vector(0).insert(i, CString::new(e.path.clone())?);
            output.flat_vector(1).insert(i, CString::new(e.path_type.clone())?);
            output.flat_vector(2).as_mut_slice::<f64>()[i] = e.estimated_gb;
            output.flat_vector(3).as_mut_slice::<u64>()[i] = e.file_count;
            output.flat_vector(4).insert(i, CString::new(e.format.clone())?);
        }
        init.current.store(current + batch, Ordering::Relaxed);
        output.set_len(batch);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}
