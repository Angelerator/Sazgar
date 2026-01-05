//! Sazgar Smart Routing Module v1.0.0
//!
//! Routes queries to remote databases and returns actual data.
//!
//! ## Usage
//! ```sql
//! -- Register a target
//! SELECT * FROM sazgar_target('tavana', 'host=tavana.example.com port=443 user=x password=y sslmode=require');
//!
//! -- Execute query on remote target and get actual data back
//! SELECT * FROM sazgar_route('SELECT * FROM users LIMIT 10', 'tavana', 'TRUE', '');
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

// PostgreSQL client
use postgres::{Client, NoTls};
use postgres::types::Type as PgType;

// For SSL connections (optional feature)
#[cfg(feature = "tls")]
use native_tls::TlsConnector;
#[cfg(feature = "tls")]
use postgres_native_tls::MakeTlsConnector;

// ============================================================================
// Global Target Registry
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
        Ok(output) if output.status.success() => {
            let translated = String::from_utf8_lossy(&output.stdout).trim().to_string();
            Ok(if translated.is_empty() { sql.to_string() } else { translated })
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("SQLGlot error: {}", stderr.trim()))
        }
        Err(_) => Ok(sql.to_string()), // Fall back to original if Python not available
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
        _ => Err("SQLGlot not available".to_string()),
    }
}

// ============================================================================
// Connection String Parsing
// ============================================================================

#[derive(Clone, Debug)]
#[allow(dead_code)]
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
        
        // PostgreSQL detection
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
        if conn_str.ends_with(".db") || conn_str.ends_with(".sqlite") {
            return Self { provider_type: "sqlite".into(), dialect: "sqlite".into(), connection: conn_str.into(), is_local: false };
        }
        
        Self { provider_type: "duckdb".into(), dialect: "duckdb".into(), connection: conn_str.into(), is_local: conn_str.is_empty() }
    }
}

// ============================================================================
// PostgreSQL Connection Helper
// ============================================================================

fn connect_postgres(conn_str: &str) -> Result<Client, String> {
    // Check if SSL/TLS is needed
    let needs_ssl = conn_str.contains("sslmode=require") || 
                    conn_str.contains("sslmode=verify") ||
                    conn_str.contains("port=443");
    
    if needs_ssl {
        #[cfg(feature = "tls")]
        {
            // Create TLS connector that accepts any certificate (for dev/testing)
            let tls_builder = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .map_err(|e| format!("TLS builder error: {}", e))?;
            let connector = MakeTlsConnector::new(tls_builder);
            
            Client::connect(conn_str, connector)
                .map_err(|e| format!("PostgreSQL connection failed: {}", e))
        }
        #[cfg(not(feature = "tls"))]
        {
            // TLS not available - try without SSL (remove sslmode from connection string)
            let conn_str_no_ssl = conn_str
                .replace("sslmode=require", "sslmode=disable")
                .replace("sslmode=verify-full", "sslmode=disable")
                .replace("sslmode=verify-ca", "sslmode=disable");
            
            Client::connect(&conn_str_no_ssl, NoTls)
                .map_err(|e| format!("PostgreSQL connection failed (TLS not available, tried without SSL): {}", e))
        }
    } else {
        Client::connect(conn_str, NoTls)
            .map_err(|e| format!("PostgreSQL connection failed: {}", e))
    }
}

// ============================================================================
// sazgar_target() - Register Named Targets
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
            let registry = TARGET_REGISTRY.read().unwrap();
            if let Some(target) = registry.get(&name) {
                (target.dialect.clone(), target.provider_type.clone(), "found".to_string())
            } else {
                ("".to_string(), "".to_string(), "not_found".to_string())
            }
        } else {
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
// sazgar_targets() - List All Targets
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
// sazgar_route() - EXECUTE QUERY AND RETURN DATA
// ============================================================================

/// Type enum that we can Clone (since LogicalTypeId doesn't implement Clone)
#[derive(Clone, Debug, Copy, PartialEq)]
enum DuckType {
    Boolean,
    Smallint,
    Integer,
    Bigint,
    Float,
    Double,
    Varchar,
    Blob,
    Date,
    Time,
    Timestamp,
}

impl DuckType {
    fn to_logical_type(&self) -> LogicalTypeHandle {
        match self {
            DuckType::Boolean => LogicalTypeHandle::from(LogicalTypeId::Boolean),
            DuckType::Smallint => LogicalTypeHandle::from(LogicalTypeId::Smallint),
            DuckType::Integer => LogicalTypeHandle::from(LogicalTypeId::Integer),
            DuckType::Bigint => LogicalTypeHandle::from(LogicalTypeId::Bigint),
            DuckType::Float => LogicalTypeHandle::from(LogicalTypeId::Float),
            DuckType::Double => LogicalTypeHandle::from(LogicalTypeId::Double),
            DuckType::Varchar => LogicalTypeHandle::from(LogicalTypeId::Varchar),
            DuckType::Blob => LogicalTypeHandle::from(LogicalTypeId::Blob),
            DuckType::Date => LogicalTypeHandle::from(LogicalTypeId::Date),
            DuckType::Time => LogicalTypeHandle::from(LogicalTypeId::Time),
            DuckType::Timestamp => LogicalTypeHandle::from(LogicalTypeId::Timestamp),
        }
    }
}

/// Convert PostgreSQL type to our DuckType enum
fn pg_type_to_duck(pg_type: &PgType) -> DuckType {
    match *pg_type {
        PgType::BOOL => DuckType::Boolean,
        PgType::INT2 => DuckType::Smallint,
        PgType::INT4 => DuckType::Integer,
        PgType::INT8 => DuckType::Bigint,
        PgType::FLOAT4 => DuckType::Float,
        PgType::FLOAT8 | PgType::NUMERIC => DuckType::Double,
        PgType::DATE => DuckType::Date,
        PgType::TIME => DuckType::Time,
        PgType::TIMESTAMP | PgType::TIMESTAMPTZ => DuckType::Timestamp,
        PgType::BYTEA => DuckType::Blob,
        _ => DuckType::Varchar, // Default to VARCHAR
    }
}

/// Stores column metadata discovered from PostgreSQL
#[derive(Clone, Debug)]
struct ColumnMeta {
    name: String,
    duck_type: DuckType,
    #[allow(dead_code)]
    pg_type: PgType,  // Kept for future binary protocol support
}

/// Stores a row of data as strings (will be converted to proper types on output)
#[derive(Clone, Debug)]
struct DataRow {
    values: Vec<String>,
}

#[repr(C)]
pub struct RouteBindData {
    query: String,
    target_name: String,
    condition: String,
    remote_query: String,
    connection_string: String,
    columns: Vec<ColumnMeta>,
    error: Option<String>,
}

#[repr(C)]
pub struct RouteInitData {
    current_idx: AtomicUsize,
    columns: Vec<ColumnMeta>,
    rows: Vec<DataRow>,
    error: Option<String>,
}

pub struct CustomRouteVTab;

impl VTab for CustomRouteVTab {
    type InitData = RouteInitData;
    type BindData = RouteBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        // Parse parameters
        let query = bind.get_parameter(0).to_string().trim_matches('"').to_string();
        let target_name = bind.get_parameter(1).to_string().trim_matches('"').to_string();
        let condition = bind.get_parameter(2).to_string().trim_matches('"').to_string();
        let remote_query_param = bind.get_parameter(3).to_string().trim_matches('"').to_string();
        
        // Determine the query to execute
        let remote_query = if remote_query_param.is_empty() {
            query.clone()
        } else {
            remote_query_param
        };
        
        // Resolve target connection string
        let connection_string = {
            let registry = TARGET_REGISTRY.read().unwrap();
            if let Some(target) = registry.get(&target_name) {
                target.connection_string.clone()
            } else if target_name.contains("host=") || target_name.contains("://") {
                target_name.clone()
            } else {
                return Err(format!("Target '{}' not found. Register it first with sazgar_target()", target_name).into());
            }
        };
        
        let conn_info = ConnectionInfo::parse(&connection_string);
        
        // Translate query if needed
        let translated_query = if conn_info.dialect != "duckdb" && conn_info.dialect != "postgres" {
            sqlglot_transpile(&remote_query, &conn_info.dialect).unwrap_or(remote_query.clone())
        } else {
            remote_query.clone()
        };
        
        // Connect to PostgreSQL and discover schema
        let (columns, error) = match connect_postgres(&connection_string) {
            Ok(mut client) => {
                // Execute query with LIMIT 0 to get schema only (for bind phase)
                // But we also need to prepare for actual data fetch
                let schema_query = format!("SELECT * FROM ({}) AS _sazgar_schema LIMIT 0", translated_query);
                
                match client.query(&schema_query, &[]) {
                    Ok(rows) => {
                        if rows.is_empty() {
                            // Need to get columns from statement
                            match client.prepare(&translated_query) {
                                Ok(stmt) => {
                                    let cols: Vec<ColumnMeta> = stmt.columns().iter().map(|c| {
                                        ColumnMeta {
                                            name: c.name().to_string(),
                                            duck_type: pg_type_to_duck(c.type_()),
                                            pg_type: c.type_().clone(),
                                        }
                                    }).collect();
                                    (cols, None)
                                }
                                Err(e) => (vec![], Some(format!("Query preparation failed: {}", e)))
                            }
                        } else {
                            // Get columns from first row's metadata
                            let cols: Vec<ColumnMeta> = rows[0].columns().iter().map(|c| {
                                ColumnMeta {
                                    name: c.name().to_string(),
                                    duck_type: pg_type_to_duck(c.type_()),
                                    pg_type: c.type_().clone(),
                                }
                            }).collect();
                            (cols, None)
                        }
                    }
                    Err(e) => {
                        // If LIMIT 0 fails, try prepare
                        match client.prepare(&translated_query) {
                            Ok(stmt) => {
                                let cols: Vec<ColumnMeta> = stmt.columns().iter().map(|c| {
                                    ColumnMeta {
                                        name: c.name().to_string(),
                                        duck_type: pg_type_to_duck(c.type_()),
                                        pg_type: c.type_().clone(),
                                    }
                                }).collect();
                                (cols, None)
                            }
                            Err(_) => (vec![], Some(format!("Query failed: {}", e)))
                        }
                    }
                }
            }
            Err(e) => (vec![], Some(e))
        };
        
        // If we have an error or no columns, add an error column
        if columns.is_empty() {
            bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            return Ok(RouteBindData {
                query,
                target_name,
                condition,
                remote_query: translated_query,
                connection_string,
                columns: vec![ColumnMeta {
                    name: "error".to_string(),
                    duck_type: DuckType::Varchar,
                    pg_type: PgType::TEXT,
                }],
                error,
            });
        }
        
        // Add discovered columns to the result
        for col in &columns {
            bind.add_result_column(&col.name, col.duck_type.to_logical_type());
        }
        
        Ok(RouteBindData {
            query,
            target_name,
            condition,
            remote_query: translated_query,
            connection_string,
            columns,
            error,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind = info.get_bind_data::<RouteBindData>();
        let remote_query = unsafe { (*bind).remote_query.clone() };
        let connection_string = unsafe { (*bind).connection_string.clone() };
        let columns = unsafe { (*bind).columns.clone() };
        let bind_error = unsafe { (*bind).error.clone() };
        
        // If there was an error in bind, return it
        if let Some(err) = bind_error {
            return Ok(RouteInitData {
                current_idx: AtomicUsize::new(0),
                columns: vec![ColumnMeta {
                    name: "error".to_string(),
                    duck_type: DuckType::Varchar,
                    pg_type: PgType::TEXT,
                }],
                rows: vec![DataRow { values: vec![err] }],
                error: None,
            });
        }
        
        // Execute the actual query using simple_query (text protocol)
        // This is more compatible with DuckDB/Tavana than binary protocol
        let rows = match connect_postgres(&connection_string) {
            Ok(mut client) => {
                match client.simple_query(&remote_query) {
                    Ok(results) => {
                        let mut data_rows: Vec<DataRow> = Vec::new();
                        
                        for result in results {
                            if let postgres::SimpleQueryMessage::Row(row) = result {
                                // simple_query returns SimpleQueryRow with text values
                                let values: Vec<String> = (0..columns.len()).map(|idx| {
                                    row.get(idx).unwrap_or("").to_string()
                                }).collect();
                                data_rows.push(DataRow { values });
                            }
                        }
                        
                        data_rows
                    }
                    Err(e) => {
                        return Ok(RouteInitData {
                            current_idx: AtomicUsize::new(0),
                            columns: columns.clone(),
                            rows: vec![],
                            error: Some(format!("Query execution failed: {}", e)),
                        });
                    }
                }
            }
            Err(e) => {
                return Ok(RouteInitData {
                    current_idx: AtomicUsize::new(0),
                    columns: columns.clone(),
                    rows: vec![],
                    error: Some(e),
                });
            }
        };
        
        Ok(RouteInitData {
            current_idx: AtomicUsize::new(0),
            columns,
            rows,
            error: None,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        let current = init.current_idx.load(Ordering::Relaxed);
        
        // Check if we're done
        if current >= init.rows.len() {
            output.set_len(0);
            return Ok(());
        }
        
        // Calculate batch size
        let batch_size = std::cmp::min(2048, init.rows.len() - current);
        
        // Output rows
        for i in 0..batch_size {
            let row = &init.rows[current + i];
            
            for (col_idx, col) in init.columns.iter().enumerate() {
                let value = row.values.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                
                match col.duck_type {
                    DuckType::Boolean => {
                        let bool_val = value.eq_ignore_ascii_case("true") || value == "t" || value == "1";
                        output.flat_vector(col_idx).as_mut_slice::<bool>()[i] = bool_val;
                    }
                    DuckType::Smallint => {
                        let int_val: i16 = value.parse().unwrap_or(0);
                        output.flat_vector(col_idx).as_mut_slice::<i16>()[i] = int_val;
                    }
                    DuckType::Integer => {
                        let int_val: i32 = value.parse().unwrap_or(0);
                        output.flat_vector(col_idx).as_mut_slice::<i32>()[i] = int_val;
                    }
                    DuckType::Bigint => {
                        let int_val: i64 = value.parse().unwrap_or(0);
                        output.flat_vector(col_idx).as_mut_slice::<i64>()[i] = int_val;
                    }
                    DuckType::Float => {
                        let float_val: f32 = value.parse().unwrap_or(0.0);
                        output.flat_vector(col_idx).as_mut_slice::<f32>()[i] = float_val;
                    }
                    DuckType::Double => {
                        let float_val: f64 = value.parse().unwrap_or(0.0);
                        output.flat_vector(col_idx).as_mut_slice::<f64>()[i] = float_val;
                    }
                    _ => {
                        // VARCHAR and other types - insert as string
                        output.flat_vector(col_idx).insert(i, CString::new(value)?);
                    }
                }
            }
        }
        
        init.current_idx.store(current + batch_size, Ordering::Relaxed);
        output.set_len(batch_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),  // query
            LogicalTypeHandle::from(LogicalTypeId::Varchar),  // target
            LogicalTypeHandle::from(LogicalTypeId::Varchar),  // condition
            LogicalTypeHandle::from(LogicalTypeId::Varchar),  // remote_query (optional override)
        ])
    }
}

// ============================================================================
// sazgar_translate() - Direct SQL Translation
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
            "postgres".to_string()
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
// sazgar_estimate() - Estimate Data Size
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
