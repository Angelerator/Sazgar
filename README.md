# Sazgar - DuckDB Extension for System Monitoring & Query Routing

[![DuckDB Community Extension](https://img.shields.io/badge/DuckDB-Community%20Extension-yellow?logo=duckdb)](https://duckdb.org/community_extensions/extensions/sazgar)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Sazgar** (Persian: سازگار, meaning "compatible") is a DuckDB extension that does two things:

1. **System Monitoring** - Query your CPU, memory, disk, network, and processes using SQL
2. **Query Routing** - Run SQL queries on remote databases (PostgreSQL, Tavana, etc.) and get results back

---

## Installation

```sql
INSTALL sazgar FROM community;
LOAD sazgar;
```

That's it! You're ready to go.

---

## Part 1: System Monitoring

Query your system like a database:

```sql
-- Get system overview
SELECT * FROM sazgar_system();

-- Check memory usage
SELECT * FROM sazgar_memory();

-- Find top 5 memory-hungry processes
SELECT pid, name, memory, cpu_percent
FROM sazgar_processes()
ORDER BY memory DESC
LIMIT 5;

-- Check disk space
SELECT name, mount_point, usage_percent
FROM sazgar_disks();
```

### Available Functions

| Function | What it does |
|----------|--------------|
| `sazgar_system()` | Overall system info (CPU, RAM, uptime) |
| `sazgar_memory()` | RAM usage details |
| `sazgar_cpu()` | CPU info per core |
| `sazgar_disks()` | Disk space usage |
| `sazgar_network()` | Network interface stats |
| `sazgar_processes()` | Running processes |
| `sazgar_ports()` | Open network ports |
| `sazgar_docker()` | Docker containers |
| `sazgar_services()` | System services |

All memory/disk functions accept a `unit` parameter: `'bytes'`, `'KB'`, `'MB'`, `'GB'`, `'TB'`

```sql
-- Memory in GB instead of default MB
SELECT * FROM sazgar_memory(unit := 'GB');
```

---

## Part 2: Query Routing (The Simple Way)

**What is it?** Run SQL on a remote database and get the results in DuckDB.

### Step 1: Register Your Database

```sql
-- Register a database connection (do this once)
SELECT * FROM sazgar_target(
  'mydb',  -- give it a name
  'host=db.example.com port=5432 user=admin password=secret dbname=mydb'
);
```

### Step 2: Run Queries

```sql
-- Run a query on the remote database
SELECT * FROM sazgar_route(
  'SELECT * FROM users LIMIT 10',  -- your query
  'mydb',                           -- target name from step 1
  'TRUE',                           -- condition (just use 'TRUE')
  ''                                -- leave empty for auto-translation
);
```

**That's it!** The results come back to your local DuckDB.

### Simple Examples

```sql
-- Example 1: Simple SELECT
SELECT * FROM sazgar_route('SELECT * FROM orders LIMIT 5', 'mydb', 'TRUE', '');

-- Example 2: With WHERE clause
SELECT * FROM sazgar_route(
  'SELECT * FROM products WHERE price > 100', 
  'mydb', 
  'TRUE', 
  ''
);

-- Example 3: Aggregation
SELECT * FROM sazgar_route(
  'SELECT status, COUNT(*) FROM orders GROUP BY status', 
  'mydb', 
  'TRUE', 
  ''
);
```

### Custom Remote Query

Sometimes the remote database has different table names. Use the 4th parameter:

```sql
SELECT * FROM sazgar_route(
  'SELECT * FROM local_data',           -- ignored when 4th param is provided
  'mydb',                                -- target
  'TRUE',                                -- condition
  'SELECT * FROM production.orders'      -- this exact query runs on remote
);
```

### What are the 4 parameters?

| # | Parameter | Required | What it means |
|---|-----------|----------|---------------|
| 1 | `query` | Yes | Your SQL query (DuckDB syntax) |
| 2 | `target` | Yes | Name you gave in `sazgar_target()` |
| 3 | `condition` | Yes | For documentation (currently not evaluated, use `'TRUE'`) |
| 4 | `remote_query` | Yes | Custom query for remote, or `''` to use param 1 |

> **Note:** All 4 parameters are required. You cannot omit `remote_query` - pass `''` to use the first parameter's query.

---

## Common Recipes

### Recipe 1: Connect to PostgreSQL

```sql
-- Register
SELECT * FROM sazgar_target(
  'postgres_prod',
  'host=prod.example.com port=5432 user=reader password=pass dbname=analytics'
);

-- Query
SELECT * FROM sazgar_route('SELECT * FROM sales LIMIT 100', 'postgres_prod', 'TRUE', '');
```

### Recipe 2: Connect to Tavana

```sql
-- Register (note: Tavana uses port 443 and SSL)
SELECT * FROM sazgar_target(
  'tavana',
  'host=tavana.example.com port=443 user=admin password=pass sslmode=require'
);

-- Query
SELECT * FROM sazgar_route('SELECT * FROM bronze.events LIMIT 100', 'tavana', 'TRUE', '');
```

### Recipe 3: List All Registered Targets

```sql
SELECT * FROM sazgar_targets();
```

### Recipe 4: Conditional Routing (Manual)

The `condition` parameter is stored for documentation but **not yet evaluated**. To implement conditional routing now, use a CTE or wrapper:

```sql
-- Check resources first, then decide whether to route
WITH should_route AS (
  SELECT memory_usage_percent > 80 AS route_remote FROM sazgar_memory()
)
SELECT * FROM sazgar_route(
  'SELECT * FROM big_table',
  'remote_db',
  'TRUE',
  ''
)
WHERE (SELECT route_remote FROM should_route);

-- Alternative: Use CASE in your application logic
-- If RAM > 80%, query remote; otherwise query local
```

> **Future:** The condition parameter will be evaluated automatically in a future version. For now, use the patterns above.

---

## SQL Translation

Sazgar can translate DuckDB SQL to other dialects automatically.

### Check Translation

```sql
-- See how your query translates to MySQL
SELECT * FROM sazgar_translate('SELECT x::int FROM t', 'mysql');
-- Result: SELECT CAST(x AS SIGNED) FROM t
```

### Check SQLGlot Status

```sql
SELECT * FROM sazgar_sqlglot();
```

> **Note:** Install SQLGlot for auto-translation: `pip install sqlglot`
> 
> If you always provide a custom `remote_query`, SQLGlot is NOT needed.

---

## Full Function Reference

### System Monitoring

| Function | Parameters | Description |
|----------|------------|-------------|
| `sazgar_system(unit)` | `unit`: 'MB' (default) | System overview |
| `sazgar_memory(unit)` | `unit`: 'MB' (default) | Memory usage |
| `sazgar_cpu()` | none | CPU per-core info |
| `sazgar_cpu_cores()` | none | CPU cores with usage |
| `sazgar_disks(unit)` | `unit`: 'GB' (default) | Disk usage |
| `sazgar_network(unit)` | `unit`: 'MB' (default) | Network stats |
| `sazgar_processes(unit)` | `unit`: 'MB' (default) | Running processes |
| `sazgar_ports(filter)` | `filter`: '' for all, 'TCP', 'UDP' | Open ports |
| `sazgar_docker()` | none | Docker containers |
| `sazgar_services()` | none | System services |
| `sazgar_load()` | none | Load averages |
| `sazgar_uptime()` | none | System uptime |
| `sazgar_users()` | none | System users |
| `sazgar_swap(unit)` | `unit`: 'GB' (default) | Swap memory |
| `sazgar_components()` | none | Temperature sensors |
| `sazgar_environment(filter)` | `filter`: '' for all | Environment variables |
| `sazgar_gpu()` | none | NVIDIA GPU info |
| `sazgar_fds(pid)` | `pid`: 0 for all | File descriptors (Linux) |
| `sazgar_os()` | none | OS details |
| `sazgar_version()` | none | Extension version |

### Query Routing

| Function | Description |
|----------|-------------|
| `sazgar_target(name, connection)` | Register a database connection |
| `sazgar_targets()` | List all registered connections |
| `sazgar_route(query, target, condition, remote_query)` | Execute query on remote database |
| `sazgar_translate(query, dialect)` | Translate SQL to another dialect |
| `sazgar_sqlglot()` | Check if SQLGlot is available |
| `sazgar_estimate(path)` | Estimate data size for a path |

---

## Platform Support

| Platform | Status |
|----------|--------|
| Linux (x86_64, ARM64) | ✅ Full |
| macOS (Intel, Apple Silicon) | ✅ Full |
| Windows (x86_64, ARM64) | ✅ Full |
| Android | ⚠️ Partial |
| iOS | ⚠️ Partial |

---

## Building from Source

```bash
git clone --recurse-submodules https://github.com/Angelerator/Sazgar.git
cd Sazgar

make configure
make release

# Output: build/release/sazgar.duckdb_extension
```

### Load Local Build

```sql
-- Start DuckDB with: duckdb -unsigned
LOAD '/path/to/sazgar.duckdb_extension';
```

### Build with TLS Support

For SSL connections (`sslmode=require`):

```bash
cargo build --release --features tls
make release
```

---

## FAQ

**Q: Do I need Python?**
A: Only if you use auto-translation (empty `remote_query`). Install with `pip install sqlglot`.

**Q: What databases are supported?**
A: PostgreSQL-compatible databases: PostgreSQL, Tavana, and others using the PostgreSQL wire protocol.

**Q: Can I use connection URLs?**
A: Yes! Both formats work:
- `postgres://user:pass@host:port/db`
- `host=... port=... user=... password=... dbname=...`

---

## License

MIT License

---

## Links

- [DuckDB Community Extensions](https://duckdb.org/community_extensions/extensions/sazgar)
- [GitHub Repository](https://github.com/Angelerator/Sazgar)
- [Report Issues](https://github.com/Angelerator/Sazgar/issues)
