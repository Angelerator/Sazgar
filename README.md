# Sazgar - DuckDB System Monitoring Extension

[![DuckDB Community Extension](https://img.shields.io/badge/DuckDB-Community%20Extension-yellow?logo=duckdb)](https://duckdb.org/community_extensions/extensions/sazgar)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/Angelerator/Sazgar)](https://github.com/Angelerator/Sazgar/stargazers)

**Sazgar** (Persian: سازگار, meaning "compatible/harmonious") is a comprehensive DuckDB extension for system resource monitoring. Built in pure Rust, it provides SQL table functions to query CPU, memory, disk, network, processes, and more.

> **Install:** `INSTALL sazgar FROM community;` • **Load:** `LOAD sazgar;`

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Functions Reference](#functions-reference)
  - [sazgar_system()](#sazgar_systemunit--mb)
  - [sazgar_version()](#sazgar_version)
  - [sazgar_os()](#sazgar_os)
  - [sazgar_memory()](#sazgar_memoryunit--mb)
  - [sazgar_cpu()](#sazgar_cpu)
  - [sazgar_disks()](#sazgar_disksunit--gb)
  - [sazgar_network()](#sazgar_networkunit--mb)
  - [sazgar_processes()](#sazgar_processesunit--mb)
  - [sazgar_load()](#sazgar_load)
  - [sazgar_users()](#sazgar_users)
  - [sazgar_components()](#sazgar_components)
  - [sazgar_environment()](#sazgar_environmentfilter)
  - [sazgar_uptime()](#sazgar_uptime)
  - [sazgar_swap()](#sazgar_swapunit--gb)
  - [sazgar_cpu_cores()](#sazgar_cpu_cores)
  - [sazgar_ports()](#sazgar_portsprotocol_filter)
  - [sazgar_gpu()](#sazgar_gpu)
  - [sazgar_docker()](#sazgar_docker)
  - [sazgar_services()](#sazgar_services)
  - [sazgar_fds()](#sazgar_fdspid)
- [Use Cases](#use-cases)
- [Building from Source](#building-from-source)
- [Platform Support](#platform-support)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Cross-Platform**: Works on Linux, macOS, Windows, Android, and iOS
- **Pure Rust**: No C/C++ dependencies required
- **20 Table Functions**: Comprehensive system monitoring
- **Unit Conversion**: Query memory/disk in bytes, KB, MB, GB, TB (both SI and binary)
- **Real-time Data**: Get live system metrics directly in SQL

### Available Functions

| Function                 | Description                         |
| ------------------------ | ----------------------------------- |
| `sazgar_system(unit)`    | Comprehensive system overview       |
| `sazgar_cpu()`           | CPU information                     |
| `sazgar_cpu_cores()`     | Per-core CPU usage                  |
| `sazgar_memory(unit)`    | RAM usage with unit conversion      |
| `sazgar_swap(unit)`      | Swap/virtual memory info            |
| `sazgar_os()`            | Operating system details            |
| `sazgar_disks(unit)`     | Disk usage information              |
| `sazgar_network(unit)`   | Network interface statistics        |
| `sazgar_ports(filter)`   | Open network ports and connections  |
| `sazgar_processes(unit)` | Running processes                   |
| `sazgar_services()`      | System services (systemd/launchctl) |
| `sazgar_docker()`        | Docker containers                   |
| `sazgar_load()`          | System load averages                |
| `sazgar_uptime()`        | Detailed uptime information         |
| `sazgar_users()`         | System users                        |
| `sazgar_environment()`   | Environment variables               |
| `sazgar_components()`    | Temperature sensors                 |
| `sazgar_gpu()`           | NVIDIA GPU info (optional feature)  |
| `sazgar_fds(pid)`        | File descriptor counts (Linux)      |
| `sazgar_version()`       | Extension version                   |

## Quick Start

```sql
-- Install and load (one-time)
INSTALL sazgar FROM community;
LOAD sazgar;

-- Get system overview (memory in MB by default)
SELECT * FROM sazgar_system();

-- Check memory usage in GB
SELECT * FROM sazgar_memory(unit := 'GB');

-- Find top memory-consuming processes (memory in MB by default)
SELECT pid, name, memory, cpu_percent
FROM sazgar_processes()
ORDER BY memory DESC
LIMIT 10;

-- Network traffic in GB
SELECT interface_name, rx, tx, unit
FROM sazgar_network(unit := 'GB')
WHERE rx > 0;
```

## Installation

### From DuckDB Community Extensions (Recommended)

Sazgar is available on the [DuckDB Community Extensions](https://duckdb.org/community_extensions/extensions/sazgar) repository:

```sql
-- Install the extension (one-time)
INSTALL sazgar FROM community;

-- Load the extension
LOAD sazgar;
```

That's it! The extension will be automatically downloaded and installed for your platform.

### From Source

If you want to build from source or contribute:

```bash
# Clone the repository
git clone --recurse-submodules https://github.com/Angelerator/Sazgar.git
cd Sazgar

# Configure and build
make configure
make release

# The extension will be at: build/release/sazgar.duckdb_extension
```

```sql
-- Load local build (requires -unsigned flag)
-- Start DuckDB with: duckdb -unsigned
LOAD '/path/to/sazgar.duckdb_extension';
```

---

## Functions Reference

### System Overview

#### `sazgar_system(unit := 'MB')`

Returns a comprehensive system overview in a single row.

**Parameters:**

- `unit` (optional): Unit for memory values. Default: `MB`. Options: `bytes`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`

```sql
-- Default (MB)
SELECT * FROM sazgar_system();

-- Memory in GB
SELECT * FROM sazgar_system(unit := 'GB');
```

**Sample Output:**

```
┌─────────┬────────────┬──────────────────────┬──────────────┬───────────┬─────────────────────┬──────────────────────────┬──────────────────────────────┬──────────────┬─────────────┬──────────────────┬──────────────────────┬────────────────┬───────────────┬─────────┐
│ os_name │ os_version │       hostname       │ architecture │ cpu_count │ physical_core_count │        cpu_brand         │ global_cpu_usage_percent     │ total_memory │ used_memory │ available_memory │ memory_usage_percent │ uptime_seconds │ process_count │  unit   │
│ varchar │  varchar   │       varchar        │   varchar    │  uint64   │       uint64        │         varchar          │            float             │    double    │   double    │      double      │        float         │     uint64     │    uint64     │ varchar │
├─────────┼────────────┼──────────────────────┼──────────────┼───────────┼─────────────────────┼──────────────────────────┼──────────────────────────────┼──────────────┼─────────────┼──────────────────┼──────────────────────┼────────────────┼───────────────┼─────────┤
│ Darwin  │ 14.6.1     │ MacBook-Pro.local    │ arm64        │         8 │                   8 │ Apple M1                 │                        42.5  │      8192.0  │      6456.0 │           1736.0 │                 78.8 │        4368630 │           466 │ MB      │
└─────────┴────────────┴──────────────────────┴──────────────┴───────────┴─────────────────────┴──────────────────────────┴──────────────────────────────┴──────────────┴─────────────┴──────────────────┴──────────────────────┴────────────────┴───────────────┴─────────┘
```

| Column                   | Type    | Description                      |
| ------------------------ | ------- | -------------------------------- |
| os_name                  | VARCHAR | Operating system name            |
| os_version               | VARCHAR | OS version                       |
| hostname                 | VARCHAR | System hostname                  |
| architecture             | VARCHAR | CPU architecture (x86_64, arm64) |
| cpu_count                | UBIGINT | Number of logical CPUs           |
| physical_core_count      | UBIGINT | Number of physical cores         |
| cpu_brand                | VARCHAR | CPU brand/model                  |
| global_cpu_usage_percent | FLOAT   | Overall CPU usage %              |
| total_memory             | DOUBLE  | Total RAM in specified unit      |
| used_memory              | DOUBLE  | Used RAM in specified unit       |
| available_memory         | DOUBLE  | Available RAM in specified unit  |
| memory_usage_percent     | FLOAT   | Memory usage %                   |
| uptime_seconds           | UBIGINT | System uptime in seconds         |
| process_count            | UBIGINT | Number of running processes      |
| unit                     | VARCHAR | Unit used for memory values      |

#### `sazgar_version()`

Returns the extension version.

```sql
SELECT * FROM sazgar_version();
```

**Sample Output:**

```
┌─────────┐
│ version │
│ varchar │
├─────────┤
│ 0.3.0   │
└─────────┘
```

---

### Operating System

#### `sazgar_os()`

Returns detailed operating system information.

```sql
SELECT * FROM sazgar_os();
```

**Sample Output:**

```
┌─────────┬────────────┬────────────────┬──────────────────────┬──────────────┬─────────────────┬────────────────┬────────────┬───────────────┐
│ os_name │ os_version │ kernel_version │       hostname       │ architecture │ distribution_id │ uptime_seconds │ boot_time  │ process_count │
│ varchar │  varchar   │    varchar     │       varchar        │   varchar    │     varchar     │     uint64     │   uint64   │    uint64     │
├─────────┼────────────┼────────────────┼──────────────────────┼──────────────┼─────────────────┼────────────────┼────────────┼───────────────┤
│ Darwin  │ 14.6.1     │ 23.6.0         │ MacBook-Pro.local    │ arm64        │ macos           │        4368629 │ 1761468497 │           451 │
└─────────┴────────────┴────────────────┴──────────────────────┴──────────────┴─────────────────┴────────────────┴────────────┴───────────────┘
```

| Column          | Type    | Description                      |
| --------------- | ------- | -------------------------------- |
| os_name         | VARCHAR | OS name (Darwin, Linux, Windows) |
| os_version      | VARCHAR | OS version string                |
| kernel_version  | VARCHAR | Kernel version                   |
| hostname        | VARCHAR | System hostname                  |
| architecture    | VARCHAR | CPU architecture                 |
| distribution_id | VARCHAR | Linux distribution ID            |
| uptime_seconds  | UBIGINT | System uptime                    |
| boot_time       | UBIGINT | Boot timestamp (Unix epoch)      |
| process_count   | UBIGINT | Number of processes              |

---

### Memory

#### `sazgar_memory(unit := 'MB')`

Returns memory and swap usage information.

**Parameters:**

- `unit` (optional): Unit for values. Default: `MB`. Options: `bytes`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`

```sql
-- Default (MB)
SELECT * FROM sazgar_memory();

-- In gigabytes (SI)
SELECT * FROM sazgar_memory(unit := 'GB');

-- In gibibytes (binary)
SELECT * FROM sazgar_memory(unit := 'GiB');
```

**Sample Output (GB):**

```
┌─────────┬──────────────┬─────────────┬─────────────┬──────────────────┬──────────────────────┬────────────┬───────────┬───────────┬────────────────────┐
│  unit   │ total_memory │ used_memory │ free_memory │ available_memory │ memory_usage_percent │ total_swap │ used_swap │ free_swap │ swap_usage_percent │
│ varchar │    double    │   double    │   double    │      double      │        float         │   double   │  double   │  double   │       float        │
├─────────┼──────────────┼─────────────┼─────────────┼──────────────────┼──────────────────────┼────────────┼───────────┼───────────┼────────────────────┤
│ GB      │ 8.589934592  │ 6.771245056 │ 0.117440512 │     1.818689536  │                 78.8 │ 8.589934592│ 7.787577344│0.802357248│              90.66 │
└─────────┴──────────────┴─────────────┴─────────────┴──────────────────┴──────────────────────┴────────────┴───────────┴───────────┴────────────────────┘
```

| Column               | Type    | Description           |
| -------------------- | ------- | --------------------- |
| unit                 | VARCHAR | Unit used for values  |
| total_memory         | DOUBLE  | Total physical memory |
| used_memory          | DOUBLE  | Used memory           |
| free_memory          | DOUBLE  | Free memory           |
| available_memory     | DOUBLE  | Available memory      |
| memory_usage_percent | FLOAT   | Memory usage %        |
| total_swap           | DOUBLE  | Total swap space      |
| used_swap            | DOUBLE  | Used swap             |
| free_swap            | DOUBLE  | Free swap             |
| swap_usage_percent   | FLOAT   | Swap usage %          |

---

### CPU

#### `sazgar_cpu()`

Returns per-core CPU information.

```sql
SELECT * FROM sazgar_cpu();
```

**Sample Output:**

```
┌─────────┬─────────┬───────────────┬───────────────┬──────────┬───────────┬───────────────┐
│ core_id │  name   │ usage_percent │ frequency_mhz │  brand   │ vendor_id │  byte_order   │
│ uint64  │ varchar │     float     │    uint64     │ varchar  │  varchar  │    varchar    │
├─────────┼─────────┼───────────────┼───────────────┼──────────┼───────────┼───────────────┤
│       0 │ 1       │         47.62 │          3204 │ Apple M1 │ Apple     │ Little Endian │
│       1 │ 2       │         40.00 │          3204 │ Apple M1 │ Apple     │ Little Endian │
│       2 │ 3       │         40.91 │          3204 │ Apple M1 │ Apple     │ Little Endian │
│       3 │ 4       │         30.00 │          3204 │ Apple M1 │ Apple     │ Little Endian │
│       4 │ 5       │         12.50 │          3204 │ Apple M1 │ Apple     │ Little Endian │
│       5 │ 6       │         10.00 │          3204 │ Apple M1 │ Apple     │ Little Endian │
│       6 │ 7       │          8.33 │          3204 │ Apple M1 │ Apple     │ Little Endian │
│       7 │ 8       │          5.00 │          3204 │ Apple M1 │ Apple     │ Little Endian │
└─────────┴─────────┴───────────────┴───────────────┴──────────┴───────────┴───────────────┘
```

| Column        | Type    | Description                           |
| ------------- | ------- | ------------------------------------- |
| core_id       | UBIGINT | Core index (0-based)                  |
| name          | VARCHAR | Core name/identifier                  |
| usage_percent | FLOAT   | Current CPU usage %                   |
| frequency_mhz | UBIGINT | Current frequency in MHz              |
| brand         | VARCHAR | CPU brand string                      |
| vendor_id     | VARCHAR | CPU vendor (Intel, AMD, Apple)        |
| byte_order    | VARCHAR | System byte order (Little/Big Endian) |

---

### Disk

#### `sazgar_disks(unit := 'GB')`

Returns disk/filesystem information. Automatically filters out virtual filesystems.

**Parameters:**

- `unit` (optional): Unit for space values. Default: `GB`. Options: `bytes`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`

```sql
-- Default (GB)
SELECT * FROM sazgar_disks();

-- In terabytes
SELECT * FROM sazgar_disks(unit := 'TB');

-- Human-readable disk usage
SELECT
    name,
    mount_point,
    round(used_space, 2) as used,
    round(total_space, 2) as total,
    round(usage_percent, 1) as pct,
    unit
FROM sazgar_disks();
```

**Sample Output (GB):**

```
┌──────────────┬──────────────────────┬─────────────┬──────┬─────────────┬─────────────────┬────────────┬───────────────┬──────────────┬─────────┐
│     name     │     mount_point      │ file_system │ unit │ total_space │ available_space │ used_space │ usage_percent │ is_removable │  kind   │
│   varchar    │       varchar        │   varchar   │varchar│   double    │     double      │   double   │     float     │   boolean    │ varchar │
├──────────────┼──────────────────────┼─────────────┼──────┼─────────────┼─────────────────┼────────────┼───────────────┼──────────────┼─────────┤
│ Macintosh HD │ /                    │ apfs        │ GB   │      228.27 │           15.42 │     212.85 │         93.24 │ false        │ SSD     │
│ Macintosh HD │ /System/Volumes/Data │ apfs        │ GB   │      228.27 │           15.42 │     212.85 │         93.24 │ false        │ SSD     │
└──────────────┴──────────────────────┴─────────────┴──────┴─────────────┴─────────────────┴────────────┴───────────────┴──────────────┴─────────┘
```

| Column          | Type    | Description                        |
| --------------- | ------- | ---------------------------------- |
| name            | VARCHAR | Disk/volume name                   |
| mount_point     | VARCHAR | Mount path                         |
| file_system     | VARCHAR | Filesystem type (ext4, apfs, ntfs) |
| unit            | VARCHAR | Unit used for values               |
| total_space     | DOUBLE  | Total space                        |
| available_space | DOUBLE  | Available space                    |
| used_space      | DOUBLE  | Used space                         |
| usage_percent   | FLOAT   | Usage %                            |
| is_removable    | BOOLEAN | Is removable media                 |
| kind            | VARCHAR | Disk type (SSD, HDD, Unknown)      |

---

### Network

#### `sazgar_network(unit := 'MB')`

Returns network interface information and statistics.

**Parameters:**

- `unit` (optional): Unit for rx/tx byte values. Default: `MB`. Options: `bytes`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`

```sql
-- Default (MB)
SELECT * FROM sazgar_network();

-- Find interfaces with traffic (in GB)
SELECT interface_name, rx, tx, unit
FROM sazgar_network(unit := 'GB')
WHERE rx > 0 OR tx > 0;
```

**Sample Output:**

```
┌────────────────┬───────────────────┬──────────┬──────────┬────────────┬────────────┬───────────┬───────────┬─────────┐
│ interface_name │    mac_address    │    rx    │    tx    │ rx_packets │ tx_packets │ rx_errors │ tx_errors │  unit   │
│    varchar     │      varchar      │  double  │  double  │   uint64   │   uint64   │  uint64   │  uint64   │ varchar │
├────────────────┼───────────────────┼──────────┼──────────┼────────────┼────────────┼───────────┼───────────┼─────────┤
│ lo0            │ 00:00:00:00:00:00 │   2499.7 │   2499.7 │   15234567 │   15234567 │         0 │         0 │ MB      │
│ en0            │ aa:bb:cc:dd:ee:ff │   1516.2 │    234.6 │    9876543 │    1234567 │         0 │         0 │ MB      │
│ utun4          │ 00:00:00:00:00:00 │      0.5 │      0.1 │       4321 │       1234 │         0 │         0 │ MB      │
│ awdl0          │ 11:22:33:44:55:66 │     22.3 │      5.7 │     123456 │      56789 │         0 │         0 │ MB      │
└────────────────┴───────────────────┴──────────┴──────────┴────────────┴────────────┴───────────┴───────────┴─────────┘
```

| Column         | Type    | Description                      |
| -------------- | ------- | -------------------------------- |
| interface_name | VARCHAR | Interface name (eth0, en0, etc.) |
| mac_address    | VARCHAR | MAC address                      |
| rx             | DOUBLE  | Total data received (in unit)    |
| tx             | DOUBLE  | Total data transmitted (in unit) |
| rx_packets     | UBIGINT | Total packets received           |
| tx_packets     | UBIGINT | Total packets transmitted        |
| rx_errors      | UBIGINT | Receive errors                   |
| tx_errors      | UBIGINT | Transmit errors                  |
| unit           | VARCHAR | Unit used for rx/tx values       |

---

### Processes

#### `sazgar_processes(unit := 'MB')`

Returns information about all running processes.

**Parameters:**

- `unit` (optional): Unit for memory values. Default: `MB`. Options: `bytes`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`

```sql
-- Default (MB)
SELECT * FROM sazgar_processes();

-- Top 10 CPU consumers
SELECT pid, name, cpu_percent, status
FROM sazgar_processes()
ORDER BY cpu_percent DESC
LIMIT 10;

-- Find processes using more than 100MB
SELECT pid, name, memory, unit
FROM sazgar_processes()
WHERE memory > 100
ORDER BY memory DESC;
```

**Sample Output (top 5 by memory):**

```
┌────────┬─────────────────────────┬─────────────┬──────────┬────────────────┬──────────┬─────────┐
│  pid   │          name           │ cpu_percent │  memory  │ memory_percent │  status  │  unit   │
│ uint32 │         varchar         │    float    │  double  │     float      │ varchar  │ varchar │
├────────┼─────────────────────────┼─────────────┼──────────┼────────────────┼──────────┼─────────┤
│  89588 │ Cursor Helper (Renderer)│         0.0 │    391.8 │           4.78 │ Running  │ MB      │
│  59471 │ Google Chrome           │         0.0 │    137.8 │           1.68 │ Running  │ MB      │
│  91463 │ Cursor Helper (Plugin)  │         0.0 │    123.0 │           1.50 │ Running  │ MB      │
│  89585 │ Cursor Helper (Renderer)│         0.0 │    102.7 │           1.25 │ Running  │ MB      │
│  89584 │ Cursor Helper (Renderer)│         0.0 │    102.1 │           1.25 │ Running  │ MB      │
└────────┴─────────────────────────┴─────────────┴──────────┴────────────────┴──────────┴─────────┘
```

| Column           | Type     | Description                      |
| ---------------- | -------- | -------------------------------- |
| pid              | UINTEGER | Process ID                       |
| name             | VARCHAR  | Process name                     |
| exe_path         | VARCHAR  | Executable path                  |
| status           | VARCHAR  | Status (Running, Sleeping, etc.) |
| cpu_percent      | FLOAT    | CPU usage %                      |
| memory           | DOUBLE   | Memory usage (in unit)           |
| memory_percent   | FLOAT    | Memory usage %                   |
| start_time       | UBIGINT  | Start timestamp (Unix epoch)     |
| run_time_seconds | UBIGINT  | Total run time in seconds        |
| user             | VARCHAR  | User ID running the process      |
| unit             | VARCHAR  | Unit used for memory values      |

---

### Load Average

#### `sazgar_load()`

Returns system load averages (Unix/macOS/Linux). Returns 0 on Windows.

```sql
SELECT * FROM sazgar_load();
```

**Sample Output:**

```
┌──────────────┬───────────────┬───────────────┐
│  load_1min   │   load_5min   │  load_15min   │
│    double    │    double     │    double     │
├──────────────┼───────────────┼───────────────┤
│ 3.6552734375 │ 3.99658203125 │ 5.27099609375 │
└──────────────┴───────────────┴───────────────┘
```

| Column     | Type   | Description            |
| ---------- | ------ | ---------------------- |
| load_1min  | DOUBLE | 1-minute load average  |
| load_5min  | DOUBLE | 5-minute load average  |
| load_15min | DOUBLE | 15-minute load average |

---

### Users

#### `sazgar_users()`

Returns system users.

```sql
SELECT * FROM sazgar_users();
```

**Sample Output:**

```
┌─────────┬─────────┬──────────────┐
│   uid   │   gid   │     name     │
│ varchar │ varchar │   varchar    │
├─────────┼─────────┼──────────────┤
│ 501     │ 20      │ john         │
│ 0       │ 0       │ root         │
│ 248     │ 248     │ _mbsetupuser │
└─────────┴─────────┴──────────────┘
```

| Column | Type    | Description |
| ------ | ------- | ----------- |
| uid    | VARCHAR | User ID     |
| gid    | VARCHAR | Group ID    |
| name   | VARCHAR | Username    |

---

### Temperature Sensors

#### `sazgar_components()`

Returns hardware temperature sensor readings.

```sql
SELECT * FROM sazgar_components();

-- Find hottest components
SELECT label, temperature_celsius
FROM sazgar_components()
ORDER BY temperature_celsius DESC
LIMIT 5;
```

**Sample Output:**

```
┌──────────────────────────┬─────────────────────┬─────────────────────────┬──────────────────────────────┐
│          label           │ temperature_celsius │ max_temperature_celsius │ critical_temperature_celsius │
│         varchar          │        float        │          float          │            float             │
├──────────────────────────┼─────────────────────┼─────────────────────────┼──────────────────────────────┤
│ pACC MTR Temp Sensor0    │              55.81  │                   65.0  │                          0.0 │
│ pACC MTR Temp Sensor3    │              53.27  │                   62.0  │                          0.0 │
│ pACC MTR Temp Sensor1    │              52.50  │                   60.0  │                          0.0 │
│ PMU tdie7                │              50.81  │                   55.0  │                          0.0 │
│ pACC MTR Temp Sensor2    │              50.59  │                   58.0  │                          0.0 │
└──────────────────────────┴─────────────────────┴─────────────────────────┴──────────────────────────────┘
```

| Column                       | Type    | Description         |
| ---------------------------- | ------- | ------------------- |
| label                        | VARCHAR | Sensor label        |
| temperature_celsius          | FLOAT   | Current temperature |
| max_temperature_celsius      | FLOAT   | Maximum recorded    |
| critical_temperature_celsius | FLOAT   | Critical threshold  |

---

### Environment Variables

#### `sazgar_environment(filter)`

Returns environment variables, optionally filtered by name pattern.

```sql
-- Get all environment variables
SELECT * FROM sazgar_environment('');

-- Filter by pattern
SELECT * FROM sazgar_environment('PATH');
```

**Sample Output:**

```
┌──────────┬────────────────────────────────────────────────────────────────────┐
│   name   │                               value                                │
│ varchar  │                              varchar                               │
├──────────┼────────────────────────────────────────────────────────────────────┤
│ PATH     │ /usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin                       │
│ INFOPATH │ /opt/homebrew/share/info:                                          │
└──────────┴────────────────────────────────────────────────────────────────────┘
```

| Column | Type    | Description    |
| ------ | ------- | -------------- |
| name   | VARCHAR | Variable name  |
| value  | VARCHAR | Variable value |

---

### System Uptime

#### `sazgar_uptime()`

Returns detailed system uptime information.

```sql
SELECT * FROM sazgar_uptime();
```

**Sample Output:**

```
┌────────────────┬───────────────────┬──────────────┬─────────────┬──────────────────┬─────────────────┐
│ uptime_seconds │  uptime_minutes   │ uptime_hours │ uptime_days │ uptime_formatted │ boot_time_epoch │
│     int64      │      double       │    double    │   double    │     varchar      │      int64      │
├────────────────┼───────────────────┼──────────────┼─────────────┼──────────────────┼─────────────────┤
│    4371412     │ 72856.86666666667 │   1214.28    │    50.59    │ 50d 14h 16m 52s  │   1761468497    │
└────────────────┴───────────────────┴──────────────┴─────────────┴──────────────────┴─────────────────┘
```

| Column           | Type    | Description                 |
| ---------------- | ------- | --------------------------- |
| uptime_seconds   | BIGINT  | Uptime in seconds           |
| uptime_minutes   | DOUBLE  | Uptime in minutes           |
| uptime_hours     | DOUBLE  | Uptime in hours             |
| uptime_days      | DOUBLE  | Uptime in days              |
| uptime_formatted | VARCHAR | Human-readable format       |
| boot_time_epoch  | BIGINT  | Boot timestamp (Unix epoch) |

---

### Swap Memory

#### `sazgar_swap(unit := 'GB')`

Returns swap/virtual memory information.

**Parameters:**

- `unit` (optional): Unit for values. Default: `GB`. Options: `bytes`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB`, `TiB`

```sql
-- Default (GB)
SELECT * FROM sazgar_swap();

-- Get swap in GiB (binary)
SELECT * FROM sazgar_swap(unit := 'GiB');
```

**Sample Output:**

```
┌────────────┬──────────────┬──────────────┬────────────────────┬─────────┐
│ total_swap │  used_swap   │  free_swap   │ swap_usage_percent │  unit   │
│   double   │    double    │    double    │       double       │ varchar │
├────────────┼──────────────┼──────────────┼────────────────────┼─────────┤
│    10.0    │ 9.0205078125 │ 0.9794921875 │    90.205078125    │ GiB     │
└────────────┴──────────────┴──────────────┴────────────────────┴─────────┘
```

| Column             | Type    | Description         |
| ------------------ | ------- | ------------------- |
| total_swap         | DOUBLE  | Total swap          |
| used_swap          | DOUBLE  | Used swap           |
| free_swap          | DOUBLE  | Free swap           |
| swap_usage_percent | DOUBLE  | Swap usage %        |
| unit               | VARCHAR | Unit of measurement |

---

### CPU Cores

#### `sazgar_cpu_cores()`

Returns per-core CPU information.

```sql
SELECT * FROM sazgar_cpu_cores();
```

**Sample Output:**

```
┌─────────┬───────────────┬───────────────┬─────────┬──────────┐
│ core_id │ usage_percent │ frequency_mhz │ vendor  │  brand   │
│  int32  │     float     │     int64     │ varchar │ varchar  │
├─────────┼───────────────┼───────────────┼─────────┼──────────┤
│       0 │          10.0 │          3204 │ Apple   │ Apple M1 │
│       1 │      9.523809 │          3204 │ Apple   │ Apple M1 │
│       2 │      9.523809 │          3204 │ Apple   │ Apple M1 │
│       3 │     23.809525 │          3204 │ Apple   │ Apple M1 │
│       4 │     14.285715 │          3204 │ Apple   │ Apple M1 │
└─────────┴───────────────┴───────────────┴─────────┴──────────┘
```

| Column        | Type    | Description       |
| ------------- | ------- | ----------------- |
| core_id       | INTEGER | Core index        |
| usage_percent | FLOAT   | CPU usage %       |
| frequency_mhz | BIGINT  | Current frequency |
| vendor        | VARCHAR | CPU vendor        |
| brand         | VARCHAR | CPU brand/model   |

---

### Network Ports

#### `sazgar_ports(protocol_filter)`

Returns open network ports and connections.

```sql
-- Get all ports
SELECT * FROM sazgar_ports('');

-- Filter by protocol (TCP or UDP)
SELECT * FROM sazgar_ports('TCP') WHERE local_port < 1024;
```

**Sample Output:**

```
┌──────────┬───────────────┬────────────┬────────────────┬─────────────┬───────────────┬───────┬──────────────────────┐
│ protocol │ local_address │ local_port │ remote_address │ remote_port │     state     │  pid  │     process_name     │
│ varchar  │    varchar    │   int32    │    varchar     │    int32    │    varchar    │ int32 │       varchar        │
├──────────┼───────────────┼────────────┼────────────────┼─────────────┼───────────────┼───────┼──────────────────────┤
│ TCP      │ 192.168.1.10  │      51379 │ 140.82.112.25  │         443 │ Established   │ 91463 │ Cursor Helper        │
│ TCP      │ 127.0.0.1     │       8831 │ 0.0.0.0        │           0 │ Listen        │ 91463 │ Cursor Helper        │
│ UDP      │ 0.0.0.0       │       5353 │                │           0 │               │  1234 │ mDNSResponder        │
└──────────┴───────────────┴────────────┴────────────────┴─────────────┴───────────────┴───────┴──────────────────────┘
```

| Column         | Type    | Description        |
| -------------- | ------- | ------------------ |
| protocol       | VARCHAR | TCP or UDP         |
| local_address  | VARCHAR | Local IP address   |
| local_port     | INTEGER | Local port number  |
| remote_address | VARCHAR | Remote IP address  |
| remote_port    | INTEGER | Remote port number |
| state          | VARCHAR | Connection state   |
| pid            | INTEGER | Process ID         |
| process_name   | VARCHAR | Process name       |

---

### GPU Information

#### `sazgar_gpu()`

Returns NVIDIA GPU information (requires nvidia feature and NVIDIA drivers).

```sql
SELECT * FROM sazgar_gpu();
```

**Sample Output (with NVIDIA GPU):**

```
┌───────┬──────────────────┬────────────────┬─────────────────┬────────────────┬────────────────┬────────────────────┬──────────────────┐
│ index │       name       │ driver_version │ memory_total_mb │ memory_used_mb │ memory_free_mb │ temperature_celsius│ utilization_%    │
│ int32 │     varchar      │    varchar     │     int64       │     int64      │     int64      │       int32        │      int32       │
├───────┼──────────────────┼────────────────┼─────────────────┼────────────────┼────────────────┼────────────────────┼──────────────────┤
│     0 │ NVIDIA RTX 4090  │ 545.29.06      │           24564 │          12345 │          12219 │                 65 │               45 │
└───────┴──────────────────┴────────────────┴─────────────────┴────────────────┴────────────────┴────────────────────┴──────────────────┘
```

| Column                     | Type    | Description           |
| -------------------------- | ------- | --------------------- |
| index                      | INTEGER | GPU index             |
| name                       | VARCHAR | GPU name              |
| driver_version             | VARCHAR | NVIDIA driver version |
| memory_total_mb            | BIGINT  | Total VRAM (MB)       |
| memory_used_mb             | BIGINT  | Used VRAM (MB)        |
| memory_free_mb             | BIGINT  | Free VRAM (MB)        |
| temperature_celsius        | INTEGER | GPU temperature       |
| power_usage_watts          | INTEGER | Power consumption     |
| utilization_gpu_percent    | INTEGER | GPU utilization %     |
| utilization_memory_percent | INTEGER | Memory utilization %  |

---

### Docker Containers

#### `sazgar_docker()`

Returns Docker container information (requires Docker to be running).

```sql
SELECT * FROM sazgar_docker();
```

**Sample Output:**

```
┌──────────────┬─────────────────┬──────────────────┬──────────────────────┬─────────┬─────────────────────────┐
│      id      │      name       │      image       │        status        │  state  │         created         │
│   varchar    │     varchar     │     varchar      │       varchar        │ varchar │         varchar         │
├──────────────┼─────────────────┼──────────────────┼──────────────────────┼─────────┼─────────────────────────┤
│ abc123def456 │ my-postgres     │ postgres:15      │ Up 3 days            │ running │ 2024-01-15 10:30:00     │
│ def456ghi789 │ my-redis        │ redis:7-alpine   │ Up 3 days            │ running │ 2024-01-15 10:30:00     │
└──────────────┴─────────────────┴──────────────────┴──────────────────────┴─────────┴─────────────────────────┘
```

| Column  | Type    | Description        |
| ------- | ------- | ------------------ |
| id      | VARCHAR | Container ID       |
| name    | VARCHAR | Container name     |
| image   | VARCHAR | Docker image       |
| status  | VARCHAR | Container status   |
| state   | VARCHAR | Container state    |
| created | VARCHAR | Creation timestamp |

---

### System Services

#### `sazgar_services()`

Returns running system services (macOS: launchctl, Linux: systemd).

```sql
SELECT * FROM sazgar_services() WHERE status = 'running' LIMIT 10;
```

**Sample Output:**

```
┌──────────────────────────────────┬─────────┬─────────────┐
│               name               │ status  │ description │
│             varchar              │ varchar │   varchar   │
├──────────────────────────────────┼─────────┼─────────────┤
│ com.apple.Finder                 │ running │             │
│ com.apple.homed                  │ running │             │
│ com.apple.bird                   │ running │             │
│ com.apple.nsurlsessiond          │ running │             │
└──────────────────────────────────┴─────────┴─────────────┘
```

| Column      | Type    | Description         |
| ----------- | ------- | ------------------- |
| name        | VARCHAR | Service name        |
| status      | VARCHAR | Service status      |
| description | VARCHAR | Service description |

---

### File Descriptors

#### `sazgar_fds(pid)`

Returns file descriptor counts per process (Linux only, returns 0 on other platforms).

```sql
-- Get FD counts for all processes
SELECT * FROM sazgar_fds(0);

-- Get FD count for specific PID
SELECT * FROM sazgar_fds(1234);
```

| Column       | Type    | Description     |
| ------------ | ------- | --------------- |
| pid          | INTEGER | Process ID      |
| process_name | VARCHAR | Process name    |
| fd_count     | INTEGER | Open file count |

---

## Use Cases

### System Health Dashboard

```sql
-- Create a system health view
SELECT
    os_name || ' ' || os_version as os,
    hostname,
    cpu_count || ' cores' as cpu,
    cpu_brand,
    round(global_cpu_usage_percent, 1) || '%' as cpu_usage,
    round(total_memory_bytes / 1e9, 1) || ' GB' as total_ram,
    round(memory_usage_percent, 1) || '%' as ram_usage,
    (uptime_seconds / 86400) || ' days' as uptime,
    process_count as processes
FROM sazgar_system();
```

### Disk Space Monitoring

```sql
-- Alert on disks over 80% full
SELECT name, mount_point,
       round(usage_percent, 1) as pct_used,
       round(available_space, 2) as available_gb
FROM sazgar_disks(unit := 'GB')
WHERE usage_percent > 80;
```

### Process Memory Analysis

```sql
-- Memory usage by process, grouped
SELECT
    name,
    count(*) as instances,
    round(sum(memory_bytes) / 1e9, 2) as total_gb
FROM sazgar_processes()
GROUP BY name
ORDER BY total_gb DESC
LIMIT 10;
```

### Network Traffic Summary

```sql
SELECT
    interface_name,
    round(rx_bytes / 1e9, 2) as rx_gb,
    round(tx_bytes / 1e9, 2) as tx_gb
FROM sazgar_network()
WHERE rx_bytes > 0
ORDER BY rx_bytes DESC;
```

---

## Building from Source

### Prerequisites

- Rust toolchain (1.70+)
- Python 3.8+
- Make
- Git

### Build Commands

```bash
# Configure (creates Python venv, downloads DuckDB)
make configure

# Debug build
make debug

# Release build (optimized)
make release

# Run tests
make test_release

# Clean build artifacts
make clean

# Full clean (including venv)
make clean_all
```

### Build Output

The extension is created at:

- Debug: `build/debug/sazgar.duckdb_extension`
- Release: `build/release/sazgar.duckdb_extension`

### Cross-Compilation for Mobile

For Android and iOS targets, you'll need the appropriate Rust targets installed:

```bash
# Android
rustup target add aarch64-linux-android
rustup target add armv7-linux-androideabi
rustup target add x86_64-linux-android

# iOS
rustup target add aarch64-apple-ios
rustup target add x86_64-apple-ios
```

---

## Platform Support

| Platform | Architecture          | Status             |
| -------- | --------------------- | ------------------ |
| Linux    | x86_64                | ✅ Full support    |
| Linux    | ARM64                 | ✅ Full support    |
| macOS    | x86_64 (Intel)        | ✅ Full support    |
| macOS    | ARM64 (Apple Silicon) | ✅ Full support    |
| Windows  | x86_64                | ✅ Full support    |
| Windows  | ARM64                 | ✅ Full support    |
| Android  | ARM64                 | ⚠️ Partial support |
| Android  | x86_64                | ⚠️ Partial support |
| iOS      | ARM64                 | ⚠️ Partial support |

### Platform-Specific Notes

| Feature             | Linux | macOS | Windows | Android | iOS |
| ------------------- | ----- | ----- | ------- | ------- | --- |
| CPU Info            | ✅    | ✅    | ✅      | ✅      | ⚠️  |
| Memory Info         | ✅    | ✅    | ✅      | ✅      | ⚠️  |
| Disk Info           | ✅    | ✅    | ✅      | ⚠️      | ⚠️  |
| Network Stats       | ✅    | ✅    | ✅      | ✅      | ⚠️  |
| Processes           | ✅    | ✅    | ✅      | ⚠️      | ❌  |
| Load Average        | ✅    | ✅    | ❌      | ✅      | ❌  |
| Temperature Sensors | ✅    | ⚠️    | ⚠️      | ⚠️      | ❌  |
| Users               | ✅    | ✅    | ✅      | ⚠️      | ⚠️  |

Legend: ✅ Full support | ⚠️ Partial/Limited | ❌ Not available

**Notes:**

- **Android**: Process listing requires root or special permissions. Some features limited by Android security model.
- **iOS**: Most process-related features unavailable due to iOS sandbox restrictions. Basic system info works.
- **Windows**: Load averages not available (Windows uses different metrics).
- **VMs/Containers**: Temperature sensors may not be exposed.

---

## Dependencies

- [sysinfo](https://crates.io/crates/sysinfo) - Cross-platform system information
- [duckdb-rs](https://crates.io/crates/duckdb) - DuckDB Rust bindings

---

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

### Development Setup

```bash
git clone --recurse-submodules https://github.com/Angelerator/Sazgar.git
cd sazgar
make configure
make debug
make test_debug
```

### Adding New Features

1. Add new `VTab` implementation in `src/lib.rs`
2. Register the function in `extension_entrypoint`
3. Add tests in `test/sql/sazgar.test`
4. Update documentation

---

## License

This project is licensed under the MIT License.

---

## Acknowledgments

- DuckDB team for the excellent extension system
- The Rust `sysinfo` crate maintainers
- Contributors and users of this extension
