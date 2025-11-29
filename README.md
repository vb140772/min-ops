# MinIO Pool Analyzer

A Python tool for analyzing and visualizing MinIO cluster diagnostic data. This script parses MinIO diagnostic JSON files and provides comprehensive insights into cluster health, disk status, pool configuration, and storage capacity.

## Features

- **Cluster Summary Statistics**: Overview of total disks, health status, capacity, and usage across the entire cluster
- **Pool Analysis**: Detailed breakdown of each storage pool including erasure sets, disk counts, and capacity metrics
- **Erasure Coding Detection**: Automatically detects and displays erasure coding configuration (EC parity level)
- **Color-Coded Output**: Visual indicators for disk health (green/yellow/red) and usage levels
- **Multiple Output Modes**:
  - Full detailed view with disk-by-disk information
  - Summary mode for quick overview with average statistics
  - Scanning mode to identify disks currently being scanned
  - Failed disk mode to show only problematic disks
  - Low-space mode to identify disks/erasure sets with low free space
  - Pager mode for paginated output
- **Flexible Input Format**: Supports both standard JSON and NDJSON (newline-delimited JSON) formats
- **Detailed Disk Information**: Shows disk paths, UUIDs, space usage (total, used, free), inode statistics, and metrics

## Requirements

- Python 3.x
- `tabulate` package

## Installation

Install the required dependency:

```bash
pip install tabulate
```

## Usage

### Basic Usage

```bash
python scripts/print_minio_pools.py <json_file>
```

### Command-Line Options

- `--summary`: Display a summary view with disk counts and average statistics per erasure set instead of detailed tables
- `--scanning`: Filter output to show only disks that are currently being scanned
- `--failed`: Show only failed/faulty disks (state != 'ok'). In regular mode, displays a consolidated table. In summary mode, shows only erasure sets containing failed disks
- `--low-space=<percentage>`: Filter erasure sets with average free space below the specified threshold (requires `--summary`). Results are sorted by utilization
- `--pager`: Enable paginated output - pauses after each screen and waits for spacebar to continue

### Examples

**Full detailed view:**
```bash
python scripts/print_minio_pools.py prod.json
```

**Summary mode:**
```bash
python scripts/print_minio_pools.py prod.json --summary
```

**Show only scanning disks:**
```bash
python scripts/print_minio_pools.py prod.json --scanning
```

**Show only failed/faulty disks:**
```bash
python scripts/print_minio_pools.py prod.json --failed
```

**Show erasure sets with low free space (summary mode):**
```bash
python scripts/print_minio_pools.py prod.json --summary --low-space=10
```

**Show failed disks with pagination:**
```bash
python scripts/print_minio_pools.py prod.json --failed --pager
```

**Combine multiple options:**
```bash
python scripts/print_minio_pools.py prod.json --summary --failed --low-space=5 --pager
```

## Output Description

The script provides three main sections of output:

### 1. Cluster Summary

Displays overall cluster statistics including:
- Deployment ID
- Total disk counts (healthy, problematic, scanning)
- Cluster health percentage
- Raw and usable capacity (accounting for erasure coding overhead)
- Space usage and availability
- Number of pools, servers, and erasure sets

### 2. Pool Summary

For each storage pool, shows:
- Number of erasure sets
- Disk counts (total, healthy, problematic, scanning)
- Pool health percentage
- Raw and usable capacity
- Usage percentage and available space

### 3. Erasure Set Summary (with --summary)

When using `--summary` mode, each erasure set displays:
- Good, bad, and scanning disk counts
- Average Space Used (percentage, color-coded)
- Average Free Space (percentage, color-coded)
- Average Inodes Used (percentage, color-coded)

When combined with `--low-space`, only erasure sets below the threshold are shown, sorted by utilization.

### 3. Detailed Disk Information

When not in summary mode, displays a table with:
- Pool and Erasure Set identifiers
- Disk Index
- Server hostname
- Disk path
- State (ok/problematic)
- Scanning status
- UUID (truncated for readability)
- Total Space (GB)
- Space Used (GB and percentage)
- Free Space (GB and percentage)
- Inode usage (count and percentage)
- Local disk indicator
- Metrics (if available)

**Note**: When using `--failed` option in regular mode, all failed disks are displayed in a single consolidated table without per-erasure-set headers for cleaner output.

## Color Coding

The output uses ANSI color codes for quick visual assessment:

- **Green**: Healthy status, good health percentages, low usage
- **Yellow**: Warning conditions (scanning disks, moderate usage)
- **Red**: Problematic status, poor health, high usage

## Input Format

The script expects MinIO diagnostic JSON files containing:
- Server information with drive details
- Pool and erasure set configuration
- Disk state, capacity, and usage information
- Optional erasure coding configuration in environment variables

The script can handle:
- Standard JSON format with `minio.info.servers` or `info.servers` structure
- NDJSON (newline-delimited JSON) format where each line is a separate JSON object

## Example Output

```
Detected Erasure Coding Configuration: EC:2

MinIO Cluster Summary
==================================================
Deployment ID: abc123-def456-ghi789
Total Disks: 24
Scanning Disks: 2
Healthy Disks: 22
Problem Disks: 2
Health: 91.7%
Raw Capacity: 100.0 TB
Usable Capacity: 66.7 TB
Used Space: 50.0 TB (75.0%)
Available Space: 16.7 TB
Pools: 1
Servers: 3
Erasure Sets: 2
==================================================
```

## Notes

- The script automatically calculates usable capacity based on detected erasure coding parity level
- Disk paths are extracted from endpoint URLs if not directly provided in the JSON
- Health percentages are color-coded: ≥90% (green), ≥75% (yellow), <75% (red)
- Usage percentages are color-coded: <80% (green), <95% (yellow), ≥95% (red)
- Free space percentages are color-coded: >20% (green), >5% (yellow), ≤5% (red)
- `--low-space` option requires `--summary` mode
- `--failed` option works in both regular and summary modes:
  - Regular mode: Shows a single consolidated table of all failed disks
  - Summary mode: Shows only erasure sets that contain failed disks
- `--pager` mode pauses output after each screen - press SPACE to continue, 'q' to quit
- All options can be combined for flexible filtering and display

