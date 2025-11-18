# MinIO Pool Analyzer

A Python tool for analyzing and visualizing MinIO cluster diagnostic data. This script parses MinIO diagnostic JSON files and provides comprehensive insights into cluster health, disk status, pool configuration, and storage capacity.

## Features

- **Cluster Summary Statistics**: Overview of total disks, health status, capacity, and usage across the entire cluster
- **Pool Analysis**: Detailed breakdown of each storage pool including erasure sets, disk counts, and capacity metrics
- **Erasure Coding Detection**: Automatically detects and displays erasure coding configuration (EC parity level)
- **Color-Coded Output**: Visual indicators for disk health (green/yellow/red) and usage levels
- **Multiple Output Modes**:
  - Full detailed view with disk-by-disk information
  - Summary mode for quick overview
  - Scanning mode to identify disks currently being scanned
- **Flexible Input Format**: Supports both standard JSON and NDJSON (newline-delimited JSON) formats
- **Detailed Disk Information**: Shows disk paths, UUIDs, space usage, inode statistics, and metrics

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

- `--summary`: Display a summary view with disk counts per erasure set instead of detailed tables
- `--scanning`: Filter output to show only disks that are currently being scanned

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

### 3. Detailed Disk Information

When not in summary mode, displays a table with:
- Pool and Erasure Set identifiers
- Disk Index
- Server hostname
- Disk path
- State (ok/problematic)
- Scanning status
- UUID (truncated for readability)
- Space usage (GB and percentage)
- Inode usage (count and percentage)
- Local disk indicator
- Metrics (if available)

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

