# Comparison: mdb vs stats Utilities

## Overview
This document compares the current `mdb` utility (`/Users/vb140772/src/min-ops/tools/mdb`) with the `stats` utility (`/Users/vb140772/src/min-tools/stats`) for analyzing MinIO diagnostic JSON files.

## Test Data
Both utilities were tested with `/Users/vb140772/Downloads/info.json` containing data from a MinIO cluster with:
- 256 total disks
- 3 pools
- 26 erasure sets
- 8 servers
- EC-3 erasure coding configuration

---

## Feature Comparison

### 1. **Server Information Display**

#### stats (ADVANTAGE)
- **Shows detailed server metadata per pool:**
  - Server state (online/offline)
  - Edition (e.g., "aistor")
  - Version
  - Commit ID
  - Memory allocation stats
  - ILM (Information Lifecycle Management) expiry status
  - Server uptime (human-readable format)
- **Pool-aware server grouping:** Shows which servers belong to which pool

#### mdb
- Does NOT show server-level details (version, edition, commit ID, memory, uptime)
- Only shows server endpoint names in disk listings

---

### 2. **Metrics Display Format**

#### stats (ADVANTAGE)
- **Compact, structured metrics format:**
  ```
  [tokens=X, write=Y, del=Z, waiting=W, tout=T, err=E]
  ```
- **Conditional display:** Only shows metrics when non-zero values exist
- **Separates timeout and availability errors** when they differ
- **More readable:** Metrics are clearly labeled and compact

#### mdb
- Shows metrics as raw Go map format: `map[totalErrorsAvailability:1186 totalErrorsTimeout:1186 totalWaiting:1]`
- Less readable, requires parsing map structure
- Always displays full map even if empty

---

### 3. **Overall Cluster Statistics**

#### stats (ADVANTAGE)
- **Scanner status:** Shows buckets, objects, versions, deletemarkers, and usage
  ```
  scanner_status: buckets=68, objects=312792964, versions=0, deletemarkers=0, usage=984 TiB
  ```
- **Backend configuration details:**
  ```
  totalSets=[3 3 20], standardSCParity=3, rrSCParity=1, totalDriversPerSet=[16 16 8]
  ```
- Shows array of totalSets per pool (useful for multi-pool clusters)
- Shows both standard and reduced redundancy storage class parity settings

#### mdb
- Does NOT show scanner status (buckets, objects, versions, deletemarkers)
- Does NOT show backend configuration details
- Only detects single parity count from environment variables

---

### 4. **Output Format**

#### stats (ADVANTAGE)
- **Compact one-line-per-disk format:**
  ```
  MINIO-C01P02-01:/minio/disk1 = ok disk=15%[15 TiB], inode=2% [waiting=1, tout=1682, err=1686]
  ```
- Easier to scan and grep
- More concise for large clusters
- Shows all critical info in one line

#### mdb
- **Detailed table format** with many columns
- Better for detailed analysis but more verbose
- Takes more screen space
- Better for focused investigation

---

### 5. **Filtering and Query Capabilities**

#### mdb (ADVANTAGE)
- **Extensive filtering options:**
  - `--summary`: Summary view only
  - `--scanning`: Show only scanning disks
  - `--failed`: Show only failed/faulty disks
  - `--low-space=<percentage>`: Filter erasure sets by free space threshold
  - `--min-bad-disks=<number>`: Filter erasure sets with minimum bad disk count
  - `--pager`: Enable pagination for long output
- **Advanced filtering combinations:** Can combine filters for specific use cases
- **Color-coded health indicators:** Visual health status with color thresholds

#### stats
- **No filtering options:** Always shows all data
- No way to filter by scanning, failed disks, or low space
- Must manually parse/grep output for filtering

---

### 6. **Capacity and Health Calculations**

#### mdb (ADVANTAGE)
- **Calculates usable capacity** based on erasure coding:
  - Accounts for parity disks
  - Shows both raw and usable capacity
  - Calculates usage percentage based on usable capacity (more accurate)
- **Health percentages:**
  - Cluster-wide health percentage
  - Per-pool health percentage
  - Color-coded thresholds (green/yellow/red)
- **Pool summaries:**
  - Per-pool capacity breakdown
  - Per-pool health metrics
  - Per-pool usage statistics

#### stats
- Only shows raw capacity (total and used)
- Does NOT calculate usable capacity
- Does NOT show health percentages
- Shows drive status summary (count per state) but no health calculations

---

### 7. **User Interface**

#### mdb (ADVANTAGE)
- **Pagination support:** Can pause output for long listings
- **Color-coded output:** Green/yellow/red for health status
- **Formatted tables:** Structured table output
- **Interactive pager:** Press space to continue, 'q' to quit

#### stats
- No pagination
- No color coding
- Plain text output
- Better for scripting/grep processing

---

### 8. **Data Structure Handling**

#### stats (ADVANTAGE)
- **Uses official madmin-go library:**
  - Type-safe data structures
  - Handles MinIO API structures properly
  - Better compatibility with MinIO diagnostic formats
- **Handles domain trimming:** Optional domain suffix removal for cleaner output
- **Natural sorting:** Uses natural sort order for endpoints

#### mdb
- Custom data structures
- Manual JSON parsing
- More prone to format variations

---

### 9. **Domain/Endpoint Handling**

#### stats (ADVANTAGE)
- **Domain trimming option:** Can remove domain suffixes from endpoint names
  ```bash
  go run main.go cluster-info.json ".example.com"
  ```
- **Better endpoint normalization:** Handles various endpoint formats
- **Natural sorting:** Endpoints sorted naturally (not alphabetically)

#### mdb
- No domain trimming
- Basic endpoint extraction
- Standard sorting

---

### 10. **Code Architecture**

#### stats
- Uses official MinIO libraries (madmin-go)
- Simpler codebase
- Infrastructure for future TUI (commented out)

#### mdb
- More feature-rich implementation
- Custom flag parsing
- More complex but more flexible

---

## Summary of Advantages

### stats Utility Advantages:
1. ✅ **Server metadata** (version, edition, commit ID, memory, uptime, ILM status)
2. ✅ **Better metrics display** (compact, structured format)
3. ✅ **Scanner status** (buckets, objects, versions, deletemarkers)
4. ✅ **Backend configuration** (totalSets array, parity configs per pool)
5. ✅ **Compact output format** (one line per disk, easier to scan)
6. ✅ **Domain trimming** for cleaner endpoint names
7. ✅ **Official library usage** (madmin-go for type safety)
8. ✅ **Drive status summary** per pool

### mdb Utility Advantages:
1. ✅ **Extensive filtering** (scanning, failed, low-space, min-bad-disks)
2. ✅ **Usable capacity calculations** (accounts for erasure coding)
3. ✅ **Health percentage metrics** (cluster and per-pool)
4. ✅ **Color-coded output** (visual health indicators)
5. ✅ **Pagination support** (interactive pager)
6. ✅ **Detailed table format** (better for detailed analysis)
7. ✅ **Pool-level summaries** (capacity, health, usage per pool)

---

## Recommendations

### Use stats when:
- You need **server metadata** (version, memory, uptime)
- You need **scanner status** (buckets, objects, versions)
- You need **backend configuration details** (parity settings per pool)
- You prefer **compact, grep-friendly output**
- You want **better formatted metrics** display
- You're **scripting/automating** analysis

### Use mdb when:
- You need **filtering capabilities** (scanning disks, failed disks, low space)
- You need **health percentages** and capacity calculations
- You need **visual indicators** (color-coded health)
- You need **pagination** for long output
- You need **detailed per-pool analysis**
- You're doing **interactive investigation**

---

## Potential Improvements for mdb

Consider adding from stats:
1. **Server metadata display** (version, edition, commit ID, memory, uptime, ILM status)
2. **Scanner status** (buckets, objects, versions, deletemarkers, usage)
3. **Backend configuration details** (totalSets array, standardSCParity, rrSCParity, totalDriversPerSet)
4. **Better metrics formatting** (compact structured format like stats)
5. **Domain trimming option** for cleaner output
6. **Official madmin-go library** for better type safety and compatibility

## Potential Improvements for stats

Consider adding from mdb:
1. **Filtering options** (--scanning, --failed, --low-space, --min-bad-disks)
2. **Usable capacity calculations** (accounting for erasure coding)
3. **Health percentage metrics** (cluster and per-pool)
4. **Color-coded output** for visual health indicators
5. **Pagination support** for long output
6. **Per-pool summary statistics** (capacity, health, usage)

---

## Conclusion

Both utilities have their strengths:
- **stats** is better for **overview and server-level information** with compact output
- **mdb** is better for **filtering, analysis, and health monitoring** with interactive features

For a comprehensive MinIO cluster analysis tool, combining the best features of both would be ideal:
- Server metadata from stats
- Filtering and health calculations from mdb
- Scanner status from stats
- Capacity calculations from mdb

