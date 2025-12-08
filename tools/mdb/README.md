# mdb - MinIO Debug Tool

A command-line tool for analyzing MinIO cluster diagnostic JSON files. `mdb` provides comprehensive insights into cluster health, disk status, server information, erasure sets, and storage capacity.

## Features

- **Multi-Configuration Support**: Manage multiple JSON file configurations with easy switching
- **Cluster Summary**: Overview of total disks, health status, capacity, and usage
- **Server Information**: Detailed server metadata (version, edition, memory, uptime, state)
- **Erasure Set Analysis**: Statistics per erasure set including disk counts and capacity metrics
- **Disk Monitoring**: Individual disk status, space usage, and health metrics
- **Filtering Options**: Filter by failed disks, scanning disks, low space, and more
- **Interactive Pager**: Paginated output for large datasets
- **Color-Coded Output**: Visual indicators for health status and usage levels

## Installation

### Prerequisites

- Go 1.24 or later

### Build from Source

```bash
cd tools/mdb
go build -o mdb main.go
```

Or use the Makefile:

```bash
make
```

The binary will be created as `mdb` in the current directory.

### Using GoReleaser (Release Builds)

This project uses [GoReleaser](https://goreleaser.com) for automated cross-platform builds and releases.

**Prerequisites**:
```bash
go install github.com/goreleaser/goreleaser/v2@latest
```

**Available Makefile targets**:
```bash
# Test GoReleaser configuration
make release-test

# Build release artifacts (without publishing)
make release-build

# Create snapshot release (local, no git tag required)
make release-snapshot

# Validate configuration
make release-validate

# Create full release (requires git tag and GITHUB_TOKEN)
make release
```

**Creating a release**:
```bash
# 1. Create and push a git tag
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0

# 2. Set GITHUB_TOKEN and create release
export GITHUB_TOKEN=your_token_here
make release
```

GoReleaser will automatically:
- Build binaries for multiple platforms (Linux, macOS, Windows on amd64 and arm64)
- Create archives (tar.gz for Unix, zip for Windows)
- Generate checksums
- Create GitHub release with changelog

### Version Information

Version information is automatically embedded at build time:
- **Version**: Git tag or commit hash (with `-dirty` suffix if there are uncommitted changes)
- **Commit**: Short git commit hash
- **Build Date**: UTC timestamp of when the binary was built

**Check version**:
```bash
./mdb version
# or
./mdb --version
```

**Example output**:
```
mdb version 1.0.0
commit: 4bf643c
build date: 2025-12-08T18:52:24Z
go version: go1.24.11
```

## Quick Start

1. **Check version**:
   ```bash
   ./mdb version
   # or
   ./mdb --version
   ```

2. **Add a configuration**:
   ```bash
   ./mdb config add prod /path/to/diagnostic.json
   ```

3. **View cluster summary**:
   ```bash
   ./mdb show summary
   ```

4. **List all servers**:
   ```bash
   ./mdb show servers
   ```

## Configuration Management

`mdb` uses a configuration system similar to `kubectl config` or `mc config`, allowing you to manage multiple JSON file configurations.

### Add a Configuration

```bash
mdb config add <name> <file.json>
```

Adds a new configuration with a given name. The first configuration added becomes the current one.

**Example**:
```bash
mdb config add prod /Users/admin/minio-prod-diagnostics.json
mdb config add staging /Users/admin/minio-staging-diagnostics.json
```

### List Configurations

```bash
mdb config list
```

Lists all available configurations with their deployment IDs, file paths, and creation timestamps. The current configuration is marked with an asterisk (*).

**Example output**:
```
NAME                 DEPLOYMENT ID                          FILE                                                         CREATED
------------------------------------------------------------------------------------------------------------------------------------------------------
 prod                8ff9bc4a-206c-4ede-b5b2-043fa6ce7f0e   /Users/admin/minio-prod-diagnostics.json                    2025-12-08 12:14:32
*staging             83c8c99b-4414-4aaa-a62e-f35dd2783e46   /Users/admin/minio-staging-diagnostics.json                2025-12-08 12:18:01

* = current config
```

### Show Current Configuration

```bash
mdb config info
```

Displays information about the currently active configuration.

### Switch Configuration

```bash
mdb config switch <name>
```

Switches to a different configuration.

**Example**:
```bash
mdb config switch prod
```

### Remove Configuration

```bash
mdb config remove <name>
```

Removes a configuration. If the removed configuration was current, automatically switches to another available configuration.

## Viewing Cluster Information

### Show All (Default)

```bash
mdb show
```

Shows summary, servers, and erasure sets (equivalent to `mdb show summary` with servers and sets).

### Show Summary Only

```bash
mdb show summary
```

Displays cluster-wide summary including:
- Deployment ID
- Backend configuration (total sets, parity settings, drives per set)
- Total disks, scanning disks, healthy/problem disks
- Health percentage
- Raw and usable capacity
- Used and available space
- Number of pools, servers, and erasure sets
- Scanner status (buckets, objects, versions, deletemarkers, usage)

### Show Servers

```bash
mdb show servers
```

Displays all online servers with:
- Pool membership
- Server name
- State (online/offline)
- Edition and version
- Commit ID
- Memory usage
- ILM status
- Uptime

**Show only offline servers**:
```bash
mdb show servers --failed
```

### Show Erasure Sets

```bash
mdb show sets
```

Displays erasure set statistics including:
- Pool and erasure set indices
- Good/bad/scanning disk counts
- Average space used/free percentages
- Average inodes used percentage

**Filter options**:
- `--failed`: Show only erasure sets with failed disks
- `--scanning`: Show only erasure sets with scanning disks
- `--low-space <percentage>`: Filter by free space percentage
- `--min-bad-disks <number>`: Filter by minimum bad disks (requires `--failed`)

**Examples**:
```bash
# Show erasure sets with failed disks
mdb show sets --failed

# Show erasure sets with at least 2 bad disks
mdb show sets --failed --min-bad-disks 2

# Show erasure sets with low free space (< 10%)
mdb show sets --low-space 10
```

### Show Disks

```bash
mdb show disks
```

Displays individual disk information including:
- Pool, erasure set, and disk index
- Server and disk path
- State (ok/offline/faulty)
- Scanning status
- UUID
- Total, used, and free space
- Inodes used
- Local/remote status
- Metrics

**Filter options**:
- `--failed`: Show only failed/faulty disks
- `--scanning`: Show only scanning disks
- `--low-space <percentage>`: Filter by free space percentage

**Examples**:
```bash
# Show only failed disks
mdb show disks --failed

# Show only scanning disks
mdb show disks --scanning

# Show disks with low free space
mdb show disks --low-space 5
```

## Global Options

These options are available for all `show` subcommands:

### Pager

```bash
mdb show <command> --pager
```

Enables interactive pagination for long output. Use:
- `↑/↓` or `j/k`: Scroll line by line
- `Space`: Page down
- `g/G`: Go to top/bottom
- `q`: Quit

**Example**:
```bash
mdb show disks --pager
```

### Trim Domain

```bash
mdb show <command> --trim-domain ".example.com"
```

Trims domain suffix from endpoint names for cleaner display.

**Example**:
```bash
mdb show servers --trim-domain ".minio.local"
```

## Flag Validation

- `--failed` and `--scanning` cannot be used together
- `--low-space` can only be used with `show sets` or `show disks`
- `--min-bad-disks` can only be used with `show sets` and requires `--failed`

## Examples

### Basic Usage

```bash
# Add a configuration
mdb config add prod /path/to/diagnostics.json

# View cluster summary
mdb show summary

# View all servers
mdb show servers

# View erasure sets with failed disks
mdb show sets --failed

# View failed disks with pagination
mdb show disks --failed --pager
```

### Advanced Usage

```bash
# Switch between configurations
mdb config switch staging
mdb show summary

# Find erasure sets with multiple bad disks
mdb show sets --failed --min-bad-disks 2

# Find disks with low space
mdb show disks --low-space 10

# View offline servers only
mdb show servers --failed

# View with domain trimming
mdb show servers --trim-domain ".internal.company.com"
```

## Configuration Storage

Configurations are stored in:
- Config file: `~/.mdb/configs.json`
- Configs directory: `~/.mdb/configs/` (created automatically)

The config file contains:
- List of all configurations with metadata
- Current active configuration name

## Output Format

- **Color coding**:
  - Green: Healthy/OK status
  - Yellow: Warning/scanning status
  - Red: Error/failed/offline status
  - Blue: Index numbers

- **Tables**: Formatted with proper column alignment
- **Human-readable**: Sizes and durations are formatted (e.g., "10 days 4 hours", "256.5 TB")

## Troubleshooting

### No Configuration Set

If you see "no current config set", add a configuration:
```bash
mdb config add <name> <file.json>
```

### Configuration File Not Found

If a configured file no longer exists, you'll get an error. Update the configuration:
```bash
mdb config remove <name>
mdb config add <name> <new-file.json>
```

### Invalid JSON Format

`mdb` supports multiple JSON formats:
- Direct MinIO diagnostic format
- Wrapped format with `{"minio": {...}}`
- NDJSON (newline-delimited JSON) format

If parsing fails, verify your JSON file is a valid MinIO diagnostic output.

## License

This tool is part of the MinIO project ecosystem.

