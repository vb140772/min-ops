import json
import sys
import os
import termios
import tty
from tabulate import tabulate

# ANSI color codes for highlighting
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
BOLD = '\033[1m'
RESET = '\033[0m'

# Paging class for output pagination
class Pager:
    def __init__(self, enabled=False):
        self.enabled = enabled
        self.lines_printed = 0
        self.terminal_height = self._get_terminal_height()
        # Reserve 2 lines at bottom for prompt
        self.lines_per_page = max(1, self.terminal_height - 2) if enabled else 999999
    
    def _get_terminal_height(self):
        """Get terminal height, default to 24 if unable to determine"""
        try:
            # Try to get terminal size (works on most systems)
            if hasattr(os, 'get_terminal_size'):
                return os.get_terminal_size().lines
            # Fallback for older Python versions (Unix-like systems)
            try:
                import struct
                import fcntl
                h, w = struct.unpack('hh', fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, '1234'))
                return h
            except (ImportError, OSError, IOError):
                pass
        except (OSError, IOError):
            pass
        return 24
    
    def _wait_for_space(self):
        """Wait for spacebar input"""
        if not self.enabled:
            return
        try:
            # Save terminal settings
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                # Wait for spacebar (ASCII 32) or 'q' to quit
                while True:
                    ch = sys.stdin.read(1)
                    if ch == ' ':
                        break
                    elif ch == 'q':
                        sys.exit(0)
                    elif ch == '\x03':  # Ctrl-C
                        sys.exit(1)
            finally:
                # Restore terminal settings
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        except (termios.error, OSError):
            # If we can't control terminal, just continue
            pass
    
    def print(self, text='', end='\n'):
        """Print with paging support"""
        if not self.enabled:
            print(text, end=end)
            return
        
        # Count lines in the text (handle multi-line strings)
        lines = text.split('\n')
        for i, line in enumerate(lines):
            print(line, end='\n' if i < len(lines) - 1 or end == '\n' else end)
            self.lines_printed += 1
            
            # Check if we need to pause
            if self.lines_printed >= self.lines_per_page:
                print(f"\n{YELLOW}-- Press SPACE to continue, 'q' to quit --{RESET}", end='', flush=True)
                self._wait_for_space()
                print('\r' + ' ' * 50 + '\r', end='')  # Clear the prompt line
                self.lines_printed = 0
    
    def reset(self):
        """Reset line counter"""
        self.lines_printed = 0

# Check for summary mode and get JSON file name
if len(sys.argv) < 2:
    print("Usage: python print_minio_pools.py <json_file> [--summary] [--scanning] [--low-space=<percentage>] [--pager] [--failed]")
    print("Example: python print_minio_pools.py prod.json --summary")
    print("Example: python print_minio_pools.py prod.json --scanning")
    print("Example: python print_minio_pools.py prod.json --summary --low-space=10")
    print("Example: python print_minio_pools.py prod.json --pager")
    print("Example: python print_minio_pools.py prod.json --failed")
    print("Example: python print_minio_pools.py prod.json --summary --failed")
    print("Note: --low-space requires --summary mode")
    print("Note: --pager pauses output after each screen (press space to continue)")
    print("Note: --failed shows only failed/faulty disks (not 'ok' state)")
    print("Note: This script requires the 'tabulate' package. Install with: pip install tabulate")
    sys.exit(1)

json_file = sys.argv[1]
summary_mode = '--summary' in sys.argv
scanning_mode = '--scanning' in sys.argv
pager_mode = '--pager' in sys.argv
failed_mode = '--failed' in sys.argv

# Create pager instance
pager = Pager(enabled=pager_mode)

# Parse --low-space option
low_space_threshold = None
for arg in sys.argv:
    if arg.startswith('--low-space='):
        try:
            low_space_threshold = float(arg.split('=')[1])
        except (ValueError, IndexError):
            print(f"{RED}Error: Invalid --low-space value. Must be a number.{RESET}")
            sys.exit(1)

# Validate that --low-space is only used with --summary
if low_space_threshold is not None and not summary_mode:
    print(f"{RED}Error: --low-space option requires --summary mode.{RESET}")
    sys.exit(1)

# Load the JSON data
try:
    with open(json_file, 'r') as f:
        # Try loading as single JSON first
        try:
            data = json.load(f)
            # Check if this JSON has the expected structure
            # If it doesn't have 'info' or 'minio', it might be NDJSON format
            if 'info' not in data and 'minio' not in data:
                # This might be NDJSON - try reading line by line
                f.seek(0)
                lines = f.readlines()
                for line in lines:
                    try:
                        line_data = json.loads(line)
                        # Look for MinIO diagnostic data
                        if 'minio' in line_data:
                            data = line_data
                            break
                        elif 'info' in line_data:
                            data = line_data
                            break
                    except json.JSONDecodeError:
                        continue
        except json.JSONDecodeError:
            # If single JSON load fails, try NDJSON format (newline-delimited JSON)
            f.seek(0)
            lines = f.readlines()
            data = None
            for line in lines:
                try:
                    line_data = json.loads(line)
                    # Look for MinIO diagnostic data
                    if 'minio' in line_data:
                        data = line_data
                        break
                    elif 'info' in line_data:
                        data = line_data
                        break
                except json.JSONDecodeError:
                    continue
            
            if data is None:
                raise json.JSONDecodeError("No valid JSON found", json_file, 0)
except FileNotFoundError:
    print(f"{RED}Error: File '{json_file}' not found{RESET}")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"{RED}Error: File '{json_file}' is not valid JSON{RESET}")
    sys.exit(1)

# Get the servers info - handle both diagnostic format and standard format
if 'minio' in data and 'info' in data['minio']:
    # Diagnostic format: minio.info.servers
    servers = data['minio']['info']['servers']
    info_path = data['minio']['info']
elif 'info' in data:
    # Standard format: info.servers
    servers = data['info']['servers']
    info_path = data['info']
else:
    print(f"{RED}Error: Could not find servers in JSON structure{RESET}")
    print(f"Available top-level keys: {list(data.keys())}")
    sys.exit(1)

# Get the pools - either from top-level or build from drives
if 'pools' in info_path:
    pools = info_path['pools']
else:
    # Build pools structure from drive information
    pools = {}
    for server in servers:
        for drive in server.get('drives', []):
            pool_index = str(drive.get('pool_index', 0))
            set_index = str(drive.get('set_index', 0))
            
            if pool_index not in pools:
                pools[pool_index] = {}
            if set_index not in pools[pool_index]:
                pools[pool_index][set_index] = {}

# Extract erasure coding configuration from MinIO environment variables
def get_erasure_coding_config(servers):
    """Extract EC configuration from server environment variables"""
    for server in servers:
        if 'minio_env_vars' in server and 'MINIO_STORAGE_CLASS_STANDARD' in server['minio_env_vars']:
            ec_config = server['minio_env_vars']['MINIO_STORAGE_CLASS_STANDARD']
            # Parse EC:3 format to get parity count
            if ec_config.startswith('EC:'):
                try:
                    return int(ec_config.split(':')[1])
                except (ValueError, IndexError):
                    pass
    # Default to EC-2 if not found
    return 2

# Get the erasure coding configuration
parity_disks = get_erasure_coding_config(servers)
pager.print(f"Detected Erasure Coding Configuration: EC:{parity_disks}")
pager.print()

# Build a mapping of (pool_index, set_index) -> list of drives
pool_set_drives = {}

# Collect all drives for summary statistics
all_drives = []
total_disks = 0
scanning_disks = 0
ok_disks = 0
bad_disks = 0
total_space = 0
used_space = 0

for server in servers:
    for drive in server.get('drives', []):
        total_disks += 1
        
        # Count scanning disks
        if drive.get('scanning', False):
            scanning_disks += 1
            
        # Count disk states
        if drive.get('state', 'offline') == 'ok':
            ok_disks += 1
        else:
            bad_disks += 1
            
        # Accumulate space statistics
        total_space += drive.get('totalspace', 0)
        used_space += drive.get('usedspace', 0)
        
        # If scanning mode is enabled, only include scanning disks
        if scanning_mode and not drive.get('scanning', False):
            continue
        
        # If failed mode is enabled, only include non-ok disks
        if failed_mode and drive.get('state', 'unknown') == 'ok':
            continue
        
        # Extract path from endpoint if path is not provided
        # endpoint formats:
        #   - "https://server.example.com:9000/data1/minio"
        #   - "https://server.example.com:21000/hadoop/data1/minio"
        drive_path = drive.get('path', '')
        if not drive_path and 'endpoint' in drive:
            # Extract path from endpoint URL
            endpoint = drive['endpoint']
            if '/' in endpoint:
                # Handle /hadoop/ prefix (diagnostic format)
                if '/hadoop/' in endpoint:
                    drive_path = endpoint.split('/hadoop/')[1]
                else:
                    # Get everything after the host:port (standard format)
                    parts = endpoint.split('/', 3)
                    if len(parts) > 3:
                        drive_path = '/' + parts[3]
        
        # Extract disk status information
        disk_info = {
            'server': server.get('endpoint', 'unknown'),
            'path': drive_path,
            'state': drive.get('state', 'unknown'),
            'uuid': drive.get('uuid', 'N/A'),
            'status': drive.get('state', 'unknown'),  # Disk status is the same as 'state' in this JSON
            'scanning': drive.get('scanning', False),
            'disk_index': drive.get('disk_index', 'N/A'),
            'total_space': drive.get('totalspace', 0),
            'used_space': drive.get('usedspace', 0),
            'available_space': drive.get('availspace', 0),
            'used_inodes': drive.get('used_inodes', 0),
            'free_inodes': drive.get('free_inodes', 0),
            'local': drive.get('local', False),
            'pool_index': drive.get('pool_index', 0),
            'set_index': drive.get('set_index', 0)
        }
        
        # Add metrics if available
        if 'metrics' in drive and drive['metrics']:
            disk_info['metrics'] = drive['metrics']
        
        # Calculate free space percentage
        drive_total_space = disk_info['total_space']
        drive_available_space = disk_info['available_space']
        free_space_pct = (drive_available_space / drive_total_space * 100) if drive_total_space > 0 else 0
        disk_info['free_space_pct'] = free_space_pct
        disk_info['used_space_pct'] = (disk_info['used_space'] / drive_total_space * 100) if drive_total_space > 0 else 0
            
        pool_index = disk_info['pool_index']
        set_index = disk_info['set_index']
        key = (pool_index, set_index)
        if key not in pool_set_drives:
            pool_set_drives[key] = []
        
        pool_set_drives[key].append(disk_info)
        all_drives.append(disk_info)

# Print summary statistics
pager.print(f"{BOLD}MinIO Cluster Summary{RESET}")
pager.print("=" * 50)

# Display deployment ID if available
deployment_id = info_path.get('deploymentID', None)
if deployment_id:
    pager.print(f"Deployment ID: {deployment_id}")
else:
    pager.print("Deployment ID: Not available")
pager.print()

# Basic disk counts
pager.print(f"Total Disks: {total_disks}")
pager.print(f"Scanning Disks: {YELLOW}{scanning_disks}{RESET}")
pager.print(f"Healthy Disks: {GREEN}{ok_disks}{RESET}")
pager.print(f"Problem Disks: {RED}{bad_disks}{RESET}")

# Calculate health percentage
if total_disks > 0:
    health_pct = (ok_disks / total_disks) * 100
    health_color = GREEN if health_pct >= 90 else YELLOW if health_pct >= 75 else RED
    pager.print(f"Health: {health_color}{health_pct:.1f}%{RESET}")

# Space statistics
if total_space > 0:
    total_tb = total_space / (1024**4)  # Convert to TB
    used_tb = used_space / (1024**4)
    
    # Calculate usable capacity across all pools
    total_usable_space = 0
    for pool_idx, sets in pools.items():
        for set_idx, set_info in sets.items():
            drives = pool_set_drives.get((int(pool_idx), int(set_idx)), [])
            total_disks_in_set = len(drives)
            if total_disks_in_set > 0:
                # Calculate data disks based on detected parity configuration
                if total_disks_in_set >= parity_disks:
                    data_disks = total_disks_in_set - parity_disks
                    usable_ratio = data_disks / total_disks_in_set
                else:
                    # Not enough disks for the configured parity level
                    usable_ratio = 0
                
                # Add usable space from this set
                for drive in drives:
                    total_usable_space += drive.get('total_space', 0) * usable_ratio
    
    usable_tb = total_usable_space / (1024**4) if total_usable_space > 0 else 0
    usage_pct = (used_space / total_usable_space * 100) if total_usable_space > 0 else 0
    usage_color = GREEN if usage_pct < 80 else YELLOW if usage_pct < 95 else RED
    
    pager.print(f"Raw Capacity: {total_tb:.1f} TB")
    pager.print(f"Usable Capacity: {usable_tb:.1f} TB")
    pager.print(f"Used Space: {used_tb:.1f} TB ({usage_color}{usage_pct:.1f}%{RESET})")
    pager.print(f"Available Space: {usable_tb - used_tb:.1f} TB")

# Pool and server statistics
pager.print(f"Pools: {len(pools)}")
pager.print(f"Servers: {len(servers)}")

# Count erasure sets
total_erasure_sets = sum(len(sets) for sets in pools.values())
pager.print(f"Erasure Sets: {total_erasure_sets}")

pager.print("=" * 50)

# Calculate and display pool-specific summaries
pager.print(f"{BOLD}Pool Summary{RESET}")
pager.print("-" * 50)

for pool_idx, sets in pools.items():
    # Calculate pool statistics
    pool_total_disks = 0
    pool_ok_disks = 0
    pool_bad_disks = 0
    pool_scanning_disks = 0
    pool_total_space = 0
    pool_used_space = 0
    pool_usable_space = 0
    
    # Count erasure sets in this pool
    pool_erasure_sets = len(sets)
    
    # Calculate statistics for all drives in this pool
    for set_idx, set_info in sets.items():
        drives = pool_set_drives.get((int(pool_idx), int(set_idx)), [])
        
        # Calculate erasure coding ratio for this set
        total_disks_in_set = len(drives)
        if total_disks_in_set > 0:
            # Calculate data disks based on detected parity configuration
            if total_disks_in_set >= parity_disks:
                data_disks = total_disks_in_set - parity_disks
                usable_ratio = data_disks / total_disks_in_set
            else:
                # Not enough disks for the configured parity level
                usable_ratio = 0
        else:
            usable_ratio = 0
        
        for drive in drives:
            pool_total_disks += 1
            
            # Count disk states
            if drive['state'] == 'ok':
                pool_ok_disks += 1
            else:
                pool_bad_disks += 1
                
            # Count scanning disks
            if drive.get('scanning', False):
                pool_scanning_disks += 1
                
            # Accumulate space statistics
            drive_total_space = drive.get('total_space', 0)
            drive_used_space = drive.get('used_space', 0)
            
            pool_total_space += drive_total_space
            pool_used_space += drive_used_space
            
            # Add to usable space (only the data portion)
            pool_usable_space += drive_total_space * usable_ratio
    
    # Calculate health percentage for this pool
    pool_health_pct = (pool_ok_disks / pool_total_disks * 100) if pool_total_disks > 0 else 0
    pool_health_color = GREEN if pool_health_pct >= 90 else YELLOW if pool_health_pct >= 75 else RED
    
    # Calculate usage percentage for this pool (based on usable capacity)
    pool_usage_pct = (pool_used_space / pool_usable_space * 100) if pool_usable_space > 0 else 0
    pool_usage_color = GREEN if pool_usage_pct < 80 else YELLOW if pool_usage_pct < 95 else RED
    
    # Convert space to TB
    pool_total_tb = pool_total_space / (1024**4) if pool_total_space > 0 else 0
    pool_usable_tb = pool_usable_space / (1024**4) if pool_usable_space > 0 else 0
    pool_used_tb = pool_used_space / (1024**4) if pool_used_space > 0 else 0
    
    # Display pool summary
    pager.print(f"Pool {pool_idx}:")
    pager.print(f"  Erasure Sets: {pool_erasure_sets}")
    pager.print(f"  Disks: {pool_total_disks} total ({GREEN}{pool_ok_disks} ok{RESET}, {RED}{pool_bad_disks} bad{RESET}, {YELLOW}{pool_scanning_disks} scanning{RESET})")
    pager.print(f"  Health: {pool_health_color}{pool_health_pct:.1f}%{RESET}")
    pager.print(f"  Raw Capacity: {pool_total_tb:.1f} TB")
    pager.print(f"  Usable Capacity: {pool_usable_tb:.1f} TB")
    pager.print(f"  Usage: {pool_used_tb:.1f} TB ({pool_usage_color}{pool_usage_pct:.1f}%{RESET})")
    pager.print(f"  Available: {pool_usable_tb - pool_used_tb:.1f} TB")
    pager.print()

pager.print("=" * 50)
pager.print()

# Print the pools, sets, and drives
if scanning_mode:
    pager.print(f"{BOLD}MinIO Scanning Disks from: {json_file}{RESET}")
elif failed_mode:
    pager.print(f"{BOLD}MinIO Failed/Faulty Disks from: {json_file}{RESET}")
else:
    pager.print(f"{BOLD}MinIO Pool Information from: {json_file}{RESET}")
pager.print("=" * 80)

# Check if any scanning disks were found when in scanning mode
if scanning_mode and not pool_set_drives:
    pager.print(f"{YELLOW}No scanning disks found in the provided data.{RESET}")
    sys.exit(0)

# Check if any failed disks were found when in failed mode
if failed_mode and not pool_set_drives:
    pager.print(f"{YELLOW}No failed/faulty disks found in the provided data.{RESET}")
    sys.exit(0)

# If low-space mode with summary, collect and filter erasure sets
if summary_mode and low_space_threshold is not None:
    # Collect erasure sets with their metrics
    erasure_sets = []
    for pool_idx, sets in pools.items():
        for set_idx, set_info in sets.items():
            drives = pool_set_drives.get((int(pool_idx), int(set_idx)), [])
            if not drives:
                continue
            
            # Calculate averages for this erasure set
            total_drives = len(drives)
            avg_total_space = sum(d.get('total_space', 0) for d in drives) / total_drives
            avg_used_space = sum(d.get('used_space', 0) for d in drives) / total_drives
            avg_free_space = sum(d.get('available_space', 0) for d in drives) / total_drives
            
            # Calculate percentages
            if avg_total_space > 0:
                avg_space_used_pct = (avg_used_space / avg_total_space) * 100
                avg_free_space_pct = (avg_free_space / avg_total_space) * 100
            else:
                avg_space_used_pct = 0
                avg_free_space_pct = 0
            
            # Only include erasure sets with free space below threshold
            if avg_free_space_pct < low_space_threshold:
                erasure_sets.append({
                    'pool_idx': pool_idx,
                    'set_idx': set_idx,
                    'drives': drives,
                    'avg_space_used_pct': avg_space_used_pct,
                    'avg_free_space_pct': avg_free_space_pct,
                    'good': sum(1 for d in drives if d['state'] == 'ok'),
                    'bad': sum(1 for d in drives if d['state'] != 'ok'),
                    'scanning': sum(1 for d in drives if d.get('scanning', False))
                })
    
    # Sort by utilization (used space percentage) descending
    erasure_sets.sort(key=lambda x: x['avg_space_used_pct'], reverse=True)
    
    if not erasure_sets:
        pager.print(f"{YELLOW}No erasure sets found with average free space less than {low_space_threshold}%.{RESET}")
        sys.exit(0)
    
    # Display filtered erasure sets
    pager.print(f"{BOLD}Erasure Sets with Average Free Space < {low_space_threshold}% (sorted by utilization){RESET}")
    pager.print("=" * 80)
    
    for es in erasure_sets:
        good_text = f"{GREEN}{es['good']}{RESET}" if es['good'] > 0 else "0"
        bad_text = f"{RED}{es['bad']}{RESET}" if es['bad'] > 0 else "0"
        scanning_text = f"{YELLOW}{es['scanning']}{RESET}" if es['scanning'] > 0 else "0"
        
        # Color code the percentages
        space_used_color = GREEN if es['avg_space_used_pct'] < 80 else YELLOW if es['avg_space_used_pct'] < 95 else RED
        free_space_color = GREEN if es['avg_free_space_pct'] > 20 else YELLOW if es['avg_free_space_pct'] > 5 else RED
        
        # Calculate average inodes for display
        drives = es['drives']
        total_drives = len(drives)
        if total_drives > 0:
            avg_used_inodes = sum(d.get('used_inodes', 0) for d in drives) / total_drives
            avg_free_inodes = sum(d.get('free_inodes', 0) for d in drives) / total_drives
            avg_total_inodes = avg_used_inodes + avg_free_inodes
            if avg_total_inodes > 0:
                avg_inodes_used_pct = (avg_used_inodes / avg_total_inodes) * 100
            else:
                avg_inodes_used_pct = 0
            inodes_used_color = GREEN if avg_inodes_used_pct < 80 else YELLOW if avg_inodes_used_pct < 95 else RED
            avg_inodes_used = f"{inodes_used_color}{avg_inodes_used_pct:.1f}%{RESET}"
        else:
            avg_inodes_used = "N/A"
        
        avg_space_used = f"{space_used_color}{es['avg_space_used_pct']:.1f}%{RESET}"
        avg_free_space_str = f"{free_space_color}{es['avg_free_space_pct']:.1f}%{RESET}"
        
        pager.print(f"  Pool {es['pool_idx']}, Erasure Set {es['set_idx']}: Good disks: {good_text}, Bad disks: {bad_text}, Scanning: {scanning_text}, Avg Space Used: {avg_space_used}, Avg Free Space: {avg_free_space_str}, Avg Inodes Used: {avg_inodes_used}")
    
    sys.exit(0)

# If failed mode and not summary, collect all failed disks and display in one table
if failed_mode and not summary_mode:
    all_failed_drives = []
    for pool_idx, sets in pools.items():
        for set_idx, set_info in sets.items():
            drives = pool_set_drives.get((int(pool_idx), int(set_idx)), [])
            failed_drives = [d for d in drives if d['state'] != 'ok']
            all_failed_drives.extend(failed_drives)
    
    if not all_failed_drives:
        pager.print(f"{YELLOW}No failed/faulty disks found in the provided data.{RESET}")
        sys.exit(0)
    
    # Display all failed disks in a single table
    table_data = []
    headers = ['Pool', 'Erasure Set', 'Disk Index', 'Server', 'Disk Path', 'State', 'Scanning', 'UUID', 'Total Space', 'Space Used', 'Free Space', 'Inodes Used', 'Local', 'Metrics']
    
    for drive in all_failed_drives:
        # Format space information
        if drive['total_space'] > 0:
            total_gb = drive['total_space'] / (1024**3)
            used_gb = drive['used_space'] / (1024**3)
            free_gb = drive['available_space'] / (1024**3)
            usage_pct = (drive['used_space'] / drive['total_space']) * 100
            free_pct = (drive['available_space'] / drive['total_space']) * 100
            
            # Color code usage percentages
            usage_color = GREEN if usage_pct < 80 else YELLOW if usage_pct < 95 else RED
            # Free space color is inverse of usage (more free = better)
            free_color = GREEN if free_pct > 20 else YELLOW if free_pct > 5 else RED
            
            total_space_info = f"{total_gb:.1f}GB"
            space_info = f"{used_gb:.1f}GB ({usage_color}{usage_pct:.1f}%{RESET})"
            free_space_info = f"{free_gb:.1f}GB ({free_color}{free_pct:.1f}%{RESET})"
        else:
            total_space_info = "N/A"
            space_info = "N/A"
            free_space_info = "N/A"
        
        # Format inode information
        if drive['used_inodes'] > 0:
            total_inodes = drive['used_inodes'] + drive['free_inodes']
            inode_usage_pct = (drive['used_inodes'] / total_inodes) * 100
            inode_color = GREEN if inode_usage_pct < 80 else YELLOW if inode_usage_pct < 95 else RED
            inode_info = f"{drive['used_inodes']:,} ({inode_color}{inode_usage_pct:.1f}%{RESET})"
        else:
            inode_info = "N/A"
        
        # Format metrics
        metrics_info = str(drive.get('metrics', '')) if drive.get('metrics') else ''
        
        # Color code the state
        state_color = GREEN if drive['state'] == 'ok' else RED
        state_text = f"{state_color}{drive['state']}{RESET}"
        
        # Color code scanning status
        scanning_color = YELLOW if drive['scanning'] else GREEN
        scanning_text = f"{scanning_color}{'Yes' if drive['scanning'] else 'No'}{RESET}"
        
        # Color code local status
        local_color = GREEN if drive['local'] else YELLOW
        local_text = f"{local_color}{'Yes' if drive['local'] else 'No'}{RESET}"
        
        # Add row to table
        table_data.append([
            f"{BLUE}{drive['pool_index']}{RESET}",
            f"{BLUE}{drive['set_index']}{RESET}",
            drive['disk_index'],
            drive['server'].split('.')[0],  # Extract only hostname part
            drive['path'],
            state_text,
            scanning_text,
            drive['uuid'][:16] + "...",  # Truncate UUID for readability
            total_space_info,
            space_info,
            free_space_info,
            inode_info,
            local_text,
            metrics_info
        ])
    
    # Print the consolidated table
    try:
        # Sort the table data by pool, erasure set, and disk index
        table_data.sort(key=lambda x: (int(x[0].replace('\033[94m', '').replace('\033[0m', '')), 
                                       int(x[1].replace('\033[94m', '').replace('\033[0m', '')), 
                                       int(x[2])))
        table_output = tabulate(table_data, headers=headers, tablefmt="simple", stralign="left")
        pager.print(table_output)
    except ImportError:
        pager.print(f"{RED}Error: 'tabulate' package not found. Install with: pip install tabulate{RESET}")
        pager.print("Falling back to simple format...")
        # Fallback to simple format
        for drive in all_failed_drives:
            pager.print(f"    {drive['server']} - disk{drive['disk_index']}: {drive['state']} (Scanning: {drive['scanning']})")
    
    sys.exit(0)

for pool_idx, sets in pools.items():
    # Check if this pool has any erasure sets with failed disks (for failed mode)
    pool_has_failed = False
    if failed_mode:
        for set_idx_check, set_info_check in sets.items():
            drives_check = pool_set_drives.get((int(pool_idx), int(set_idx_check)), [])
            if any(d['state'] != 'ok' for d in drives_check):
                pool_has_failed = True
                break
        if not pool_has_failed:
            continue  # Skip entire pool if no failed disks
    
    pager.print(f"{BLUE}Pool {pool_idx}:{RESET}")
    for set_idx, set_info in sets.items():
        drives = pool_set_drives.get((int(pool_idx), int(set_idx)), [])
        
        # Skip empty sets in scanning mode
        if scanning_mode and not drives:
            continue
        
        # Filter to only failed disks in failed mode (for summary mode)
        if failed_mode and summary_mode:
            failed_drives = [d for d in drives if d['state'] != 'ok']
            if not failed_drives:
                continue  # Skip erasure sets with no failed disks
            drives = failed_drives  # Use filtered list for display
            
        if summary_mode:
            good = sum(1 for d in drives if d['state'] == 'ok')
            bad = sum(1 for d in drives if d['state'] != 'ok')
            scanning = sum(1 for d in drives if d.get('scanning', False))
            
            # Calculate averages for space and inode metrics
            total_drives = len(drives)
            if total_drives > 0:
                # Calculate average total space, used space, and free space
                avg_total_space = sum(d.get('total_space', 0) for d in drives) / total_drives
                avg_used_space = sum(d.get('used_space', 0) for d in drives) / total_drives
                avg_free_space = sum(d.get('available_space', 0) for d in drives) / total_drives
                
                # Calculate average inodes used and total
                avg_used_inodes = sum(d.get('used_inodes', 0) for d in drives) / total_drives
                avg_free_inodes = sum(d.get('free_inodes', 0) for d in drives) / total_drives
                avg_total_inodes = avg_used_inodes + avg_free_inodes
                
                # Calculate percentages
                if avg_total_space > 0:
                    avg_space_used_pct = (avg_used_space / avg_total_space) * 100
                    avg_free_space_pct = (avg_free_space / avg_total_space) * 100
                else:
                    avg_space_used_pct = 0
                    avg_free_space_pct = 0
                
                if avg_total_inodes > 0:
                    avg_inodes_used_pct = (avg_used_inodes / avg_total_inodes) * 100
                else:
                    avg_inodes_used_pct = 0
                
                # Color code the percentages
                # Space used: green if < 80%, yellow if < 95%, red if >= 95%
                space_used_color = GREEN if avg_space_used_pct < 80 else YELLOW if avg_space_used_pct < 95 else RED
                # Free space: green if > 20%, yellow if > 5%, red if <= 5%
                free_space_color = GREEN if avg_free_space_pct > 20 else YELLOW if avg_free_space_pct > 5 else RED
                # Inodes used: green if < 80%, yellow if < 95%, red if >= 95%
                inodes_used_color = GREEN if avg_inodes_used_pct < 80 else YELLOW if avg_inodes_used_pct < 95 else RED
                
                # Format the averages as percentages with colors
                avg_space_used = f"{space_used_color}{avg_space_used_pct:.1f}%{RESET}"
                avg_free_space_str = f"{free_space_color}{avg_free_space_pct:.1f}%{RESET}"
                avg_inodes_used = f"{inodes_used_color}{avg_inodes_used_pct:.1f}%{RESET}"
            else:
                avg_space_used = "N/A"
                avg_free_space_str = "N/A"
                avg_inodes_used = "N/A"
            
            # Color code the summary
            good_text = f"{GREEN}{good}{RESET}" if good > 0 else "0"
            bad_text = f"{RED}{bad}{RESET}" if bad > 0 else "0"
            scanning_text = f"{YELLOW}{scanning}{RESET}" if scanning > 0 else "0"
            
            pager.print(f"  Erasure Set {set_idx}: Good disks: {good_text}, Bad disks: {bad_text}, Scanning: {scanning_text}, Avg Space Used: {avg_space_used}, Avg Free Space: {avg_free_space_str}, Avg Inodes Used: {avg_inodes_used}")
        else:
            # Skip if no drives to display (shouldn't happen due to filtering above, but safety check)
            if not drives:
                continue
                
            pager.print(f"  {BLUE}Erasure Set {set_idx}:{RESET}")
            
            # Prepare table data
            table_data = []
            headers = ['Pool', 'Erasure Set', 'Disk Index', 'Server', 'Disk Path', 'State', 'Scanning', 'UUID', 'Total Space', 'Space Used', 'Free Space', 'Inodes Used', 'Local', 'Metrics']
            
            for drive in drives:
                # Format space information
                if drive['total_space'] > 0:
                    total_gb = drive['total_space'] / (1024**3)
                    used_gb = drive['used_space'] / (1024**3)
                    free_gb = drive['available_space'] / (1024**3)
                    usage_pct = (drive['used_space'] / drive['total_space']) * 100
                    free_pct = (drive['available_space'] / drive['total_space']) * 100
                    
                    # Color code usage percentages
                    usage_color = GREEN if usage_pct < 80 else YELLOW if usage_pct < 95 else RED
                    # Free space color is inverse of usage (more free = better)
                    free_color = GREEN if free_pct > 20 else YELLOW if free_pct > 5 else RED
                    
                    total_space_info = f"{total_gb:.1f}GB"
                    space_info = f"{used_gb:.1f}GB ({usage_color}{usage_pct:.1f}%{RESET})"
                    free_space_info = f"{free_gb:.1f}GB ({free_color}{free_pct:.1f}%{RESET})"
                else:
                    total_space_info = "N/A"
                    space_info = "N/A"
                    free_space_info = "N/A"
                
                # Format inode information
                if drive['used_inodes'] > 0:
                    total_inodes = drive['used_inodes'] + drive['free_inodes']
                    inode_usage_pct = (drive['used_inodes'] / total_inodes) * 100
                    inode_color = GREEN if inode_usage_pct < 80 else YELLOW if inode_usage_pct < 95 else RED
                    inode_info = f"{drive['used_inodes']:,} ({inode_color}{inode_usage_pct:.1f}%{RESET})"
                else:
                    inode_info = "N/A"
                
                # Format metrics
                metrics_info = str(drive.get('metrics', '')) if drive.get('metrics') else ''
                
                # Color code the state
                state_color = GREEN if drive['state'] == 'ok' else RED
                state_text = f"{state_color}{drive['state']}{RESET}"
                
                # Color code scanning status
                scanning_color = YELLOW if drive['scanning'] else GREEN
                scanning_text = f"{scanning_color}{'Yes' if drive['scanning'] else 'No'}{RESET}"
                
                # Color code local status
                local_color = GREEN if drive['local'] else YELLOW
                local_text = f"{local_color}{'Yes' if drive['local'] else 'No'}{RESET}"
                
                if drive['used_inodes'] > 0:
                    total_inodes = drive['used_inodes'] + drive['free_inodes']
                    inode_usage_pct = (drive['used_inodes'] / total_inodes) * 100
                    inode_color = GREEN if inode_usage_pct < 80 else YELLOW if inode_usage_pct < 95 else RED
                    inode_info = f"{drive['used_inodes']:,} ({inode_color}{inode_usage_pct:.1f}%{RESET})"
                
                # Add row to table with Pool and Erasure Set as leading columns
                table_data.append([
                    f"{BLUE}{pool_idx}{RESET}",
                    f"{BLUE}{set_idx}{RESET}",
                    drive['disk_index'],
                    drive['server'].split('.')[0],  # Extract only hostname part
                    drive['path'],
                    state_text,
                    scanning_text,
                    drive['uuid'][:16] + "...",  # Truncate UUID for readability
                    total_space_info,
                    space_info,
                    free_space_info,
                    inode_info,
                    local_text,
                    metrics_info
                ])
            
            # Print the table
            try:
                # Sort the table data by pool, erasure set, and disk index
                table_data.sort(key=lambda x: (int(x[0].replace('\033[94m', '').replace('\033[0m', '')), 
                                               int(x[1].replace('\033[94m', '').replace('\033[0m', '')), 
                                               int(x[2])))
                table_output = tabulate(table_data, headers=headers, tablefmt="simple", stralign="left")
                pager.print(table_output)
            except ImportError:
                pager.print(f"{RED}Error: 'tabulate' package not found. Install with: pip install tabulate{RESET}")
                pager.print("Falling back to simple format...")
                # Fallback to simple format
                for drive in drives:
                    pager.print(f"    {drive['server']} - disk{drive['disk_index']}: {drive['state']} (Scanning: {drive['scanning']})")
            
            pager.print()  # Empty line for readability