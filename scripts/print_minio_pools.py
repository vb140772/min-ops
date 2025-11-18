import json
import sys
from tabulate import tabulate

# ANSI color codes for highlighting
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
BOLD = '\033[1m'
RESET = '\033[0m'

# Check for summary mode and get JSON file name
if len(sys.argv) < 2:
    print("Usage: python print_minio_pools.py <json_file> [--summary] [--scanning]")
    print("Example: python print_minio_pools.py prod.json --summary")
    print("Example: python print_minio_pools.py prod.json --scanning")
    print("Note: This script requires the 'tabulate' package. Install with: pip install tabulate")
    sys.exit(1)

json_file = sys.argv[1]
summary_mode = '--summary' in sys.argv
scanning_mode = '--scanning' in sys.argv

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
print(f"Detected Erasure Coding Configuration: EC:{parity_disks}")
print()

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
            
        pool_index = drive.get('pool_index', 0)
        set_index = drive.get('set_index', 0)
        key = (pool_index, set_index)
        if key not in pool_set_drives:
            pool_set_drives[key] = []
        
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
            'local': drive.get('local', False)
        }
        
        # Add metrics if available
        if 'metrics' in drive and drive['metrics']:
            disk_info['metrics'] = drive['metrics']
        
        pool_set_drives[key].append(disk_info)
        all_drives.append(disk_info)

# Print summary statistics
print(f"{BOLD}MinIO Cluster Summary{RESET}")
print("=" * 50)

# Display deployment ID if available
deployment_id = info_path.get('deploymentID', None)
if deployment_id:
    print(f"Deployment ID: {deployment_id}")
else:
    print("Deployment ID: Not available")
print()

# Basic disk counts
print(f"Total Disks: {total_disks}")
print(f"Scanning Disks: {YELLOW}{scanning_disks}{RESET}")
print(f"Healthy Disks: {GREEN}{ok_disks}{RESET}")
print(f"Problem Disks: {RED}{bad_disks}{RESET}")

# Calculate health percentage
if total_disks > 0:
    health_pct = (ok_disks / total_disks) * 100
    health_color = GREEN if health_pct >= 90 else YELLOW if health_pct >= 75 else RED
    print(f"Health: {health_color}{health_pct:.1f}%{RESET}")

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
    
    print(f"Raw Capacity: {total_tb:.1f} TB")
    print(f"Usable Capacity: {usable_tb:.1f} TB")
    print(f"Used Space: {used_tb:.1f} TB ({usage_color}{usage_pct:.1f}%{RESET})")
    print(f"Available Space: {usable_tb - used_tb:.1f} TB")

# Pool and server statistics
print(f"Pools: {len(pools)}")
print(f"Servers: {len(servers)}")

# Count erasure sets
total_erasure_sets = sum(len(sets) for sets in pools.values())
print(f"Erasure Sets: {total_erasure_sets}")

print("=" * 50)

# Calculate and display pool-specific summaries
print(f"{BOLD}Pool Summary{RESET}")
print("-" * 50)

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
    print(f"Pool {pool_idx}:")
    print(f"  Erasure Sets: {pool_erasure_sets}")
    print(f"  Disks: {pool_total_disks} total ({GREEN}{pool_ok_disks} ok{RESET}, {RED}{pool_bad_disks} bad{RESET}, {YELLOW}{pool_scanning_disks} scanning{RESET})")
    print(f"  Health: {pool_health_color}{pool_health_pct:.1f}%{RESET}")
    print(f"  Raw Capacity: {pool_total_tb:.1f} TB")
    print(f"  Usable Capacity: {pool_usable_tb:.1f} TB")
    print(f"  Usage: {pool_used_tb:.1f} TB ({pool_usage_color}{pool_usage_pct:.1f}%{RESET})")
    print(f"  Available: {pool_usable_tb - pool_used_tb:.1f} TB")
    print()

print("=" * 50)
print()

# Print the pools, sets, and drives
if scanning_mode:
    print(f"{BOLD}MinIO Scanning Disks from: {json_file}{RESET}")
else:
    print(f"{BOLD}MinIO Pool Information from: {json_file}{RESET}")
print("=" * 80)

# Check if any scanning disks were found when in scanning mode
if scanning_mode and not pool_set_drives:
    print(f"{YELLOW}No scanning disks found in the provided data.{RESET}")
    sys.exit(0)

for pool_idx, sets in pools.items():
    print(f"{BLUE}Pool {pool_idx}:{RESET}")
    for set_idx, set_info in sets.items():
        drives = pool_set_drives.get((int(pool_idx), int(set_idx)), [])
        
        # Skip empty sets in scanning mode
        if scanning_mode and not drives:
            continue
            
        if summary_mode:
            good = sum(1 for d in drives if d['state'] == 'ok')
            bad = sum(1 for d in drives if d['state'] != 'ok')
            scanning = sum(1 for d in drives if d.get('scanning', False))
            
            # Color code the summary
            good_text = f"{GREEN}{good}{RESET}" if good > 0 else "0"
            bad_text = f"{RED}{bad}{RESET}" if bad > 0 else "0"
            scanning_text = f"{YELLOW}{scanning}{RESET}" if scanning > 0 else "0"
            
            print(f"  Erasure Set {set_idx}: Good disks: {good_text}, Bad disks: {bad_text}, Scanning: {scanning_text}")
        else:
            print(f"  {BLUE}Erasure Set {set_idx}:{RESET}")
            
            # Prepare table data
            table_data = []
            headers = ['Pool', 'Erasure Set', 'Disk Index', 'Server', 'Disk Path', 'State', 'Scanning', 'UUID', 'Space Used', 'Inodes Used', 'Local', 'Metrics']
            
            for drive in drives:
                # Format space information
                if drive['total_space'] > 0:
                    used_gb = drive['used_space'] / (1024**3)
                    usage_pct = (drive['used_space'] / drive['total_space']) * 100
                    space_info = f"{used_gb:.1f}GB ({usage_pct:.1f}%)"
                else:
                    space_info = "N/A"
                
                # Format inode information
                if drive['used_inodes'] > 0:
                    total_inodes = drive['used_inodes'] + drive['free_inodes']
                    inode_usage_pct = (drive['used_inodes'] / total_inodes) * 100
                    inode_info = f"{drive['used_inodes']:,} ({inode_usage_pct:.1f}%)"
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
                
                # Color code usage percentages
                if drive['total_space'] > 0:
                    usage_pct = (drive['used_space'] / drive['total_space']) * 100
                    usage_color = GREEN if usage_pct < 80 else YELLOW if usage_pct < 95 else RED
                    space_info = f"{used_gb:.1f}GB ({usage_color}{usage_pct:.1f}%{RESET})"
                
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
                    space_info,
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
                print(tabulate(table_data, headers=headers, tablefmt="simple", stralign="left"))
            except ImportError:
                print(f"{RED}Error: 'tabulate' package not found. Install with: pip install tabulate{RESET}")
                print("Falling back to simple format...")
                # Fallback to simple format
                for drive in drives:
                    print(f"    {drive['server']} - disk{drive['disk_index']}: {drive['state']} (Scanning: {drive['scanning']})")
            
            print()  # Empty line for readability