package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
)

// ANSI color codes
const (
	Green  = "\033[92m"
	Red    = "\033[91m"
	Yellow = "\033[93m"
	Blue   = "\033[94m"
	Bold   = "\033[1m"
	Reset  = "\033[0m"
)

// Config holds command-line configuration
type Config struct {
	JSONFile          string
	SummaryMode       bool
	ScanningMode      bool
	PagerMode         bool
	FailedMode        bool
	LowSpaceThreshold *float64
	MinBadDisks       *int
}

// DiskInfo represents a single disk
type DiskInfo struct {
	Server         string
	Path           string
	State          string
	UUID           string
	Scanning       bool
	DiskIndex      interface{}
	TotalSpace     int64
	UsedSpace      int64
	AvailableSpace int64
	UsedInodes     int64
	FreeInodes     int64
	Local          bool
	Metrics        interface{}
	PoolIndex      int
	SetIndex       int
	FreeSpacePct   float64
	UsedSpacePct   float64
}

// ErasureSetInfo holds information about an erasure set
type ErasureSetInfo struct {
	PoolIdx          int
	SetIdx           int
	Drives           []DiskInfo
	AvgSpaceUsedPct  float64
	AvgFreeSpacePct  float64
	AvgInodesUsedPct float64
	Good             int
	Bad              int
	Scanning         int
}

// ClusterStats holds cluster-wide statistics
type ClusterStats struct {
	TotalDisks    int
	ScanningDisks int
	OkDisks       int
	BadDisks      int
	TotalSpace    int64
	UsedSpace     int64
	DeploymentID  string
	ParityDisks   int
}

// Pager handles paginated output
type Pager struct {
	enabled      bool
	linesPrinted int
	linesPerPage int
}

func NewPager(enabled bool) *Pager {
	p := &Pager{enabled: enabled}
	if enabled {
		p.linesPerPage = 22 // Default terminal height - 2
	} else {
		p.linesPerPage = 999999
	}
	return p
}

func (p *Pager) Printf(format string, args ...interface{}) {
	if !p.enabled {
		fmt.Printf(format, args...)
		return
	}

	text := fmt.Sprintf(format, args...)
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		fmt.Print(line)
		if i < len(lines)-1 {
			fmt.Print("\n")
		}
		p.linesPrinted++

		if p.linesPrinted >= p.linesPerPage {
			fmt.Printf("\n%s-- Press SPACE to continue, 'q' to quit --%s", Yellow, Reset)
			p.waitForSpace()
			fmt.Print("\r" + strings.Repeat(" ", 50) + "\r")
			p.linesPrinted = 0
		}
	}
}

func (p *Pager) waitForSpace() {
	if !p.enabled {
		return
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		char, _, err := reader.ReadRune()
		if err != nil {
			return
		}
		if char == ' ' {
			break
		}
		if char == 'q' {
			os.Exit(0)
		}
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "mdb"
	app.Usage = "MinIO Debug - analyze MinIO diagnostic JSON files"
	app.Action = mainMDB
	app.Flags = mdbFlags
	app.CustomAppHelpTemplate = mdbHelpTemplate

	if err := app.Run(os.Args); err != nil {
		console.Fatalln(err)
	}
}

func mainMDB(ctx *cli.Context) error {
	config := parseFlags(ctx)

	if config.JSONFile == "" {
		cli.ShowAppHelp(ctx)
		return fmt.Errorf("JSON file is required")
	}

	data, err := loadJSON(config.JSONFile)
	if err != nil {
		return fmt.Errorf("failed to load JSON file '%s': %v", config.JSONFile, err)
	}

	servers, infoPath, err := extractServers(data)
	if err != nil {
		return fmt.Errorf("failed to extract servers: %v", err)
	}

	pools := extractPools(infoPath, servers)
	parityDisks := getErasureCodingConfig(servers)

	pager := NewPager(config.PagerMode)
	pager.Printf("%sDetected Erasure Coding Configuration: EC:%d%s\n", Bold, parityDisks, Reset)
	pager.Printf("\n")

	poolSetDrives := make(map[string][]DiskInfo)
	allPoolSetDrives := make(map[string][]DiskInfo) // For capacity calculations (all drives)
	stats := ClusterStats{ParityDisks: parityDisks}

	// Process all drives
	for _, server := range servers {
		drives := getDrives(server)
		for _, drive := range drives {
			stats.TotalDisks++
			if drive.Scanning {
				stats.ScanningDisks++
			}
			if drive.State == "ok" {
				stats.OkDisks++
			} else {
				stats.BadDisks++
			}
			stats.TotalSpace += drive.TotalSpace
			stats.UsedSpace += drive.UsedSpace

			// Store all drives for capacity calculations
			key := fmt.Sprintf("%d:%d", drive.PoolIndex, drive.SetIndex)
			allPoolSetDrives[key] = append(allPoolSetDrives[key], drive)

			// Apply filters for display
			if config.ScanningMode && !drive.Scanning {
				continue
			}
			if config.FailedMode && drive.State == "ok" {
				continue
			}

			poolSetDrives[key] = append(poolSetDrives[key], drive)
		}
	}

	stats.DeploymentID = getDeploymentID(infoPath)

	// Print cluster summary (use all drives for capacity calculation)
	printClusterSummary(pager, stats, pools, allPoolSetDrives, servers, config)

	// Handle special modes
	if config.FailedMode && !config.SummaryMode {
		printFailedDisksTable(pager, poolSetDrives, config)
		return nil
	}

	if config.SummaryMode && config.LowSpaceThreshold != nil {
		printLowSpaceErasureSets(pager, pools, poolSetDrives, *config.LowSpaceThreshold, config)
		return nil
	}

	// Print pools and erasure sets
	printPoolsAndSets(pager, pools, poolSetDrives, allPoolSetDrives, config)
	return nil
}

var mdbFlags = []cli.Flag{
	cli.BoolFlag{
		Name:  "summary",
		Usage: "Display summary view",
	},
	cli.BoolFlag{
		Name:  "scanning",
		Usage: "Show only scanning disks",
	},
	cli.BoolFlag{
		Name:  "pager",
		Usage: "Enable pagination (pauses output after each screen, press space to continue)",
	},
	cli.BoolFlag{
		Name:  "failed",
		Usage: "Show only failed/faulty disks (not 'ok' state)",
	},
	cli.StringFlag{
		Name:  "low-space",
		Usage: "Filter by free space percentage (requires --summary)",
	},
	cli.StringFlag{
		Name:  "min-bad-disks",
		Usage: "Filter by minimum bad disks (requires --summary --failed)",
	},
}

var mdbHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} [FLAGS] JSON_FILE

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
  1. Display summary view of MinIO cluster.
     {{.Prompt}} {{.HelpName}} prod.json --summary

  2. Show only scanning disks.
     {{.Prompt}} {{.HelpName}} prod.json --scanning

  3. Show erasure sets with low free space.
     {{.Prompt}} {{.HelpName}} prod.json --summary --low-space=10

  4. Enable pagination for long output.
     {{.Prompt}} {{.HelpName}} prod.json --pager

  5. Show only failed disks.
     {{.Prompt}} {{.HelpName}} prod.json --failed

  6. Show summary of failed disks.
     {{.Prompt}} {{.HelpName}} prod.json --summary --failed

  7. Filter erasure sets with minimum bad disks.
     {{.Prompt}} {{.HelpName}} prod.json --summary --failed --min-bad-disks=2

NOTES:
  - --low-space requires --summary mode
  - --min-bad-disks requires --summary --failed mode
  - --pager pauses output after each screen (press space to continue, 'q' to quit)
`

func parseFlags(ctx *cli.Context) *Config {
	config := &Config{}

	// Get JSON file from arguments
	if ctx.NArg() < 1 {
		return config // Will be caught in mainMDB
	}
	config.JSONFile = ctx.Args().Get(0)

	config.SummaryMode = ctx.Bool("summary")
	config.ScanningMode = ctx.Bool("scanning")
	config.PagerMode = ctx.Bool("pager")
	config.FailedMode = ctx.Bool("failed")

	// Parse low-space threshold
	if lowSpaceStr := ctx.String("low-space"); lowSpaceStr != "" {
		val, err := strconv.ParseFloat(lowSpaceStr, 64)
		if err != nil {
			console.Fatalln(fmt.Errorf("Invalid --low-space value. Must be a number: %v", err))
		}
		config.LowSpaceThreshold = &val
		if !config.SummaryMode {
			console.Fatalln(fmt.Errorf("--low-space option requires --summary mode"))
		}
	}

	// Parse min-bad-disks threshold
	if minBadDisksStr := ctx.String("min-bad-disks"); minBadDisksStr != "" {
		val, err := strconv.Atoi(minBadDisksStr)
		if err != nil || val < 0 {
			console.Fatalln(fmt.Errorf("Invalid --min-bad-disks value. Must be a non-negative integer: %v", err))
		}
		config.MinBadDisks = &val
		if !config.SummaryMode || !config.FailedMode {
			console.Fatalln(fmt.Errorf("--min-bad-disks option requires --summary --failed mode"))
		}
	}

	return config
}

func loadJSON(filename string) (map[string]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("file '%s' not found: %v", filename, err)
	}
	defer file.Close()

	// Try loading as single JSON first
	decoder := json.NewDecoder(file)
	var data map[string]interface{}
	err = decoder.Decode(&data)
	if err == nil {
		// Check if it has the expected structure
		if _, hasInfo := data["info"]; !hasInfo {
			if minio, ok := data["minio"].(map[string]interface{}); ok {
				if _, hasMinioInfo := minio["info"]; !hasMinioInfo {
					// Might be NDJSON, try reading line by line
					return loadNDJSON(filename)
				}
			} else {
				return loadNDJSON(filename)
			}
		}
		return data, nil
	}

	// If single JSON load fails, try NDJSON format
	return loadNDJSON(filename)
}

func loadNDJSON(filename string) (map[string]interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var lineData map[string]interface{}
		if err := json.Unmarshal(line, &lineData); err != nil {
			continue
		}
		// Look for MinIO diagnostic data
		if _, ok := lineData["minio"]; ok {
			return lineData, nil
		}
		if _, ok := lineData["info"]; ok {
			return lineData, nil
		}
	}
	return nil, fmt.Errorf("no valid JSON found")
}

func extractServers(data map[string]interface{}) ([]map[string]interface{}, map[string]interface{}, error) {
	var servers []map[string]interface{}
	var infoPath map[string]interface{}

	// Try diagnostic format: minio.info.servers
	if minio, ok := data["minio"].(map[string]interface{}); ok {
		if info, ok := minio["info"].(map[string]interface{}); ok {
			if srv, ok := info["servers"].([]interface{}); ok {
				servers = make([]map[string]interface{}, len(srv))
				for i, s := range srv {
					servers[i] = s.(map[string]interface{})
				}
				infoPath = info
				return servers, infoPath, nil
			}
		}
	}

	// Try standard format: info.servers
	if info, ok := data["info"].(map[string]interface{}); ok {
		if srv, ok := info["servers"].([]interface{}); ok {
			servers = make([]map[string]interface{}, len(srv))
			for i, s := range srv {
				servers[i] = s.(map[string]interface{})
			}
			infoPath = info
			return servers, infoPath, nil
		}
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	return nil, nil, fmt.Errorf("could not find servers in JSON structure. Available top-level keys: %v", keys)
}

func extractPools(infoPath map[string]interface{}, servers []map[string]interface{}) map[string]map[string]interface{} {
	pools := make(map[string]map[string]interface{})

	if poolsData, ok := infoPath["pools"].(map[string]interface{}); ok {
		for poolIdx, setsData := range poolsData {
			if sets, ok := setsData.(map[string]interface{}); ok {
				pools[poolIdx] = sets
			}
		}
		return pools
	}

	// Build pools structure from drive information
	for _, server := range servers {
		drives, _ := server["drives"].([]interface{})
		for _, d := range drives {
			drive := d.(map[string]interface{})
			poolIdx := getInt(drive, "pool_index", 0)
			setIdx := getInt(drive, "set_index", 0)
			poolKey := strconv.Itoa(poolIdx)
			setKey := strconv.Itoa(setIdx)

			if pools[poolKey] == nil {
				pools[poolKey] = make(map[string]interface{})
			}
			if pools[poolKey][setKey] == nil {
				pools[poolKey][setKey] = make(map[string]interface{})
			}
		}
	}
	return pools
}

func getErasureCodingConfig(servers []map[string]interface{}) int {
	for _, server := range servers {
		if envVars, ok := server["minio_env_vars"].(map[string]interface{}); ok {
			if ecConfig, ok := envVars["MINIO_STORAGE_CLASS_STANDARD"].(string); ok {
				if strings.HasPrefix(ecConfig, "EC:") {
					parts := strings.Split(ecConfig, ":")
					if len(parts) > 1 {
						if val, err := strconv.Atoi(parts[1]); err == nil {
							return val
						}
					}
				}
			}
		}
	}
	return 2 // Default to EC-2
}

func getDeploymentID(infoPath map[string]interface{}) string {
	if id, ok := infoPath["deploymentID"].(string); ok {
		return id
	}
	return "Not available"
}

func getDrives(server map[string]interface{}) []DiskInfo {
	drivesData, ok := server["drives"].([]interface{})
	if !ok {
		return nil
	}

	serverEndpoint := getString(server, "endpoint", "unknown")
	drives := make([]DiskInfo, 0, len(drivesData))

	for _, d := range drivesData {
		drive := d.(map[string]interface{})
		diskInfo := DiskInfo{
			Server:         serverEndpoint,
			Path:           getString(drive, "path", ""),
			State:          getString(drive, "state", "unknown"),
			UUID:           getString(drive, "uuid", "N/A"),
			Scanning:       getBool(drive, "scanning", false),
			DiskIndex:      drive["disk_index"],
			TotalSpace:     getInt64(drive, "totalspace", 0),
			UsedSpace:      getInt64(drive, "usedspace", 0),
			AvailableSpace: getInt64(drive, "availspace", 0),
			UsedInodes:     getInt64(drive, "used_inodes", 0),
			FreeInodes:     getInt64(drive, "free_inodes", 0),
			Local:          getBool(drive, "local", false),
			Metrics:        drive["metrics"],
			PoolIndex:      getInt(drive, "pool_index", 0),
			SetIndex:       getInt(drive, "set_index", 0),
		}

		// Extract path from endpoint if path is not provided
		if diskInfo.Path == "" {
			if endpoint, ok := drive["endpoint"].(string); ok {
				diskInfo.Path = extractPathFromEndpoint(endpoint)
			}
		}

		// Calculate percentages
		if diskInfo.TotalSpace > 0 {
			diskInfo.FreeSpacePct = float64(diskInfo.AvailableSpace) / float64(diskInfo.TotalSpace) * 100
			diskInfo.UsedSpacePct = float64(diskInfo.UsedSpace) / float64(diskInfo.TotalSpace) * 100
		}

		drives = append(drives, diskInfo)
	}

	return drives
}

func extractPathFromEndpoint(endpoint string) string {
	if strings.Contains(endpoint, "/hadoop/") {
		parts := strings.Split(endpoint, "/hadoop/")
		if len(parts) > 1 {
			return "/" + parts[1]
		}
	}
	parts := strings.Split(endpoint, "/")
	if len(parts) > 3 {
		return "/" + strings.Join(parts[3:], "/")
	}
	return ""
}

func getString(m map[string]interface{}, key string, defaultValue string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return defaultValue
}

func getInt(m map[string]interface{}, key string, defaultValue int) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return defaultValue
}

func getInt64(m map[string]interface{}, key string, defaultValue int64) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	}
	return defaultValue
}

func getBool(m map[string]interface{}, key string, defaultValue bool) bool {
	if val, ok := m[key].(bool); ok {
		return val
	}
	return defaultValue
}

func printClusterSummary(pager *Pager, stats ClusterStats, pools map[string]map[string]interface{}, poolSetDrives map[string][]DiskInfo, servers []map[string]interface{}, config *Config) {
	pager.Printf("%sMinIO Cluster Summary%s\n", Bold, Reset)
	pager.Printf("==================================================\n")

	if stats.DeploymentID != "" {
		pager.Printf("Deployment ID: %s\n", stats.DeploymentID)
	} else {
		pager.Printf("Deployment ID: Not available\n")
	}
	pager.Printf("\n")

	pager.Printf("Total Disks: %d\n", stats.TotalDisks)
	pager.Printf("Scanning Disks: %s%d%s\n", Yellow, stats.ScanningDisks, Reset)
	pager.Printf("Healthy Disks: %s%d%s\n", Green, stats.OkDisks, Reset)
	pager.Printf("Problem Disks: %s%d%s\n", Red, stats.BadDisks, Reset)

	if stats.TotalDisks > 0 {
		healthPct := float64(stats.OkDisks) / float64(stats.TotalDisks) * 100
		var healthColor string
		if healthPct >= 90 {
			healthColor = Green
		} else if healthPct >= 75 {
			healthColor = Yellow
		} else {
			healthColor = Red
		}
		pager.Printf("Health: %s%.1f%%%s\n", healthColor, healthPct, Reset)
	}

	if stats.TotalSpace > 0 {
		totalTB := float64(stats.TotalSpace) / (1024 * 1024 * 1024 * 1024)
		usedTB := float64(stats.UsedSpace) / (1024 * 1024 * 1024 * 1024)

		// Calculate usable capacity
		totalUsableSpace := int64(0)
		for poolIdx, sets := range pools {
			for setIdx := range sets {
				key := fmt.Sprintf("%s:%s", poolIdx, setIdx)
				drives := poolSetDrives[key]
				totalDisksInSet := len(drives)
				if totalDisksInSet > 0 {
					if totalDisksInSet >= stats.ParityDisks {
						dataDisks := totalDisksInSet - stats.ParityDisks
						usableRatio := float64(dataDisks) / float64(totalDisksInSet)
						for _, drive := range drives {
							totalUsableSpace += int64(float64(drive.TotalSpace) * usableRatio)
						}
					}
				}
			}
		}

		usableTB := float64(totalUsableSpace) / (1024 * 1024 * 1024 * 1024)
		usagePct := float64(stats.UsedSpace) / float64(totalUsableSpace) * 100
		if totalUsableSpace == 0 {
			usagePct = 0
		}
		var usageColor string
		if usagePct < 80 {
			usageColor = Green
		} else if usagePct < 95 {
			usageColor = Yellow
		} else {
			usageColor = Red
		}

		pager.Printf("Raw Capacity: %.1f TB\n", totalTB)
		pager.Printf("Usable Capacity: %.1f TB\n", usableTB)
		pager.Printf("Used Space: %.1f TB (%s%.1f%%%s)\n", usedTB, usageColor, usagePct, Reset)
		pager.Printf("Available Space: %.1f TB\n", usableTB-usedTB)
	}

	pager.Printf("Pools: %d\n", len(pools))
	pager.Printf("Servers: %d\n", len(servers))

	totalErasureSets := 0
	for _, sets := range pools {
		totalErasureSets += len(sets)
	}
	pager.Printf("Erasure Sets: %d\n", totalErasureSets)
	pager.Printf("==================================================\n")

	// Pool Summary
	pager.Printf("%sPool Summary%s\n", Bold, Reset)
	pager.Printf("--------------------------------------------------\n")

	for poolIdx, sets := range pools {
		poolTotalDisks := 0
		poolOkDisks := 0
		poolBadDisks := 0
		poolScanningDisks := 0
		poolTotalSpace := int64(0)
		poolUsedSpace := int64(0)
		poolUsableSpace := int64(0)

		for setIdx := range sets {
			key := fmt.Sprintf("%s:%s", poolIdx, setIdx)
			drives := poolSetDrives[key]
			totalDisksInSet := len(drives)
			if totalDisksInSet > 0 {
				var usableRatio float64
				if totalDisksInSet >= stats.ParityDisks {
					dataDisks := totalDisksInSet - stats.ParityDisks
					usableRatio = float64(dataDisks) / float64(totalDisksInSet)
				}

				for _, drive := range drives {
					poolTotalDisks++
					if drive.State == "ok" {
						poolOkDisks++
					} else {
						poolBadDisks++
					}
					if drive.Scanning {
						poolScanningDisks++
					}
					poolTotalSpace += drive.TotalSpace
					poolUsedSpace += drive.UsedSpace
					poolUsableSpace += int64(float64(drive.TotalSpace) * usableRatio)
				}
			}
		}

		poolHealthPct := float64(poolOkDisks) / float64(poolTotalDisks) * 100
		if poolTotalDisks == 0 {
			poolHealthPct = 0
		}
		var poolHealthColor string
		if poolHealthPct >= 90 {
			poolHealthColor = Green
		} else if poolHealthPct >= 75 {
			poolHealthColor = Yellow
		} else {
			poolHealthColor = Red
		}

		poolUsagePct := float64(poolUsedSpace) / float64(poolUsableSpace) * 100
		if poolUsableSpace == 0 {
			poolUsagePct = 0
		}
		var poolUsageColor string
		if poolUsagePct < 80 {
			poolUsageColor = Green
		} else if poolUsagePct < 95 {
			poolUsageColor = Yellow
		} else {
			poolUsageColor = Red
		}

		poolTotalTB := float64(poolTotalSpace) / (1024 * 1024 * 1024 * 1024)
		poolUsableTB := float64(poolUsableSpace) / (1024 * 1024 * 1024 * 1024)
		poolUsedTB := float64(poolUsedSpace) / (1024 * 1024 * 1024 * 1024)

		pager.Printf("Pool %s:\n", poolIdx)
		pager.Printf("  Erasure Sets: %d\n", len(sets))
		pager.Printf("  Disks: %d total (%s%d ok%s, %s%d bad%s, %s%d scanning%s)\n",
			poolTotalDisks, Green, poolOkDisks, Reset, Red, poolBadDisks, Reset, Yellow, poolScanningDisks, Reset)
		pager.Printf("  Health: %s%.1f%%%s\n", poolHealthColor, poolHealthPct, Reset)
		pager.Printf("  Raw Capacity: %.1f TB\n", poolTotalTB)
		pager.Printf("  Usable Capacity: %.1f TB\n", poolUsableTB)
		pager.Printf("  Usage: %.1f TB (%s%.1f%%%s)\n", poolUsedTB, poolUsageColor, poolUsagePct, Reset)
		pager.Printf("  Available: %.1f TB\n", poolUsableTB-poolUsedTB)
		pager.Printf("\n")
	}

	pager.Printf("==================================================\n")
	pager.Printf("\n")
}

func printFailedDisksTable(pager *Pager, poolSetDrives map[string][]DiskInfo, config *Config) {
	allFailedDrives := make([]DiskInfo, 0)
	for _, drives := range poolSetDrives {
		for _, drive := range drives {
			if drive.State != "ok" {
				allFailedDrives = append(allFailedDrives, drive)
			}
		}
	}

	if len(allFailedDrives) == 0 {
		pager.Printf("%sNo failed/faulty disks found in the provided data.%s\n", Yellow, Reset)
		return
	}

	// Sort by pool, set, disk index
	sort.Slice(allFailedDrives, func(i, j int) bool {
		if allFailedDrives[i].PoolIndex != allFailedDrives[j].PoolIndex {
			return allFailedDrives[i].PoolIndex < allFailedDrives[j].PoolIndex
		}
		if allFailedDrives[i].SetIndex != allFailedDrives[j].SetIndex {
			return allFailedDrives[i].SetIndex < allFailedDrives[j].SetIndex
		}
		return fmt.Sprintf("%v", allFailedDrives[i].DiskIndex) < fmt.Sprintf("%v", allFailedDrives[j].DiskIndex)
	})

	pager.Printf("%sMinIO Failed/Faulty Disks from: %s%s\n", Bold, config.JSONFile, Reset)
	pager.Printf("================================================================================\n")

	printTable(pager, allFailedDrives, config)
}

func printLowSpaceErasureSets(pager *Pager, pools map[string]map[string]interface{}, poolSetDrives map[string][]DiskInfo, threshold float64, config *Config) {
	erasureSets := make([]ErasureSetInfo, 0)

	for poolIdx, sets := range pools {
		for setIdx := range sets {
			key := fmt.Sprintf("%s:%s", poolIdx, setIdx)
			drives := poolSetDrives[key]
			if len(drives) == 0 {
				continue
			}

			// Calculate averages
			totalDrives := len(drives)
			avgTotalSpace := int64(0)
			avgUsedSpace := int64(0)
			avgFreeSpace := int64(0)
			for _, d := range drives {
				avgTotalSpace += d.TotalSpace
				avgUsedSpace += d.UsedSpace
				avgFreeSpace += d.AvailableSpace
			}
			avgTotalSpace /= int64(totalDrives)
			avgUsedSpace /= int64(totalDrives)
			avgFreeSpace /= int64(totalDrives)

			var avgFreeSpacePct float64
			if avgTotalSpace > 0 {
				avgFreeSpacePct = float64(avgFreeSpace) / float64(avgTotalSpace) * 100
			}

			// Only include erasure sets with free space below threshold
			if avgFreeSpacePct < threshold {
				poolIdxInt, _ := strconv.Atoi(poolIdx)
				setIdxInt, _ := strconv.Atoi(setIdx)
				es := ErasureSetInfo{
					PoolIdx:         poolIdxInt,
					SetIdx:          setIdxInt,
					Drives:          drives,
					AvgFreeSpacePct: avgFreeSpacePct,
					AvgSpaceUsedPct: float64(avgUsedSpace) / float64(avgTotalSpace) * 100,
				}
				if avgTotalSpace > 0 {
					es.AvgSpaceUsedPct = float64(avgUsedSpace) / float64(avgTotalSpace) * 100
				}
				erasureSets = append(erasureSets, es)
			}
		}
	}

	// Sort by utilization (used space percentage) descending
	sort.Slice(erasureSets, func(i, j int) bool {
		return erasureSets[i].AvgSpaceUsedPct > erasureSets[j].AvgSpaceUsedPct
	})

	if len(erasureSets) == 0 {
		pager.Printf("%sNo erasure sets found with average free space less than %.1f%%.%s\n", Yellow, threshold, Reset)
		return
	}

	pager.Printf("%sErasure Sets with Average Free Space < %.1f%% (sorted by utilization)%s\n", Bold, threshold, Reset)
	pager.Printf("================================================================================\n")

	for _, es := range erasureSets {
		good := 0
		bad := 0
		scanning := 0
		for _, d := range es.Drives {
			if d.State == "ok" {
				good++
			} else {
				bad++
			}
			if d.Scanning {
				scanning++
			}
		}

		goodText := fmt.Sprintf("%d", good)
		if good > 0 {
			goodText = fmt.Sprintf("%s%d%s", Green, good, Reset)
		}
		badText := fmt.Sprintf("%d", bad)
		if bad > 0 {
			badText = fmt.Sprintf("%s%d%s", Red, bad, Reset)
		}
		scanningText := fmt.Sprintf("%d", scanning)
		if scanning > 0 {
			scanningText = fmt.Sprintf("%s%d%s", Yellow, scanning, Reset)
		}

		spaceUsedColor := Green
		if es.AvgSpaceUsedPct >= 95 {
			spaceUsedColor = Red
		} else if es.AvgSpaceUsedPct >= 80 {
			spaceUsedColor = Yellow
		}
		freeSpaceColor := Green
		if es.AvgFreeSpacePct <= 5 {
			freeSpaceColor = Red
		} else if es.AvgFreeSpacePct <= 20 {
			freeSpaceColor = Yellow
		}

		avgUsedInodes := int64(0)
		avgFreeInodes := int64(0)
		for _, d := range es.Drives {
			avgUsedInodes += d.UsedInodes
			avgFreeInodes += d.FreeInodes
		}
		avgTotalInodes := avgUsedInodes + avgFreeInodes
		var avgInodesUsedPct float64
		if avgTotalInodes > 0 {
			avgInodesUsedPct = float64(avgUsedInodes) / float64(avgTotalInodes) * 100
		}
		inodesColor := Green
		if avgInodesUsedPct >= 95 {
			inodesColor = Red
		} else if avgInodesUsedPct >= 80 {
			inodesColor = Yellow
		}

		pager.Printf("  Pool %d, Erasure Set %d: Good disks: %s, Bad disks: %s, Scanning: %s, Avg Space Used: %s%.1f%%%s, Avg Free Space: %s%.1f%%%s, Avg Inodes Used: %s%.1f%%%s\n",
			es.PoolIdx, es.SetIdx, goodText, badText, scanningText,
			spaceUsedColor, es.AvgSpaceUsedPct, Reset,
			freeSpaceColor, es.AvgFreeSpacePct, Reset,
			inodesColor, avgInodesUsedPct, Reset)
	}
}

func printPoolsAndSets(pager *Pager, pools map[string]map[string]interface{}, poolSetDrives map[string][]DiskInfo, allPoolSetDrives map[string][]DiskInfo, config *Config) {
	var title string
	if config.ScanningMode {
		title = "MinIO Scanning Disks"
	} else if config.FailedMode {
		title = "MinIO Failed/Faulty Disks"
	} else {
		title = "MinIO Pool Information"
	}
	pager.Printf("%s%s from: %s%s\n", Bold, title, config.JSONFile, Reset)
	pager.Printf("================================================================================\n")

	for poolIdx, sets := range pools {
		// Check if pool has failed disks (for failed mode) - use all drives for checking
		poolHasFailed := false
		if config.FailedMode {
			for setIdx := range sets {
				key := fmt.Sprintf("%s:%s", poolIdx, setIdx)
				drives := allPoolSetDrives[key]
				if len(drives) == 0 {
					drives = poolSetDrives[key] // Fallback
				}
				for _, d := range drives {
					if d.State != "ok" {
						poolHasFailed = true
						break
					}
				}
				if poolHasFailed {
					break
				}
			}
			if !poolHasFailed {
				continue
			}
		}

		pager.Printf("%sPool %s:%s\n", Blue, poolIdx, Reset)
		for setIdx := range sets {
			key := fmt.Sprintf("%s:%s", poolIdx, setIdx)
			allDrives := poolSetDrives[key] // All drives (may be filtered by scanning/failed already)

			// For summary mode with failed, we need ALL drives to count properly
			// So we need to get them from allPoolSetDrives instead
			var drivesForCounting []DiskInfo
			if config.SummaryMode && config.FailedMode {
				// Get all drives from the original map for counting
				allKey := fmt.Sprintf("%s:%s", poolIdx, setIdx)
				drivesForCounting = allPoolSetDrives[allKey]
				if len(drivesForCounting) == 0 {
					// Fallback to poolSetDrives if not found
					drivesForCounting = allDrives
				}
			} else {
				drivesForCounting = allDrives
			}

			drives := allDrives

			if config.ScanningMode && len(drives) == 0 {
				continue
			}

			// Filter to only failed disks in failed mode (for summary mode)
			if config.FailedMode && config.SummaryMode {
				failedDrives := make([]DiskInfo, 0)
				for _, d := range drivesForCounting {
					if d.State != "ok" {
						failedDrives = append(failedDrives, d)
					}
				}
				if len(failedDrives) == 0 {
					continue
				}
				// For counting, use the filtered failed drives (to match Python behavior)
				drivesForCounting = failedDrives
			}

			if config.SummaryMode {
				good := 0
				bad := 0
				scanning := 0
				for _, d := range drivesForCounting {
					if d.State == "ok" {
						good++
					} else {
						bad++
					}
					if d.Scanning {
						scanning++
					}
				}

				// Filter by minimum bad disks threshold if specified
				if config.MinBadDisks != nil {
					if bad < *config.MinBadDisks {
						continue
					}
				}

				// Calculate averages - use all drives for averaging, not just filtered ones
				totalDrives := len(drivesForCounting)
				if totalDrives > 0 {
					avgTotalSpace := int64(0)
					avgUsedSpace := int64(0)
					avgFreeSpace := int64(0)
					avgUsedInodes := int64(0)
					avgFreeInodes := int64(0)
					for _, d := range drivesForCounting {
						avgTotalSpace += d.TotalSpace
						avgUsedSpace += d.UsedSpace
						avgFreeSpace += d.AvailableSpace
						avgUsedInodes += d.UsedInodes
						avgFreeInodes += d.FreeInodes
					}
					avgTotalSpace /= int64(totalDrives)
					avgUsedSpace /= int64(totalDrives)
					avgFreeSpace /= int64(totalDrives)
					avgUsedInodes /= int64(totalDrives)
					avgFreeInodes /= int64(totalDrives)

					avgTotalInodes := avgUsedInodes + avgFreeInodes
					var avgSpaceUsedPct, avgFreeSpacePct, avgInodesUsedPct float64
					if avgTotalSpace > 0 {
						avgSpaceUsedPct = float64(avgUsedSpace) / float64(avgTotalSpace) * 100
						avgFreeSpacePct = float64(avgFreeSpace) / float64(avgTotalSpace) * 100
					}
					if avgTotalInodes > 0 {
						avgInodesUsedPct = float64(avgUsedInodes) / float64(avgTotalInodes) * 100
					}

					spaceUsedColor := Green
					if avgSpaceUsedPct >= 95 {
						spaceUsedColor = Red
					} else if avgSpaceUsedPct >= 80 {
						spaceUsedColor = Yellow
					}
					freeSpaceColor := Green
					if avgFreeSpacePct <= 5 {
						freeSpaceColor = Red
					} else if avgFreeSpacePct <= 20 {
						freeSpaceColor = Yellow
					}
					inodesColor := Green
					if avgInodesUsedPct >= 95 {
						inodesColor = Red
					} else if avgInodesUsedPct >= 80 {
						inodesColor = Yellow
					}

					goodText := fmt.Sprintf("%d", good)
					if good > 0 {
						goodText = fmt.Sprintf("%s%d%s", Green, good, Reset)
					}
					badText := fmt.Sprintf("%d", bad)
					if bad > 0 {
						badText = fmt.Sprintf("%s%d%s", Red, bad, Reset)
					}
					scanningText := fmt.Sprintf("%d", scanning)
					if scanning > 0 {
						scanningText = fmt.Sprintf("%s%d%s", Yellow, scanning, Reset)
					}

					pager.Printf("  Erasure Set %s: Good disks: %s, Bad disks: %s, Scanning: %s, Avg Space Used: %s%.1f%%%s, Avg Free Space: %s%.1f%%%s, Avg Inodes Used: %s%.1f%%%s\n",
						setIdx, goodText, badText, scanningText,
						spaceUsedColor, avgSpaceUsedPct, Reset,
						freeSpaceColor, avgFreeSpacePct, Reset,
						inodesColor, avgInodesUsedPct, Reset)
				}
			} else {
				if len(drives) == 0 {
					continue
				}
				pager.Printf("  %sErasure Set %s:%s\n", Blue, setIdx, Reset)
				printTable(pager, drives, config)
				pager.Printf("\n")
			}
		}
	}
}

func printTable(pager *Pager, drives []DiskInfo, config *Config) {
	if len(drives) == 0 {
		return
	}

	headers := []string{"Pool", "Erasure Set", "Disk Index", "Server", "Disk Path", "State", "Scanning", "UUID", "Total Space", "Space Used", "Free Space", "Inodes Used", "Local", "Metrics"}

	rows := make([][]string, 0, len(drives))
	for _, drive := range drives {
		row := make([]string, len(headers))

		poolIdxStr := fmt.Sprintf("%d", drive.PoolIndex)
		setIdxStr := fmt.Sprintf("%d", drive.SetIndex)
		diskIdxStr := fmt.Sprintf("%v", drive.DiskIndex)

		serverParts := strings.Split(drive.Server, ".")
		serverName := serverParts[0]

		stateColor := Green
		if drive.State != "ok" {
			stateColor = Red
		}
		stateText := fmt.Sprintf("%s%s%s", stateColor, drive.State, Reset)

		scanningColor := Yellow
		if !drive.Scanning {
			scanningColor = Green
		}
		scanningText := fmt.Sprintf("%s%s%s", scanningColor, boolToYesNo(drive.Scanning), Reset)

		uuid := drive.UUID
		if len(uuid) > 16 {
			uuid = uuid[:16] + "..."
		}

		var totalSpaceStr, spaceUsedStr, freeSpaceStr string
		if drive.TotalSpace > 0 {
			totalGB := float64(drive.TotalSpace) / (1024 * 1024 * 1024)
			usedGB := float64(drive.UsedSpace) / (1024 * 1024 * 1024)
			freeGB := float64(drive.AvailableSpace) / (1024 * 1024 * 1024)

			usageColor := Green
			if drive.UsedSpacePct >= 95 {
				usageColor = Red
			} else if drive.UsedSpacePct >= 80 {
				usageColor = Yellow
			}
			freeColor := Green
			if drive.FreeSpacePct <= 5 {
				freeColor = Red
			} else if drive.FreeSpacePct <= 20 {
				freeColor = Yellow
			}

			totalSpaceStr = fmt.Sprintf("%.1fGB", totalGB)
			spaceUsedStr = fmt.Sprintf("%.1fGB (%s%.1f%%%s)", usedGB, usageColor, drive.UsedSpacePct, Reset)
			freeSpaceStr = fmt.Sprintf("%.1fGB (%s%.1f%%%s)", freeGB, freeColor, drive.FreeSpacePct, Reset)
		} else {
			totalSpaceStr = "N/A"
			spaceUsedStr = "N/A"
			freeSpaceStr = "N/A"
		}

		var inodeStr string
		if drive.UsedInodes > 0 {
			totalInodes := drive.UsedInodes + drive.FreeInodes
			inodePct := float64(drive.UsedInodes) / float64(totalInodes) * 100
			inodeColor := Green
			if inodePct >= 95 {
				inodeColor = Red
			} else if inodePct >= 80 {
				inodeColor = Yellow
			}
			inodeStr = fmt.Sprintf("%s (%s%.1f%%%s)", formatInt(drive.UsedInodes), inodeColor, inodePct, Reset)
		} else {
			inodeStr = "N/A"
		}

		localColor := Green
		if !drive.Local {
			localColor = Yellow
		}
		localText := fmt.Sprintf("%s%s%s", localColor, boolToYesNo(drive.Local), Reset)

		metricsStr := ""
		if drive.Metrics != nil {
			metricsStr = fmt.Sprintf("%v", drive.Metrics)
		}

		row[0] = fmt.Sprintf("%s%s%s", Blue, poolIdxStr, Reset)
		row[1] = fmt.Sprintf("%s%s%s", Blue, setIdxStr, Reset)
		row[2] = diskIdxStr
		row[3] = serverName
		row[4] = drive.Path
		row[5] = stateText
		row[6] = scanningText
		row[7] = uuid
		row[8] = totalSpaceStr
		row[9] = spaceUsedStr
		row[10] = freeSpaceStr
		row[11] = inodeStr
		row[12] = localText
		row[13] = metricsStr

		rows = append(rows, row)
	}

	// Calculate column widths
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = utf8.RuneCountInString(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			// Strip ANSI codes for width calculation
			cleanCell := stripANSI(cell)
			if w := utf8.RuneCountInString(cleanCell); w > widths[i] {
				widths[i] = w
			}
		}
	}

	// Print header
	pager.Printf("  ")
	for i, h := range headers {
		pager.Printf("%-*s", widths[i], h)
		if i < len(headers)-1 {
			pager.Printf("  ")
		}
	}
	pager.Printf("\n")

	// Print separator
	pager.Printf("  ")
	for i, w := range widths {
		pager.Printf("%s", strings.Repeat("-", w))
		if i < len(widths)-1 {
			pager.Printf("  ")
		}
	}
	pager.Printf("\n")

	// Print rows
	for _, row := range rows {
		pager.Printf("  ")
		for i, cell := range row {
			pager.Printf("%-*s", widths[i], cell)
			if i < len(row)-1 {
				pager.Printf("  ")
			}
		}
		pager.Printf("\n")
	}
}

func stripANSI(s string) string {
	var result strings.Builder
	inANSI := false
	for _, r := range s {
		if r == '\033' {
			inANSI = true
			continue
		}
		if inANSI {
			if r == 'm' {
				inANSI = false
			}
			continue
		}
		result.WriteRune(r)
	}
	return result.String()
}

func boolToYesNo(b bool) string {
	if b {
		return "Yes"
	}
	return "No"
}

func formatInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var result strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(r)
	}
	return result.String()
}
