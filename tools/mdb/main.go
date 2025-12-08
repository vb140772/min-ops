package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/madmin-go/v3"
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

// clusterStruct wraps Info message together with fields "Status" and "Error"
type clusterStruct struct {
	Status string             `json:"status"`
	Error  string             `json:"error,omitempty"`
	Info   madmin.InfoMessage `json:"info,omitempty"`
}

// Config holds command-line configuration
type Config struct {
	JSONFile          string
	SummaryMode       bool
	ScanningMode      bool
	PagerMode         bool
	FailedMode        bool
	LowSpaceThreshold *float64
	MinBadDisks       *int
	TrimDomain        string
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
	Metrics        *madmin.DiskMetrics
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

// Pager handles paginated output using bubbletea and viewport
type Pager struct {
	enabled bool
	buffer  *strings.Builder
}

func NewPager(enabled bool) *Pager {
	return &Pager{
		enabled: enabled,
		buffer:  &strings.Builder{},
	}
}

func (p *Pager) Printf(format string, args ...interface{}) {
	text := fmt.Sprintf(format, args...)
	if p.enabled {
		p.buffer.WriteString(text)
	} else {
		fmt.Printf(format, args...)
	}
}

// Show displays the buffered output using bubbletea viewport
func (p *Pager) Show() {
	if !p.enabled {
		return
	}

	content := p.buffer.String()
	if content == "" {
		return
	}

	pager := newViewportModel(content)
	if err := tea.NewProgram(pager, tea.WithAltScreen()).Start(); err != nil {
		fmt.Print(content)
	}
}

// viewportModel holds the state for the viewport pager
type viewportModel struct {
	viewport viewport.Model
	content  string
}

func newViewportModel(content string) viewportModel {
	vp := viewport.New(0, 0)
	vp.SetContent(content)

	return viewportModel{
		viewport: vp,
		content:  content,
	}
}

func (m viewportModel) Init() tea.Cmd {
	// Request initial window size
	return tea.WindowSize()
}

func (m viewportModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.viewport.Width = msg.Width
		m.viewport.Height = msg.Height - 1 // Reserve space for help text
		m.viewport.SetContent(m.content) // Re-set content with new dimensions
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "Q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			m.viewport.LineUp(1)
			return m, nil
		case "down", "j":
			m.viewport.LineDown(1)
			return m, nil
		case " ":
			// Space scrolls half a page down (more convenient for quick navigation)
			m.viewport.HalfViewDown()
			return m, nil
		case "pgdown", "ctrl+f":
			m.viewport.HalfViewDown()
			return m, nil
		case "pgup", "ctrl+b":
			m.viewport.HalfViewUp()
			return m, nil
		case "home", "g":
			m.viewport.GotoTop()
			return m, nil
		case "end", "G":
			m.viewport.GotoBottom()
			return m, nil
		}
	}

	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m viewportModel) View() string {
	helpText := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		Render(" ↑/↓/j/k: scroll  space: page down  g/G: top/bottom  q: quit")
	
	return fmt.Sprintf("%s\n%s", m.viewport.View(), helpText)
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
	// Check for --help or -h flag anywhere in arguments
	for _, arg := range os.Args[1:] {
		if arg == "--help" || arg == "-h" {
			cli.ShowAppHelp(ctx)
			return nil
		}
	}
	
	config := parseFlags(ctx)

	if config.JSONFile == "" {
		cli.ShowAppHelp(ctx)
		return fmt.Errorf("JSON file is required")
	}

	infoStruct, err := loadJSON(config.JSONFile)
	if err != nil {
		return fmt.Errorf("failed to load JSON file '%s': %v", config.JSONFile, err)
	}

	servers := infoStruct.Info.Servers
	pools := extractPoolsFromServers(servers)
	parityDisks := int(infoStruct.Info.Backend.StandardSCParity)
	if parityDisks == 0 {
		parityDisks = 2 // Default to EC-2
	}

	pager := NewPager(config.PagerMode)
	
	pager.Printf("%sDetected Erasure Coding Configuration: EC:%d%s\n", Bold, parityDisks, Reset)
	pager.Printf("\n")

	poolSetDrives := make(map[string][]DiskInfo)
	allPoolSetDrives := make(map[string][]DiskInfo) // For capacity calculations (all drives)
	stats := ClusterStats{ParityDisks: parityDisks}

	// Process all drives
	for _, server := range servers {
		drives := getDrives(server, config.TrimDomain)
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

	stats.DeploymentID = infoStruct.Info.DeploymentID

	// Print cluster summary (use all drives for capacity calculation)
	printClusterSummary(pager, stats, pools, allPoolSetDrives, servers, infoStruct, config)

	// Handle special modes
	if config.FailedMode && !config.SummaryMode {
		printFailedDisksTable(pager, poolSetDrives, config)
		pager.Show()
		return nil
	}

	if config.SummaryMode && config.LowSpaceThreshold != nil {
		printLowSpaceErasureSets(pager, pools, poolSetDrives, *config.LowSpaceThreshold, config)
		pager.Show()
		return nil
	}

	// Print pools and erasure sets
	printPoolsAndSets(pager, pools, poolSetDrives, allPoolSetDrives, config, servers)
	
	// Show the pager if enabled
	pager.Show()
	
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
	cli.StringFlag{
		Name:  "trim-domain",
		Usage: "Trim domain suffix from endpoint names for cleaner display (e.g., '.example.com')",
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

	// Get all arguments manually to handle flags that come after positional args
	// This handles both: mdb --pager file.json and mdb file.json --pager
	allArgs := os.Args[1:] // Skip program name
	
	// Find JSON file (first non-flag argument)
	var jsonFile string
	for _, arg := range allArgs {
		if !strings.HasPrefix(arg, "-") && arg != "" {
			jsonFile = arg
			break
		}
	}
	
	// Manually parse all flags from command line
	// This ensures flags work whether they come before or after the file
	for _, arg := range allArgs {
		if arg == "--pager" {
			config.PagerMode = true
		}
		if arg == "--summary" {
			config.SummaryMode = true
		}
		if arg == "--scanning" {
			config.ScanningMode = true
		}
		if arg == "--failed" {
			config.FailedMode = true
		}
		if strings.HasPrefix(arg, "--trim-domain=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				config.TrimDomain = parts[1]
			}
		}
		if strings.HasPrefix(arg, "--low-space=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				if val, err := strconv.ParseFloat(parts[1], 64); err == nil {
					config.LowSpaceThreshold = &val
				}
			}
		}
		if strings.HasPrefix(arg, "--min-bad-disks=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				if val, err := strconv.Atoi(parts[1]); err == nil && val >= 0 {
					config.MinBadDisks = &val
				}
			}
		}
	}
	
	// Also check context (in case flags were parsed before positional args)
	// Use OR to combine with manually parsed flags
	config.SummaryMode = config.SummaryMode || ctx.Bool("summary")
	config.ScanningMode = config.ScanningMode || ctx.Bool("scanning")
	config.PagerMode = config.PagerMode || ctx.Bool("pager")
	config.FailedMode = config.FailedMode || ctx.Bool("failed")
	if config.TrimDomain == "" {
		config.TrimDomain = ctx.String("trim-domain")
	}
	
	config.JSONFile = jsonFile
	
	// Use ctx.Args() as fallback if manual parsing didn't find the file
	if config.JSONFile == "" && ctx.NArg() >= 1 {
		config.JSONFile = ctx.Args().Get(0)
	}

	// Validate flag dependencies
	if config.LowSpaceThreshold != nil && !config.SummaryMode {
		console.Fatalln(fmt.Errorf("--low-space option requires --summary mode"))
	}
	if config.MinBadDisks != nil && (!config.SummaryMode || !config.FailedMode) {
		console.Fatalln(fmt.Errorf("--min-bad-disks option requires --summary --failed mode"))
	}

	return config
}

func loadJSON(filename string) (*clusterStruct, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("file '%s' not found: %v", filename, err)
	}
	defer file.Close()

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file '%s': %v", filename, err)
	}

	// Check for raw prefix and remove it (like stats does)
	data = []byte(strings.Replace(string(data), `{"version":"3"}`, "", 1))

	infoStruct := clusterStruct{}
	err = json.Unmarshal(data, &infoStruct)
	if err != nil {
		// Try with minio wrapper format
		anotherFormat := struct {
			InfoStruct clusterStruct `json:"minio"`
		}{}
		err = json.Unmarshal(data, &anotherFormat)
		if err != nil {
			// Try NDJSON format
			return loadNDJSON(filename)
		}
		return &anotherFormat.InfoStruct, nil
	}

	// If there is no server found on the first try, trying with different format
	// data could be from subnet diagnostics page
	if len(infoStruct.Info.Servers) == 0 {
		anotherFormat := struct {
			InfoStruct clusterStruct `json:"minio"`
		}{}
		err = json.Unmarshal(data, &anotherFormat)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
		}
		return &anotherFormat.InfoStruct, nil
	}

	return &infoStruct, nil
}

func loadNDJSON(filename string) (*clusterStruct, error) {
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
		var infoStruct clusterStruct
		if err := json.Unmarshal(line, &infoStruct); err == nil {
			if len(infoStruct.Info.Servers) > 0 {
				return &infoStruct, nil
			}
		}
		// Try with minio wrapper
		anotherFormat := struct {
			InfoStruct clusterStruct `json:"minio"`
		}{}
		if err := json.Unmarshal(line, &anotherFormat); err == nil {
			if len(anotherFormat.InfoStruct.Info.Servers) > 0 {
				return &anotherFormat.InfoStruct, nil
			}
		}
	}
	return nil, fmt.Errorf("no valid JSON found")
}

func extractPoolsFromServers(servers []madmin.ServerProperties) map[string]map[string]interface{} {
	pools := make(map[string]map[string]interface{})

	// Build pools structure from drive information
	for _, server := range servers {
		for _, disk := range server.Disks {
			poolIdx := disk.PoolIndex
			setIdx := disk.SetIndex
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

func getErasureCodingConfig(servers []madmin.ServerProperties) int {
	// Try to determine from backend info if available in infoStruct
	// For now, use standardSCParity if available, otherwise default
	// This will be set from infoStruct.Info.Backend.StandardSCParity in mainMDB
	return 2 // Default to EC-2, will be updated if available from backend
}

func getDrives(server madmin.ServerProperties, trimDomain string) []DiskInfo {
	serverEndpoint := trimDomainData(server.Endpoint, trimDomain)
	drives := make([]DiskInfo, 0, len(server.Disks))

	for _, disk := range server.Disks {
		diskInfo := DiskInfo{
			Server:         serverEndpoint,
			Path:           disk.DrivePath,
			State:          disk.State,
			UUID:           disk.UUID,
			Scanning:       disk.Healing,
			DiskIndex:      disk.DiskIndex,
			TotalSpace:     int64(disk.TotalSpace),
			UsedSpace:      int64(disk.UsedSpace),
			AvailableSpace: int64(disk.AvailableSpace),
			UsedInodes:     int64(disk.UsedInodes),
			FreeInodes:     int64(disk.FreeInodes),
			Local:          disk.Local,
			Metrics:        disk.Metrics,
			PoolIndex:      disk.PoolIndex,
			SetIndex:       disk.SetIndex,
		}

		// Extract path from endpoint if path is not provided
		if diskInfo.Path == "" && disk.Endpoint != "" {
			diskInfo.Path = extractPathFromEndpoint(disk.Endpoint)
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

func printClusterSummary(pager *Pager, stats ClusterStats, pools map[string]map[string]interface{}, poolSetDrives map[string][]DiskInfo, servers []madmin.ServerProperties, infoStruct *clusterStruct, config *Config) {
	pager.Printf("%sSummary%s\n", Bold, Reset)

	if stats.DeploymentID != "" {
		pager.Printf("  Deployment ID: %s\n", stats.DeploymentID)
	} else {
		pager.Printf("  Deployment ID: Not available\n")
	}

	// Backend configuration
	if infoStruct != nil && len(infoStruct.Info.Backend.TotalSets) > 0 {
		totalSetsStr := fmt.Sprintf("%v", infoStruct.Info.Backend.TotalSets)
		pager.Printf("  Backend: totalSets=%s, standardSCParity=%d, rrSCParity=%d, drivesPerSet=%v\n",
			totalSetsStr, infoStruct.Info.Backend.StandardSCParity, infoStruct.Info.Backend.RRSCParity, infoStruct.Info.Backend.DrivesPerSet)
	}

	pager.Printf("\n")

	pager.Printf("  Total Disks: %d\n", stats.TotalDisks)
	pager.Printf("  Scanning Disks: %s%d%s\n", Yellow, stats.ScanningDisks, Reset)
	pager.Printf("  Healthy Disks: %s%d%s\n", Green, stats.OkDisks, Reset)
	pager.Printf("  Problem Disks: %s%d%s\n", Red, stats.BadDisks, Reset)

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
		pager.Printf("  Health: %s%.1f%%%s\n", healthColor, healthPct, Reset)
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

		pager.Printf("  Raw Capacity: %.1f TB\n", totalTB)
		pager.Printf("  Usable Capacity: %.1f TB\n", usableTB)
		pager.Printf("  Used Space: %.1f TB (%s%.1f%%%s)\n", usedTB, usageColor, usagePct, Reset)
		pager.Printf("  Available Space: %.1f TB\n", usableTB-usedTB)
	}

	pager.Printf("  Pools: %d\n", len(pools))
	pager.Printf("  Servers: %d\n", len(servers))

	totalErasureSets := 0
	for _, sets := range pools {
		totalErasureSets += len(sets)
	}
	pager.Printf("  Erasure Sets: %d\n", totalErasureSets)

	// Scanner status
	if infoStruct != nil {
		pager.Printf("  Scanner Status: buckets=%d, objects=%d, versions=%d, deletemarkers=%d, usage=%s\n",
			infoStruct.Info.Buckets.Count, infoStruct.Info.Objects.Count,
			infoStruct.Info.Versions.Count, infoStruct.Info.DeleteMarkers.Count,
			humanize.IBytes(infoStruct.Info.Usage.Size))
	}

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

// printServerInfo prints server metadata for all servers in table format
func printServerInfo(pager *Pager, servers []madmin.ServerProperties, pools map[string]map[string]interface{}, trimDomain string) {
	pager.Printf("%sServers%s\n", Bold, Reset)
	
	// Collect all unique servers and determine which pools each belongs to
	serversData := make(map[string]struct {
		server  madmin.ServerProperties
		pools   []int
	})
	
	// Build map of valid pool indices from pools map
	validPools := make(map[int]bool)
	for poolKey := range pools {
		if poolIdx, err := strconv.Atoi(poolKey); err == nil {
			validPools[poolIdx] = true
		}
	}

	// Build map of servers to their pool membership
	for _, server := range servers {
		endpointName := trimDomainData(server.Endpoint, trimDomain)
		
		// Collect all pools this server belongs to by checking its disks
		// Only include pools that exist in the valid pools map
		poolSet := make(map[int]bool)
		for _, disk := range server.Disks {
			if validPools[disk.PoolIndex] {
				poolSet[disk.PoolIndex] = true
			}
		}
		
		// Convert pool set to sorted slice
		var poolList []int
		for poolIdx := range poolSet {
			poolList = append(poolList, poolIdx)
		}
		sort.Ints(poolList)
		
		// Store or update server data
		if existing, exists := serversData[endpointName]; exists {
			// Merge pool lists and deduplicate
			allPools := make(map[int]bool)
			for _, p := range existing.pools {
				allPools[p] = true
			}
			for _, p := range poolList {
				allPools[p] = true
			}
			var mergedPools []int
			for p := range allPools {
				mergedPools = append(mergedPools, p)
			}
			sort.Ints(mergedPools)
			serversData[endpointName] = struct {
				server  madmin.ServerProperties
				pools   []int
			}{server: existing.server, pools: mergedPools}
		} else {
			serversData[endpointName] = struct {
				server  madmin.ServerProperties
				pools   []int
			}{server: server, pools: poolList}
		}
	}

	if len(serversData) == 0 {
		pager.Printf("\n")
		return
	}

	// Get sorted server names (natural/alphanumeric sort)
	serverNames := make([]string, 0, len(serversData))
	for name := range serversData {
		serverNames = append(serverNames, name)
	}
	sort.Slice(serverNames, func(i, j int) bool {
		return naturalLess(serverNames[i], serverNames[j])
	})

	// Prepare table data
	headers := []string{"Pool", "Server", "State", "Edition", "Version", "Commit ID", "Memory", "ILM Status", "Uptime"}
	rows := make([][]string, 0, len(serverNames))

	for _, serverName := range serverNames {
		data := serversData[serverName]
		server := data.server

		// Format pool list
		var poolStr string
		if len(data.pools) == 0 {
			poolStr = "N/A"
		} else {
			poolStrs := make([]string, len(data.pools))
			for i, p := range data.pools {
				poolStrs[i] = strconv.Itoa(p)
			}
			poolStr = strings.Join(poolStrs, ",")
		}

		// Color code state
		stateColor := Green
		if server.State == "offline" {
			stateColor = Red
		}
		stateText := fmt.Sprintf("%s%s%s", stateColor, server.State, Reset)

		// Format commit ID (use full commit ID, no truncation)
		commitID := server.CommitID

		// Format ILM status
		ilmStatus := "false"
		if server.ILMExpiryInProgress {
			ilmStatus = "true"
		}

		// Format uptime
		uptime := humanizeDuration(time.Duration(server.Uptime) * time.Second)

		row := make([]string, len(headers))
		row[0] = poolStr
		row[1] = serverName
		row[2] = stateText
		row[3] = server.Edition
		row[4] = server.Version
		row[5] = commitID
		row[6] = humanize.IBytes(server.MemStats.Alloc)
		row[7] = ilmStatus
		if server.State == "offline" {
			row[8] = "N/A"
		} else {
			row[8] = uptime
		}

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
		pager.Printf("%s", padString(h, widths[i]))
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
			pager.Printf("%s", padString(cell, widths[i]))
			if i < len(row)-1 {
				pager.Printf("  ")
			}
		}
		pager.Printf("\n")
	}
	pager.Printf("\n")
}

func printPoolsAndSets(pager *Pager, pools map[string]map[string]interface{}, poolSetDrives map[string][]DiskInfo, allPoolSetDrives map[string][]DiskInfo, config *Config, servers []madmin.ServerProperties) {
	// Print server information once for all pools
	printServerInfo(pager, servers, pools, config.TrimDomain)

	// Collect all drives from all pools and erasure sets
	allDrives := make([]DiskInfo, 0)
	
	// For summary mode, collect erasure set statistics and display in table format
	if config.SummaryMode {
		type ErasureSetSummary struct {
			PoolIndex       int
			SetIndex        int
			GoodDisks       int
			BadDisks        int
			ScanningDisks   int
			AvgSpaceUsedPct float64
			AvgFreeSpacePct float64
			AvgInodesUsedPct float64
		}
		
		erasureSetSummaries := make([]ErasureSetSummary, 0)
		
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

			for setIdx := range sets {
				key := fmt.Sprintf("%s:%s", poolIdx, setIdx)
				allDrivesForSet := poolSetDrives[key] // All drives (may be filtered by scanning/failed already)

				// For summary mode with failed, we need ALL drives to count properly
				// So we need to get them from allPoolSetDrives instead
				var drivesForCounting []DiskInfo
				if config.FailedMode {
					// Get all drives from the original map for counting
					allKey := fmt.Sprintf("%s:%s", poolIdx, setIdx)
					drivesForCounting = allPoolSetDrives[allKey]
					if len(drivesForCounting) == 0 {
						// Fallback to poolSetDrives if not found
						drivesForCounting = allDrivesForSet
					}
				} else {
					drivesForCounting = allDrivesForSet
				}

				if config.ScanningMode && len(allDrivesForSet) == 0 {
					continue
				}

				// Filter to only failed disks in failed mode (for summary mode)
				if config.FailedMode {
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

					poolIdxInt, _ := strconv.Atoi(poolIdx)
					setIdxInt, _ := strconv.Atoi(setIdx)
					
					erasureSetSummaries = append(erasureSetSummaries, ErasureSetSummary{
						PoolIndex:        poolIdxInt,
						SetIndex:         setIdxInt,
						GoodDisks:        good,
						BadDisks:         bad,
						ScanningDisks:    scanning,
						AvgSpaceUsedPct:  avgSpaceUsedPct,
						AvgFreeSpacePct:  avgFreeSpacePct,
						AvgInodesUsedPct: avgInodesUsedPct,
					})
				}
			}
		}
		
		// Sort erasure sets by Pool and Erasure Set
		sort.Slice(erasureSetSummaries, func(i, j int) bool {
			if erasureSetSummaries[i].PoolIndex != erasureSetSummaries[j].PoolIndex {
				return erasureSetSummaries[i].PoolIndex < erasureSetSummaries[j].PoolIndex
			}
			return erasureSetSummaries[i].SetIndex < erasureSetSummaries[j].SetIndex
		})
		
		// Print Erasure Sets table
		if len(erasureSetSummaries) > 0 {
			pager.Printf("%sErasure Sets%s\n", Bold, Reset)
			
			headers := []string{"Pool", "Erasure Set", "Good Disks", "Bad Disks", "Scanning", "Avg Space Used", "Avg Free Space", "Avg Inodes Used"}
			rows := make([][]string, 0, len(erasureSetSummaries))
			
			for _, es := range erasureSetSummaries {
				row := make([]string, len(headers))
				
				poolIdxStr := fmt.Sprintf("%d", es.PoolIndex)
				setIdxStr := fmt.Sprintf("%d", es.SetIndex)
				
				goodText := fmt.Sprintf("%d", es.GoodDisks)
				if es.GoodDisks > 0 {
					goodText = fmt.Sprintf("%s%d%s", Green, es.GoodDisks, Reset)
				}
				
				badText := fmt.Sprintf("%d", es.BadDisks)
				if es.BadDisks > 0 {
					badText = fmt.Sprintf("%s%d%s", Red, es.BadDisks, Reset)
				}
				
				scanningText := fmt.Sprintf("%d", es.ScanningDisks)
				if es.ScanningDisks > 0 {
					scanningText = fmt.Sprintf("%s%d%s", Yellow, es.ScanningDisks, Reset)
				}
				
				spaceUsedColor := Green
				if es.AvgSpaceUsedPct >= 95 {
					spaceUsedColor = Red
				} else if es.AvgSpaceUsedPct >= 80 {
					spaceUsedColor = Yellow
				}
				spaceUsedText := fmt.Sprintf("%s%.1f%%%s", spaceUsedColor, es.AvgSpaceUsedPct, Reset)
				
				freeSpaceColor := Green
				if es.AvgFreeSpacePct <= 5 {
					freeSpaceColor = Red
				} else if es.AvgFreeSpacePct <= 20 {
					freeSpaceColor = Yellow
				}
				freeSpaceText := fmt.Sprintf("%s%.1f%%%s", freeSpaceColor, es.AvgFreeSpacePct, Reset)
				
				inodesColor := Green
				if es.AvgInodesUsedPct >= 95 {
					inodesColor = Red
				} else if es.AvgInodesUsedPct >= 80 {
					inodesColor = Yellow
				}
				inodesText := fmt.Sprintf("%s%.1f%%%s", inodesColor, es.AvgInodesUsedPct, Reset)
				
				row[0] = fmt.Sprintf("%s%s%s", Blue, poolIdxStr, Reset)
				row[1] = fmt.Sprintf("%s%s%s", Blue, setIdxStr, Reset)
				row[2] = goodText
				row[3] = badText
				row[4] = scanningText
				row[5] = spaceUsedText
				row[6] = freeSpaceText
				row[7] = inodesText
				
				rows = append(rows, row)
			}
			
			// Calculate column widths
			widths := make([]int, len(headers))
			for i, h := range headers {
				widths[i] = utf8.RuneCountInString(h)
			}
			for _, row := range rows {
				for i, cell := range row {
					cleanCell := stripANSI(cell)
					if w := utf8.RuneCountInString(cleanCell); w > widths[i] {
						widths[i] = w
					}
				}
			}
			
			// Print header
			pager.Printf("  ")
			for i, h := range headers {
				pager.Printf("%s", padString(h, widths[i]))
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
			
			// Print rows with spacing
			for _, row := range rows {
				pager.Printf("  ")
				for i, cell := range row {
					pager.Printf("%s", padString(cell, widths[i]))
					if i < len(row)-1 {
						pager.Printf("  ")
					}
				}
				pager.Printf("\n")
			}
			pager.Printf("\n")
		}
	}

	// Collect all drives for the table (for non-summary mode or if we also want table in summary mode)
	if !config.SummaryMode {
		for _, drives := range poolSetDrives {
			allDrives = append(allDrives, drives...)
		}
	}

	// Sort all drives by Pool, Erasure Set, Disk Index
	sort.Slice(allDrives, func(i, j int) bool {
		if allDrives[i].PoolIndex != allDrives[j].PoolIndex {
			return allDrives[i].PoolIndex < allDrives[j].PoolIndex
		}
		if allDrives[i].SetIndex != allDrives[j].SetIndex {
			return allDrives[i].SetIndex < allDrives[j].SetIndex
		}
		// Compare DiskIndex - handle interface{} type
		diStr := fmt.Sprintf("%v", allDrives[i].DiskIndex)
		djStr := fmt.Sprintf("%v", allDrives[j].DiskIndex)
		// Try numeric comparison first
		if di, err1 := strconv.Atoi(diStr); err1 == nil {
			if dj, err2 := strconv.Atoi(djStr); err2 == nil {
				return di < dj
			}
		}
		return diStr < djStr
	})

	// Print single table with all drives
	if len(allDrives) > 0 {
		pager.Printf("%sDrives%s\n", Bold, Reset)
		printTable(pager, allDrives, config)
		pager.Printf("\n")
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

		metricsStr := formatMetrics(drive.Metrics)

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
		pager.Printf("%s", padString(h, widths[i]))
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
			pager.Printf("%s", padString(cell, widths[i]))
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

// padString pads a string to the specified width, accounting for ANSI codes
func padString(s string, width int) string {
	visibleWidth := utf8.RuneCountInString(stripANSI(s))
	if visibleWidth >= width {
		return s
	}
	padding := width - visibleWidth
	return s + strings.Repeat(" ", padding)
}

func boolToYesNo(b bool) string {
	if b {
		return "Yes"
	}
	return "No"
}

// naturalLess compares two strings using natural/alphanumeric sorting
// This ensures that "rack2" comes before "rack10"
func naturalLess(a, b string) bool {
	aRunes := []rune(a)
	bRunes := []rune(b)
	
	i, j := 0, 0
	for i < len(aRunes) && j < len(bRunes) {
		aRune := aRunes[i]
		bRune := bRunes[j]
		
		// If both are digits, compare as numbers
		if aRune >= '0' && aRune <= '9' && bRune >= '0' && bRune <= '9' {
			// Extract full number from both strings
			aNumStr := ""
			bNumStr := ""
			
			// Extract number from a
			for i < len(aRunes) && aRunes[i] >= '0' && aRunes[i] <= '9' {
				aNumStr += string(aRunes[i])
				i++
			}
			
			// Extract number from b
			for j < len(bRunes) && bRunes[j] >= '0' && bRunes[j] <= '9' {
				bNumStr += string(bRunes[j])
				j++
			}
			
			// Compare as numbers
			aNum, errA := strconv.Atoi(aNumStr)
			bNum, errB := strconv.Atoi(bNumStr)
			
			if errA == nil && errB == nil {
				if aNum != bNum {
					return aNum < bNum
				}
				continue
			}
			
			// Fallback to string comparison if conversion fails
			if aNumStr != bNumStr {
				return aNumStr < bNumStr
			}
			continue
		}
		
		// Compare as runes (case-insensitive)
		aLower := aRune
		bLower := bRune
		if aLower >= 'A' && aLower <= 'Z' {
			aLower += 32
		}
		if bLower >= 'A' && bLower <= 'Z' {
			bLower += 32
		}
		
		if aLower != bLower {
			return aLower < bLower
		}
		
		i++
		j++
	}
	
	// If we've exhausted one string, the shorter one comes first
	return len(aRunes) < len(bRunes)
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

// trimDomainData trims domain suffix from endpoint for cleaner display
func trimDomainData(endpoint, domainString string) string {
	// Normalize endpoint to extract host (remove scheme, path and port)
	host := endpoint

	// If endpoint contains a scheme or a path, try parsing it as a URL
	if strings.Contains(host, "://") {
		if u, err := url.Parse(host); err == nil {
			host = u.Host
		}
	} else if strings.Contains(host, "/") {
		// try parsing by adding a scheme so url.Parse treats the first part as host
		if u, err := url.Parse("http://" + host); err == nil {
			host = u.Host
		}
	}

	// Strip port if present (handles host:port and [ipv6]:port)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	// If host is an IP address (v4 or v6), return it as-is
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		return ip.String()
	}

	// Fallback to previous behaviour for domain names
	if domainString == "" {
		return strings.SplitN(host, ".", 2)[0]
	}
	return strings.TrimSuffix(strings.TrimSuffix(host, domainString), ".")
}

// humanizeDuration humanizes time.Duration output to a meaningful value
func humanizeDuration(duration time.Duration) string {
	if duration.Seconds() < 60.0 {
		return fmt.Sprintf("%d seconds", int64(duration.Seconds()))
	}
	if duration.Minutes() < 60.0 {
		remainingSeconds := math.Mod(duration.Seconds(), 60)
		return fmt.Sprintf("%d minutes %d seconds", int64(duration.Minutes()), int64(remainingSeconds))
	}
	if duration.Hours() < 24.0 {
		remainingMinutes := math.Mod(duration.Minutes(), 60)
		remainingSeconds := math.Mod(duration.Seconds(), 60)
		return fmt.Sprintf("%d hours %d minutes %d seconds",
			int64(duration.Hours()), int64(remainingMinutes), int64(remainingSeconds))
	}
	remainingHours := math.Mod(duration.Hours(), 24)
	remainingMinutes := math.Mod(duration.Minutes(), 60)
	remainingSeconds := math.Mod(duration.Seconds(), 60)
	return fmt.Sprintf("%d days %d hours %d minutes %d seconds",
		int64(duration.Hours()/24), int64(remainingHours),
		int64(remainingMinutes), int64(remainingSeconds))
}

// formatMetrics formats disk metrics in compact format
func formatMetrics(metrics *madmin.DiskMetrics) string {
	if metrics == nil {
		return ""
	}

	metricBuilder := strings.Builder{}
	builderFn := func(key string, value uint64) {
		if value == 0 {
			return
		}
		if metricBuilder.Len() > 0 {
			metricBuilder.WriteString(", ")
		}
		metricBuilder.WriteString(fmt.Sprintf("%s=%d", key, value))
	}

	builderFn("tokens", uint64(metrics.TotalTokens))
	builderFn("write", metrics.TotalWrites)
	builderFn("del", metrics.TotalDeletes)
	builderFn("waiting", uint64(metrics.TotalWaiting))
	builderFn("tout", metrics.TotalErrorsTimeout)
	if metrics.TotalErrorsTimeout != metrics.TotalErrorsAvailability {
		builderFn("err", metrics.TotalErrorsAvailability)
	}

	if metricBuilder.Len() > 0 {
		return fmt.Sprintf("[%s]", metricBuilder.String())
	}
	return ""
}
