package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "beegfs_quota"

// spaceUnits maps BeeGFS binary unit suffixes to byte multipliers.
var spaceUnits = map[string]float64{
	"B":   1,
	"KiB": 1024,
	"MiB": 1024 * 1024,
	"GiB": 1024 * 1024 * 1024,
	"TiB": 1024 * 1024 * 1024 * 1024,
	"PiB": 1024 * 1024 * 1024 * 1024 * 1024,
}

// inodeUnits maps BeeGFS SI suffixes to count multipliers.
var inodeUnits = map[string]float64{
	"":  1,
	"k": 1_000,
	"M": 1_000_000,
	"G": 1_000_000_000,
}

type quotaEntry struct {
	gid        string
	pool       string
	spaceUsed  float64
	spaceLimit float64
	inodeUsed  float64
	inodeLimit float64
}

type beegfsCollector struct {
	beegfsCmd string

	mu         sync.Mutex
	groupCache map[string]string // gid -> name
	cacheTime  time.Time

	spaceUsed     *prometheus.Desc
	spaceLimit    *prometheus.Desc
	inodeUsed     *prometheus.Desc
	inodeLimit    *prometheus.Desc
	spaceExceeded *prometheus.Desc
	inodeExceeded *prometheus.Desc
	scrapeSuccess *prometheus.Desc
	scrapeDuration *prometheus.Desc
}

func newBeegfsCollector(beegfsCmd string) *beegfsCollector {
	labels := []string{"unix_group", "gid", "pool"}
	return &beegfsCollector{
		beegfsCmd:  beegfsCmd,
		groupCache: make(map[string]string),
		spaceUsed: prometheus.NewDesc(
			namespace+"_space_used_bytes",
			"BeeGFS quota space currently used, in bytes.",
			labels, nil,
		),
		spaceLimit: prometheus.NewDesc(
			namespace+"_space_limit_bytes",
			"BeeGFS quota space limit, in bytes.",
			labels, nil,
		),
		inodeUsed: prometheus.NewDesc(
			namespace+"_inode_used_total",
			"BeeGFS quota inodes currently used.",
			labels, nil,
		),
		inodeLimit: prometheus.NewDesc(
			namespace+"_inode_limit_total",
			"BeeGFS quota inode limit.",
			labels, nil,
		),
		spaceExceeded: prometheus.NewDesc(
			namespace+"_space_exceeded",
			"1 if the group's space quota is at or above the limit, 0 otherwise.",
			labels, nil,
		),
		inodeExceeded: prometheus.NewDesc(
			namespace+"_inode_exceeded",
			"1 if the group's inode quota is at or above the limit, 0 otherwise.",
			labels, nil,
		),
		scrapeSuccess: prometheus.NewDesc(
			namespace+"_scrape_success",
			"1 if the last BeeGFS quota scrape succeeded, 0 otherwise.",
			nil, nil,
		),
		scrapeDuration: prometheus.NewDesc(
			namespace+"_scrape_duration_seconds",
			"Duration of the last BeeGFS quota scrape in seconds.",
			nil, nil,
		),
	}
}

func (c *beegfsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.spaceUsed
	ch <- c.spaceLimit
	ch <- c.inodeUsed
	ch <- c.inodeLimit
	ch <- c.spaceExceeded
	ch <- c.inodeExceeded
	ch <- c.scrapeSuccess
	ch <- c.scrapeDuration
}

func (c *beegfsCollector) Collect(ch chan<- prometheus.Metric) {
	start := time.Now()
	entries, err := c.fetchQuota()
	elapsed := time.Since(start).Seconds()

	ch <- prometheus.MustNewConstMetric(c.scrapeDuration, prometheus.GaugeValue, elapsed)

	if err != nil {
		log.Printf("scrape error: %v", err)
		ch <- prometheus.MustNewConstMetric(c.scrapeSuccess, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.scrapeSuccess, prometheus.GaugeValue, 1)

	for _, e := range entries {
		groupName := c.resolveGroup(e.gid)
		labels := []string{groupName, e.gid, e.pool}

		ch <- prometheus.MustNewConstMetric(c.spaceUsed, prometheus.GaugeValue, e.spaceUsed, labels...)
		ch <- prometheus.MustNewConstMetric(c.spaceLimit, prometheus.GaugeValue, e.spaceLimit, labels...)
		ch <- prometheus.MustNewConstMetric(c.inodeUsed, prometheus.GaugeValue, e.inodeUsed, labels...)
		ch <- prometheus.MustNewConstMetric(c.inodeLimit, prometheus.GaugeValue, e.inodeLimit, labels...)

		spaceExceeded := boolFloat(e.spaceLimit > 0 && e.spaceUsed >= e.spaceLimit)
		inodeExceeded := boolFloat(e.inodeLimit > 0 && e.inodeUsed >= e.inodeLimit)
		ch <- prometheus.MustNewConstMetric(c.spaceExceeded, prometheus.GaugeValue, spaceExceeded, labels...)
		ch <- prometheus.MustNewConstMetric(c.inodeExceeded, prometheus.GaugeValue, inodeExceeded, labels...)
	}
}

func (c *beegfsCollector) resolveGroup(gid string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Since(c.cacheTime) > time.Hour {
		c.refreshGroupCache()
	}

	if name, ok := c.groupCache[gid]; ok {
		return name
	}

	// Single lookup for groups added since last full refresh.
	out, err := exec.Command("getent", "group", gid).Output()
	if err == nil {
		parts := strings.SplitN(strings.TrimSpace(string(out)), ":", 4)
		if len(parts) >= 3 && parts[0] != "" {
			c.groupCache[gid] = parts[0]
			return parts[0]
		}
	}

	return gid
}

func (c *beegfsCollector) refreshGroupCache() {
	out, err := exec.Command("getent", "group").Output()
	if err != nil {
		log.Printf("getent group failed: %v", err)
		return
	}

	cache := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 4)
		if len(parts) >= 3 {
			cache[parts[2]] = parts[0] // gid -> name
		}
	}
	c.groupCache = cache
	c.cacheTime = time.Now()
	log.Printf("group cache refreshed: %d entries", len(cache))
}

func (c *beegfsCollector) fetchQuota() ([]quotaEntry, error) {
	out, err := exec.Command(c.beegfsCmd, "quota", "list-usage", "--gids", "all").Output()
	if err != nil {
		return nil, fmt.Errorf("beegfs command failed: %w", err)
	}

	var entries []quotaEntry
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "NAME") || strings.HasPrefix(line, "INFO:") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}

		// fields: NAME ID TYPE POOL SPACE INODE
		gid := fields[1]
		pool := fields[3]

		spaceUsed, spaceLimit, err := parseUsageField(fields[4], spaceUnits)
		if err != nil {
			log.Printf("skip gid %s: bad space field %q: %v", gid, fields[4], err)
			continue
		}

		inodeUsed, inodeLimit, err := parseUsageField(fields[5], inodeUnits)
		if err != nil {
			log.Printf("skip gid %s: bad inode field %q: %v", gid, fields[5], err)
			continue
		}

		entries = append(entries, quotaEntry{
			gid:        gid,
			pool:       pool,
			spaceUsed:  spaceUsed,
			spaceLimit: spaceLimit,
			inodeUsed:  inodeUsed,
			inodeLimit: inodeLimit,
		})
	}

	return entries, nil
}

// parseUsageField parses "used/limit" with unit suffixes using the provided unit map.
func parseUsageField(field string, units map[string]float64) (used, limit float64, err error) {
	parts := strings.SplitN(field, "/", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected used/limit, got %q", field)
	}
	if used, err = parseWithUnit(parts[0], units); err != nil {
		return 0, 0, fmt.Errorf("used: %w", err)
	}
	if limit, err = parseWithUnit(parts[1], units); err != nil {
		return 0, 0, fmt.Errorf("limit: %w", err)
	}
	return used, limit, nil
}

// parseWithUnit splits a string like "393.51GiB" or "524.41k" into number × unit.
func parseWithUnit(s string, units map[string]float64) (float64, error) {
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	val, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("not a number: %q", s[:i])
	}
	unit := s[i:]
	mult, ok := units[unit]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q", unit)
	}
	return val * mult, nil
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func main() {
	host := flag.String("host", "localhost", "Host address to listen on (use 0.0.0.0 to listen on all interfaces)")
	port := flag.Int("port", 9742, "Port to listen on")
	beegfsCmd := flag.String("beegfs", "beegfs", "Path to the beegfs CLI binary")
	flag.Parse()

	collector := newBeegfsCollector(*beegfsCmd)
	prometheus.MustRegister(collector)

	addr := fmt.Sprintf("%s:%d", *host, *port)
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Starting beegfs-quota-exporter on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
