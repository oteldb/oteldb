// Command embedded-bench orchestrates a realistic PromQL benchmark of oteldb's embedded storage
// engine over HTTP, replacing the previous run.sh. It builds the oteldb (and otelbench) binaries on
// the host — where the oteldb repo's go.mod replace directives resolve the local storage /
// promql-engine sources — then drives a thin docker-compose stack (live node_exporter → vmagent as
// BENCH_NODES hosts → two oteldb --embedded instances off the same ingest stream: the `file` backend
// and the go-faster/fs `s3` backend, capped) and runs the canonical query suite against each via
// otelbench. Numbers are comparable to the oteldb / oteldb-s3 columns of
// /src/oteldb/benchmark/results/REPORT.md. Alongside the latency numbers it samples each container's
// peak resident set size (RSS) and renders a side-by-side REPORT.md.
//
// Run from this directory:
//
//	go run .                                    # nodes=100, prewarm 2m30s, 20 runs/query, both backends
//	go run . -s3=false                          # bench only the file backend
//	go run . -prewarm 70m                       # also populate the [30m]/[1h] instant selectors
//	go run . -gomaxprocs 2 -gomemlimit 1GiB     # tighten caps
//
// Run `go run . -h` for all flags.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	nodes      int
	prewarm    time.Duration
	runs       int
	warmup     int
	lookback   time.Duration
	healthWait time.Duration

	gomaxprocs string
	gomemlimit string
	goarch     string

	cleanup bool
	s3      bool

	concurrency int

	addr     string
	addrS3   string
	queries  string
	benchDir string
	repoRoot string
}

// target is one oteldb instance under test: the `file` backend or the S3 (go-faster/fs) backend.
// Both run off the same live ingest stream; the orchestrator benches each and reports side by side.
type target struct {
	name      string // "file" | "s3" — the column label in REPORT.md.
	service   string // docker compose service name (for RSS sampling + log tailing).
	promAddr  string // PromQL HTTP API base, e.g. http://127.0.0.1:9090.
	healthURL string // /liveness URL for the health poll.
}

// targets returns the instances to bench: always `file`, plus `s3` unless it was disabled with
// -s3=false. Health URLs are the PromQL address with the port swapped to the published health port
// (13133 for file, 13134 for s3), matching docker-compose.yml.
func (cfg config) targets() []target {
	ts := []target{{
		name:      "file",
		service:   "oteldb",
		promAddr:  cfg.addr,
		healthURL: strings.Replace(cfg.addr, ":9090", ":13133", 1) + "/liveness",
	}}
	if cfg.s3 {
		ts = append(ts, target{
			name:      "s3",
			service:   "oteldb-s3",
			promAddr:  cfg.addrS3,
			healthURL: strings.Replace(cfg.addrS3, ":9093", ":13134", 1) + "/liveness",
		})
	}
	return ts
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "!! %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Build oteldb + otelbench on the host (the only compile step). The image's Dockerfile just
	// COPYs ./oteldb in; otelbench is invoked as a host binary so the loop doesn't recompile it.
	if err := buildBinaries(cfg); err != nil {
		return err
	}

	// Full bring-up. Tear down on exit unless -cleanup=false.
	defer maybeCleanup(cfg)
	if err := fullBringup(cfg); err != nil {
		return err
	}

	// Prewarm: let vmagent remote-write enough history that the [now-lookback, now] window the range
	// queries scan is fully populated with flushed parts. The default (2m30s) covers the 120s lookback;
	// the [30m]/[1h] instant selectors (topk_free_mem, load_quantile) still want -prewarm 70m to be
	// fully populated — oteldb still answers over whatever has arrived (--allow-empty keeps it going).
	fmt.Printf(">> Prewarming ingest for %s (nodes=%d ⇒ ~%d series)\n", cfg.prewarm, cfg.nodes, cfg.nodes*1400)
	time.Sleep(cfg.prewarm)

	return benchTargets(cfg)
}

// benchTargets gates each instance on data (the per-tenant engine is created lazily on the first
// write, so before vmagent's first remote-write lands the count() pushdown sees an empty fetcher and
// 422s), benches it, then renders one side-by-side report from the collected results.
func benchTargets(cfg config) error {
	var results []targetResult
	for _, tgt := range cfg.targets() {
		if err := waitData(cfg, tgt); err != nil {
			return err
		}
		res, err := bench(cfg, tgt)
		if err != nil {
			return err
		}
		results = append(results, res)
	}
	return renderComparison(cfg, results)
}

func loadConfig() (config, error) {
	cfg := config{}

	// Working directory: this file's dir (dev/local/embedded-bench). Resolve the oteldb repo root
	// as three levels up — where go.mod with the replace directives lives.
	cwd, err := os.Getwd()
	if err != nil {
		return cfg, err
	}
	cfg.benchDir = cwd
	cfg.repoRoot = filepath.Join(cwd, "..", "..", "..")
	if abs, err := filepath.Abs(cfg.repoRoot); err == nil {
		cfg.repoRoot = abs
	}

	flag.IntVar(&cfg.nodes, "nodes", envInt("BENCH_NODES", 100), "synthetic node_exporter hosts vmagent fans out (~1400 series each)")
	// Default prewarm covers the 120s lookback with margin. The measured cost is dominated by how many
	// flushed parts the query window spans, so a prewarm shorter than the lookback leaves the window
	// only partially populated and the numbers collapse to unrepresentatively fast (and never exercise
	// the s3 read path). The benchmark's own rule: lookback ≤ prewarm. See loadConfig's warning below.
	flag.DurationVar(&cfg.prewarm, "prewarm", envDur("PREWARM", 150*time.Second), "ingest window before querying (must be ≥ -lookback to fully populate it)")
	flag.IntVar(&cfg.runs, "runs", envInt("BENCH_RUNS", 20), "measured runs per query (after warmup)")
	flag.IntVar(&cfg.warmup, "warmup", envInt("BENCH_WARMUP", 5), "unmeasured warmup runs")
	flag.DurationVar(&cfg.lookback, "lookback", envDur("LOOKBACK", 120*time.Second), "range-query window back from now")
	flag.DurationVar(&cfg.healthWait, "health-wait", envDur("HEALTH_WAIT", 120*time.Second), "max wait for oteldb /liveness")

	flag.StringVar(&cfg.gomaxprocs, "gomaxprocs", envStr("GOMAXPROCS", "4"), "oteldb CPU cap")
	flag.StringVar(&cfg.gomemlimit, "gomemlimit", envStr("GOMEMLIMIT", "1GiB"), "oteldb soft memory cap")
	flag.StringVar(&cfg.goarch, "goarch", envStr("GOARCH", "amd64"), "target arch (matches the container)")

	flag.BoolVar(&cfg.cleanup, "cleanup", envBool("CLEANUP", true), "tear the stack down on exit (-cleanup=false leaves it up to re-run / grab pprof)")
	flag.BoolVar(&cfg.s3, "s3", envBool("BENCH_S3", true), "also bench the S3 (go-faster/fs) backend alongside the file backend")

	flag.IntVar(&cfg.concurrency, "concurrency", envInt("CONCURRENCY", 8), "concurrent in-flight queries for the verify pass")

	flag.StringVar(&cfg.addr, "addr", envStr("ADDR", "http://127.0.0.1:9090"), "oteldb (file backend) PromQL API address")
	flag.StringVar(&cfg.addrS3, "addr-s3", envStr("ADDR_S3", "http://127.0.0.1:9093"), "oteldb-s3 (S3 backend) PromQL API address")
	flag.StringVar(&cfg.queries, "queries", envStr("BENCH_QUERIES", "queries.promql.yml"), "otelbench query suite file")
	flag.Parse()

	// The range queries evaluate over [now-lookback, now]; that window is only as populated as the
	// prewarm allows. A prewarm shorter than the lookback measures a mostly-empty window — queries
	// scan a fraction of the parts they would at steady state and report unrepresentatively low
	// latencies (and barely touch S3, collapsing the file-vs-s3 gap). Warn rather than hard-fail so a
	// deliberate quick smoke run (-prewarm 10s) is still possible.
	if cfg.prewarm < cfg.lookback {
		fmt.Fprintf(os.Stderr, "!! warning: -prewarm=%s < -lookback=%s: the query window will be under-populated "+
			"and latencies will read low (not comparable to REPORT.md). Use -prewarm ≥ %s.\n",
			cfg.prewarm, cfg.lookback, cfg.lookback)
	}

	return cfg, nil
}

// buildBinaries compiles oteldb and otelbench from the oteldb repo root, where the go.mod replace
// directives point at local /src/oteldb/{storage,promql-engine}. Both land in the bench dir so the
// Dockerfile COPYs ./oteldb and the bench phase invokes ./otelbench without recompiling per run.
func buildBinaries(cfg config) error {
	env := append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+cfg.goarch,
	)
	for _, target := range []struct{ pkg, out string }{
		{"./cmd/oteldb", "oteldb"},
		{"github.com/oteldb/oteldb/cmd/otelbench", "otelbench"},
	} {
		fmt.Printf(">> Building %s (CGO_ENABLED=0 GOOS=linux GOARCH=%s)\n", target.out, cfg.goarch)
		out := filepath.Join(cfg.benchDir, target.out)
		cmd := exec.Command("go", "build", "-trimpath", "-o", out, target.pkg)
		cmd.Dir = cfg.repoRoot
		cmd.Env = env
		wire(cmd)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("build %s: %w", target.out, err)
		}
	}
	return nil
}

func fullBringup(cfg config) error {
	if err := genScrape(cfg); err != nil {
		return err
	}
	backends := "file backend"
	if cfg.s3 {
		backends = "file + s3 backends"
	}
	fmt.Printf(">> Starting stack (oteldb --embedded on %s, GOMAXPROCS=%s, GOMEMLIMIT=%s, nodes=%d)\n",
		backends, cfg.gomaxprocs, cfg.gomemlimit, cfg.nodes)
	// docker compose picks up GOMAXPROCS/GOMEMLIMIT from our env via the compose ${VAR:-…} substitution.
	cmd := exec.Command("docker", "compose", "up", "-d", "--build", "--remove-orphans")
	cmd.Dir = cfg.benchDir
	cmd.Env = append(os.Environ(), "GOMAXPROCS="+cfg.gomaxprocs, "GOMEMLIMIT="+cfg.gomemlimit)
	wire(cmd)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}
	return waitHealthAll(cfg)
}

// waitHealthAll waits for every benched instance's /liveness to answer.
func waitHealthAll(cfg config) error {
	for _, tgt := range cfg.targets() {
		if err := waitHealth(cfg, tgt); err != nil {
			return err
		}
	}
	return nil
}

// genScrape writes vmagent's scrape config: one node_exporter × BENCH_NODES synthetic hosts.
func genScrape(cfg config) error {
	fmt.Printf(">> Generating vmagent scrape config for %d hosts\n", cfg.nodes)
	var b strings.Builder
	fmt.Fprintf(&b, "# Generated by embedded-bench from -nodes=%d — do not edit by hand.\n", cfg.nodes)
	b.WriteString("global:\n  scrape_interval: 2s\nscrape_configs:\n  - job_name: node_exporter\n    static_configs:\n")
	for i := range cfg.nodes {
		fmt.Fprintf(&b, "      - { targets: ['node-cache:9100'], labels: { instance: 'host-%d' } }\n", i)
	}
	return os.WriteFile(filepath.Join(cfg.benchDir, "vmagent-scrape.yml"), []byte(b.String()), 0o644)
}

// waitHealth polls oteldb's published /liveness port until it answers or the deadline expires.
// On timeout it tails oteldb logs + status so a crash/config error is visible.
func waitHealth(cfg config, tgt target) error {
	fmt.Printf(">> Waiting for %s (%s) health (up to %s)\n", tgt.service, tgt.name, cfg.healthWait)
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(cfg.healthWait)
	for time.Now().Before(deadline) {
		resp, err := client.Get(tgt.healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Println(" (healthy)")
				return nil
			}
		}
		fmt.Print(".")
		time.Sleep(time.Second)
	}
	fmt.Println()
	fmt.Fprintf(os.Stderr, "!! %s did not become healthy; tailing logs:\n", tgt.service)
	_ = composeLogs(cfg, tgt.service, 100)
	fmt.Fprintln(os.Stderr, "!! container status:")
	_ = composePs(cfg)
	return fmt.Errorf("%s health check timed out", tgt.service)
}

// targetResult holds one instance's benched numbers: the per-query latency samples (parsed from the
// otelbench report) in suite order, plus the peak RSS sampled across the run. renderComparison turns
// a slice of these into the side-by-side REPORT.md.
type targetResult struct {
	target  target
	order   []string             // query titles in suite order.
	groups  map[string][]float64 // title → latency samples in nanoseconds.
	peakRSS int64                // peak container RSS in bytes (0 if unsampled).
}

// bench runs a result-verification pass (one query each, asserting non-empty and positive counts),
// then the warmup + measured timing passes, against one instance. The verification pass exists
// because otelbench reports only latency — a query that silently returns the empty vector (or {0}
// for a count) still counts as a "successful" run, so a correctness regression surfaces as faster
// numbers, not a failure. It returns the parsed report + peak RSS for the side-by-side render.
func bench(cfg config, tgt target) (targetResult, error) {
	end := time.Now()
	start := end.Add(-cfg.lookback)

	fmt.Printf(">> [%s] Benching %s at %s\n", tgt.name, tgt.service, tgt.promAddr)

	// Sample the instance's resident set size across the whole bench (verify + warmup + measured),
	// tracking the peak. RSS is the headline memory cost to pair with the latency numbers.
	stopRSS := make(chan struct{})
	peakRSSCh := sampleRSS(cfg, tgt.service, stopRSS)
	drainRSS := func() int64 { close(stopRSS); return <-peakRSSCh }

	fmt.Printf(">> [%s] Verify (one query each, %d concurrent; non-empty + positive counts)\n", tgt.name, cfg.concurrency)
	if err := verify(cfg, tgt, start, end); err != nil {
		drainRSS()
		return targetResult{}, err
	}

	fmt.Printf(">> [%s] Warmup (%d runs/query, unmeasured)\n", tgt.name, cfg.warmup)
	if err := runOtelbench(cfg, tgt, 0, cfg.warmup, start, end, ""); err != nil {
		drainRSS()
		return targetResult{}, err
	}

	fmt.Printf(">> [%s] Benchmark (%d measured runs/query, window %s)\n", tgt.name, cfg.runs, cfg.lookback)
	report := filepath.Join(cfg.benchDir, "report-"+tgt.name+".yml")
	if err := runOtelbench(cfg, tgt, cfg.runs, 0, start, end, report); err != nil {
		drainRSS()
		return targetResult{}, err
	}

	peakRSS := drainRSS()
	if peakRSS > 0 {
		fmt.Printf(">> [%s] Peak RSS during bench: %s\n", tgt.name, humanBytes(peakRSS))
	} else {
		fmt.Fprintf(os.Stderr, "!! [%s] warning: could not sample RSS (docker compose exec failed)\n", tgt.name)
	}

	order, groups, err := parseReport(cfg, report)
	if err != nil {
		return targetResult{}, err
	}
	return targetResult{target: tgt, order: order, groups: groups, peakRSS: peakRSS}, nil
}

// sampleRSS polls the oteldb container's RSS (VmRSS from /proc/1/status, where the static oteldb
// binary runs as PID 1) every 500ms until stop is closed, then sends the peak in bytes. A failed
// poll (container momentarily unavailable) is skipped; a peak of 0 means no poll ever succeeded.
func sampleRSS(cfg config, service string, stop <-chan struct{}) <-chan int64 {
	out := make(chan int64, 1)
	go func() {
		var peak int64
		sample := func() {
			if rss, ok := readRSS(cfg, service); ok && rss > peak {
				peak = rss
			}
		}
		// Take one reading immediately so even a sub-tick-length bench (e.g. a small -nodes / -runs
		// smoke run that finishes in under 500ms) still records a non-zero peak.
		sample()
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				out <- peak
				return
			case <-t.C:
				sample()
			}
		}
	}()
	return out
}

// readRSS reads VmRSS from the given container's /proc/1/status via `docker compose exec`, returning
// the resident set size in bytes.
func readRSS(cfg config, service string) (int64, bool) {
	cmd := exec.Command("docker", "compose", "exec", "-T", service, "cat", "/proc/1/status")
	cmd.Dir = cfg.benchDir
	out, err := cmd.Output()
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(out), "\n") {
		rest, ok := strings.CutPrefix(line, "VmRSS:")
		if !ok {
			continue
		}
		// "VmRSS:\t  123456 kB" — value is in kibibytes.
		fields := strings.Fields(rest)
		if len(fields) < 1 {
			return 0, false
		}
		kib, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, false
		}
		return kib * 1024, true
	}
	return 0, false
}

// humanBytes renders a byte count with a binary (KiB/MiB/GiB…) unit.
func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// benchQuery is one entry of the suite: a kind (instant/range), a human title, and the query text.
type benchQuery struct {
	kind  string // "instant" | "range"
	title string
	query string
}

// parseSuite reads the bench's queries.promql.yml and returns its instant/range entries. The suite
// has a fixed, simple schema (a `step:` line, then `instant:`/`range:` sections whose items carry
// `- query:` and `title:` fields on single lines), so a focused line scanner keeps this orchestrator
// stdlib-only without pulling in a YAML dependency.
func parseSuite(path string) ([]benchQuery, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read suite: %w", err)
	}

	var (
		out   []benchQuery
		kind  string
		cur   *benchQuery
		flush = func() {
			if cur != nil && cur.query != "" {
				out = append(out, *cur)
			}
			cur = nil
		}
	)
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimRight(raw, "\r ")
		t := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(t, "instant:"):
			flush()
			kind = "instant"
		case strings.HasPrefix(t, "range:"):
			flush()
			kind = "range"
		case strings.HasPrefix(t, "- query:"):
			flush()
			if kind == "" {
				continue
			}
			cur = &benchQuery{kind: kind, query: trimYAMLValue(strings.TrimPrefix(t, "- query:"))}
		case strings.HasPrefix(t, "query:") && cur != nil:
			cur.query = trimYAMLValue(strings.TrimPrefix(t, "query:"))
		case strings.HasPrefix(t, "title:") && cur != nil:
			cur.title = trimYAMLValue(strings.TrimPrefix(t, "title:"))
		}
	}
	flush()

	if len(out) == 0 {
		return nil, errors.New("no queries parsed from suite (schema changed?)")
	}
	return out, nil
}

// trimYAMLValue strips the surrounding quotes from a single-line YAML scalar value.
func trimYAMLValue(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

// verify issues each suite query once and asserts it returns data. For count-shaped queries it also
// asserts the value is positive — the count-pushdown has historically regressed to the empty vector
// or {0} on instant `count(selector)`, and otelbench would happily report a (faster!) latency for
// the wrong answer. This pass makes such a regression fail the bench loudly.
// verifyResult is one suite query's verification outcome: exactly one of line (the success line to
// print) or problem (a failure description) is non-empty.
type verifyResult struct {
	line    string
	problem string
}

// verifyOne issues a single suite query and asserts it returns data (and a positive value for
// count queries). It returns a verifyResult rather than printing/erroring so callers can run it
// concurrently and aggregate in suite order.
func verifyOne(client *http.Client, addr string, q benchQuery, start, end time.Time) verifyResult {
	u, err := suiteURL(addr, q, start, end)
	if err != nil {
		return verifyResult{problem: fmt.Sprintf("  %s: build url: %v", q.title, err)}
	}

	body, err := getJSON(client, u)
	if err != nil {
		return verifyResult{problem: fmt.Sprintf("  %s: request error: %v", q.title, err)}
	}

	var resp struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			ResultType string            `json:"resultType"`
			Result     []json.RawMessage `json:"result"`
		} `json:"data"`
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return verifyResult{problem: fmt.Sprintf("  %s: decode response: %v", q.title, err)}
	}
	if resp.Status != "success" {
		return verifyResult{problem: fmt.Sprintf("  %s: status=%q error=%q", q.title, resp.Status, resp.Error)}
	}
	if len(resp.Data.Result) == 0 {
		return verifyResult{problem: fmt.Sprintf("  %s: EMPTY result (query returned no series)", q.title)}
	}

	// For count(...) queries, assert the value is positive. Covers both the ungrouped instant
	// pushdown (a single {} sample) and grouped counts. A zero here means the pushdown counted
	// nothing over the window — the lookback-clamp regression's signature.
	if isCountQuery(q) && firstNonPositiveSample(resp.Data.Result) {
		return verifyResult{problem: fmt.Sprintf("  %s: count returned a non-positive value (pushdown regression?)", q.title)}
	}

	return verifyResult{line: fmt.Sprintf("   %-32s ok (%d series)\n", q.title, len(resp.Data.Result))}
}

func verify(cfg config, tgt target, start, end time.Time) error {
	qPath := cfg.queries
	if !filepath.IsAbs(qPath) {
		qPath = filepath.Join(cfg.benchDir, qPath)
	}
	queries, err := parseSuite(qPath)
	if err != nil {
		return err
	}

	// Issue the suite concurrently (up to cfg.concurrency in flight). Each query writes its outcome
	// into results[i] — no shared mutation — so success lines and problems can be printed in suite
	// order afterwards, keeping deterministic output despite the concurrent fan-out.
	client := &http.Client{Timeout: 2 * time.Minute}
	results := make([]verifyResult, len(queries))
	sem := make(chan struct{}, max(cfg.concurrency, 1))
	var wg sync.WaitGroup
	for i, q := range queries {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, q benchQuery) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = verifyOne(client, tgt.promAddr, q, start, end)
		}(i, q)
	}
	wg.Wait()

	var problems []string
	for _, r := range results {
		if r.problem != "" {
			problems = append(problems, r.problem)
			continue
		}
		fmt.Print(r.line)
	}

	if len(problems) > 0 {
		fmt.Fprintln(os.Stderr, "!! verification failed:")
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, p)
		}
		return fmt.Errorf("%d/%d queries failed verification", len(problems), len(queries))
	}
	return nil
}

// suiteURL builds the Prometheus HTTP API URL for one suite entry over the bench window.
func suiteURL(addr string, q benchQuery, start, end time.Time) (string, error) {
	v := url.Values{}
	v.Set("query", q.query)
	switch q.kind {
	case "instant":
		v.Set("time", strconv.FormatInt(end.Unix(), 10))
		return addr + "/api/v1/query?" + v.Encode(), nil
	case "range":
		v.Set("start", strconv.FormatInt(start.Unix(), 10))
		v.Set("end", strconv.FormatInt(end.Unix(), 10))
		v.Set("step", "15")
		return addr + "/api/v1/query_range?" + v.Encode(), nil
	default:
		return "", fmt.Errorf("unknown query kind %q", q.kind)
	}
}

func getJSON(c *http.Client, u string) ([]byte, error) {
	resp, err := c.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return body, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// isCountQuery reports whether q is a count() aggregation, by title or query text. The suite titles
// carry "count" (e.g. "Count CPU cores", "Worst case full series count") and the query text starts
// with `count(` or wraps a nested count.
func isCountQuery(q benchQuery) bool {
	tl := strings.ToLower(q.title + " " + q.query)
	return strings.Contains(tl, "count")
}

// firstNonPositiveSample reports whether any sample value in the result set is ≤ 0. A count result
// is always ≥ 0; a zero means "counted nothing", which for these suites means a bug.
func firstNonPositiveSample(result []json.RawMessage) bool {
	for _, raw := range result {
		// Each result item is {"metric":{...},"value":[t,"v"]} (instant) or {"metric":{...},"values":[[t,"v"],...]} (range).
		var item struct {
			Value  *[2]any   `json:"value"`
			Values []*[2]any `json:"values"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		if item.Value != nil {
			if f, ok := parseJSONSample(*item.Value); ok && f <= 0 {
				return true
			}
		}
		for _, pair := range item.Values {
			if f, ok := parseJSONSample(*pair); ok && f <= 0 {
				return true
			}
		}
	}
	return false
}

// parseJSONSample parses the value half of a Prometheus [timestamp, "value"] sample pair.
func parseJSONSample(pair [2]any) (float64, bool) {
	s, ok := pair[1].(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

// parseReport runs `otelbench promql analyze` over one instance's report and returns its per-query
// latency samples (nanoseconds) keyed by title, plus the titles in suite order.
func parseReport(cfg config, reportPath string) ([]string, map[string][]float64, error) {
	out, err := exec.Command(filepath.Join(cfg.benchDir, "otelbench"),
		"promql", "analyze", "-f", "benchstat", "-i", reportPath).Output()
	if err != nil {
		return nil, nil, fmt.Errorf("otelbench analyze: %w", err)
	}

	// benchstat line: `BenchmarkPromQL/<Title> 0 <nanos> ns/op [extra cols…]`
	groups := map[string][]float64{}
	var order []string
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, "BenchmarkPromQL/") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 4 || f[3] != "ns/op" {
			continue
		}
		title := strings.TrimPrefix(f[0], "BenchmarkPromQL/")
		nanos, perr := strconv.ParseFloat(f[2], 64)
		if perr != nil {
			continue
		}
		if _, ok := groups[title]; !ok {
			order = append(order, title)
		}
		groups[title] = append(groups[title], nanos)
	}
	return order, groups, nil
}

// renderComparison writes a human-readable REPORT.md placing each benched backend side by side:
// p50/p90/p99/max per query for every instance, plus each instance's peak RSS. With only the file
// backend selected (-s3=false) it degenerates to the single-backend table.
func renderComparison(cfg config, results []targetResult) error {
	if len(results) == 0 {
		return errors.New("no results to render")
	}

	var b strings.Builder
	fmt.Fprintln(&b, "# embedded-bench — oteldb PromQL report")
	fmt.Fprintln(&b)
	backends := make([]string, len(results))
	for i, r := range results {
		backends[i] = "`" + r.target.name + "`"
	}
	fmt.Fprintf(&b, "_oteldb `--embedded`, backends: %s, GOMAXPROCS=%s GOMEMLIMIT=%s, "+
		"nodes=%d (~%d series), window=%s, %d runs/query_\n",
		strings.Join(backends, " vs "), cfg.gomaxprocs, cfg.gomemlimit, cfg.nodes, cfg.nodes*1400, cfg.lookback, cfg.runs)
	fmt.Fprintln(&b)

	// Peak RSS per backend.
	for _, r := range results {
		if r.peakRSS > 0 {
			fmt.Fprintf(&b, "- Peak `%s` RSS during bench: **%s**.\n", r.target.name, humanBytes(r.peakRSS))
		}
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Latencies in milliseconds.")
	fmt.Fprintln(&b)

	// Header: query | runs | <backend> p50 | p90 | p99 | max | … per backend.
	b.WriteString("| query | runs |")
	for _, r := range results {
		fmt.Fprintf(&b, " %s p50 | p90 | p99 | max |", r.target.name)
	}
	b.WriteString("\n|---|---:|")
	for range results {
		b.WriteString("---:|---:|---:|---:|")
	}
	b.WriteString("\n")

	// Row order follows the first backend's suite order (identical across backends — same suite).
	order := results[0].order
	if len(order) == 0 {
		b.WriteString("| _no parsed rows_ |\n")
	}
	for _, title := range order {
		fmt.Fprintf(&b, "| `%s` | %d |", humanTitle(title), len(results[0].groups[title]))
		for _, r := range results {
			xs := append([]float64(nil), r.groups[title]...)
			if len(xs) == 0 {
				b.WriteString(" — | — | — | — |")
				continue
			}
			slices.Sort(xs)
			fmt.Fprintf(&b, " %.1f | %.1f | %.1f | %.1f |",
				ms(pct(xs, 0.50)), ms(pct(xs, 0.90)), ms(pct(xs, 0.99)), ms(xs[len(xs)-1]))
		}
		b.WriteString("\n")
	}

	report := filepath.Join(cfg.benchDir, "REPORT.md")
	if err := os.WriteFile(report, []byte(b.String()), 0o644); err != nil {
		return err
	}
	fmt.Printf(">> REPORT.md written to %s\n\n%s", report, b.String())
	return nil
}

// pct returns the q-quantile of an already-sorted slice (nearest-rank).
func pct(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// ms converts nanoseconds to a milliseconds display value.
func ms(nanos float64) float64 { return nanos / 1e6 }

// humanTitle turns the benchstat-normalized title ("Count_CPU_cores") back into the query id
// shape used by the suite ("count cpu cores") for easier eyeballing against REPORT.md rows.
func humanTitle(title string) string {
	return strings.ToLower(strings.ReplaceAll(title, "_", " "))
}

// runOtelbench invokes the host-built otelbench binary with the suite + window. reportOut is empty
// for the unmeasured warmup pass (otelbench then prints but writes no file).
func runOtelbench(cfg config, tgt target, count, warmup int, start, end time.Time, reportOut string) error {
	// Resolve the queries file relative to the bench dir regardless of cwd.
	qPath := cfg.queries
	if !filepath.IsAbs(qPath) {
		qPath = filepath.Join(cfg.benchDir, qPath)
	}
	args := []string{
		"promql", "bench",
		"--addr", tgt.promAddr,
		"-i", qPath,
		"--warmup", strconv.Itoa(warmup),
		"--count", strconv.Itoa(count),
		"--start", strconv.FormatInt(start.Unix(), 10),
		"--end", strconv.FormatInt(end.Unix(), 10),
		// Fail on empty results: the verify pass already confirmed every query returns data, so an
		// empty run here is a regression (e.g. the count-pushdown empty-vector bug), not a cold cache.
		"--allow-empty=false",
	}
	if reportOut != "" {
		args = append(args, "-o", reportOut)
	}
	cmd := exec.Command(filepath.Join(cfg.benchDir, "otelbench"), args...)
	cmd.Dir = cfg.benchDir
	wire(cmd)
	return cmd.Run()
}

func maybeCleanup(cfg config) {
	if cfg.cleanup {
		fmt.Println(">> Stopping (-cleanup=false to keep the stack up to re-run / grab pprof)")
		_ = exec.Command("docker", "compose", "down", "-v").Run()
		return
	}
	fmt.Printf(">> Leaving stack up: oteldb (file) at %s, oteldb-s3 at %s\n", cfg.addr, cfg.addrS3)
	fmt.Println(">>   pprof: file http://127.0.0.1:9010, s3 http://127.0.0.1:9012")
}

// waitData polls a trivial selector until it returns a sample, guaranteeing the per-tenant engine
// exists (it is created lazily on first write) before the count() pushdown runs. Without this the
// suite's count({...}) query races vmagent's first remote-write and 422s with "count pushdown not
// supported by fetcher" — the empty pre-write fetcher is not a Counter.
func waitData(cfg config, tgt target) error {
	fmt.Printf(">> [%s] Waiting for first queryable sample (up to %s)\n", tgt.name, cfg.healthWait)
	q := tgt.promAddr + "/api/v1/query?query=node_load1"
	deadline := time.Now().Add(cfg.healthWait)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(q)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && strings.Contains(string(body), `"result":[{`) {
				fmt.Println(" (data present)")
				return nil
			}
		}
		fmt.Print(".")
		time.Sleep(time.Second)
	}
	fmt.Println()
	return errors.New("timed out waiting for oteldb to serve a sample (vmagent not remote-writing?)")
}

// --- exec helpers ---------------------------------------------------------------------------------

// wire streams a command's stdout/stderr straight through so docker/go output is live.
func wire(cmd *exec.Cmd) {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
}

func composeLogs(cfg config, service string, tail int) error {
	cmd := exec.Command("docker", "compose", "logs", "--tail", strconv.Itoa(tail), service)
	cmd.Dir = cfg.benchDir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func composePs(cfg config) error {
	cmd := exec.Command("docker", "compose", "ps")
	cmd.Dir = cfg.benchDir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// --- env-defaulted flag helpers ------------------------------------------------------------------

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes":
			return true
		case "0", "false", "no":
			return false
		}
	}
	return def
}
