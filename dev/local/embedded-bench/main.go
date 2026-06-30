// Command embedded-bench orchestrates a realistic PromQL benchmark of oteldb's embedded storage
// engine over HTTP, replacing the previous run.sh. It builds the oteldb (and otelbench) binaries on
// the host — where the oteldb repo's go.mod replace directives resolve the local storage /
// promql-engine sources — then drives a thin docker-compose stack (live node_exporter → vmagent as
// BENCH_NODES hosts → oteldb --embedded on the `file` backend, capped) and runs the canonical query
// suite via otelbench. Numbers are comparable to the oteldb column of
// /src/oteldb/benchmark/results/REPORT.md.
//
// Run from this directory:
//
//	go run .                                    # BENCH_NODES=10, prewarm 10s, 20 runs/query
//	go run . -nodes 100                         # match REPORT.md cardinality
//	go run . -replace                           # rebuild oteldb, swap it in place, re-bench
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

	replace bool
	cleanup bool

	addr     string
	queries  string
	benchDir string
	repoRoot string
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

	// In REPLACE mode the stack is assumed up (node-exporter/vmagent/data volume keep running);
	// rebuild the one-copy-layer image, swap oteldb, wait for health, then bench. No teardown.
	if cfg.replace {
		return replaceAndBench(cfg)
	}

	// Full bring-up. Tear down on exit unless -cleanup=false.
	defer maybeCleanup(cfg)
	if err := fullBringup(cfg); err != nil {
		return err
	}

	// Prewarm: let vmagent remote-write enough history for the suite's range vectors.
	// -prewarm=10s covers [1m]/[5m]; [30m]/[1h] (topk_free_mem, load_quantile) need 30m/70m to be
	// fully populated — oteldb still answers over whatever has arrived (--allow-empty keeps it going).
	fmt.Printf(">> Prewarming ingest for %s (nodes=%d ⇒ ~%d series)\n", cfg.prewarm, cfg.nodes, cfg.nodes*1400)
	time.Sleep(cfg.prewarm)

	// Gate on data: the per-tenant engine is created lazily on the first write, so before vmagent's
	// first remote-write lands the count() pushdown sees an empty fetcher and 422s. Poll a trivial
	// selector until it returns a sample — guarantees the engine exists before the suite runs.
	if err := waitData(cfg); err != nil {
		return err
	}
	return bench(cfg)
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
	flag.DurationVar(&cfg.prewarm, "prewarm", envDur("PREWARM", 10*time.Second), "ingest window before querying")
	flag.IntVar(&cfg.runs, "runs", envInt("BENCH_RUNS", 20), "measured runs per query (after warmup)")
	flag.IntVar(&cfg.warmup, "warmup", envInt("BENCH_WARMUP", 5), "unmeasured warmup runs")
	flag.DurationVar(&cfg.lookback, "lookback", envDur("LOOKBACK", 120*time.Second), "range-query window back from now")
	flag.DurationVar(&cfg.healthWait, "health-wait", envDur("HEALTH_WAIT", 120*time.Second), "max wait for oteldb /liveness")

	flag.StringVar(&cfg.gomaxprocs, "gomaxprocs", envStr("GOMAXPROCS", "4"), "oteldb CPU cap")
	flag.StringVar(&cfg.gomemlimit, "gomemlimit", envStr("GOMEMLIMIT", "1GiB"), "oteldb soft memory cap")
	flag.StringVar(&cfg.goarch, "goarch", envStr("GOARCH", "amd64"), "target arch (matches the container)")

	flag.BoolVar(&cfg.replace, "replace", envBool("REPLACE", false), "rebuild oteldb, swap it in place (stack already up), re-bench")
	flag.BoolVar(&cfg.cleanup, "cleanup", !cfg.replace, "tear the stack down on exit (default true unless -replace)")

	flag.StringVar(&cfg.addr, "addr", envStr("ADDR", "http://127.0.0.1:9090"), "oteldb PromQL API address")
	flag.StringVar(&cfg.queries, "queries", envStr("BENCH_QUERIES", "queries.promql.yml"), "otelbench query suite file")
	flag.Parse()

	// In replace mode, never tear down — we don't own the full stack bring-up.
	if cfg.replace {
		cfg.cleanup = false
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
	fmt.Printf(">> Starting stack (oteldb --embedded on file backend, GOMAXPROCS=%s, GOMEMLIMIT=%s, nodes=%d)\n",
		cfg.gomaxprocs, cfg.gomemlimit, cfg.nodes)
	// docker compose picks up GOMAXPROCS/GOMEMLIMIT from our env via the compose ${VAR:-…} substitution.
	cmd := exec.Command("docker", "compose", "up", "-d", "--build", "--remove-orphans")
	cmd.Dir = cfg.benchDir
	cmd.Env = append(os.Environ(), "GOMAXPROCS="+cfg.gomaxprocs, "GOMEMLIMIT="+cfg.gomemlimit)
	wire(cmd)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}
	return waitHealth(cfg)
}

func replaceAndBench(cfg config) error {
	fmt.Println(">> REPLACE: rebuilding oteldb image (one COPY layer) and swapping the container in place")
	fmt.Println(">>   (node-exporter / vmagent / data volume keep running — no re-ingest)")
	cmd := exec.Command("docker", "compose", "up", "-d", "--build", "--force-recreate", "--no-deps", "oteldb")
	cmd.Dir = cfg.benchDir
	wire(cmd)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up (replace): %w", err)
	}
	if err := waitHealth(cfg); err != nil {
		return err
	}
	if err := waitData(cfg); err != nil {
		return err
	}
	// vmagent kept remote-writing, so the new oteldb's head refills within a couple scrapes.
	fmt.Println(">> Settling 5s for ingest to refill the new oteldb head")
	time.Sleep(5 * time.Second)
	return bench(cfg)
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
func waitHealth(cfg config) error {
	fmt.Printf(">> Waiting for oteldb health (up to %s)\n", cfg.healthWait)
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(cfg.healthWait)
	for time.Now().Before(deadline) {
		resp, err := client.Get(cfg.addrLiveness())
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
	fmt.Fprintln(os.Stderr, "!! oteldb did not become healthy; tailing logs:")
	_ = composeLogs(cfg, "oteldb", 100)
	fmt.Fprintln(os.Stderr, "!! container status:")
	_ = composePs(cfg)
	return errors.New("oteldb health check timed out")
}

func (c config) addrLiveness() string {
	// The PromQL API is on :9090; the health endpoint is the same host on :13133.
	return strings.Replace(c.addr, ":9090", ":13133", 1) + "/liveness"
}

// bench runs a result-verification pass (one query each, asserting non-empty and positive counts),
// then the warmup + measured timing passes. The verification pass exists because otelbench reports
// only latency — a query that silently returns the empty vector (or {0} for a count) still counts
// as a "successful" run, so a correctness regression surfaces as faster numbers, not a failure.
func bench(cfg config) error {
	end := time.Now()
	start := end.Add(-cfg.lookback)

	fmt.Printf(">> Verify (one query each; non-empty + positive counts)\n")
	if err := verify(cfg, start, end); err != nil {
		return err
	}

	fmt.Printf(">> Warmup (%d runs/query, unmeasured)\n", cfg.warmup)
	if err := runOtelbench(cfg, 0, cfg.warmup, start, end, ""); err != nil {
		return err
	}

	fmt.Printf(">> Benchmark (%d measured runs/query, window %s)\n", cfg.runs, cfg.lookback)
	report := filepath.Join(cfg.benchDir, "report.yml")
	if err := runOtelbench(cfg, cfg.runs, 0, start, end, report); err != nil {
		return err
	}
	fmt.Printf(">> Report written to %s\n", report)
	if err := renderReport(cfg, report); err != nil {
		// Non-fatal: the raw report.yml is still there; the markdown render is a convenience.
		fmt.Fprintf(os.Stderr, "!! warning: render REPORT.md: %v\n", err)
	}
	return nil
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
func verify(cfg config, start, end time.Time) error {
	qPath := cfg.queries
	if !filepath.IsAbs(qPath) {
		qPath = filepath.Join(cfg.benchDir, qPath)
	}
	queries, err := parseSuite(qPath)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 2 * time.Minute}
	var problems []string

	for _, q := range queries {
		u, err := suiteURL(cfg.addr, q, start, end)
		if err != nil {
			return fmt.Errorf("build url for %q: %w", q.title, err)
		}

		body, err := getJSON(client, u)
		if err != nil {
			problems = append(problems, fmt.Sprintf("  %s: request error: %v", q.title, err))
			continue
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
			problems = append(problems, fmt.Sprintf("  %s: decode response: %v", q.title, err))
			continue
		}
		if resp.Status != "success" {
			problems = append(problems, fmt.Sprintf("  %s: status=%q error=%q", q.title, resp.Status, resp.Error))
			continue
		}
		if len(resp.Data.Result) == 0 {
			problems = append(problems, fmt.Sprintf("  %s: EMPTY result (query returned no series)", q.title))
			continue
		}

		// For count(...) queries, assert the value is positive. Covers both the ungrouped instant
		// pushdown (a single {} sample) and grouped counts. A zero here means the pushdown counted
		// nothing over the window — the lookback-clamp regression's signature.
		if isCountQuery(q) {
			if nonPositive := firstNonPositiveSample(resp.Data.Result); nonPositive {
				problems = append(problems, fmt.Sprintf("  %s: count returned a non-positive value (pushdown regression?)", q.title))
			}
		}

		fmt.Printf("   %-32s ok (%d series)\n", q.title, len(resp.Data.Result))
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

// renderReport writes a human-readable REPORT.md (p50/p90/p99 per query) by parsing the benchstat
// rows otelbench emits for report.yml.
func renderReport(cfg config, reportPath string) error {
	out, err := exec.Command(filepath.Join(cfg.benchDir, "otelbench"),
		"promql", "analyze", "-f", "benchstat", "-i", reportPath).Output()
	if err != nil {
		return fmt.Errorf("otelbench analyze: %w", err)
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

	var b strings.Builder
	fmt.Fprintln(&b, "# embedded-bench — oteldb PromQL report")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "_oteldb `--embedded` on the `file` backend, GOMAXPROCS=%s GOMEMLIMIT=%s, "+
		"nodes=%d (~%d series), window=%s, %d runs/query_\n",
		cfg.gomaxprocs, cfg.gomemlimit, cfg.nodes, cfg.nodes*1400, cfg.lookback, cfg.runs)
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Latencies in milliseconds.")
	fmt.Fprintln(&b)
	b.WriteString("| query | runs | p50 | p90 | p99 | max |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|\n")
	if len(order) == 0 {
		b.WriteString("| _no parsed rows_ | | | | | |\n")
	}
	for _, title := range order {
		xs := groups[title]
		slices.Sort(xs)
		fmt.Fprintf(&b, "| `%s` | %d | %.1f | %.1f | %.1f | %.1f |\n",
			humanTitle(title), len(xs),
			ms(pct(xs, 0.50)), ms(pct(xs, 0.90)), ms(pct(xs, 0.99)), ms(xs[len(xs)-1]))
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
func runOtelbench(cfg config, count, warmup int, start, end time.Time, reportOut string) error {
	// Resolve the queries file relative to the bench dir regardless of cwd.
	qPath := cfg.queries
	if !filepath.IsAbs(qPath) {
		qPath = filepath.Join(cfg.benchDir, qPath)
	}
	args := []string{
		"promql", "bench",
		"--addr", cfg.addr,
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
		fmt.Println(">> Stopping (-cleanup=false to keep the stack up for -replace runs)")
		_ = exec.Command("docker", "compose", "down", "-v").Run()
		return
	}
	fmt.Printf(">> Leaving stack up: oteldb at %s, pprof at http://127.0.0.1:9010\n", cfg.addr)
	fmt.Println(">>   iterate with: go run . -replace")
}

// waitData polls a trivial selector until it returns a sample, guaranteeing the per-tenant engine
// exists (it is created lazily on first write) before the count() pushdown runs. Without this the
// suite's count({...}) query races vmagent's first remote-write and 422s with "count pushdown not
// supported by fetcher" — the empty pre-write fetcher is not a Counter.
func waitData(cfg config) error {
	fmt.Printf(">> Waiting for first queryable sample (up to %s)\n", cfg.healthWait)
	q := cfg.addr + "/api/v1/query?query=node_load1"
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
