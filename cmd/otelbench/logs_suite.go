package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/spf13/cobra"

	"github.com/oteldb/oteldb/cmd/otelbench/logqlbench"
	"github.com/oteldb/oteldb/cmd/otelbench/logsbench"
)

type LogsSuite struct {
	Size     string
	Repeat   int
	Count    int
	Warmup   int
	Out      string
	CacheDir string
	Suite    string
	NoUp     bool
	Keep     bool

	Target          string
	LokiAddr        string
	TempoAddr       string
	ClickhouseAddr  string
	OteldbImage     string
	ChotelImage     string
	ClickhouseImage string
	TempoImage      string
	OtelcolImage    string
	Total           int64
	Entries         int
	Rate            time.Duration
}

const (
	defaultOteldbImage     = "ghcr.io/oteldb/oteldb:latest"
	defaultChotelImage     = "ghcr.io/oteldb/oteldb/chotel:latest"
	defaultClickhouseImage = "clickhouse/clickhouse-server:25.9"
	defaultTempoImage      = "ghcr.io/go-faster/tempo:latest"
	defaultOtelcolImage    = "ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector:0.154.0"
)

func newLogsSuiteCommand() *cobra.Command {
	s := &LogsSuite{}
	cmd := &cobra.Command{
		Use:   "suite",
		Short: "Run the embedded oteldb logs benchmark suite",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return s.Run(cmd.Context(), cmd.OutOrStdout())
		},
	}
	f := cmd.Flags()
	f.StringVar(&s.Size, "size", string(logsbench.DatasetMedium), "Loghub dataset size: small, medium, or large")
	f.IntVar(&s.Repeat, "repeat", 0, "Number of extra dataset replay loops (0 replays once)")
	f.IntVar(&s.Count, "count", 15, "Number of query repetitions")
	f.IntVar(&s.Warmup, "warmup", 1, "Number of query warmup repetitions")
	f.StringVar(&s.Out, "out", "results", "Directory for benchmark result runs")
	f.StringVar(&s.CacheDir, "cache-dir", logsbench.DefaultCacheDir(), "Dataset cache directory")
	f.StringVar(&s.Suite, "suite", "", "Override LogQL suite file")
	f.BoolVar(&s.NoUp, "no-up", false, "Use an already-running stack instead of docker compose up/down")
	f.BoolVar(&s.Keep, "keep", false, "Leave compose services running after the suite")
	f.StringVar(&s.Target, "target", "127.0.0.1:4318", "OTLP gRPC target for log ingest")
	f.StringVar(&s.LokiAddr, "loki-addr", "http://127.0.0.1:3100", "Loki-compatible query endpoint")
	f.StringVar(&s.TempoAddr, "tempo-addr", "http://127.0.0.1:3200", "Tempo endpoint for query trace lookup")
	f.StringVar(&s.ClickhouseAddr, "clickhouse-addr", "127.0.0.1:9000", "ClickHouse native endpoint for ingest stats")
	f.StringVar(&s.OteldbImage, "oteldb-image", defaultOteldbImage, "oteldb container image used by docker compose")
	f.StringVar(&s.ChotelImage, "chotel-image", defaultChotelImage, "chotel container image used by docker compose")
	f.StringVar(&s.ClickhouseImage, "clickhouse-image", defaultClickhouseImage, "ClickHouse container image used by docker compose")
	f.StringVar(&s.TempoImage, "tempo-image", defaultTempoImage, "Tempo container image used by docker compose")
	f.StringVar(&s.OtelcolImage, "otelcol-image", defaultOtelcolImage, "OpenTelemetry Collector container image used by docker compose")
	f.Int64Var(&s.Total, "total", 0, "Cap ingested log lines (0 disables cap)")
	f.IntVar(&s.Entries, "entries", 1000, "Log entries per ingest batch")
	f.DurationVar(&s.Rate, "rate", 0, "Delay between ingest batches (<=0 sends as fast as possible)")
	return cmd
}

func (s *LogsSuite) Run(ctx context.Context, output io.Writer) error {
	started := time.Now().UTC()
	runDir, err := s.createRunDir(started)
	if err != nil {
		return err
	}
	fmt.Fprintln(output, "run directory:", runDir)

	workDir := filepath.Join(runDir, "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return errors.Wrap(err, "create work directory")
	}
	if err := logsbench.WriteAssets(workDir); err != nil {
		return errors.Wrap(err, "write embedded assets")
	}
	if err := s.writeEnv(ctx, runDir, started); err != nil {
		return errors.Wrap(err, "write env")
	}

	fmt.Fprintln(output, "downloading dataset")
	dataset, err := logsbench.DownloadLoghub(ctx, logsbench.DownloadOptions{
		Size:     logsbench.DatasetSize(s.Size),
		CacheDir: s.CacheDir,
	})
	if err != nil {
		return errors.Wrap(err, "download dataset")
	}
	fmt.Fprintln(output, "downloaded dataset:", dataset.Dir)

	if !s.NoUp {
		fmt.Fprintln(output, "starting compose stack")
		if err := s.compose(ctx, workDir, "up", "-d"); err != nil {
			return errors.Wrap(err, "compose up")
		}
		if !s.Keep {
			defer func() {
				fmt.Fprintln(output, "stopping compose stack")
				if err := s.compose(context.Background(), workDir, "down", "-v"); err != nil {
					fmt.Fprintln(os.Stderr, "compose down:", err)
				}
			}()
		}
		fmt.Fprintln(output, "compose is up")
	}
	fmt.Fprintln(output, "waiting for stack to be ready")
	if err := waitLogsStack(ctx, s.LokiAddr, s.TempoAddr); err != nil {
		return errors.Wrap(err, "wait stack")
	}

	ingestReport := filepath.Join(runDir, "ingest.json")
	fmt.Fprintln(output, "ingesting logs")
	bench := &LogsBench{
		seed:            1,
		resourceCount:   3,
		entriesPerBatch: s.Entries,
		rate:            s.Rate,
		limit:           s.Total,
		start:           lokiTimeVar{Value: started},
		sourceDir:       dataset.Dir,
		repeat:          s.Repeat,
		reportPath:      ingestReport,
		clickhouseAddr:  s.ClickhouseAddr,
	}
	if err := bench.prepareTargets(ctx, []string{s.Target}); err != nil {
		return errors.Wrap(err, "prepare ingest target")
	}
	if err := bench.Run(ctx); err != nil {
		return errors.Wrap(err, "ingest")
	}

	ingest, err := readLogsBenchReport(ingestReport)
	if err != nil {
		return err
	}
	suitePath := s.Suite
	if suitePath == "" {
		suitePath = logsbench.DefaultSuitePath(workDir)
	}
	if err := copyFile(suitePath, filepath.Join(runDir, "suite.yml")); err != nil {
		return errors.Wrap(err, "copy suite")
	}

	queryReport := filepath.Join(runDir, "report.yml")
	fmt.Fprintln(output, "running LogQL queries")
	query := &logqlbench.LogQLBenchmark{
		Addr:           s.LokiAddr,
		Count:          s.Count,
		Warmup:         s.Warmup,
		StartTime:      ingest.Window.Start.Format(time.RFC3339Nano),
		EndTime:        ingest.Window.End.Format(time.RFC3339Nano),
		AllowEmpty:     true,
		Input:          suitePath,
		Output:         queryReport,
		RequestTimeout: 30 * time.Second,
	}
	query.TrackerOptions.Trace = true
	query.TrackerOptions.TempoAddr = s.TempoAddr
	setupCmd := &cobra.Command{}
	setupCmd.SetContext(ctx)
	if err := query.Setup(setupCmd); err != nil {
		return errors.Wrap(err, "setup LogQL bench")
	}
	if err := query.Run(ctx); err != nil {
		return errors.Wrap(err, "run LogQL bench")
	}

	fmt.Fprintln(output, "writing LogQL benchstat:", runDir)
	if err := writeLogQLBenchstat(queryReport, filepath.Join(runDir, "benchstat.txt")); err != nil {
		return errors.Wrap(err, "write benchstat")
	}
	fmt.Fprintln(output, "suite complete:", runDir)
	return nil
}

func (s *LogsSuite) createRunDir(ts time.Time) (string, error) {
	sha := gitShortSHA()
	name := ts.Format("20060102-150405") + "-" + sha
	dir := filepath.Join(s.Out, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", errors.Wrap(err, "create run directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", errors.Wrap(err, "resolve run directory")
	}
	return abs, nil
}

func (s *LogsSuite) compose(ctx context.Context, workDir string, args ...string) error {
	fullArgs := append([]string{"compose", "-f", filepath.Join(workDir, "docker-compose.yml")}, args...)
	cmd := exec.CommandContext(ctx, "docker", fullArgs...)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(),
		"OTELDB_IMAGE="+s.OteldbImage,
		"CHOTEL_IMAGE="+s.ChotelImage,
		"CLICKHOUSE_IMAGE="+s.ClickhouseImage,
		"TEMPO_IMAGE="+s.TempoImage,
		"OTELCOL_IMAGE="+s.OtelcolImage,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func waitLogsStack(ctx context.Context, lokiAddr, tempoAddr string) error {
	client := &http.Client{Timeout: time.Second}
	checks := []string{
		strings.TrimRight(lokiAddr, "/") + "/ready",
		strings.TrimRight(tempoAddr, "/") + "/ready",
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var ready bool
		for _, url := range checks {
			ok, err := httpReady(ctx, client, url)
			if err != nil || !ok {
				ready = false
				break
			}
			ready = true
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("stack readiness timeout")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func httpReady(ctx context.Context, client *http.Client, url string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

func readLogsBenchReport(path string) (logsBenchReport, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is internal runDir/ingest.json constructed by the suite runner
	if err != nil {
		return logsBenchReport{}, errors.Wrap(err, "read ingest report")
	}
	var report logsBenchReport
	if err := json.Unmarshal(data, &report); err != nil {
		return logsBenchReport{}, errors.Wrap(err, "decode ingest report")
	}
	return report, nil
}

func writeLogQLBenchstat(reportPath, benchstatPath string) error {
	data, err := os.ReadFile(reportPath) // #nosec G304 -- reportPath is internal runDir/report.yml constructed by the suite runner
	if err != nil {
		return errors.Wrap(err, "read report")
	}
	var report logqlbench.LogQLReport
	if err := yamlx.Unmarshal(data, &report); err != nil {
		return errors.Wrap(err, "unmarshal report")
	}
	out, err := os.Create(benchstatPath) // #nosec G304 -- benchstatPath is internal runDir/benchstat.txt constructed by the suite runner
	if err != nil {
		return errors.Wrap(err, "create benchstat")
	}
	defer func() {
		_ = out.Close()
	}()
	return (LogQLAnalyze{Format: "benchstat"}).renderBenchstat(report, out)
}

func copyFile(from, to string) error {
	data, err := os.ReadFile(from) // #nosec G304 -- from may be user-provided --suite path for local benchmark runs; to is always under controlled runDir
	if err != nil {
		return errors.Wrap(err, "read file")
	}
	if err := os.WriteFile(to, data, 0o644); err != nil {
		return errors.Wrap(err, "write file")
	}
	return nil
}

type logsSuiteEnv struct {
	StartedAt       time.Time `json:"started_at"`
	GitSHA          string    `json:"git_sha"`
	GOOS            string    `json:"goos"`
	GOARCH          string    `json:"goarch"`
	GoVersion       string    `json:"go_version"`
	NumCPU          int       `json:"num_cpu"`
	Kernel          string    `json:"kernel,omitempty"`
	DatasetSize     string    `json:"dataset_size"`
	DatasetRepeat   int       `json:"dataset_repeat"`
	QueryCount      int       `json:"query_count"`
	QueryWarmup     int       `json:"query_warmup"`
	ClickhouseAddr  string    `json:"clickhouse_addr"`
	LokiAddr        string    `json:"loki_addr"`
	TempoAddr       string    `json:"tempo_addr"`
	Target          string    `json:"target"`
	OteldbImage     string    `json:"oteldb_image"`
	ChotelImage     string    `json:"chotel_image"`
	ClickhouseImage string    `json:"clickhouse_image"`
	TempoImage      string    `json:"tempo_image"`
	OtelcolImage    string    `json:"otelcol_image"`
}

func (s *LogsSuite) writeEnv(ctx context.Context, runDir string, started time.Time) error {
	env := logsSuiteEnv{
		StartedAt:       started,
		GitSHA:          gitShortSHA(),
		GOOS:            runtime.GOOS,
		GOARCH:          runtime.GOARCH,
		GoVersion:       runtime.Version(),
		NumCPU:          runtime.NumCPU(),
		Kernel:          uname(ctx),
		DatasetSize:     s.Size,
		DatasetRepeat:   s.Repeat,
		QueryCount:      s.Count,
		QueryWarmup:     s.Warmup,
		ClickhouseAddr:  s.ClickhouseAddr,
		LokiAddr:        s.LokiAddr,
		TempoAddr:       s.TempoAddr,
		Target:          s.Target,
		OteldbImage:     s.OteldbImage,
		ChotelImage:     s.ChotelImage,
		ClickhouseImage: s.ClickhouseImage,
		TempoImage:      s.TempoImage,
		OtelcolImage:    s.OtelcolImage,
	}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal env")
	}
	if err := os.WriteFile(filepath.Join(runDir, "env.json"), append(data, '\n'), 0o644); err != nil {
		return errors.Wrap(err, "write env")
	}
	return nil
}

func gitShortSHA() string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "unknown"
	}
	return strings.TrimSpace(out.String())
}

func uname(ctx context.Context) string {
	cmd := exec.CommandContext(ctx, "uname", "-a")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return ""
	}
	return strings.TrimSpace(out.String())
}
