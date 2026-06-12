package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/spf13/cobra"

	"github.com/oteldb/oteldb/cmd/otelbench/chtracker"
	"github.com/oteldb/oteldb/cmd/otelbench/logqlbench"
)

type LogsReport struct {
	Output string
}

func newLogsReportCommand() *cobra.Command {
	r := &LogsReport{}
	cmd := &cobra.Command{
		Use:   "report <run-dir>",
		Short: "Render a logs suite run directory as Markdown",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return r.Run(args[0])
		},
	}
	cmd.Flags().StringVarP(&r.Output, "output", "o", "", "Output Markdown file (default stdout)")
	return cmd
}

func (r *LogsReport) Run(dir string) error {
	markdown, err := renderLogsReport(dir)
	if err != nil {
		return err
	}
	if r.Output == "" {
		fmt.Print(markdown)
		return nil
	}
	if err := os.WriteFile(r.Output, []byte(markdown), 0o644); err != nil {
		return errors.Wrap(err, "write markdown")
	}
	return nil
}

func renderLogsReport(dir string) (string, error) {
	ingest, err := readLogsBenchReport(filepath.Join(dir, "ingest.json"))
	if err != nil {
		return "", err
	}
	env, err := readLogsSuiteEnv(filepath.Join(dir, "env.json"))
	if err != nil {
		return "", err
	}
	queryReport, err := readLogQLReport(filepath.Join(dir, "report.yml"))
	if err != nil {
		return "", err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# oteldb Logs Benchmark\n\n")
	fmt.Fprintf(&b, "Run directory: `%s`\n\n", dir)
	fmt.Fprintf(&b, "## Environment\n\n")
	fmt.Fprintf(&b, "| Field | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Started | %s |\n", env.StartedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "| Git SHA | `%s` |\n", env.GitSHA)
	fmt.Fprintf(&b, "| Go | `%s %s/%s` |\n", env.GoVersion, env.GOOS, env.GOARCH)
	fmt.Fprintf(&b, "| CPUs | %d |\n", env.NumCPU)
	fmt.Fprintf(&b, "| Dataset | `%s`, repeat `%d` |\n", env.DatasetSize, env.DatasetRepeat)
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "## Ingest\n\n")
	fmt.Fprintf(&b, "| Metric | Value |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Lines | %s |\n", fmtInt(int(ingest.Throughput.Lines)))
	fmt.Fprintf(&b, "| Bytes | %s |\n", humanize.Bytes(uint64(ingest.Throughput.Bytes)))
	fmt.Fprintf(&b, "| Lines/s | %.0f |\n", ingest.Throughput.LinesPerSecond)
	fmt.Fprintf(&b, "| Bytes/s | %s |\n", humanize.Bytes(uint64(ingest.Throughput.BytesPerSecond)))
	fmt.Fprintf(&b, "| Window | `%s` to `%s` |\n", ingest.Window.Start.Format(time.RFC3339Nano), ingest.Window.End.Format(time.RFC3339Nano))
	if ingest.Storage != nil {
		fmt.Fprintf(&b, "| ClickHouse rows | %s |\n", fmtInt(ingest.Storage.Rows))
		fmt.Fprintf(&b, "| Compressed size | %s |\n", humanize.Bytes(uint64(ingest.Storage.CompressedSize)))
		fmt.Fprintf(&b, "| Compression ratio | %.2fx |\n", ingest.Storage.CompressRatio)
		if ingest.Storage.Rows > 0 {
			bytesPerPoint := float64(ingest.Storage.CompressedSize) / float64(ingest.Storage.Rows)
			fmt.Fprintf(&b, "| Bytes/point | %.1f |\n", bytesPerPoint)
		}
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "## Queries\n\n")
	fmt.Fprintf(&b, "| Query | Tier | Type | Runs | p50 | p90 | CH read | CH rows |\n")
	fmt.Fprintf(&b, "|---|---|---|---:|---:|---:|---:|---:|\n")
	for _, row := range summarizeLogQLQueries(queryReport.Queries) {
		fmt.Fprintf(&b, "| %s | %s | `%s` | %d | %s | %s | %s | %s |\n",
			escapeMarkdown(row.Title), row.Tier, row.Type, row.Runs,
			row.P50.Round(time.Millisecond/20), row.P90.Round(time.Millisecond/20),
			humanize.Bytes(uint64(row.ReadBytes)), fmtInt(int(row.ReadRows)),
		)
	}
	return b.String(), nil
}

func readLogsSuiteEnv(path string) (logsSuiteEnv, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return logsSuiteEnv{}, errors.Wrap(err, "read env")
	}
	var env logsSuiteEnv
	if err := json.Unmarshal(data, &env); err != nil {
		return logsSuiteEnv{}, errors.Wrap(err, "decode env")
	}
	return env, nil
}

func readLogQLReport(path string) (logqlbench.LogQLReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return logqlbench.LogQLReport{}, errors.Wrap(err, "read query report")
	}
	var report logqlbench.LogQLReport
	if err := yamlx.Unmarshal(data, &report); err != nil {
		return logqlbench.LogQLReport{}, errors.Wrap(err, "decode query report")
	}
	return report, nil
}

type logQLSummaryRow struct {
	Title     string
	Tier      string
	Type      string
	Runs      int
	P50       time.Duration
	P90       time.Duration
	ReadBytes int64
	ReadRows  int64
}

func summarizeLogQLQueries(queries []logqlbench.LogQLReportQuery) []logQLSummaryRow {
	type group struct {
		title       string
		tier        string
		typ         string
		durations   []time.Duration
		queryReport []chtracker.QueryReport
	}
	groups := make(map[string]*group)
	for _, q := range queries {
		if q.ReportError != "" {
			continue
		}
		key := q.Type + "\x00" + q.Title
		g := groups[key]
		if g == nil {
			g = &group{title: q.Title, tier: queryTier(q.Description), typ: q.Type}
			groups[key] = g
		}
		g.durations = append(g.durations, time.Duration(q.DurationNanos))
		g.queryReport = append(g.queryReport, q.Queries...)
	}
	rows := make([]logQLSummaryRow, 0, len(groups))
	for _, g := range groups {
		sort.Slice(g.durations, func(i, j int) bool { return g.durations[i] < g.durations[j] })
		var readBytes, readRows int64
		for _, q := range g.queryReport {
			readBytes += q.ReadBytes
			readRows += q.ReadRows
		}
		rows = append(rows, logQLSummaryRow{
			Title:     g.title,
			Tier:      g.tier,
			Type:      g.typ,
			Runs:      len(g.durations),
			P50:       percentileDuration(g.durations, 0.50),
			P90:       percentileDuration(g.durations, 0.90),
			ReadBytes: readBytes,
			ReadRows:  readRows,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Type+rows[i].Title < rows[j].Type+rows[j].Title
	})
	return rows
}

func percentileDuration(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}

func queryTier(description string) string {
	lower := strings.ToLower(description)
	switch {
	case strings.Contains(lower, "best-case"):
		return "best"
	case strings.Contains(lower, "mid-case"):
		return "mid"
	case strings.Contains(lower, "worst-case"):
		return "worst"
	case strings.Contains(lower, "metadata"):
		return "metadata"
	default:
		return "unknown"
	}
}

func escapeMarkdown(s string) string {
	var b bytes.Buffer
	for _, r := range s {
		if r == '|' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
