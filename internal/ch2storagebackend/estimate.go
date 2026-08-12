package ch2storagebackend

import (
	"context"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/chstorage"
)

// Throughput defaults, in rows per second, for the projected wall-clock in an [Estimate].
//
// These are the *serial* end-to-end rates of the current migration path — ClickHouse scan, decode,
// pdata construction, and ingest all run in one goroutine, so the pipeline is bounded by their sum
// rather than by the slowest stage. Metrics is measured: NumberPointsToMetrics sustains ~3.3M
// points/s and the engine's ingest+flush ~3.3M points/s on a 5950X, which compose to ~1.6M
// points/s before the ClickHouse scan is counted. Logs and traces are set an order of magnitude
// lower because their rows are far larger. They are deliberately crude — the estimate exists to
// separate "an hour" from "a week", not to predict a finish time — and are overridable.
const (
	DefaultMetricsRowsPerSecond = 1_500_000
	DefaultLogsRowsPerSecond    = 150_000
	DefaultTracesRowsPerSecond  = 200_000
)

// RowsPerSecond returns the default projected throughput for a signal.
func RowsPerSecond(signal string) float64 {
	switch signal {
	case SignalMetrics:
		return DefaultMetricsRowsPerSecond
	case SignalTraces:
		return DefaultTracesRowsPerSecond
	default:
		return DefaultLogsRowsPerSecond
	}
}

// Estimate is the pre-flight sizing of one signal's migration window: what it will read, how big it
// is, and roughly how long it will take. It is produced without ingesting anything.
type Estimate struct {
	Signal string
	// From/To are the effective window, after intersecting the request with the source's range.
	From time.Time
	To   time.Time
	// Days is the per-UTC-day row count. Days already recorded in the checkpoint have Skipped set.
	Days []EstimateDay
	// Rows is the total across Days; Remaining excludes the skipped ones.
	Rows      uint64
	Remaining uint64
	// Size is the source table's whole-table footprint, the basis for the per-row byte averages.
	Size chstorage.TableSize
	// RowsPerSecond is the throughput the projection assumes.
	RowsPerSecond float64
}

// EstimateDay is one day of an [Estimate].
type EstimateDay struct {
	Day     time.Time
	Rows    uint64
	Skipped bool
}

// Bytes returns the compressed and uncompressed size of the remaining rows, projected from the
// source table's average bytes per row.
//
// Both are ClickHouse-side figures and they differ by roughly an order of magnitude on telemetry,
// which is why they are reported as a pair rather than as a single "size". Neither predicts what
// the target engine writes: it applies its own codecs, and its own MaxPartSize limit is itself
// expressed in *uncompressed* bytes.
func (e Estimate) Bytes() (compressed, uncompressed uint64) {
	perCompressed, perUncompressed := e.Size.BytesPerRow()
	return uint64(perCompressed * float64(e.Remaining)), uint64(perUncompressed * float64(e.Remaining))
}

// Duration returns the projected wall-clock for the remaining rows.
func (e Estimate) Duration() time.Duration {
	if e.RowsPerSecond <= 0 {
		return 0
	}
	return time.Duration(float64(e.Remaining) / e.RowsPerSecond * float64(time.Second))
}

// EstimateLogs sizes a log migration over w without ingesting anything.
func (m *Migrator) EstimateLogs(ctx context.Context, w chstorage.Window) (Estimate, error) {
	mint, maxt, err := m.logs.Range(ctx)
	if err != nil {
		return Estimate{}, errors.Wrap(err, "resolve range")
	}
	return m.estimate(ctx, SignalLogs, w, mint, maxt, m.logs.Counts, m.logs.Size)
}

// EstimateTraces sizes a trace migration over w without ingesting anything.
func (m *Migrator) EstimateTraces(ctx context.Context, w chstorage.Window) (Estimate, error) {
	mint, maxt, err := m.traces.Range(ctx)
	if err != nil {
		return Estimate{}, errors.Wrap(err, "resolve range")
	}
	return m.estimate(ctx, SignalTraces, w, mint, maxt, m.traces.Counts, m.traces.Size)
}

// EstimateMetrics sizes a metrics migration over w without ingesting anything.
func (m *Migrator) EstimateMetrics(ctx context.Context, w chstorage.Window) (Estimate, error) {
	mint, maxt, err := m.metrics.Range(ctx)
	if err != nil {
		return Estimate{}, errors.Wrap(err, "resolve range")
	}
	return m.estimate(ctx, SignalMetrics, w, mint, maxt, m.metrics.Counts, m.metrics.Size)
}

type (
	countsFunc func(ctx context.Context, from, to time.Time) ([]chstorage.DayCount, error)
	sizeFunc   func(ctx context.Context) (chstorage.TableSize, error)
)

func (m *Migrator) estimate(
	ctx context.Context,
	signal string,
	w chstorage.Window,
	mint, maxt time.Time,
	counts countsFunc,
	size sizeFunc,
) (Estimate, error) {
	est := Estimate{Signal: signal, RowsPerSecond: RowsPerSecond(signal)}

	days, from, to, ok := plan(w, mint, maxt)
	if !ok {
		return est, nil
	}
	est.From, est.To = from, to

	byDay, err := counts(ctx, from, to)
	if err != nil {
		return est, errors.Wrap(err, "count rows")
	}
	rows := make(map[time.Time]uint64, len(byDay))
	for _, c := range byDay {
		rows[c.Day] = c.Rows
	}

	for _, d := range days {
		day := EstimateDay{
			Day:     d.Day,
			Rows:    rows[d.Day.UTC()],
			Skipped: m.checkpoint.Done(signal, d.Day),
		}
		est.Rows += day.Rows
		if !day.Skipped {
			est.Remaining += day.Rows
		}
		est.Days = append(est.Days, day)
	}

	if est.Size, err = size(ctx); err != nil {
		return est, errors.Wrap(err, "size table")
	}
	return est, nil
}

// String renders e as an operator-readable report.
func (e Estimate) String() string {
	if len(e.Days) == 0 {
		return fmt.Sprintf("%s: nothing to migrate\n\n", e.Signal)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s .. %s (%d days)\n",
		e.Signal, e.From.UTC().Format(time.DateTime), e.To.UTC().Format(time.DateTime), len(e.Days))

	tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', tabwriter.AlignRight)
	_, _ = fmt.Fprintln(tw, "  day\trows\t")
	for _, d := range e.Days {
		note := ""
		if d.Skipped {
			note = "  (done)"
		}
		_, _ = fmt.Fprintf(tw, "  %s\t%s\t%s\n", d.Day.UTC().Format(time.DateOnly), formatCount(d.Rows), note)
	}
	_, _ = fmt.Fprintf(tw, "  total\t%s\t\n", formatCount(e.Rows))
	if e.Remaining != e.Rows {
		_, _ = fmt.Fprintf(tw, "  remaining\t%s\t\n", formatCount(e.Remaining))
	}
	// tabwriter only fails on the underlying writer, and a strings.Builder never errors.
	_ = tw.Flush()

	compressed, uncompressed := e.Bytes()
	fmt.Fprintf(&b, "  source size: %s compressed / %s uncompressed (ClickHouse)\n",
		formatBytes(compressed), formatBytes(uncompressed))
	fmt.Fprintf(&b, "  projected:   %s at %s rows/s (serial path; target engine size will differ)\n\n",
		formatDuration(e.Duration()), formatCount(uint64(e.RowsPerSecond)))

	return b.String()
}

func formatProgress(i, total int) string {
	return fmt.Sprintf("%d/%d", i, total)
}

// formatCount renders a row count with thousands separators, which is what makes the difference
// between 2.7e9 and 2.7e6 legible at a glance in a sizing report.
func formatCount(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var b strings.Builder
	if lead := len(s) % 3; lead > 0 {
		b.WriteString(s[:lead])
		s = s[lead:]
	}
	for i := 0; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}

	div, exp := uint64(unit), 0
	for n/div >= unit && exp < 4 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

func formatDuration(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	case d >= time.Minute:
		return fmt.Sprintf("%.1fm", d.Minutes())
	default:
		return d.Round(time.Second).String()
	}
}
