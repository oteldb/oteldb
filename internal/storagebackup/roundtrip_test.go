package storagebackup_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/storagebackup"
)

// writeSample fills store with one day of logs, traces and metrics exercising every field the
// engine persists, so a dropped column shows up as a mismatch rather than as an untested field.
func writeSample(tb testing.TB, store *storage.Storage) {
	tb.Helper()

	var logs siglog.Logs
	for _, svc := range []string{"api", "worker"} {
		rl := logs.AddResource()
		rl.Resource = signal.Resource{
			SchemaURL:  []byte("https://opentelemetry.io/schemas/1.24.0"),
			Attributes: attrs(str("service.name", svc), num("service.instance", 7)),
		}
		sl := rl.AddScope()
		sl.Scope = signal.Scope{
			Name:       []byte("scope-" + svc),
			Version:    []byte("v1.2.3"),
			SchemaURL:  []byte("https://opentelemetry.io/schemas/1.24.0"),
			Attributes: attrs(str("scope.kind", "library")),
		}
		for i := range 3 {
			r := sl.AddRecord()
			r.Timestamp = at(1, i)
			r.ObservedTimestamp = at(1, i) + 500
			r.SeverityNumber = int32(9 + i)
			r.SeverityText = []byte("INFO")
			r.Body = []byte(svc + "-body-" + string(rune('a'+i)))
			r.TraceID = []byte("0123456789abcdef")
			r.SpanID = []byte("01234567")
			r.Flags = uint32(i + 1)
			r.Dropped = uint32(i)
			r.Attributes = attrs(str("http.route", "/v1/x"), num("http.status", int64(200+i)))
		}
	}
	acc, err := store.WriteLogs(tb.Context(), logs)
	require.NoError(tb, err)
	require.Zero(tb, acc.Rejected)

	var traces sigtrace.Traces
	rs := traces.AddResource()
	rs.Resource = signal.Resource{Attributes: attrs(str("service.name", "api"))}
	ss := rs.AddScope()
	ss.Scope = signal.Scope{Name: []byte("tracer"), Version: []byte("v0.1.0")}
	for i := range 3 {
		sp := ss.AddSpan()
		sp.TraceID = []byte("trace-id-16bytes")
		sp.SpanID = []byte{byte(i), 1, 2, 3, 4, 5, 6, 7}
		if i > 0 {
			sp.ParentSpanID = []byte{0, 1, 2, 3, 4, 5, 6, 7}
		}
		sp.Name = []byte("span-" + string(rune('a'+i)))
		sp.Kind = int32(i + 1)
		sp.StatusCode = int32(i % 3)
		sp.StatusMessage = []byte("ok")
		sp.Start = at(2, i)
		sp.End = at(2, i) + 1_000_000
		sp.Attributes = attrs(str("db.system", "clickhouse"), num("retries", int64(i)))

		ev := sp.AddEvent()
		ev.Time = at(2, i) + 10
		ev.Name = []byte("exception")
		ev.Attributes = attrs(str("exception.type", "boom"))

		ln := sp.AddLink()
		ln.TraceID = []byte("other-trace-id16")
		ln.SpanID = []byte{9, 9, 9, 9, 9, 9, 9, 9}
		ln.Attributes = attrs(str("link.kind", "follows"))
	}
	acc, err = store.WriteTraces(tb.Context(), traces)
	require.NoError(tb, err)
	require.Zero(tb, acc.Rejected)

	var metrics sigmetric.Metrics
	rm := metrics.AddResource()
	rm.Resource = signal.Resource{Attributes: attrs(str("service.name", "api"))}
	sm := rm.AddScope()
	sm.Scope = signal.Scope{Name: []byte("meter")}

	// One metric is finished before the next is added: AddMetric may reallocate the scope's metric
	// slice, which would leave a pointer taken earlier writing into an abandoned array.
	gauge := sm.AddMetric()
	gauge.Name = []byte("queue_depth")
	gauge.Unit = []byte("1")
	gauge.Kind = sigmetric.KindGauge
	for i := range 3 {
		p := gauge.AddPoint()
		p.Attributes = attrs(str("queue", "ingest"))
		p.Ts = at(3, i)
		p.Value = float64(i) + 0.5
	}

	counter := sm.AddMetric()
	counter.Name = []byte("requests_total")
	counter.Unit = []byte("{request}")
	counter.Kind = sigmetric.KindSum
	counter.Temporality = sigmetric.TemporalityCumulative
	counter.Monotonic = true
	for i := range 3 {
		c := counter.AddPoint()
		c.Attributes = attrs(str("method", "GET"), num("code", 200))
		c.Ts = at(3, i)
		c.Value = float64(10 * i)
	}
	acc, err = store.WriteMetrics(tb.Context(), metrics)
	require.NoError(tb, err)
	require.Zero(tb, acc.Rejected)
}

// backupOptions pins the window to the test day, so the run never consults the wall clock.
func backupOptions() storagebackup.BackupOptions {
	return storagebackup.BackupOptions{
		From: day,
		To:   day.AddDate(0, 0, 1),
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	lg := zaptest.NewLogger(t)
	src := newStore(t, nil)
	writeSample(t, src)

	dir := t.TempDir()
	stats, err := storagebackup.NewBackup(src, lg, backupOptions()).Create(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, 3, stats.Files, "one file per signal for one day")
	require.Positive(t, stats.Rows)

	dst := newStore(t, nil)
	rstats, err := storagebackup.NewRestore(storagebackend.New(dst), lg, storagebackup.RestoreOptions{}).
		Restore(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, 3, rstats.Files)
	require.Equal(t, stats.Rows, rstats.Rows)

	const tenant = signal.TenantID("default")

	wantLogs := collectLogs(t, src, tenant)
	require.Len(t, wantLogs, 6)
	require.Equal(t, wantLogs, collectLogs(t, dst, tenant))

	wantSpans := collectSpans(t, src, tenant)
	require.Len(t, wantSpans, 3)
	require.Equal(t, wantSpans, collectSpans(t, dst, tenant))

	wantSeries := collectSeries(t, src, tenant)
	require.Len(t, wantSeries, 2)
	require.Equal(t, wantSeries, collectSeries(t, dst, tenant))

	// Spot-check a field that a column-dropping bug would silently zero on both sides only if the
	// engine never stored it, which the source assertion above already rules out.
	require.Equal(t, "api-body-a", wantLogs[0].Body)
	require.Equal(t, uint32(1), wantLogs[0].Flags)
	require.Equal(t, map[string]string{"http.route": "1:/v1/x", "http.status": "3:200"}, wantLogs[0].Attrs)
	require.Equal(t, sigmetric.TemporalityCumulative, wantSeries[1].Temporality)
	require.True(t, wantSeries[1].Monotonic)
}

func TestRestoreSignalFilter(t *testing.T) {
	t.Parallel()

	lg := zaptest.NewLogger(t)
	src := newStore(t, nil)
	writeSample(t, src)

	dir := t.TempDir()
	_, err := storagebackup.NewBackup(src, lg, backupOptions()).Create(t.Context(), dir)
	require.NoError(t, err)

	dst := newStore(t, nil)
	stats, err := storagebackup.NewRestore(storagebackend.New(dst), lg, storagebackup.RestoreOptions{
		Signals: []signal.Signal{signal.Log},
	}).Restore(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files)

	const tenant = signal.TenantID("default")
	require.Len(t, collectLogs(t, dst, tenant), 6)
	require.Empty(t, collectSpans(t, dst, tenant))
	require.Empty(t, collectSeries(t, dst, tenant))
}

func TestBackupResume(t *testing.T) {
	t.Parallel()

	lg := zaptest.NewLogger(t)
	src := newStore(t, nil)
	writeSample(t, src)

	dir := t.TempDir()
	first, err := storagebackup.NewBackup(src, lg, backupOptions()).Create(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, 3, first.Files)

	opts := backupOptions()
	opts.Resume = true
	second, err := storagebackup.NewBackup(src, lg, opts).Create(t.Context(), dir)
	require.NoError(t, err)
	require.Zero(t, second.Files, "a completed day is not rewritten")
	require.Equal(t, 3, second.Skipped)

	// The skipped run must not have damaged what the first wrote.
	dst := newStore(t, nil)
	_, err = storagebackup.NewRestore(storagebackend.New(dst), lg, storagebackup.RestoreOptions{}).
		Restore(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, collectLogs(t, src, "default"), collectLogs(t, dst, "default"))
}
