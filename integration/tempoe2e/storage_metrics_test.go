package tempoe2e_test

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
)

func TestStorageBackendMetricsHTTP(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_010, 0).UTC()
	const step = 15 * time.Second
	gap := math.NaN()
	for _, state := range []string{"head", "parts", "reopened"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			files, err := backendfile.New(filepath.Join(dir, "objects"))
			require.NoError(t, err)
			open := func() *storage.Storage {
				store, err := storage.Open(t.Context(), storage.Options{}, storage.WithBackend(files),
					storage.WithWALDir(filepath.Join(dir, "wal")), storage.WithFlushInterval(-1))
				require.NoError(t, err)
				return store
			}
			store := open()
			t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
			db := storagebackend.New(store)
			require.NoError(t, db.ConsumeTraces(t.Context(), metricsTraceFixture(base)))
			if state != "head" {
				require.NoError(t, store.Admin().Flush(t.Context(), "default", signal.Trace))
			}
			if state == "reopened" {
				require.NoError(t, store.Close(t.Context()))
				store = open()
				db = storagebackend.New(store)
			}
			q := db.Traces()
			client := setupDB(t, integration.TraceProvider(t), q, q)
			for _, tt := range []struct {
				name, query string
				want        map[string][]float64
			}{
				{"count", `{} | count_over_time()`, map[string][]float64{"": {2, 3, 4, 1, 1, 0}}},
				{"grouped count", `{} | count_over_time() by(resource.service.name)`, map[string][]float64{"checkout": {2, 2, 1, 0, 1, 0}, "payments": {0, 1, 3, 1, 0, 0}}},
				{"error rate", `{status=error} | rate() by(resource.service.name)`, map[string][]float64{"checkout": {1.0 / 15, 1.0 / 15, 1.0 / 15, 0, 1.0 / 15, 0}, "payments": {0, 1.0 / 15, 1.0 / 15, 0, 0, 0}}},
				{"duration predicate", `{duration>500ms} | count_over_time() by(resource.service.name)`, map[string][]float64{"checkout": {1, 1, 1, 0, 0, 0}}},
				{"result filter", `{} | count_over_time() by(resource.service.name) > 2`, map[string][]float64{"payments": {gap, gap, 3, gap, gap, gap}}},
				{"topk", `{} | rate() by(resource.service.name) > 0 | topk(1)`, map[string][]float64{"checkout": {2.0 / 15, 2.0 / 15, gap, gap, 1.0 / 15, gap}, "payments": {gap, gap, 3.0 / 15, 1.0 / 15, gap, gap}}},
				{"bottomk", `{} | rate() by(resource.service.name) > 0 | bottomk(1)`, map[string][]float64{"checkout": {2.0 / 15, gap, 1.0 / 15, gap, 1.0 / 15, gap}, "payments": {gap, 1.0 / 15, gap, 1.0 / 15, gap, gap}}},
				{"broadcast ratio", `({status=error} | rate() by(resource.service.name)) / ({} | rate())`, map[string][]float64{"checkout": {.5, 1.0 / 3, .25, 0, 1, gap}, "payments": {0, 1.0 / 3, .25, 0, 0, gap}}},
				{"empty selection", `{resource.service.name="missing"} | count_over_time()`, map[string][]float64{"": {0, 0, 0, 0, 0, 0}}},
				{"empty grouped selection", `{resource.service.name="missing"} | count_over_time() by(resource.service.name)`, map[string][]float64{}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					result, err := client.QueryRange(t.Context(), tempoapi.QueryRangeParams{Q: tt.query, Start: tempoTime(base), End: tempoTime(base.Add(75 * time.Second)), Step: tempoapi.NewOptPrometheusDuration("15s")})
					require.NoError(t, err)
					got := map[string][]float64{}
					for _, series := range result.Series {
						name := ""
						for _, label := range series.Labels {
							if label.Key == "resource.service.name" {
								name = label.Value.StringValue.StringValue
							}
						}
						_, duplicate := got[name]
						require.False(t, duplicate)
						values := []float64{gap, gap, gap, gap, gap, gap}
						seen := map[int]bool{}
						for _, sample := range series.Samples {
							require.True(t, sample.TimestampMs.Set)
							require.True(t, sample.Value.Set)
							offset := int64(sample.TimestampMs.Value) - base.UnixMilli()
							require.Zero(t, offset%step.Milliseconds())
							i := int(offset / step.Milliseconds())
							require.GreaterOrEqual(t, i, 0)
							require.Less(t, i, 6)
							require.False(t, seen[i])
							seen[i] = true
							values[i] = sample.Value.Value
						}
						got[name] = values
					}
					require.Len(t, got, len(tt.want))
					for name, values := range tt.want {
						require.Contains(t, got, name)
						require.Len(t, got[name], len(values))
						for i, value := range values {
							if math.IsNaN(value) {
								require.True(t, math.IsNaN(got[name][i]), "%s bucket %d", name, i)
							} else {
								require.InDelta(t, value, got[name][i], 1e-12, "%s bucket %d", name, i)
							}
						}
					}
				})
			}
			checkMetricsInstant(t, client, base)
		})
	}
}

func metricsTraceFixture(base time.Time) ptrace.Traces {
	td := ptrace.NewTraces()
	type point struct {
		start, duration time.Duration
		failed          bool
	}
	for i, service := range []struct {
		name  string
		spans []point
	}{
		{"checkout", []point{
			{-15 * time.Second, time.Millisecond, true},
			{0, 250 * time.Millisecond, false},
			{0, 750 * time.Millisecond, true},
			{15*time.Second - time.Nanosecond, 500 * time.Millisecond, true},
			{15 * time.Second, time.Second, false},
			{30 * time.Second, 1500 * time.Millisecond, true},
			{60 * time.Second, time.Millisecond, true},
		}},
		{"payments", []point{
			{time.Second, 100 * time.Millisecond, true},
			{16 * time.Second, 200 * time.Millisecond, false},
			{29 * time.Second, 300 * time.Millisecond, false},
			{30 * time.Second, 500 * time.Millisecond, true},
			{31 * time.Second, 500 * time.Millisecond, false},
		}},
	} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", service.name)
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		for j, record := range service.spans {
			span := spans.AppendEmpty()
			span.SetTraceID(pcommon.TraceID{15: byte(i + 1)})
			span.SetSpanID(pcommon.SpanID{7: byte(j + 1)})
			span.SetName("request")
			span.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(record.start)))
			span.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(record.start + record.duration)))
			if record.failed {
				span.Status().SetCode(ptrace.StatusCodeError)
			} else {
				span.Status().SetCode(ptrace.StatusCodeOk)
			}
		}
	}
	return td
}
