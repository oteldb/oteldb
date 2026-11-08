package tempoe2e_test

import (
	"context"
	"math"
	"net/http"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

// TestStorageBackendMetricsOperators covers ingestion, persisted span reads, TraceQL
// filtering, and shared numeric operators. The first-stage adapter below is test-only;
// the metrics HTTP endpoints are still unimplemented and are checked separately.
func TestStorageBackendMetricsOperators(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0).UTC()
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
			source := traceCountSource{querier: db.Traces(), start: base, step: step}
			for _, tt := range []struct {
				name, query string
				want        map[string][]float64
			}{
				{"count", `{} | count_over_time()`, map[string][]float64{"": {4, 3, 3, 0}}},
				{"grouped count", `{} | count_over_time() by(resource.service.name)`, map[string][]float64{"checkout": {3, 1, 1, 0}, "payments": {1, 2, 2, 0}}},
				{"error rate", `{status=error} | rate() by(resource.service.name)`, map[string][]float64{"checkout": {2.0 / 15, 0, 1.0 / 15, 0}, "payments": {1.0 / 15, 0, 1.0 / 15, 0}}},
				{"duration predicate", `{duration>500ms} | count_over_time() by(resource.service.name)`, map[string][]float64{"checkout": {1, 1, 1, 0}}},
				{"result filter", `{} | count_over_time() by(resource.service.name) > 2`, map[string][]float64{"checkout": {3, gap, gap, gap}}},
				{"topk", `{} | rate() by(resource.service.name) > 0 | topk(1)`, map[string][]float64{"checkout": {3.0 / 15, gap, gap, gap}, "payments": {gap, 2.0 / 15, 2.0 / 15, gap}}},
				{"bottomk", `{} | rate() by(resource.service.name) > 0 | bottomk(1)`, map[string][]float64{"checkout": {gap, 1.0 / 15, 1.0 / 15, gap}, "payments": {1.0 / 15, gap, gap, gap}}},
				{"ratio with broadcast and zero denominator", `({status=error} | rate() by(resource.service.name)) / ({} | rate())`, map[string][]float64{"checkout": {.5, 0, 1.0 / 3, gap}, "payments": {.25, 0, 1.0 / 3, gap}}},
				{"empty selection", `{resource.service.name="missing"} | count_over_time()`, map[string][]float64{}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					expr, err := traceql.Parse(tt.query)
					require.NoError(t, err)
					ec := scarecrow.NewEvalContext([]int64{base.UnixMilli(), base.Add(step).UnixMilli(), base.Add(2 * step).UnixMilli(), base.Add(3 * step).UnixMilli()}, 100)
					root, err := traceqlengine.BuildMetrics(t.Context(), expr.(traceql.MetricsExpr), source, ec)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, root.Close()) })
					schema, err := root.Schema(t.Context())
					require.NoError(t, err)
					got := map[string][]float64{}
					for {
						column, err := root.Next(t.Context())
						require.NoError(t, err)
						if column == nil {
							break
						}
						name := schema.At(column.Ref).Get("resource.service.name")
						_, duplicate := got[name]
						require.False(t, duplicate, "duplicate result series %q", name)
						values := make([]float64, len(column.V))
						for i := range values {
							values[i] = math.NaN()
							if column.IsSet(i) {
								values[i] = column.V[i]
							}
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
			{-time.Nanosecond, time.Millisecond, true},
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

// traceCountSource is a test adapter, not the production TraceQL metrics source.
// It reads real spans and supplies only count/rate buckets for operator integration.
type traceCountSource struct {
	querier traceqlengine.Querier
	start   time.Time
	step    time.Duration
}

func (s traceCountSource) Aggregation(ctx context.Context, agg *traceql.MetricsAggregation, ec *scarecrow.EvalContext) (scarecrow.Operator, error) {
	if agg.Op != traceql.MetricsOpCountOverTime && agg.Op != traceql.MetricsOpRate {
		return nil, errors.New("test source supports count and rate only")
	}
	grouped := len(agg.By) == 1 && agg.By[0] == (traceql.Attribute{Scope: traceql.ScopeResource, Name: "service.name"})
	if len(agg.By) != 0 && !grouped {
		return nil, errors.New("test source groups by service.name only")
	}
	processor, err := traceqlengine.BuildExpr(agg.Spanset)
	if err != nil {
		return nil, err
	}
	op, matchers := traceql.ExtractMatchers(agg.Spanset)
	input, err := s.querier.SelectSpansets(ctx, traceqlengine.SelectSpansetsParams{Op: op, Matchers: matchers})
	if err != nil {
		return nil, err
	}
	defer func() { _ = input.Close() }()
	counts := map[string][]float64{}
	var trace traceqlengine.Trace
	for input.Next(&trace) {
		sets, err := processor.Process([]traceqlengine.Spanset{{TraceID: trace.TraceID, Spans: trace.Spans}})
		if err != nil {
			return nil, err
		}
		for _, set := range sets {
			for _, span := range set.Spans {
				offset := span.Start.AsTime().Sub(s.start)
				if offset < 0 || offset >= time.Duration(ec.NumSteps())*s.step {
					continue
				}
				name := ""
				if grouped {
					name, _ = span.ServiceName()
				}
				if counts[name] == nil {
					counts[name] = make([]float64, ec.NumSteps())
				}
				counts[name][int(offset/s.step)]++
			}
		}
	}
	if err := input.Err(); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	slices.Sort(names)
	out := &traceCountColumns{ec: ec}
	series := make([]labels.Labels, len(names))
	for i, name := range names {
		series[i] = labels.EmptyLabels()
		if grouped {
			series[i] = labels.FromStrings("resource.service.name", name)
		}
		column := scarecrow.NewColumn(scarecrow.SeriesRef(i), ec.NumSteps())
		for bucket, count := range counts[name] {
			if agg.Op == traceql.MetricsOpRate {
				count /= s.step.Seconds()
			}
			column.Set(bucket, count)
		}
		out.columns = append(out.columns, column)
	}
	out.schema = scarecrow.NewSchema(series)
	return out, nil
}

type traceCountColumns struct {
	schema  *scarecrow.Schema
	columns []*scarecrow.Column
	ec      *scarecrow.EvalContext
	next    int
}

func (*traceCountColumns) String() string                                      { return "NativeTraceCountFixture" }
func (*traceCountColumns) Children() []scarecrow.Operator                      { return nil }
func (*traceCountColumns) Close() error                                        { return nil }
func (s *traceCountColumns) Schema(context.Context) (*scarecrow.Schema, error) { return s.schema, nil }

func (s *traceCountColumns) Next(ctx context.Context) (*scarecrow.Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.next == len(s.columns) {
		return nil, nil
	}
	column := s.columns[s.next]
	if err := s.ec.ChargeSamples(column.Count()); err != nil {
		return nil, err
	}
	s.next++
	return column, nil
}

func TestMetricsHTTPNotImplemented(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	q := storagebackend.New(store).Traces()
	client := setupDB(t, integration.TraceProvider(t), q, q)
	base := time.Unix(1_700_000_000, 0).UTC()
	for _, invalidRange := range []bool{false, true} {
		start, end := base, base.Add(time.Minute)
		want := http.StatusNotImplemented
		if invalidRange {
			start, end, want = end, start, http.StatusBadRequest
		}
		for _, instant := range []bool{false, true} {
			name := "range"
			if instant {
				name = "instant"
			}
			if invalidRange {
				name += "/invalid-range"
			}
			t.Run(name, func(t *testing.T) {
				var err error
				if instant {
					_, err = client.Query(t.Context(), tempoapi.QueryParams{Q: `{} | rate()`, Start: tempoTime(start), End: tempoTime(end)})
				} else {
					_, err = client.QueryRange(t.Context(), tempoapi.QueryRangeParams{Q: `{} | rate()`, Start: tempoTime(start), End: tempoTime(end), Step: tempoapi.NewOptPrometheusDuration("15s")})
				}
				var apiErr *tempoapi.ErrorStatusCode
				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, want, apiErr.StatusCode)
				require.NotEmpty(t, apiErr.Response)
			})
		}
	}
}
