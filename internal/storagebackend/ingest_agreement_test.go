package storagebackend_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigprofile "github.com/oteldb/storage/signal/profile"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/otlpdirect"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestLogIngestPathsAgreeOnTime pins that oteldb's two OTLP log ingest paths — odbingest's direct
// wire decoder and the collector consumer over pdataconv — store the same record at the same time,
// and refuse the same record, for every combination of the two optional OTLP timestamps.
//
// They disagreed before: the collector path fell back to the observed time and refused a record
// with neither, while the direct decoder stored both at the unix epoch, outside every query window.
func TestLogIngestPathsAgreeOnTime(t *testing.T) {
	event := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	observed := event.Add(time.Second)

	for _, tt := range []struct {
		name      string
		event     time.Time
		observed  time.Time
		want      []time.Time
		wantDrops int
	}{
		{
			name: "no time",
			// Neither time: nothing to sort or query the record by, so both paths refuse it.
			wantDrops: 1,
		},
		{
			name:     "observed only",
			observed: observed,
			want:     []time.Time{observed},
		},
		{
			name:     "both",
			event:    event,
			observed: observed,
			want:     []time.Time{event},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ld := logsAt(tt.event, tt.observed)

			viaCollector, collectorDrops := ingestViaCollector(t, ld)
			viaDirect, directDrops := ingestViaOTLPDirect(t, ld)

			require.Equal(t, tt.want, viaCollector)
			require.Equal(t, viaCollector, viaDirect, "both ingest paths must store the same record")

			require.Equal(t, tt.wantDrops, collectorDrops)
			require.Equal(t, collectorDrops, directDrops, "both ingest paths must refuse the same record")
		})
	}
}

// logsAt builds a one-record batch, leaving a zero time unset the way an OTLP sender does.
func logsAt(event, observed time.Time) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")

	rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rec.Body().SetStr("hello")
	rec.SetSeverityNumber(plog.SeverityNumberInfo)

	if !event.IsZero() {
		rec.SetTimestamp(pcommon.Timestamp(event.UnixNano()))
	}

	if !observed.IsZero() {
		rec.SetObservedTimestamp(pcommon.Timestamp(observed.UnixNano()))
	}

	return ld
}

// ingestViaCollector writes through [storagebackend.Backend.ConsumeLogs], the sink the collector
// exporter drives, and reads the drop count back off its counter — the only report that path has.
func ingestViaCollector(t *testing.T, ld plog.Logs) (stored []time.Time, dropped int) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	b, ctx := backendWithMeter(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	require.NoError(t, b.ConsumeLogs(ctx, ld))

	return storedTimestamps(ctx, t, b), int(droppedRecordCount(ctx, t, reader))
}

// ingestViaOTLPDirect posts the same batch as OTLP/HTTP to the [otlpdirect] handler odbingest
// serves, and takes the drop count from the ingest stats that feed partial_success.
func ingestViaOTLPDirect(t *testing.T, ld plog.Logs) (stored []time.Time, rejected int) {
	t.Helper()

	b, ctx := backendWithMeter(t, nil)

	var stats []otlpdirect.Stats
	h := otlpdirect.NewHandler(backendSink{b}, otlpdirect.HandlerConfig{
		Observer: func(s otlpdirect.Stats) { stats = append(stats, s) },
	})

	mux := http.NewServeMux()
	h.Register(mux)

	raw, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, otlpdirect.LogsPath, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-protobuf")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body)

	require.Len(t, stats, 1)

	return storedTimestamps(ctx, t, b), stats[0].Rejected
}

func backendWithMeter(t *testing.T, mp *sdkmetric.MeterProvider) (*storagebackend.Backend, context.Context) {
	t.Helper()

	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	var opts []storagebackend.Option
	if mp != nil {
		opts = append(opts, storagebackend.WithMeterProvider(mp))
	}

	return storagebackend.New(store, opts...), ctx
}

// storedTimestamps reads every record back over a window starting at the unix epoch, so a record
// stored at the epoch is seen rather than silently missed.
func storedTimestamps(ctx context.Context, t *testing.T, b *storagebackend.Backend) []time.Time {
	t.Helper()

	node, err := b.Logs().Query(ctx, []logql.LabelMatcher{
		{Label: "service_name", Op: logql.OpEq, Value: "api"},
	})
	require.NoError(t, err)

	it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{
		Start: time.Unix(0, 0),
		End:   time.Now().Add(time.Hour),
		Limit: -1,
	})
	require.NoError(t, err)

	var out []time.Time
	for _, e := range drain(t, it) {
		out = append(out, e.Timestamp.AsTime().UTC())
	}

	return out
}

func droppedRecordCount(ctx context.Context, t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "oteldb.storage.dropped_records" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}

	return total
}

// backendSink adapts a [storagebackend.Backend] to [otlpdirect.Sink]; the handler needs all four
// signals, and the Backend has no native profiles write.
type backendSink struct{ *storagebackend.Backend }

func (s backendSink) WriteLogs(ctx context.Context, batch siglog.Logs) error {
	return s.Backend.WriteLogs(ctx, batch)
}

func (s backendSink) WriteTraces(ctx context.Context, batch sigtrace.Traces) error {
	return s.Backend.WriteTraces(ctx, batch)
}

func (s backendSink) WriteMetrics(ctx context.Context, batch sigmetric.Metrics) error {
	return s.Backend.WriteMetrics(ctx, batch)
}

func (backendSink) WriteProfiles(context.Context, *sigprofile.Profiles) error { return nil }
