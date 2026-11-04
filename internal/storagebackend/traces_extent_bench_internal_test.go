package storagebackend

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

func BenchmarkTraceExtentFetch(b *testing.B) {
	be := backendtest.NewCounting(backend.Memory())
	store, err := storage.Open(b.Context(), storage.Options{}, storage.WithBackend(be))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close(context.Background()) })
	db := New(store)
	base := time.Unix(1_700_000_000, 0).UTC()
	const groups, tracesPerGroup, spansPerTrace, payloadBytes = 8, 8, 2, 1024
	for group := range groups {
		td := ptrace.NewTraces()
		for j := range spansPerTrace {
			rs := td.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutInt("service", int64(j))
			ss := rs.ScopeSpans().AppendEmpty()
			for i := range tracesPerGroup {
				id := group*tracesPerGroup + i
				s := ss.Spans().AppendEmpty()
				s.SetTraceID(pcommon.TraceID{15: byte(id + 1)})
				s.SetSpanID(pcommon.SpanID{7: byte(j + 1)})
				s.SetName("other")
				if group == groups-1 {
					s.SetName("target")
				}
				if j > 0 {
					s.SetParentSpanID(pcommon.SpanID{7: 1})
				}
				start := base.Add(time.Duration(id) * 10 * time.Second)
				if j > 0 {
					offset := 100 * time.Millisecond
					if group == groups-1 {
						offset = 2 * time.Second
					}
					start = start.Add(offset)
				}
				s.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
				s.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Second)))
				event := s.Events().AppendEmpty()
				event.SetName("event")
				event.Attributes().PutStr("payload", fmt.Sprintf("%d:%s", id, strings.Repeat("x", payloadBytes)))
			}
		}
		if err := db.ConsumeTraces(b.Context(), td); err != nil {
			b.Fatal(err)
		}
		if err := store.Admin().Flush(b.Context(), "default", signal.Trace); err != nil {
			b.Fatal(err)
		}
	}
	eventKeys := map[string]bool{}
	keys, err := be.Backend.List(b.Context(), "")
	if err != nil {
		b.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, "/manifest") {
			continue
		}
		data, err := be.Backend.Read(b.Context(), key)
		if err != nil {
			b.Fatal(err)
		}
		manifest, err := block.DecodeManifest(data)
		if err != nil {
			b.Fatal(err)
		}
		for i, column := range manifest.Columns {
			if column.Name == sigtrace.ColEvents {
				eventKeys[fmt.Sprintf("%s/c/%d", strings.TrimSuffix(key, "/manifest"), i)] = true
			}
		}
	}
	params := traceqlengine.EvalParams{Start: base, End: base.Add(1000 * time.Second), Limit: groups * tracesPerGroup}
	for _, tt := range []struct {
		name, query string
		want        int
	}{
		{"duration_selective", `{traceDuration >= 3s && event:name = "event"}`, tracesPerGroup},
		{"nonselective", `{traceDuration >= 1s && event:name = "event"}`, groups * tracesPerGroup},
		{"span_selective", `{name = "target" && traceDuration >= 1s && event:name = "event"}`, tracesPerGroup},
	} {
		b.Run(tt.name, func(b *testing.B) {
			engine := traceqlengine.NewEngine(db.Traces(), traceqlengine.Options{})
			before := be.Reads()
			b.ReportAllocs()
			b.SetBytes(groups * tracesPerGroup * spansPerTrace * payloadBytes)
			for b.Loop() {
				got, err := engine.Eval(b.Context(), tt.query, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(got.Traces) != tt.want {
					b.Fatalf("traces=%d want=%d", len(got.Traces), tt.want)
				}
			}
			reads, eventReads := 0, 0
			for key, n := range be.Reads() {
				reads += n - before[key]
				if eventKeys[key] {
					eventReads += n - before[key]
				}
			}
			b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
			b.ReportMetric(float64(eventReads)/float64(b.N), "event_reads/op")
		})
	}
}
