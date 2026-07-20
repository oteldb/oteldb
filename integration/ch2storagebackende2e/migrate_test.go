// Package ch2storagebackende2e_test verifies that [ch2storagebackend.Migrator] faithfully
// migrates logs out of a real ClickHouse-backed chstorage into the embedded storagebackend
// engine.
package ch2storagebackende2e_test

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/ch2storagebackend"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/lokihandler"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// buildLogs constructs two resources (serviceA, serviceB) with two log records each, at
// distinct timestamps starting at base.
func buildLogs(base time.Time) plog.Logs {
	ld := plog.NewLogs()

	addResource := func(serviceName string) plog.ScopeLogs {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", serviceName)
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("ch2storagebackend_test")
		return sl
	}

	addRecord := func(sl plog.ScopeLogs, offset time.Duration, severity plog.SeverityNumber, body string) {
		lr := sl.LogRecords().AppendEmpty()
		ts := pcommon.NewTimestampFromTime(base.Add(offset))
		lr.SetTimestamp(ts)
		lr.SetObservedTimestamp(ts)
		lr.SetSeverityNumber(severity)
		lr.SetSeverityText(severity.String())
		lr.Body().SetStr(body)
		lr.Attributes().PutStr("http.method", "GET")
	}

	slA := addResource("serviceA")
	addRecord(slA, 0*time.Second, plog.SeverityNumberInfo, "hello from A #1")
	addRecord(slA, 1*time.Second, plog.SeverityNumberWarn, "hello from A #2")

	slB := addResource("serviceB")
	addRecord(slB, 2*time.Second, plog.SeverityNumberError, "hello from B #1")
	addRecord(slB, 3*time.Second, plog.SeverityNumberDebug, "hello from B #2")

	return ld
}

func TestMigrateLogs(t *testing.T) {
	integration.Skip(t)
	var (
		ctx      = t.Context()
		provider = integration.TraceProvider(t)
	)

	_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "ch2storagebackend",
		TablePrefix:    uniqueTablePrefix(),
		TracerProvider: provider,
	})

	inserter, err := chstorage.NewInserter(client, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	consumer, err := logstorage.NewConsumer(inserter, logstorage.ConsumerOptions{})
	require.NoError(t, err)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, consumer.ConsumeLogs(ctx, buildLogs(base)))

	store, err := storage.Open(ctx, storage.Options{},
		storage.WithBackend(backend.Memory()),
		storage.WithDurability(storage.DurabilityEphemeral),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close(ctx)
	})
	back := storagebackend.New(store)

	m := ch2storagebackend.NewMigrator(client, tables, back, integration.Logger(t))
	// Use a batch size smaller than the record count so the migration exercises more than
	// one ConsumeLogs call.
	stats, err := m.MigrateLogs(ctx, 0, 2)
	require.NoError(t, err)
	require.Equal(t, 4, stats.Records)
	require.Equal(t, 2, stats.Batches)

	engine, err := logqlengine.NewEngine(back.Logs(), logqlengine.Options{
		ParseOptions:   logql.ParseOptions{AllowDots: true},
		Optimizers:     logqlengine.DefaultOptimizers(),
		TracerProvider: provider,
	})
	require.NoError(t, err)

	api := lokihandler.NewLokiAPI(back.Logs(), engine, lokihandler.LokiAPIOptions{})
	lokih, err := lokiapi.NewServer(api, lokiapi.WithTracerProvider(provider))
	require.NoError(t, err)

	s := httptest.NewServer(lokih)
	t.Cleanup(s.Close)

	c, err := lokiapi.NewClient(s.URL, lokiapi.WithClient(s.Client()), lokiapi.WithTracerProvider(provider))
	require.NoError(t, err)

	resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
		Query: `{service_name=~".+"}`,
		Start: lokiapi.NewOptLokiTime(asLokiTime(base.Add(-time.Minute))),
		End:   lokiapi.NewOptLokiTime(asLokiTime(base.Add(time.Minute))),
		Limit: lokiapi.NewOptInt(100),
	})
	require.NoError(t, err)

	streams, ok := resp.Data.GetStreamsResult()
	require.True(t, ok)

	gotBodies := map[string]struct{}{}
	var total int
	for _, stream := range streams.Result {
		require.True(t, stream.Stream.Set)
		require.Contains(t, stream.Stream.Value, "service_name")
		for _, v := range stream.Values {
			total++
			gotBodies[v.V] = struct{}{}
		}
	}
	require.Equal(t, 4, total)
	for _, want := range []string{
		"hello from A #1", "hello from A #2", "hello from B #1", "hello from B #2",
	} {
		require.Containsf(t, gotBodies, want, "missing body %q", want)
	}
}

func asLokiTime(t time.Time) lokiapi.LokiTime {
	return lokiapi.LokiTime(t.Format(time.RFC3339Nano))
}

// uniqueTablePrefix returns a per-run table prefix so tests stay idempotent against a reused
// ClickHouse container (which otherwise accumulates data across runs and breaks exact-count
// assertions).
func uniqueTablePrefix() string {
	return "ch2sb_" + strings.ReplaceAll(uuid.NewString(), "-", "")
}

// buildTraces constructs a single trace with two spans (a root and a child), belonging to
// one resource/scope.
func buildTraces(base time.Time, traceID pcommon.TraceID) ptrace.Traces {
	td := ptrace.NewTraces()

	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "traceService")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("ch2storagebackend_test")

	root := ss.Spans().AppendEmpty()
	root.SetTraceID(traceID)
	root.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	root.SetName("root")
	root.SetKind(ptrace.SpanKindServer)
	root.SetStartTimestamp(pcommon.NewTimestampFromTime(base))
	root.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(2 * time.Second)))
	root.Attributes().PutStr("http.method", "GET")

	child := ss.Spans().AppendEmpty()
	child.SetTraceID(traceID)
	child.SetSpanID(pcommon.SpanID{8, 7, 6, 5, 4, 3, 2, 1})
	child.SetParentSpanID(root.SpanID())
	child.SetName("child")
	child.SetKind(ptrace.SpanKindClient)
	child.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(500 * time.Millisecond)))
	child.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(1500 * time.Millisecond)))

	return td
}

func TestMigrateTraces(t *testing.T) {
	integration.Skip(t)
	var (
		ctx      = t.Context()
		provider = integration.TraceProvider(t)
	)

	_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "ch2storagebackend-traces",
		TablePrefix:    uniqueTablePrefix(),
		TracerProvider: provider,
	})

	inserter, err := chstorage.NewInserter(client, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	consumer := tracestorage.NewConsumer(inserter)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	traceID := pcommon.TraceID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	require.NoError(t, consumer.ConsumeTraces(ctx, buildTraces(base, traceID)))

	store, err := storage.Open(ctx, storage.Options{},
		storage.WithBackend(backend.Memory()),
		storage.WithDurability(storage.DurabilityEphemeral),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close(ctx)
	})
	back := storagebackend.New(store)

	m := ch2storagebackend.NewMigrator(client, tables, back, integration.Logger(t))
	// Use a batch size smaller than the span count so the migration exercises more than one
	// ConsumeTraces call.
	stats, err := m.MigrateTraces(ctx, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, stats.Spans)
	require.Equal(t, 2, stats.Batches)

	it, err := back.Traces().TraceByID(ctx, otelstorage.TraceID(traceID), tracestorage.TraceByIDOptions{})
	require.NoError(t, err)

	var spans []tracestorage.Span
	require.NoError(t, iterators.ForEach(it, func(s tracestorage.Span) error {
		spans = append(spans, s)
		return nil
	}))

	require.Len(t, spans, 2)
	gotNames := map[string]tracestorage.Span{}
	for _, s := range spans {
		gotNames[s.Name] = s
	}
	require.Contains(t, gotNames, "root")
	require.Contains(t, gotNames, "child")

	root := gotNames["root"]
	serviceName, ok := root.ServiceName()
	require.True(t, ok)
	require.Equal(t, "traceService", serviceName)
	if v, ok := root.Attrs.AsMap().Get("http.method"); ok {
		require.Equal(t, "GET", v.AsString())
	} else {
		t.Fatal("root span missing http.method attribute")
	}

	child := gotNames["child"]
	require.Equal(t, otelstorage.SpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8}), child.ParentSpanID)
}
