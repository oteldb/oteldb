// Package ch2storagebackende2e_test, OTLP destination.
//
// The migrator's OTLP path exists to load a cluster, where writes must go through ingest routing
// rather than into one node's backend. These tests stand in odbingest's own receiving stack --
// [otlpdirect.Handler] over gRPC -- in front of a memory-backed engine, and assert that migrating
// through it lands the same data as writing the engine directly. That equality is the point: the
// two destinations convert differently (internal model vs pdata), so they can drift.
package ch2storagebackende2e_test

import (
	"context"
	"net"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"google.golang.org/grpc"

	"github.com/oteldb/oteldb/internal/iterators"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	sigprofile "github.com/oteldb/storage/signal/profile"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/ch2storagebackend"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/lokihandler"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/otlpdirect"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// otlpSink adapts a [storagebackend.Backend] to [otlpdirect.Sink]. Only profiles need adapting:
// chstorage stores none, so the migrator never sends any and this method is unreachable.
type otlpSink struct {
	*storagebackend.Backend
}

func (otlpSink) WriteProfiles(context.Context, *sigprofile.Profiles) error {
	return errors.New("ch2storagebackend does not migrate profiles")
}

// startOTLPReceiver serves the same OTLP handler odbingest runs, writing into back, and returns
// its address.
func startOTLPReceiver(t *testing.T, back *storagebackend.Backend) string {
	t.Helper()

	h := otlpdirect.NewHandler(otlpSink{back}, otlpdirect.HandlerConfig{
		Logger: integration.Logger(t),
	})

	srv := grpc.NewServer(h.GRPCServerOptions()...)
	h.RegisterGRPC(srv)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(srv.Stop)

	return ln.Addr().String()
}

// openMemoryBackend opens an ephemeral in-memory engine.
func openMemoryBackend(t *testing.T) *storagebackend.Backend {
	t.Helper()

	store, err := storage.Open(t.Context(), storage.Options{},
		storage.WithBackend(backend.Memory()),
		storage.WithDurability(storage.DurabilityEphemeral),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close(context.WithoutCancel(t.Context()))
	})

	return storagebackend.New(store)
}

// queryLogBodies returns every log body the engine serves in the window around base, sorted.
func queryLogBodies(t *testing.T, back *storagebackend.Backend, base time.Time) []string {
	t.Helper()

	provider := integration.TraceProvider(t)

	engine, err := logqlengine.NewEngine(back.Logs(), logqlengine.Options{
		ParseOptions:   logql.ParseOptions{AllowDots: true},
		Optimizers:     logqlengine.DefaultOptimizers(),
		TracerProvider: provider,
	})
	require.NoError(t, err)

	api := lokihandler.NewLokiAPI(back.Logs(), engine, lokihandler.LokiAPIOptions{})
	h, err := lokiapi.NewServer(api, lokiapi.WithTracerProvider(provider))
	require.NoError(t, err)

	s := httptest.NewServer(h)
	t.Cleanup(s.Close)

	c, err := lokiapi.NewClient(s.URL, lokiapi.WithClient(s.Client()), lokiapi.WithTracerProvider(provider))
	require.NoError(t, err)

	resp, err := c.QueryRange(t.Context(), lokiapi.QueryRangeParams{
		Query: `{service_name=~".+"}`,
		Start: lokiapi.NewOptLokiTime(asLokiTime(base.Add(-time.Minute))),
		End:   lokiapi.NewOptLokiTime(asLokiTime(base.Add(time.Minute))),
		Limit: lokiapi.NewOptInt(100),
	})
	require.NoError(t, err)

	streams, ok := resp.Data.GetStreamsResult()
	require.True(t, ok)

	var bodies []string
	for _, stream := range streams.Result {
		require.True(t, stream.Stream.Set)
		require.Contains(t, stream.Stream.Value, "service_name")
		for _, v := range stream.Values {
			bodies = append(bodies, v.V)
		}
	}
	slices.Sort(bodies)

	return bodies
}

func TestMigrateLogsOverOTLP(t *testing.T) {
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

	// Same source, both destinations.
	direct := openMemoryBackend(t)
	viaOTLP := openMemoryBackend(t)

	dm := ch2storagebackend.NewMigrator(client, tables, direct, integration.Logger(t))
	require.NoError(t, dm.Err())
	directStats, err := dm.MigrateLogs(ctx, chstorage.Window{}, 2)
	require.NoError(t, err)

	om := ch2storagebackend.NewMigrator(client, tables, nil, integration.Logger(t),
		ch2storagebackend.WithOTLP(startOTLPReceiver(t, viaOTLP), 64<<20),
	)
	require.NoError(t, om.Err())
	t.Cleanup(func() {
		_ = om.Close()
	})
	// A batch size below the record count forces more than one export.
	otlpStats, err := om.MigrateLogs(ctx, chstorage.Window{}, 2)
	require.NoError(t, err)

	require.Equal(t, 4, otlpStats.Records)
	require.Equal(t, 2, otlpStats.Batches)
	require.Equal(t, directStats.Records, otlpStats.Records)

	want := queryLogBodies(t, direct, base)
	require.Len(t, want, 4)
	require.Equal(t, want, queryLogBodies(t, viaOTLP, base),
		"OTLP destination served different logs than the direct one")
}

func TestMigrateTracesOverOTLP(t *testing.T) {
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

	consumer := tracestorage.NewConsumer(inserter)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	traceID := pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	require.NoError(t, consumer.ConsumeTraces(ctx, buildTraces(base, traceID)))

	viaOTLP := openMemoryBackend(t)

	m := ch2storagebackend.NewMigrator(client, tables, nil, integration.Logger(t),
		ch2storagebackend.WithOTLP(startOTLPReceiver(t, viaOTLP), 64<<20),
	)
	require.NoError(t, m.Err())
	t.Cleanup(func() {
		_ = m.Close()
	})

	stats, err := m.MigrateTraces(ctx, chstorage.Window{}, 1)
	require.NoError(t, err)
	require.Equal(t, 2, stats.Spans)
	// One span per batch, so the export path runs more than once.
	require.Equal(t, 2, stats.Batches)

	it, err := viaOTLP.Traces().TraceByID(ctx, otelstorage.TraceID(traceID), tracestorage.TraceByIDOptions{})
	require.NoError(t, err)

	spans := map[string]tracestorage.Span{}
	require.NoError(t, iterators.ForEach(it, func(s tracestorage.Span) error {
		spans[s.Name] = s
		return nil
	}))
	require.Len(t, spans, 2)
	require.Contains(t, spans, "root")
	require.Contains(t, spans, "child")

	root := spans["root"]
	serviceName, ok := root.ServiceName()
	require.True(t, ok)
	require.Equal(t, "traceService", serviceName)
	v, ok := root.Attrs.AsMap().Get("http.method")
	require.True(t, ok, "root span missing http.method attribute")
	require.Equal(t, "GET", v.AsString())

	require.Equal(t,
		otelstorage.SpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8}),
		spans["child"].ParentSpanID,
	)
}
