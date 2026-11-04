package lokie2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestStorageRetentionAdmission(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	backend, closeStore, err := storagebackend.Open(ctx, storagebackend.Config{
		Backend:              "file",
		Dir:                  t.TempDir(),
		RejectOutOfRetention: true,
		Policy: &storagebackend.PolicyConfig{Retention: &storagebackend.RetentionConfig{
			MaxAge: 24 * time.Hour,
			Signals: map[string]storagebackend.SignalRetentionConfig{
				"log": {MaxAge: time.Hour},
			},
		}},
	}, zap.NewNop(), &app.Telemetry{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeStore(context.Background())) })

	now := time.Now()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("job", "retention")
	records := rl.ScopeLogs().AppendEmpty().LogRecords()
	for _, record := range []struct {
		body string
		age  time.Duration
	}{
		{body: "expired", age: 2 * time.Hour},
		{body: "fresh", age: time.Minute},
	} {
		r := records.AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-record.age)))
		r.Body().SetStr(record.body)
	}
	require.NoError(t, backend.ConsumeLogs(ctx, ld))

	querier := backend.Logs()
	client := setupDB(t, integration.TraceProvider(t), querier, querier)
	response, err := client.QueryRange(ctx, lokiapi.QueryRangeParams{
		Query: `{job="retention"}`,
		Start: lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(now.Add(-3 * time.Hour)))),
		End:   lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(now.Add(time.Second)))),
		Limit: lokiapi.NewOptInt(10),
	})
	require.NoError(t, err)
	streams, ok := response.Data.GetStreamsResult()
	require.True(t, ok)
	var bodies []string
	for _, stream := range streams.Result {
		for _, value := range stream.Values {
			bodies = append(bodies, value.V)
		}
	}
	require.Equal(t, []string{"fresh"}, bodies)

	stats := backend.Inspect()
	require.Len(t, stats.Tenants, 1)
	require.Equal(t, int64(1), stats.Tenants[0].Admission.Accepted)
	require.Equal(t, int64(1), stats.Tenants[0].Admission.RejectedRetention)
}

func TestStorageTenantRetention(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	fb, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	seed, err := storage.Open(ctx, storage.Options{},
		storage.WithBackend(fb), storage.WithFlushInterval(-1),
		storage.WithTenant(func(_ signal.Resource, scope signal.Scope) signal.TenantID {
			return signal.TenantID(scope.Name)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, seed.Close(context.Background())) })
	now := time.Now()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("job", "retention")
	ids := []signal.TenantID{"default", "team-a", "team-a/_s3"}
	for _, id := range ids {
		scope := rl.ScopeLogs().AppendEmpty()
		scope.Scope().SetName(string(id))
		for _, age := range []time.Duration{2 * time.Hour, time.Minute} {
			r := scope.LogRecords().AppendEmpty()
			r.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(-age)))
			r.Body().SetStr("retained")
		}
	}
	var batch siglog.Logs
	pdataconv.AppendLogs(&batch, ld)
	accepted, err := seed.WriteLogs(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, int64(6), accepted.Accepted)
	for _, id := range ids {
		require.NoError(t, seed.Admin().Flush(ctx, id, signal.Log))
	}
	require.NoError(t, seed.Close(ctx))

	backend, stop, err := storagebackend.Open(ctx, storagebackend.Config{
		Backend: "file", Dir: dir,
		Policy: &storagebackend.PolicyConfig{Retention: &storagebackend.RetentionConfig{MaxAge: time.Hour}},
		Tenants: map[string]*storagebackend.PolicyConfig{
			"team-a": {Retention: &storagebackend.RetentionConfig{MaxAge: 3 * time.Hour}},
		},
	}, zap.NewNop(), &app.Telemetry{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stop(context.Background())) })
	for _, tc := range []struct {
		id   signal.TenantID
		rows int
	}{
		{id: "default", rows: 1},
		{id: "team-a", rows: 2},
		{id: "team-a/_s3", rows: 2},
	} {
		t.Run(string(tc.id), func(t *testing.T) {
			it, err := backend.Store().LogFetcher(tc.id).Fetch(ctx, fetch.Request{
				Tenant: tc.id, Signal: signal.Log,
				Start: now.Add(-4 * time.Hour).UnixNano(), End: now.Add(time.Second).UnixNano(),
			})
			require.NoError(t, err)
			batches, err := fetch.Drain(ctx, it)
			require.NoError(t, err)
			rows := 0
			for _, b := range batches {
				rows += len(b.Timestamps)
			}
			require.Equal(t, tc.rows, rows)
		})
	}
	querier := backend.Logs()
	client := setupDB(t, integration.TraceProvider(t), querier, querier)
	response, err := client.QueryRange(ctx, lokiapi.QueryRangeParams{
		Query: `{job="retention"}`,
		Start: lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(now.Add(-4 * time.Hour)))),
		End:   lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(now.Add(time.Second)))),
		Limit: lokiapi.NewOptInt(10),
	})
	require.NoError(t, err)
	streams, ok := response.Data.GetStreamsResult()
	require.True(t, ok)
	rows := 0
	for _, stream := range streams.Result {
		rows += len(stream.Values)
	}
	require.Equal(t, 1, rows, "native LogQL queries still read only the default tenant")
}
