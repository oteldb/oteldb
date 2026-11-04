package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/sdk/app"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

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
