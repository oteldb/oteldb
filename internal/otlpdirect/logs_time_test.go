package otlpdirect_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// TestConvertLogsTimeFallback pins the rule for the two optional OTLP timestamps: an unset event
// time falls back to the observed time, and a record with neither is refused rather than stored at
// the unix epoch. convertBoth also asserts the pdata path answers the same way.
func TestConvertLogsTimeFallback(t *testing.T) {
	t.Parallel()

	const (
		event    = 1_700_000_000_000_000_000
		observed = 1_700_000_001_000_000_000
	)

	for _, tt := range []struct {
		name      string
		event     pcommon.Timestamp
		observed  pcommon.Timestamp
		want      int64
		wantDrops int
	}{
		{name: "no time", wantDrops: 1},
		{name: "observed only", observed: observed, want: observed},
		{name: "both", event: event, observed: observed, want: event},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ld := plog.NewLogs()
			rec := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			rec.Body().SetStr("hello")
			rec.SetTimestamp(tt.event)
			rec.SetObservedTimestamp(tt.observed)

			direct, viaPdata := convertBoth(t, ld)
			assert.Equal(t, viaPdata, direct)

			var c otlpdirect.LogsConverter

			got, dropped, err := c.Convert(marshal(t, ld))
			require.NoError(t, err)
			assert.Equal(t, tt.wantDrops, dropped)

			records := got.Resources[0].Scopes[0].Records
			if tt.wantDrops > 0 {
				assert.Empty(t, records)
				return
			}

			require.Len(t, records, 1)
			assert.Equal(t, tt.want, records[0].Timestamp)
			assert.Equal(t, int64(tt.observed), records[0].ObservedTimestamp)
		})
	}
}

// TestHandlerLogsPartialSuccess pins that a timeless record is reported to the sender as a rejected
// log record instead of vanishing, while the rest of the batch is stored.
func TestHandlerLogsPartialSuccess(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	mux, stats := serveOTLP(t, sink)

	rec := post(mux, otlpdirect.LogsPath, marshal(t, logsWithTimelessRecord()), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rejected, message := decodePartialSuccess(t, rec.Body.Bytes())
	assert.Equal(t, int64(1), rejected)
	assert.NotEmpty(t, message, "a partial success must say what happened")

	assert.Equal(t, 1, sink.logs, "the timestamped record is still stored")
	require.Len(t, *stats, 1)
	assert.Equal(t, 1, (*stats)[0].Rejected)
	assert.Equal(t, signal.Log, (*stats)[0].Signal)
}

// TestGRPCLogsPartialSuccess pins the same count over gRPC, where the generated client reads it
// back as rejected_log_records.
func TestGRPCLogsPartialSuccess(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	cc := serveGRPC(t, sink)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	req := plogotlp.NewExportRequestFromLogs(logsWithTimelessRecord())

	resp, err := plogotlp.NewGRPCClient(cc).Export(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, int64(1), resp.PartialSuccess().RejectedLogRecords())
	assert.NotEmpty(t, resp.PartialSuccess().ErrorMessage())
	assert.Equal(t, 1, sink.logs, "the timestamped record is still stored")
}

// logsWithTimelessRecord builds a batch of two records, one of which carries no time at all.
func logsWithTimelessRecord() plog.Logs {
	ld := plog.NewLogs()
	records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()

	records.AppendEmpty().Body().SetStr("timeless")

	stored := records.AppendEmpty()
	stored.SetTimestamp(1_700_000_000_000_000_000)
	stored.Body().SetStr("stored")

	return ld
}
