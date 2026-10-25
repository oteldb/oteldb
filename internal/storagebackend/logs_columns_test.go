package storagebackend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

func TestIsFixedLogColumn(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		want bool
	}{
		{"ts", true},
		{"observed", true},
		{"severity", true},
		{"flags", true},
		{"dropped", true},
		{"severity_text", true},
		{"body", true},
		{"trace_id", true},
		{"span_id", true},
		{"attrs", true},
		{"severity.text", false},
		{"http.method", false},
		{"stream", false},
		{"", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isFixedLogColumn(tt.name))
		})
	}
}

func TestLogRecordColumnCollisions(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"ts", "observed", "severity", "flags", "dropped", "severity_text", "body", "trace_id", "span_id", "attrs"} {
		for _, part := range []bool{false, true} {
			name := key + "/head"
			if part {
				name = key + "/part"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				s, err := storage.InMemory()
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close(context.Background()) })
				b := New(s)
				base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
				ld := plog.NewLogs()
				r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
				r.SetTimestamp(pcommon.Timestamp(base.UnixNano()))
				r.Body().SetStr("dev")
				r.SetSeverityText("dev")
				r.Attributes().PutStr(key, "prod")
				require.NoError(t, b.ConsumeLogs(ctx, ld))
				if part {
					require.NoError(t, s.Admin().Flush(ctx, "default", signal.Log))
				}
				for _, m := range []logql.LabelMatcher{
					{Label: logql.Label(key), Op: logql.OpNotEq, Value: "dev"},
					{Label: logql.Label(key), Op: logql.OpEq, Value: "prod"},
				} {
					n := &logStreamNode{q: b.Logs(), selector: []logql.LabelMatcher{m}}
					lo, hi := fetchWindow(base.Add(-time.Second), base.Add(time.Second))
					_, conditions, pushed := n.streamFilters(ctx, lo, hi)
					assert.False(t, pushed, "a record attribute must not be read from a native column")
					assert.Empty(t, conditions)
					it, err := n.EvalPipeline(ctx, logqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(time.Second), Limit: 1})
					require.NoError(t, err)
					var entry logqlengine.Entry
					require.True(t, it.Next(&entry))
					assert.Equal(t, "dev", entry.Line)
					assert.False(t, it.Next(&entry))
					require.NoError(t, it.Err())
				}
			})
		}
	}
}
