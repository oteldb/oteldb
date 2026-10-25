package storagebackend

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

func absentMatcherBackend(t *testing.T) (*Backend, time.Time) {
	t.Helper()
	ctx := t.Context()
	s, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	b := New(s)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ld := plog.NewLogs()
	for i, value := range []any{nil, "", "prod", "dev", int64(42), []byte{1, 2}, []any{"prod"}} {
		rl := ld.ResourceLogs().AppendEmpty()
		if value != nil {
			require.NoError(t, rl.Resource().Attributes().PutEmpty("env").FromRaw(value))
		}
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("library")
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Second).UnixNano()))
		r.Body().SetStr("message")
		if i == 2 {
			rl.Resource().Attributes().PutStr("service.name", "api")
			rl.Resource().Attributes().PutStr("http.method", "GET")
			rl.Resource().Attributes().PutStr("mixed", "prod")
			rl.Resource().Attributes().PutStr("overlap", "prod")
			sl.Scope().Attributes().PutStr("overlap", "dev")
			r.Attributes().PutStr("flag", "prod")
			r.Attributes().PutStr("severity.text", "prod")
		}
		if i == 1 {
			rl.Resource().Attributes().PutEmpty("service.name")
		}
		if i == 3 {
			rl.Resource().Attributes().PutStr("http_method", "POST")
			r.Attributes().PutStr("mixed", "dev")
			r.Attributes().PutStr("flag", "dev")
		}
	}
	require.NoError(t, b.ConsumeLogs(ctx, ld))
	return b, base
}

func TestLogAbsentMatcherPushdown(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, label, value string
		op                 logql.BinOp
		rows               int
		pushed             bool
	}{
		{"empty equality", "env", "", logql.OpEq, 2, true},
		{"negative", "env", "dev", logql.OpNotEq, 6, true},
		{"empty regexp", "env", ".*", logql.OpRe, 7, true},
		{"negative regexp", "env", "prod|dev", logql.OpNotRe, 5, true},
		{"number", "env", "42", logql.OpEq, 1, true},
		{"bytes", "env", "AQI=", logql.OpEq, 1, true},
		{"slice", "env", `["prod"]`, logql.OpEq, 1, true},
		{"record absence", "flag", "dev", logql.OpNotEq, 6, true},
		{"normalized name without raw collision", "severity_text", "prod", logql.OpEq, 1, true},
		{"collision", "http_method", "x", logql.OpNotEq, 7, false},
		{"mixed scope", "mixed", "dev", logql.OpNotEq, 6, false},
		{"resource and scope", "overlap", "prod", logql.OpNotEq, 6, false},
		{"unknown", "missing", "dev", logql.OpNotEq, 7, false},
		{"default service", "service_name", "unknown_service", logql.OpEq, 5, false},
		{"null service", "service_name", "", logql.OpEq, 1, false},
		{"named service", "service_name", "api", logql.OpEq, 1, true},
		{"negative service", "service_name", "dev", logql.OpNotEq, 7, true},
		{"scope name is not a Loki label", "otel_scope_name", "library", logql.OpNotEq, 7, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, base := absentMatcherBackend(t)
			ctx := t.Context()
			m := logql.LabelMatcher{Label: logql.Label(tt.label), Op: tt.op, Value: tt.value}
			if m.Op == logql.OpRe || m.Op == logql.OpNotRe {
				m.Re = regexp.MustCompile("^(?:" + m.Value + ")$")
			}
			n := &logStreamNode{q: b.Logs(), selector: []logql.LabelMatcher{m}}
			lo, hi := fetchWindow(base.Add(-time.Second), base.Add(time.Hour))
			matchers, _, pushed := n.streamFilters(ctx, lo, hi)
			assert.Equal(t, tt.pushed, pushed)
			for _, matcher := range matchers {
				assert.Nil(t, matcher.Spec, "arbitrary attribute types cannot promise storage's text representation")
			}
			batches, _, err := n.fetchBatches(ctx, lo, hi, fetchOptions{})
			require.NoError(t, err)
			rows := 0
			for _, batch := range batches {
				rows += len(batch.Timestamps)
			}
			if tt.pushed {
				assert.Equal(t, tt.rows, rows, "filter before materialization")
			}
			params := logqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(time.Hour), Limit: -1}
			it, err := n.EvalPipeline(ctx, params)
			require.NoError(t, err)
			var entry logqlengine.Entry
			count := 0
			for it.Next(&entry) {
				count++
			}
			require.NoError(t, it.Err())
			assert.Equal(t, tt.rows, count)
			params.Limit = 1
			it, err = n.EvalPipeline(ctx, params)
			require.NoError(t, err)
			assert.True(t, it.Next(&entry), "a residual predicate must not underfill a limited query")
			assert.False(t, it.Next(&entry))
			require.NoError(t, it.Err())
		})
	}
}
