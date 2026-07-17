package storagebackend_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

// TestLogQLSelectorPushdown covers lowering stream selector matchers to storage fetch filters.
//
// Every matcher shape is pushed to the postings index, not just equality, so a regexp selector prunes
// streams rather than being resolved after the fetch. The exception is a matcher that also matches
// the empty string: LogQL reads an absent label as "", so it matches streams that lack the label
// entirely, but the index only knows streams that have it. The "env" label is present on one stream
// and absent from the other precisely to pin that case — pushing `env!="dev"` or `env=~".*"` would
// drop the stream without the label.
func TestLogQLSelectorPushdown(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)
	ld := plog.NewLogs()
	for i, s := range []struct {
		service, env, body string
	}{
		{"api", "prod", "api line"},   // has env
		{"web", "", "web line"},       // no env label at all
		{"api", "stage", "api stage"}, // has env
	} {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", s.service)
		if s.env != "" {
			rl.Resource().Attributes().PutStr("env", s.env)
		}
		rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(ts.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		rec.Body().SetStr(s.body)
	}
	require.NoError(t, b.ConsumeLogs(ctx, ld))

	params := logqlengine.EvalParams{
		Start: ts.Add(-time.Hour), End: ts.Add(time.Hour), Step: time.Minute, Limit: 1000,
	}
	run := func(t *testing.T, query string) []string {
		t.Helper()
		var opts logqlengine.Options
		opts.ParseOptions = logql.ParseOptions{AllowDots: true}
		engine, err := logqlengine.NewEngine(b.Logs(), opts)
		require.NoError(t, err)
		q, err := engine.NewQuery(ctx, query)
		require.NoError(t, err)
		data, err := q.Eval(ctx, params)
		require.NoError(t, err)
		return normalize(t, data)
	}

	for _, tt := range []struct {
		name, query string
		want        []string
	}{
		{
			name:  "equality",
			query: `{service_name="api"}`,
			want:  []string{"line:api line", "line:api stage"},
		},
		{
			name:  "regexp alternation",
			query: `{service_name=~"api|web"}`,
			want:  []string{"line:api line", "line:web line", "line:api stage"},
		},
		{
			name:  "regexp prunes",
			query: `{service_name=~"w.+"}`,
			want:  []string{"line:web line"},
		},
		{
			// Pushable: `^(?:prod|stage)$` does not match "", so a stream lacking env cannot match.
			name:  "regexp on label absent from one stream",
			query: `{env=~"prod|stage"}`,
			want:  []string{"line:api line", "line:api stage"},
		},
		{
			// Not pushable: "" != "dev", so the stream WITHOUT env must still match.
			name:  "negated matcher keeps stream lacking the label",
			query: `{service_name=~".+", env!="dev"}`,
			want:  []string{"line:api line", "line:web line", "line:api stage"},
		},
		{
			// Not pushable: `.*` matches "", so the stream WITHOUT env must still match.
			name:  "match-all regexp keeps stream lacking the label",
			query: `{service_name=~".+", env=~".*"}`,
			want:  []string{"line:api line", "line:web line", "line:api stage"},
		},
		{
			// A pushable matcher AND an unpushable one on the same label: the pushed predicate is a
			// superset, so matchSelector must still re-check.
			name:  "mixed pushable and unpushable on one label",
			query: `{env=~"prod|stage", env!="stage"}`,
			want:  []string{"line:api line"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.ElementsMatch(t, tt.want, run(t, tt.query))
		})
	}
}
