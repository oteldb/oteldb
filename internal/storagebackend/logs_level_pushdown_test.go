package storagebackend_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
)

// TestLogLevelPushdownSuperset pins the property that makes pushing a level matcher into the fetch
// sound: the severity *number* does not decide a level on its own, so the pushed condition must let
// every record the number cannot decide through to the in-memory re-check.
//
// The corpus is built out of exactly those records — a level spelled only in the severity text, a
// text that disagrees with its number, a number OTLP does not name — so a condition that filtered
// on the number alone would answer with fewer rows than the selector matches.
func TestLogLevelPushdownSuperset(t *testing.T) {
	b, ctx := newBackend(t)

	ts := time.Now().Truncate(time.Second)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	records := []struct {
		body     string
		severity plog.SeverityNumber
		text     string
		level    string
	}{
		{"numbered", plog.SeverityNumberError, "ERROR", "error"},
		{"text only", plog.SeverityNumberUnspecified, "ERROR", "error"},
		{"text only, unknown level", plog.SeverityNumberUnspecified, "NOTICE", "notice"},
		// The number wins over a text that disagrees with it, so this is an info record.
		{"number wins", plog.SeverityNumberInfo, "ERROR", "info"},
		{"plain info", plog.SeverityNumberInfo, "INFO", "info"},
		{"no level at all", plog.SeverityNumberUnspecified, "", ""},
	}
	for _, r := range records {
		rec := sl.LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		rec.Body().SetStr(r.body)
		rec.SetSeverityNumber(r.severity)
		rec.SetSeverityText(r.text)
	}
	require.NoError(t, b.ConsumeLogs(ctx, ld))

	lq := b.Logs()
	start, end := ts.Add(-time.Hour), ts.Add(time.Hour)

	query := func(t *testing.T, matchers ...logql.LabelMatcher) []string {
		t.Helper()
		node, err := lq.Query(ctx, matchers)
		require.NoError(t, err)
		it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{Start: start, End: end, Limit: -1})
		require.NoError(t, err)

		var bodies []string
		for _, e := range drain(t, it) {
			bodies = append(bodies, e.Line)
		}
		return bodies
	}

	service := logql.LabelMatcher{Label: "service_name", Op: logql.OpEq, Value: "api"}
	for _, tc := range []struct {
		name    string
		matcher logql.LabelMatcher
		want    []string
	}{
		{
			"equal",
			logql.LabelMatcher{Label: "level", Op: logql.OpEq, Value: "error"},
			[]string{"numbered", "text only"},
		},
		{
			"regexp",
			logql.LabelMatcher{Label: "detected_level", Op: logql.OpRe, Value: "e.+", Re: regexp.MustCompile(`^(?:e.+)$`)},
			[]string{"numbered", "text only"},
		},
		{
			"not equal",
			logql.LabelMatcher{Label: "level", Op: logql.OpNotEq, Value: "error"},
			[]string{"text only, unknown level", "number wins", "plain info", "no level at all"},
		},
		{
			// No severity number spells "notice", so the pushed set is empty — every match has to
			// come from a record the number could not decide.
			"text only level",
			logql.LabelMatcher{Label: "level", Op: logql.OpEq, Value: "notice"},
			[]string{"text only, unknown level"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ElementsMatch(t, tc.want, query(t, service, tc.matcher))
		})
	}
}
