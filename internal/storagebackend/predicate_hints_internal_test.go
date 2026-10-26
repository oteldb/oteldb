package storagebackend

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
)

func TestFiniteLineRegexpHints(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, pattern string
		values        []string
		adapter       bool
		tokens        [][]byte
	}{
		{name: "alternation", pattern: `^(foo|bar)$`, values: []string{"bar", "foo"}},
		{name: "singleton", pattern: `^foo$`, values: []string{"foo"}},
		{name: "empty", pattern: `^$`, values: []string{""}},
		{name: "retain_tokens", pattern: `^foo bar baz$`, values: []string{"foo bar baz"}, tokens: [][]byte{[]byte("bar")}},
		{name: "unanchored", pattern: `foo|bar`},
		{name: "partial_anchor", pattern: `^foo|bar$`},
		{name: "folded", pattern: `(?i)^(foo|bar)$`},
		{name: "adapter", pattern: `^(foo|bar)$`, adapter: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := logqlengine.NewEngine((&Backend{}).Logs(), logqlengine.Options{OTELAdapter: tt.adapter, Optimizers: []logqlengine.Optimizer{&LogQLOptimizer{}}})
			require.NoError(t, err)
			query, err := engine.NewQuery(t.Context(), fmt.Sprintf(`{job="api"} |~ %q`, tt.pattern))
			require.NoError(t, err)
			node := query.(*logqlengine.LogQuery).Root.(*logqlengine.ProcessorNode).Input.(*logStreamNode)
			if tt.values == nil {
				require.Empty(t, node.conditions)
				return
			}
			require.Len(t, node.conditions, 1)
			require.Equal(t, fetch.AnyEqualStrings(tt.values), node.conditions[0].AnyEqual)
			require.Equal(t, tt.tokens, node.conditions[0].Tokens)
			require.Nil(t, node.conditions[0].Equal, "regexp Match remains authoritative")
		})
	}
}

func TestTraceStringPredicateHints(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, query, column string
		equal               *string
		values              []string
	}{
		{name: "literal_name", query: `{name = "foo"}`, column: sigtrace.ColName, equal: new("foo")},
		{name: "literal_message", query: `{statusMessage = "foo"}`, column: sigtrace.ColStatusMsg, equal: new("foo")},
		{name: "finite_name", query: `{name =~ "^(foo|bar)$"}`, column: sigtrace.ColName, values: []string{"bar", "foo"}},
		{name: "finite_message", query: `{statusMessage =~ "^(foo|bar)$"}`, column: sigtrace.ColStatusMsg, values: []string{"bar", "foo"}},
		{name: "unanchored", query: `{name =~ "foo|bar"}`, column: sigtrace.ColName},
		{name: "negative", query: `{name !~ "^(foo|bar)$"}`, column: sigtrace.ColName},
		{name: "empty_literal", query: `{name = ""}`, column: sigtrace.ColName, equal: new("")},
		{name: "empty_set", query: `{statusMessage =~ "^(|foo)$"}`, column: sigtrace.ColStatusMsg, values: []string{"", "foo"}},
		{name: "folded", query: `{name =~ "(?i)^(foo|bar)$"}`, column: sigtrace.ColName},
		{name: "typed_attribute", query: `{span.http.route =~ "^(/route/7|/route/8)$"}`, column: "http.route"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)
			pd, ok := buildTracePushdown(traceql.ExtractMatchers(expr))
			require.True(t, ok)
			groups := pushdownGroups(pd)
			require.Len(t, groups, 1)
			require.Len(t, groups[0].conditions, 1)
			cond := groups[0].conditions[0]
			require.Equal(t, tt.column, cond.Column)
			if tt.equal == nil {
				require.Nil(t, cond.Equal)
			} else {
				require.Equal(t, &fetch.EqualMatcher{Name: tt.column, Value: *tt.equal}, cond.Equal)
			}
			require.Equal(t, fetch.AnyEqualStrings(tt.values), cond.AnyEqual)
		})
	}
}

func TestTraceIDSetHint(t *testing.T) {
	t.Parallel()
	ids := map[otelstorage.TraceID]struct{}{
		{0: 0xff, 1: 0, 15: 1}: {},
		{0: 0, 1: 0xff, 15: 2}: {},
	}
	cond := traceIDCondition(ids)
	require.Len(t, cond.AnyEqual, 2)
	require.True(t, fetch.SortedAnyEqual(cond.AnyEqual))
	for id := range ids {
		raw := append([]byte(nil), id[:]...)
		require.True(t, fetch.InAnyEqual(cond.AnyEqual, raw))
		require.True(t, cond.Match(signal.StringValue(raw)))
		require.False(t, fetch.InAnyEqual(cond.AnyEqual, []byte(hex.EncodeToString(raw))), "trace IDs are raw binary, not hex text")
		for i := range raw {
			raw[i] = 0x77
		}
	}
	for _, raw := range cond.AnyEqual {
		require.Len(t, raw, 16)
		require.True(t, cond.Match(signal.StringValue(raw)))
	}
	require.False(t, cond.Match(signal.StringValue([]byte{0xff, 0})))
	require.False(t, cond.Match(signal.EmptyValue()))
	require.Nil(t, cond.Equal)
	empty := traceIDCondition(nil)
	require.Empty(t, empty.AnyEqual)
	require.False(t, empty.Match(signal.StringValue(make([]byte, 16))))
}
