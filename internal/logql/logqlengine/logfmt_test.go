package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

func TestLogfmtExtractor(t *testing.T) {
	tests := []struct {
		input         string
		labels        []logql.Label
		exprs         []logql.LabelExtractionExpr
		expectLabels  map[logql.Label]pcommon.Value
		wantFilterErr bool
	}{
		{``, nil, nil, nil, false},
		{``, []logql.Label{"foo"}, nil, nil, false},
		{`bar=10`, []logql.Label{"foo"}, nil, nil, false},
		{
			`foo=extract bar=not_extract`,
			[]logql.Label{"foo"},
			nil,
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("extract"),
			},
			false,
		},
		{
			`foo=extract bar=not_extract`,
			nil,
			[]logql.LabelExtractionExpr{
				{Label: "foo", Expr: "foo"},
			},
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("extract"),
			},
			false,
		},
		{
			`foo=extract bar=not_extract`,
			nil,
			[]logql.LabelExtractionExpr{
				{Label: "rename", Expr: `"foo"`},
			},
			map[logql.Label]pcommon.Value{
				"rename": pcommon.NewValueStr("extract"),
			},
			false,
		},
		{
			`foo=extract bar=extract_too`,
			[]logql.Label{"bar"},
			[]logql.LabelExtractionExpr{
				{Label: "rename", Expr: `"foo"`},
			},
			map[logql.Label]pcommon.Value{
				"rename": pcommon.NewValueStr("extract"),
				"bar":    pcommon.NewValueStr("extract_too"),
			},
			false,
		},
		{
			`str=str int=10 bool=true`,
			nil,
			nil,
			map[logql.Label]pcommon.Value{
				"str":  pcommon.NewValueStr("str"),
				"int":  pcommon.NewValueStr("10"),
				"bool": pcommon.NewValueStr("true"),
			},
			false,
		},

		{`label==`, nil, nil, nil, true},
		{`label==`, []logql.Label{"label"}, nil, nil, true},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			e, err := buildLogfmtExtractor(&logql.LogfmtExpressionParser{
				Labels: tt.labels,
				Exprs:  tt.exprs,
			})
			require.NoError(t, err)

			set := logqlabels.NewLabelSet()
			newLine, ok := e.Process(0, tt.input, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, tt.input, newLine)
			require.True(t, ok)

			if tt.wantFilterErr {
				_, ok = set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			require.False(t, ok, "got error: %s", errMsg)

			for k, expect := range tt.expectLabels {
				got, ok := set.Get(k)
				require.Truef(t, ok, "key %q", k)
				require.Equal(t, expect, got)
			}
		})
	}
}
