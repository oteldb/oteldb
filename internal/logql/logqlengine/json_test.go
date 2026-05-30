package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

func TestJSONExtractor(t *testing.T) {
	tests := []struct {
		input         string
		labels        []logql.Label
		exprs         []logql.LabelExtractionExpr
		expectLabels  map[logql.Label]pcommon.Value
		wantFilterErr bool
	}{
		{`{}`, nil, nil, nil, false},
		{`{}`, []logql.Label{"foo"}, nil, nil, false},
		{`{"foo": null}`, nil, nil, nil, false},
		{`{"foo": null}`, []logql.Label{"foo"}, nil, nil, false},
		{`{"bar": 10}`, []logql.Label{"foo"}, nil, nil, false},
		{
			`{"foo": "extract", "bar": "not extract"}`,
			[]logql.Label{"foo"},
			nil,
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("extract"),
			},
			false,
		},
		{
			`{
				"str":"str",
				"int":10,
				"double": 3.14,
				"skip": null,
				"bool": true,
				"array": [1],
				"object": {"sub_key": 1}
			}`,
			nil,
			nil,
			map[logql.Label]pcommon.Value{
				"str":    pcommon.NewValueStr("str"),
				"int":    pcommon.NewValueInt(10),
				"double": pcommon.NewValueDouble(3.14),
				"bool":   pcommon.NewValueBool(true),
				"array": func() pcommon.Value {
					r := pcommon.NewValueSlice()
					s := r.Slice()
					v := s.AppendEmpty()
					v.SetInt(1)
					return r
				}(),
				"object": func() pcommon.Value {
					r := pcommon.NewValueMap()
					m := r.Map()
					m.PutInt("sub_key", 1)
					return r
				}(),
			},
			false,
		},
		{
			`{"foo": {"sub_foo": "extract"}, "bar": "extract_too"}`,
			[]logql.Label{"bar"},
			[]logql.LabelExtractionExpr{
				{Label: "foo", Expr: "foo.sub_foo"},
			},
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("extract"),
				"bar": pcommon.NewValueStr("extract_too"),
			},
			false,
		},

		{`{"label": "foo}`, nil, nil, nil, true},
		{`{"label": "foo}`, []logql.Label{"label"}, nil, nil, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			e, err := buildJSONExtractor(&logql.JSONExpressionParser{
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

			require.Equal(t, len(tt.expectLabels), set.Len())
			for k, expect := range tt.expectLabels {
				got, ok := set.Get(k)
				require.Truef(t, ok, "key %q", k)
				require.Equal(t, expect, got)
			}
		})
	}
}

func BenchmarkJSONExtractor(b *testing.B) {
	const benchdata = `{
		"protocol": "HTTP/2.0",
		"servers": ["129.0.1.1","10.2.1.3"],
		"request": {
			"time": "6.032",
			"method": "GET",
			"host": "foo.grafana.net",
			"size": "55",
			"headers": {
			  "Accept": "*/*",
			  "User-Agent": "curl/7.68.0"
			}
		},
		"response": {
			"status": 401,
			"size": "228",
			"latency_seconds": "6.031"
		}
	}`

	benchs := []struct {
		name string
		expr *logql.JSONExpressionParser
	}{
		{
			`All`,
			&logql.JSONExpressionParser{},
		},
		{
			`OneLabel`,
			&logql.JSONExpressionParser{
				Labels: []logql.Label{`protocol`},
			},
		},
		{
			`JMESPaths`,
			&logql.JSONExpressionParser{
				Exprs: []logql.LabelExtractionExpr{
					{
						Label: "user_agent",
						Expr:  `request.headers["User-Agent"]`,
					},
					{
						Label: "status",
						Expr:  `response.status`,
					},
				},
			},
		},
	}

	for _, bb := range benchs {
		bb := bb
		b.Run(bb.name, func(b *testing.B) {
			p, err := buildJSONExtractor(bb.expr)
			require.NoError(b, err)

			set := logqlabels.NewLabelSet()
			var (
				line string
				ok   bool
			)
			b.ReportAllocs()
			b.SetBytes(int64(len(benchdata)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				set.Reset()
				line, ok = p.Process(10, benchdata, set)
			}

			if !ok {
				b.Fatal(line)
			}
		})
	}
}
