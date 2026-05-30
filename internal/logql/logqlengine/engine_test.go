// Package logqlengine implements LogQL evaluation engine.
package logqlengine

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

type inputLine struct {
	line  string
	attrs pcommon.Map
}

type resultLine struct {
	line   string
	labels map[string]string
}

type mockQuerier struct {
	lines []inputLine
	step  time.Duration
}

var _ Querier = (*mockQuerier)(nil)

func (m *mockQuerier) Capabilities() (caps QuerierCapabilities) {
	return caps
}

func (m *mockQuerier) Query(ctx context.Context, selector []logql.LabelMatcher) (PipelineNode, error) {
	return &mockPipelineNode{
		querier: m,
	}, nil
}

type mockPipelineNode struct {
	querier *mockQuerier
}

var _ PipelineNode = (*mockPipelineNode)(nil)

func (n *mockPipelineNode) Traverse(cb NodeVisitor) error {
	return cb(n)
}

func (n *mockPipelineNode) EvalPipeline(ctx context.Context, params EvalParams) (EntryIterator, error) {
	var (
		step      = n.querier.step
		ts        = params.Start
		direction = params.Direction
	)
	if step == 0 {
		step = time.Millisecond
	}

	if direction != DirectionForward {
		return nil, errors.Errorf("test: direction %q is unsupported", direction)
	}

	var (
		entries    []Entry
		scopeAttrs = pcommon.NewMap()
		resAttrs   = pcommon.NewMap()
	)
	scopeAttrs.PutStr("scope", "test")
	resAttrs.PutStr("resource", "test")

	for _, l := range n.querier.lines {
		var (
			line  = l.line
			attrs = pcommon.NewMap()
		)
		ts = ts.Add(step)
		if l.attrs != (pcommon.Map{}) {
			l.attrs.CopyTo(attrs)
		}

		if dec := jx.DecodeStr(line); dec.Next() == jx.Object {
			if err := dec.Obj(func(d *jx.Decoder, key string) error {
				switch key {
				case logstorage.LabelBody:
					v, err := d.Str()
					if err != nil {
						return err
					}
					line = v
					return nil
				case logstorage.LabelTraceID:
					v, err := d.Str()
					if err != nil {
						return err
					}
					traceID, err := otelstorage.ParseTraceID(v)
					if err != nil {
						return err
					}
					attrs.PutStr(logstorage.LabelTraceID, traceID.Hex())
					return nil
				default:
					switch d.Next() {
					case jx.String:
						v, err := d.Str()
						if err != nil {
							return err
						}
						attrs.PutStr(key, v)
						return nil
					case jx.Bool:
						v, err := d.Bool()
						if err != nil {
							return err
						}
						attrs.PutBool(key, v)
						return nil
					case jx.Number:
						v, err := d.Num()
						if err != nil {
							return err
						}
						if v.IsInt() {
							n, err := v.Int64()
							if err != nil {
								return err
							}
							attrs.PutInt(key, n)
						} else {
							n, err := v.Float64()
							if err != nil {
								return err
							}
							attrs.PutDouble(key, n)
						}
						return nil
					default:
						v, err := d.Raw()
						if err != nil {
							return err
						}
						attrs.PutStr(key, string(v))
						return nil
					}
				}
			}); err != nil {
				return nil, err
			}
		}

		set := logqlabels.NewLabelSet()
		set.SetAttrs(
			otelstorage.Attrs(attrs),
			otelstorage.Attrs(scopeAttrs),
			otelstorage.Attrs(resAttrs),
		)
		entries = append(entries, Entry{
			Timestamp: otelstorage.NewTimestampFromTime(ts),
			Line:      line,
			Set:       set,
		})
	}

	return iterators.Slice(entries), nil
}

func justLines(lines ...string) []inputLine {
	r := make([]inputLine, len(lines))
	for i, line := range lines {
		r[i] = inputLine{
			line: line,
		}
	}
	return r
}

var (
	inputLines = justLines(
		`{"id": 1, "foo": "4m", "bar": "1s", "baz": "1kb"}`,
		`{"id": 2, "foo": "5m", "bar": "2s", "baz": "1mb"}`,
		`{"id": 3, "foo": "6m", "bar": "3s", "baz": "1gb"}`,
	)
	resultLines = []resultLine{
		{
			`{"id": 1, "foo": "4m", "bar": "1s", "baz": "1kb"}`,
			map[string]string{
				"id":  "1",
				"foo": "4m",
				"bar": "1s",
				"baz": "1kb",
			},
		},
		{
			`{"id": 2, "foo": "5m", "bar": "2s", "baz": "1mb"}`,
			map[string]string{
				"id":  "2",
				"foo": "5m",
				"bar": "2s",
				"baz": "1mb",
			},
		},
		{
			`{"id": 3, "foo": "6m", "bar": "3s", "baz": "1gb"}`,
			map[string]string{
				"id":  "3",
				"foo": "6m",
				"bar": "3s",
				"baz": "1gb",
			},
		},
	}
)

func TestEngineEvalStream(t *testing.T) {
	startTime := otelstorage.Timestamp(1688833731000000000).AsTime()

	tests := []struct {
		query    string
		input    []inputLine
		wantData []resultLine
		wantErr  bool
	}{
		// Just label selector.
		{
			`{resource="test"}`,
			inputLines,
			resultLines,
			false,
		},
		{
			`{resource="not_existing"}`,
			inputLines,
			nil,
			false,
		},
		{
			`{not_existing="test"}`,
			inputLines,
			nil,
			false,
		},

		// JSON stage.
		{
			`{resource="test"} | json`,
			inputLines,
			resultLines,
			false,
		},
		{
			`{resource="test"} | json id,foo,bar,baz`,
			inputLines,
			resultLines,
			false,
		},
		{
			`{resource="test"} | json id="id",foo,bar,baz`,
			inputLines,
			resultLines,
			false,
		},
		{
			`{resource="test"} | json id="[\"id\"]",foo,bar,baz`,
			inputLines,
			resultLines,
			false,
		},

		// Line format.
		{
			`{resource="test"} |= "5m" | json | line_format "{{ .foo }}"`,
			inputLines,
			[]resultLine{
				{
					line:   "5m",
					labels: resultLines[1].labels,
				},
			},
			false,
		},

		// Line filter.
		{
			`{resource="test"} |= "5m" | json`,
			inputLines,
			[]resultLine{resultLines[1]},
			false,
		},
		{
			`{resource="test"} |~ ".*5m.*" | json`,
			inputLines,
			[]resultLine{resultLines[1]},
			false,
		},
		{
			`{resource="test"} != "5m" | json`,
			inputLines,
			[]resultLine{
				resultLines[0],
				resultLines[2],
			},
			false,
		},
		{
			`{resource="test"} !~ "5m" | json`,
			inputLines,
			[]resultLine{
				resultLines[0],
				resultLines[2],
			},
			false,
		},
		// Label matcher.
		{
			`{resource="test"} | json | foo = "5"`,
			inputLines,
			[]resultLine{},
			false,
		},
		{
			`{resource="test"} | json | foo = "5m"`,
			inputLines,
			[]resultLine{resultLines[1]},
			false,
		},
		{
			`{resource="test"} | json | foo =~ ".*5m.*"`,
			inputLines,
			[]resultLine{resultLines[1]},
			false,
		},
		{
			`{resource="test"} | json | foo != "5m"`,
			inputLines,
			[]resultLine{
				resultLines[0],
				resultLines[2],
			},
			false,
		},
		{
			`{resource="test"} | json | foo !~ "5m"`,
			inputLines,
			[]resultLine{
				resultLines[0],
				resultLines[2],
			},
			false,
		},

		// Drop expression.
		{
			`{resource="test"} | json | drop id`,
			justLines(
				`{"id": 1, "foo": "bar"}`,
				`{"id": 2, "foo": "baz"}`,
			),
			[]resultLine{
				{
					`{"id": 1, "foo": "bar"}`,
					map[string]string{"foo": "bar"},
				},
				{
					`{"id": 2, "foo": "baz"}`,
					map[string]string{"foo": "baz"},
				},
			},
			false,
		},
		{
			`{resource="test"} | json | drop clearly_not_exists`,
			justLines(
				`{"id": 1, "foo": "bar"}`,
			),
			[]resultLine{
				{
					`{"id": 1, "foo": "bar"}`,
					map[string]string{
						"id":  "1",
						"foo": "bar",
					},
				},
			},
			false,
		},

		// Distinct filter.
		{
			`{resource="test"} | json | distinct id`,
			justLines(
				`{"id": 1, "foo": "bar"}`,
				`{"id": 1, "foo": "baz"}`,
				`{"id": 2, "foo": "bar"}`,
			),
			[]resultLine{
				{
					`{"id": 1, "foo": "bar"}`,
					map[string]string{
						"id":  "1",
						"foo": "bar",
					},
				},
				{
					`{"id": 2, "foo": "bar"}`,
					map[string]string{
						"id":  "2",
						"foo": "bar",
					},
				},
			},
			false,
		},
		{
			`{resource="test"} | json | distinct id, foo`,
			justLines(
				`{"id": 1, "foo": "bar"}`,
				`{"id": 1, "foo": "baz"}`,
				`{"id": 2, "foo": "bar"}`,
			),
			[]resultLine{
				{
					`{"id": 1, "foo": "bar"}`,
					map[string]string{
						"id":  "1",
						"foo": "bar",
					},
				},
			},
			false,
		},
		{
			`{resource="test"} | json | distinct clearly_not_exists`,
			inputLines,
			resultLines,
			false,
		},

		// Complex queries.
		{
			`{resource="test"} | json | foo <= 5m, bar == 2s or baz == 1MB`,
			inputLines,
			[]resultLine{resultLines[1]},
			false,
		},

		// Invalid query.
		{
			`{resource="test",}`,
			inputLines,
			nil,
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			ctx := context.Background()

			opts := Options{
				ParseOptions: logql.ParseOptions{AllowDots: true},
			}
			e, err := NewEngine(&mockQuerier{lines: tt.input}, opts)
			require.NoError(t, err)

			gotData, err := eval(ctx, e, tt.query, EvalParams{
				Start: startTime,
				End:   startTime,
				Limit: 1000,
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			result, ok := gotData.GetStreamsResult()
			require.True(t, ok)

			type entry struct {
				ts     uint64
				line   string
				labels map[string]string
			}
			entries := make([]entry, 0, len(tt.wantData))
			for _, s := range result.Result {
				set := s.Stream.Value

				// Ensure engine passes resource and scope attrs.
				for k, v := range map[string]string{
					"scope":    "test",
					"resource": "test",
				} {
					assert.Contains(t, set, k)
					assert.Equal(t, v, set[k])
					// Do add labels to the test.
					delete(set, k)
				}

				for _, e := range s.Values {
					entries = append(entries, entry{
						ts:     e.T,
						line:   e.V,
						labels: set,
					})
				}
			}
			slices.SortFunc(entries, func(a, b entry) int { return cmp.Compare(a.ts, b.ts) })

			assert.Len(t, entries, len(tt.wantData))
			for i, e := range entries {
				result := resultLine{
					line:   e.line,
					labels: e.labels,
				}
				wanna := tt.wantData[i]
				if jx.Valid([]byte(wanna.line)) {
					assert.JSONEq(t, wanna.line, result.line)
				} else {
					assert.Equal(t, wanna.line, result.line)
				}
				assert.Equal(t, wanna.labels, result.labels)
			}
		})
	}
}

func eval(ctx context.Context, e *Engine, query string, params EvalParams) (r lokiapi.QueryResponseData, _ error) {
	q, err := e.NewQuery(ctx, query)
	if err != nil {
		return r, err
	}
	return q.Eval(ctx, params)
}

type timeRange struct {
	start uint64
	end   uint64
	step  time.Duration
}

func TestEngineEvalLiteral(t *testing.T) {
	type testCase struct {
		query   string
		tsRange timeRange

		wantData lokiapi.QueryResponseData
		wantErr  bool
	}
	test3steps := func(input, result string) testCase {
		return testCase{
			input,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000003_000000000,
				step:  time.Second,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.MatrixResultQueryResponseData,
				MatrixResult: lokiapi.MatrixResult{
					Result: lokiapi.Matrix{
						{
							Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
							Values: []lokiapi.FPoint{
								{T: 1700000001, V: result},
								{T: 1700000002, V: result},
								{T: 1700000003, V: result},
								// Loki includes one step after end, so we do.
								{T: 1700000004, V: result},
							},
						},
					},
				},
			},
			false,
		}
	}

	tests := []testCase{
		// Literal eval.
		{
			`3.14`,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000001_000000000,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.ScalarResultQueryResponseData,
				ScalarResult: lokiapi.ScalarResult{
					Result: lokiapi.FPoint{
						T: 1700000001,
						V: "3.14",
					},
				},
			},
			false,
		},
		{
			`3.14`,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000003_000000000,
				step:  time.Second,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.MatrixResultQueryResponseData,
				MatrixResult: lokiapi.MatrixResult{
					Result: lokiapi.Matrix{
						{
							Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
							Values: []lokiapi.FPoint{
								{T: 1700000001, V: "3.14"},
								{T: 1700000002, V: "3.14"},
								{T: 1700000003, V: "3.14"},
								{T: 1700000004, V: "3.14"},
							},
						},
					},
				},
			},
			false,
		},

		// Vector eval.
		{
			`vector(3.14)`,
			timeRange{
				start: 1700000001_000000000,
				end:   1700000001_000000000,
			},
			lokiapi.QueryResponseData{
				Type: lokiapi.VectorResultQueryResponseData,
				VectorResult: lokiapi.VectorResult{
					Result: lokiapi.Vector{
						{
							Metric: lokiapi.NewOptLabelSet(lokiapi.LabelSet{}),
							Value: lokiapi.FPoint{
								T: 1700000001,
								V: "3.14",
							},
						},
					},
				},
			},
			false,
		},
		test3steps(`vector(3.14)`, "3.14"),

		// Literal binary operation test.
		test3steps(`label_replace(vector(3), "dst", "$0", "src", ".+") * vector(3)`, "9"),

		// Precedence tests.
		test3steps(`vector(2)+vector(3)*vector(4)`, "14"),
		test3steps(`vector(2)*vector(3)+vector(4)`, "10"),
		test3steps(`vector(2) + vector(3)*vector(4) + vector(5)`, "19"),
		test3steps(`vector(2)+vector(3)^vector(2)`, "11"),
		test3steps(`vector(2)*vector(3)^vector(2)`, "18"),
		test3steps(`vector(2)^vector(3)*vector(2)`, "16"),
		test3steps(`vector(2)^vector(3)^vector(2)`, `512`),
		test3steps(`(vector(2)^vector(3))^vector(2)`, `64`),
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			ctx := context.Background()

			opts := Options{
				ParseOptions: logql.ParseOptions{AllowDots: true},
			}
			e, err := NewEngine(&mockQuerier{}, opts)
			require.NoError(t, err)

			gotData, err := eval(ctx, e, tt.query, EvalParams{
				Start: otelstorage.Timestamp(tt.tsRange.start).AsTime(),
				End:   otelstorage.Timestamp(tt.tsRange.end).AsTime(),
				Step:  tt.tsRange.step,
				Limit: 1000,
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantData, gotData)
		})
	}
}
