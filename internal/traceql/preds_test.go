package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type extractedMatchers struct {
	Op       SpansetOp
	Matchers []SpanMatcher
}

func TestPredicateExtraction(t *testing.T) {
	tests := []struct {
		input string
		want  extractedMatchers
	}{
		// Empty spanset selectors.
		{`{}`, extractedMatchers{Op: SpansetOpAnd}},
		{`{} && {}`, extractedMatchers{Op: SpansetOpUnion}},
		{`({} | coalesce()) && ({} | coalesce())`, extractedMatchers{Op: SpansetOpUnion}},

		// Simple field expressions.
		{
			`{ .a = 10 }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			// TODO(tdakkota): handle boolean attributes smarter: generate { .bool_attr = true }.
			`{ .bool_attr }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{Attribute: Attribute{Name: "bool_attr"}},
				},
			},
		},
		{
			`{ -(.a + 10) > 10 }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{Attribute: Attribute{Name: "a"}},
				},
			},
		},

		// Flip test.
		{
			`{ 10 = .a }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ .a > 10 }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpGt,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ 10 < .a }`, // -> { .a > 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpGt,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ .a < 10 }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpLt,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ 10 > .a }`, // -> { .a < 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpLt,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},

		// NOT expression test.
		{
			`{ !(.a = 10) }`, // -> { .a != 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpNotEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a > 10) }`, // -> { .a <= 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpLte,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a < 10) }`, // -> { .a >= 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpGte,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a >= 10) }`, // -> { .a < 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpLt,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a <= 10) }`, // -> { .a > 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpGt,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !(.a != 10) }`, // -> { .a = 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !!(.a = 10) }`, // -> { .a != 10 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !!(.a >= 10) }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpGte,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`{ !!(.a <= 10) }`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpLte,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		// https://en.wikipedia.org/wiki/De_Morgan%27s_laws
		{
			`{ !(.a = 10 || .b = 15) }`, // -> { .a != 10 && .b != 15 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpNotEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
					{
						Attribute: Attribute{Name: "b"},
						Op:        OpNotEq,
						Static:    Static{Type: TypeInt, Data: 15},
					},
				},
			},
		},
		{
			`{ !(.a =~ "foo" || .b !~ "bar") }`, // -> { .a !~ "foo" && .b =~ 15 }
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpNotRe,
						Static:    Static{Type: TypeString, Str: "foo"},
					},
					{
						Attribute: Attribute{Name: "b"},
						Op:        OpRe,
						Static:    Static{Type: TypeString, Str: "bar"},
					},
				},
			},
		},
		{
			`{ !(.a = 10 && .b = 15) }`, // -> { .a != 10 || .b != 15 }
			extractedMatchers{
				Op: SpansetOpUnion,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpNotEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
					{
						Attribute: Attribute{Name: "b"},
						Op:        OpNotEq,
						Static:    Static{Type: TypeInt, Data: 15},
					},
				},
			},
		},

		// Operation expressions tests.
		{
			`{ .a = 10 } | select(.b, .c) | by(.d)`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
					{
						Attribute: Attribute{Name: "b"},
					},
					{
						Attribute: Attribute{Name: "c"},
					},
					{
						Attribute: Attribute{Name: "d"},
					},
				},
			},
		},

		// Scalar filter test.
		{
			`{ .a = 10 } | avg(.b) > 10`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
					{
						Attribute: Attribute{Name: "b"},
					},
				},
			},
		},
		{
			`{ .a = 10 } | min(duration)+min(traceDuration) > 10`,
			extractedMatchers{
				Op: SpansetOpAnd,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
					{
						Attribute: Attribute{Prop: SpanDuration},
					},
					{
						Attribute: Attribute{Prop: TraceDuration},
					},
				},
			},
		},

		// Binary spanset expression test.
		{
			`{ .a = 10 } && { .b = 10 }`,
			extractedMatchers{
				Op: SpansetOpUnion,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
					{
						Attribute: Attribute{Name: "b"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
		{
			`({ .a = 10 } | coalesce()) && ({ .b = 10 } | coalesce())`,
			extractedMatchers{
				Op: SpansetOpUnion,
				Matchers: []SpanMatcher{
					{
						Attribute: Attribute{Name: "a"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
					{
						Attribute: Attribute{Name: "b"},
						Op:        OpEq,
						Static:    Static{Type: TypeInt, Data: 10},
					},
				},
			},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			expr, err := Parse(tt.input)
			require.NoError(t, err)

			p := &predExtractor{}
			p.Walk(expr)
			require.Equal(t, tt.want, extractedMatchers{
				Op:       p.Op,
				Matchers: p.Matchers,
			})
		})
	}
}
