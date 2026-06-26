package profileql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var cpuType = ProfileType{
	Name:       "process_cpu",
	SampleType: "cpu",
	SampleUnit: "nanoseconds",
	PeriodType: "cpu",
	PeriodUnit: "nanoseconds",
}

type parseTestCase struct {
	input   string
	want    Expr
	wantErr bool
}

var parseTests = []parseTestCase{
	// Bare profile-type selector.
	{
		"process_cpu:cpu:nanoseconds:cpu:nanoseconds",
		Expr{Type: cpuType},
		false,
	},
	// Empty matcher block.
	{
		"process_cpu:cpu:nanoseconds:cpu:nanoseconds{}",
		Expr{Type: cpuType},
		false,
	},
	// Leading/trailing whitespace is ignored by the scanner.
	{
		"  process_cpu:cpu:nanoseconds:cpu:nanoseconds  ",
		Expr{Type: cpuType},
		false,
	},
	// Whitespace inside the matcher block.
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{ service_name = "frontend" }`,
		Expr{
			Type:     cpuType,
			Matchers: []LabelMatcher{{Label: "service_name", Op: OpEq, Value: "frontend"}},
		},
		false,
	},
	// Single matcher.
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="frontend"}`,
		Expr{
			Type: cpuType,
			Matchers: []LabelMatcher{
				{Label: "service_name", Op: OpEq, Value: "frontend"},
			},
		},
		false,
	},
	// Multiple matchers, all operators, trailing comma.
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="frontend",env!="dev",region=~"eu-.*",team!~"infra",}`,
		Expr{
			Type: cpuType,
			Matchers: []LabelMatcher{
				{Label: "service_name", Op: OpEq, Value: "frontend"},
				{Label: "env", Op: OpNotEq, Value: "dev"},
				{Label: "region", Op: OpRe, Value: "eu-.*"},
				{Label: "team", Op: OpNotRe, Value: "infra"},
			},
		},
		false,
	},
	// Repeated label name is allowed (left to the storage layer to reconcile).
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{env="a",env="b"}`,
		Expr{
			Type: cpuType,
			Matchers: []LabelMatcher{
				{Label: "env", Op: OpEq, Value: "a"},
				{Label: "env", Op: OpEq, Value: "b"},
			},
		},
		false,
	},
	// Empty matcher value.
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{env=""}`,
		Expr{
			Type:     cpuType,
			Matchers: []LabelMatcher{{Label: "env", Op: OpEq, Value: ""}},
		},
		false,
	},
	// Quoted label name.
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{"service.name"="frontend"}`,
		Expr{
			Type:     cpuType,
			Matchers: []LabelMatcher{{Label: "service.name", Op: OpEq, Value: "frontend"}},
		},
		false,
	},
	// Escaped quote inside a value.
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{msg="say \"hi\""}`,
		Expr{
			Type:     cpuType,
			Matchers: []LabelMatcher{{Label: "msg", Op: OpEq, Value: `say "hi"`}},
		},
		false,
	},
	// Raw string value.
	{
		"process_cpu:cpu:nanoseconds:cpu:nanoseconds{path=`C:\\tmp`}",
		Expr{
			Type:     cpuType,
			Matchers: []LabelMatcher{{Label: "path", Op: OpEq, Value: `C:\tmp`}},
		},
		false,
	},
	// OTel dotted label name.
	{
		`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service.name="frontend"}`,
		Expr{
			Type: cpuType,
			Matchers: []LabelMatcher{
				{Label: "service.name", Op: OpEq, Value: "frontend"},
			},
		},
		false,
	},
	// Comment is skipped.
	{
		"process_cpu:cpu:nanoseconds:cpu:nanoseconds # the cpu profile\n{service_name=\"frontend\"}",
		Expr{
			Type:     cpuType,
			Matchers: []LabelMatcher{{Label: "service_name", Op: OpEq, Value: "frontend"}},
		},
		false,
	},
	// Profile type given via __name__ matcher.
	{
		`{__name__="process_cpu:cpu:nanoseconds:cpu:nanoseconds",service_name="frontend"}`,
		Expr{
			Type: cpuType,
			Matchers: []LabelMatcher{
				{Label: "service_name", Op: OpEq, Value: "frontend"},
			},
		},
		false,
	},
	// __name__ matcher with no other matchers.
	{
		`{__name__="process_cpu:cpu:nanoseconds:cpu:nanoseconds"}`,
		Expr{Type: cpuType},
		false,
	},
	// Delta profile type.
	{
		`memory:alloc_space:bytes:space:bytes:delta{service_name="frontend"}`,
		Expr{
			Type: ProfileType{
				Name:       "memory",
				SampleType: "alloc_space",
				SampleUnit: "bytes",
				PeriodType: "space",
				PeriodUnit: "bytes",
				Delta:      true,
			},
			Matchers: []LabelMatcher{
				{Label: "service_name", Op: OpEq, Value: "frontend"},
			},
		},
		false,
	},

	// Errors.
	{"", Expr{}, true},                                                                      // empty.
	{`{service_name="frontend"}`, Expr{}, true},                                             // no profile type.
	{"{}", Expr{}, true},                                                                    // empty selector, no profile type.
	{"not_a_profile_type", Expr{}, true},                                                    // invalid profile type.
	{"process_cpu:cpu:nanoseconds:cpu", Expr{}, true},                                       // too few type parts.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=}`, Expr{}, true},            // missing value.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{="frontend"}`, Expr{}, true},              // missing label name.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name~"x"}`, Expr{}, true},         // bad op.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="x"`, Expr{}, true},          // unclosed brace.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="x" env="y"}`, Expr{}, true}, // missing comma.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{,}`, Expr{}, true},                        // leading comma.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{""="x"}`, Expr{}, true},                   // empty label name.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{"a b"="x"}`, Expr{}, true},                // invalid quoted label name.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{region=~"("}`, Expr{}, true},              // bad regexp.
	{`{__name__=~"process_cpu:cpu:nanoseconds:cpu:nanoseconds"}`, Expr{}, true},             // __name__ must be eq.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds tail`, Expr{}, true},                      // tail token.
	{`process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="x"} tail`, Expr{}, true},    // tail token after block.
}

func TestParse(t *testing.T) {
	for _, tt := range parseTests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Compiled regexps cannot be compared directly; check presence and
			// clear them before comparing the rest.
			for i := range got.Matchers {
				m := &got.Matchers[i]
				if m.Op.IsRegex() {
					require.NotNil(t, m.Re, "regex matcher %q should have a compiled regexp", m.Label)
					m.Re = nil
				}
			}
			require.Equal(t, tt.want, got)

			// The String form must re-parse to an equivalent expression.
			reparsed, err := Parse(got.String())
			require.NoError(t, err, "re-parsing %q", got.String())
			for i := range reparsed.Matchers {
				reparsed.Matchers[i].Re = nil
			}
			require.Equal(t, got, reparsed)
		})
	}
}

func TestExprString(t *testing.T) {
	const input = `process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="frontend",env=~"prod|staging"}`
	expr, err := Parse(input)
	require.NoError(t, err)
	require.Equal(t, input, expr.String())

	// String output is itself parseable and stable.
	reparsed, err := Parse(expr.String())
	require.NoError(t, err)
	require.Equal(t, input, reparsed.String())
}

func TestRegexMatcherAnchored(t *testing.T) {
	expr, err := Parse(`process_cpu:cpu:nanoseconds:cpu:nanoseconds{region=~"eu"}`)
	require.NoError(t, err)
	require.Len(t, expr.Matchers, 1)

	re := expr.Matchers[0].Re
	require.NotNil(t, re)
	// The pattern is fully anchored, like Prometheus matchers.
	require.True(t, re.MatchString("eu"))
	require.False(t, re.MatchString("eu-west"))
	require.False(t, re.MatchString("xeu"))
}

func FuzzParse(f *testing.F) {
	for _, tt := range parseTests {
		f.Add(tt.input)
	}
	for _, tt := range profileTypeTests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		expr, err := Parse(input)
		if err != nil {
			return
		}

		// Any successfully parsed query must round-trip through String:
		// re-parsing must succeed and produce a stable String form.
		out := expr.String()
		reparsed, err := Parse(out)
		if err != nil {
			t.Fatalf("re-parsing %q (from input %q) failed: %v", out, input, err)
		}
		if got := reparsed.String(); got != out {
			t.Fatalf("unstable String: %q != %q (input %q)", got, out, input)
		}
	})
}
