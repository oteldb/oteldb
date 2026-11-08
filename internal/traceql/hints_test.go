package traceql

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var hintTests = []struct {
	name  string
	input string
	body  string
	hints []Hint
}{
	{
		name:  "search",
		input: `{span:status = error} with(most_recent=true)`,
		body:  `{span:status = error}`,
		hints: []Hint{{Name: "most_recent", Value: Static{Type: TypeBool, Data: 1}}},
	},
	{
		name:  "trace duration pushdown",
		input: `{trace:duration > 1s && resource.service.name = "checkout"} with(most_recent=true)`,
		body:  `{trace:duration > 1s && resource.service.name = "checkout"}`,
		hints: []Hint{{Name: "most_recent", Value: Static{Type: TypeBool, Data: 1}}},
	},
	{
		name:  "exception drilldown",
		input: `{status=error} | select(resource.service.name, event.exception.message, event.exception.stacktrace, event.exception.type) with(most_recent=true)`,
		body:  `{status=error} | select(resource.service.name, event.exception.message, event.exception.stacktrace, event.exception.type)`,
		hints: []Hint{{Name: "most_recent", Value: Static{Type: TypeBool, Data: 1}}},
	},
	{
		name:  "metrics",
		input: `{} | rate() by(resource.service.name) with(sample=true)`,
		body:  `{} | rate() by(resource.service.name)`,
		hints: []Hint{{Name: "sample", Value: Static{Type: TypeBool, Data: 1}}},
	},
	{
		name:  "metrics stages",
		input: `{} | rate() by(span.http.route) | topk(10) > 0 with(span_sample=.1, exemplars=100)`,
		body:  `{} | rate() by(span.http.route) | topk(10) > 0`,
		hints: []Hint{
			{Name: "span_sample", Value: Static{Type: TypeNumber, Data: math.Float64bits(.1)}},
			{Name: "exemplars", Value: Static{Type: TypeInt, Data: 100}},
		},
	},
	{
		name:  "metrics arithmetic",
		input: `({status=error} | rate()) / ({} | rate()) with(sample=false)`,
		body:  `({status=error} | rate()) / ({} | rate())`,
		hints: []Hint{{Name: "sample", Value: Static{Type: TypeBool}}},
	},
	{
		name:  "compare",
		input: `{} | compare({duration>100ms}, 10, 1700000000000000000, 1700000060000000000) with(trace_sample=1)`,
		body:  `{} | compare({duration>100ms}, 10, 1700000000000000000, 1700000060000000000)`,
		hints: []Hint{{Name: "trace_sample", Value: Static{Type: TypeInt, Data: 1}}},
	},
	{
		name:  "static types and unknown names",
		input: "{} with(custom=`a,b`, budget=1s, disabled=false, missing=nil, signed=-2)",
		body:  `{}`,
		hints: []Hint{
			{Name: "custom", Value: Static{Type: TypeString, Str: "a,b"}},
			{Name: "budget", Value: Static{Type: TypeDuration, Data: uint64(time.Second)}},
			{Name: "disabled", Value: Static{Type: TypeBool}},
			{Name: "missing", Value: Static{Type: TypeNil}},
			{Name: "signed", Value: Static{Type: TypeInt, Data: math.MaxUint64 - 1}},
		},
	},
	{
		name:  "duplicate names preserved",
		input: `{} with(sample=true, sample=false)`,
		body:  `{}`,
		hints: []Hint{
			{Name: "sample", Value: Static{Type: TypeBool, Data: 1}},
			{Name: "sample", Value: Static{Type: TypeBool}},
		},
	},
	{
		name:  "last block replaces earlier block",
		input: `{} with(sample=true, exemplars=10) with(most_recent=false)`,
		body:  `{}`,
		hints: []Hint{{Name: "most_recent", Value: Static{Type: TypeBool}}},
	},
	{
		name:  "comments and whitespace",
		input: "{.with = \"with\"} # selector\n with (sample = true # hint\n)",
		body:  `{.with = "with"}`,
		hints: []Hint{{Name: "sample", Value: Static{Type: TypeBool, Data: 1}}},
	},
}

func TestParseHints(t *testing.T) {
	t.Parallel()
	for _, tt := range hintTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			body, err := Parse(tt.body)
			require.NoError(t, err)
			got, err := Parse(tt.input)
			require.NoError(t, err)
			require.Equal(t, &HintedExpr{Expr: body, Hints: tt.hints}, got)

			wantOp, wantMatchers := ExtractMatchers(body)
			gotOp, gotMatchers := ExtractMatchers(got)
			require.Equal(t, wantOp, gotOp)
			require.Equal(t, wantMatchers, gotMatchers)
			require.Equal(t, ExtractTraceDurationMatchers(body), ExtractTraceDurationMatchers(got))
		})
	}
}

var invalidHintTests = []string{
	`with(sample=true)`,
	`{} with`,
	`{} with()`,
	`{} with(sample)`,
	`{} with(sample=)`,
	`{} with(sample=true`,
	`{} with(sample=true,)`,
	`{} with(,sample=true)`,
	`{} with("sample"=true)`,
	`{} with(.sample=true)`,
	`{} with(span.sample=true)`,
	`{} with(@=true)`,
	`{} with(1sample=true)`,
	`{} with(sample==true)`,
	`{} with(sample=true other=false)`,
	`{} with(sample=unknown)`,
	`{} with(sample=.attribute)`,
	`{} with(sample=(true))`,
	`{} with(exemplars=1+2)`,
	`{} with(sample=true) trailing`,
	`{} with(sample=true) | rate()`,
	`({} | rate() with(sample=true)) / ({} | rate())`,
	`{} | with(sample=true)`,
}

func TestParseInvalidHints(t *testing.T) {
	t.Parallel()
	for _, input := range invalidHintTests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(input)
			var syntaxErr *SyntaxError
			require.ErrorAs(t, err, &syntaxErr)
		})
	}
}
