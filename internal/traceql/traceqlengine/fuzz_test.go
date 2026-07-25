package traceqlengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
)

// fuzzBuildSeeds covers every stage and expression kind the builder handles, so
// the fuzzer starts from inputs that reach past [traceql.Parse].
var fuzzBuildSeeds = []string{
	`{}`,
	`{ true }`,
	`{ .a = "b" }`,
	`{ span.a != 1 && resource.b > 2s }`,
	`{ .a =~ "b.*" || .c !~ "d" }`,
	`{ name = "Span #1" }`,
	`{ duration > 1s && status = ok && kind = server }`,
	`{ parent != nil }`,
	`{ name != nil }`,
	`{ .missing = nil }`,
	`{ trace:id != nil }`,
	`{ span:parentID != nil }`,
	`{ event:name = "x" }`,
	`{ link:traceID != nil }`,
	`{ instrumentation:name = "x" }`,
	`{ ."foo bar" = 1 }`,
	`{ -(.a) < 0 }`,
	`{ !(.a) }`,
	`{ 2 + 3 * 4 = 14 }`,
	// Pipeline stages.
	`{ .a } | by(.b)`,
	`{ .a } | select(.b, .c)`,
	`{ .a } | by(.b) | coalesce()`,
	`{ .a } | count() > 1`,
	`{ .a } | avg(duration) > 1s`,
	`{ .a } | max(.b) - min(.b) > 1`,
	`{ .a } | by(resource.service.name) | count() > 1`,
	// Spanset operators.
	`{ .a } && { .b }`,
	`{ .a } || { .b }`,
	`{ .a } > { .b }`,
	`{ .a } >> { .b }`,
	`{ .a } ~ { .b }`,
	`({ .a } && { .b }) || { .c }`,
	`{ .a } && { .b } | count() > 1`,
	// Parsed, but the engine rejects these.
	`{ .a } < { .b }`,
	`{ .a } !> { .b }`,
	`{ .a } &~ { .b }`,
}

// FuzzBuildExpr checks that anything [traceql.Parse] accepts can be handed to
// the engine without panicking, both while building and while processing.
func FuzzBuildExpr(f *testing.F) {
	for _, input := range fuzzBuildSeeds {
		f.Add(input)
	}

	sets := []Spanset{
		{
			TraceID:         otelstorage.TraceID(testTraceID),
			RootSpanName:    "root",
			RootServiceName: "test.service",
			TraceDuration:   2_000000000,
			Spans: generateSpans([]spanIDs{
				{id: 1},
				{id: 2, parent: 1},
				{id: 3, parent: 2},
			}, "set"),
		},
	}

	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); r != nil || t.Failed() {
				t.Logf("Input:\n%s", input)
				panic(r)
			}
		}()

		expr, err := traceql.Parse(input)
		if err != nil {
			return
		}
		proc, err := BuildExpr(expr)
		if err != nil {
			return
		}
		require.NotNil(t, proc)

		// Processing must not panic, and must not mutate the input spansets
		// into something a second pass chokes on.
		out, err := proc.Process(sets)
		if err != nil {
			return
		}
		_, _ = proc.Process(out)
	})
}
