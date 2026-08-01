package scarecrow_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// experimentalParserOpts enables limitk/limit_ratio, which chunkQueries exercises.
var experimentalParserOpts = parser.Options{EnableExperimentalFunctions: true}

// chunkQueries covers every operator shape §4.4 calls out for chunking (aggregations, one-to-one
// binops, subqueries) plus the full-set aggregations (§ M2b) whose per-step selection is, by
// construction, independent of where a chunk boundary falls — this is what proves chunking must
// not change their result, not just that it happens not to in this corpus.
var chunkQueries = []string{
	`counter`,
	`rate(counter[30s])`,
	`sum(counter)`,
	`sum by (job) (counter)`,
	`counter + counter`,
	`counter * on(job) counter`,
	`sum_over_time(counter[1m:10s])`,
	`topk(1, counter)`,
	`bottomk(1, counter)`,
	`quantile(0.5, counter)`,
	`limitk(1, counter)`,
}

// TestChunkingMatchesUnchunked asserts that splitting a range query into several small chunks
// (M16) yields the identical result to evaluating it in one chunk, across every query shape
// chunking has to preserve. ChunkSteps=2 forces many chunk boundaries over a small range, which
// is where a concatenation bug (duplicated or dropped steps, a series lost across a chunk edge)
// would show up.
func TestChunkingMatchesUnchunked(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, diffCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	whole := scarecrow.NewEngine(scarecrow.Opts{ChunkSteps: -1, Parser: experimentalParserOpts})
	chunked := scarecrow.NewEngine(scarecrow.Opts{ChunkSteps: 2, Parser: experimentalParserOpts})

	start, end := time.Unix(0, 0), time.Unix(90, 0)
	step := 10 * time.Second

	for _, qs := range chunkQueries {
		t.Run(qs, func(t *testing.T) {
			t.Parallel()

			want := execRange(t, whole, st, qs, start, end, step)
			got := execRange(t, chunked, st, qs, start, end, step)

			requireSameValue(t, want, got)
		})
	}
}

// TestChunkingMatchesUpstream is [TestDifferentialRange] with chunking forced on at every
// boundary (ChunkSteps=1), so every step is its own chunk. This is the strongest form of the
// M16 correctness claim: not just internal consistency, but agreement with the reference engine.
func TestChunkingMatchesUpstream(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, diffCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	ours := scarecrow.NewEngine(scarecrow.Opts{
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		ChunkSteps:           1,
		Parser:               experimentalParserOpts,
	})
	theirs := promql.NewEngine(promql.EngineOpts{
		MaxSamples:               1e6,
		Timeout:                  time.Minute,
		LookbackDelta:            5 * time.Minute,
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
		Parser:                   parser.NewParser(experimentalParserOpts),
	})

	start, end := time.Unix(0, 0), time.Unix(90, 0)
	step := 10 * time.Second

	for _, qs := range chunkQueries {
		t.Run(qs, func(t *testing.T) {
			t.Parallel()

			want := execRange(t, theirs, st, qs, start, end, step)
			got := execRange(t, ours, st, qs, start, end, step)

			requireSameValue(t, want, got)
		})
	}
}

// TestChunkStepsDoesNotAffectInstantQueries guards against a chunk-splitting bug leaking into
// the instant path, which has exactly one step and nothing to chunk.
func TestChunkStepsDoesNotAffectInstantQueries(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, diffCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	chunked := scarecrow.NewEngine(scarecrow.Opts{ChunkSteps: 1})

	query, err := chunked.NewInstantQuery(context.Background(), st, nil, "sum(counter)", time.Unix(30, 0))
	require.NoError(t, err)
	defer query.Close()

	res := query.Exec(context.Background())
	require.NoError(t, res.Err)
}
