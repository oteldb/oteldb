package storagebackend_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

// limitResult is a streams result in canonical form: stream label set → its entries, each stream's
// entries ordered by ascending timestamp (the order the API itself produces).
type limitResult map[string][]lokiapi.LogEntry

// canonicalize flattens a streams result into a [limitResult].
func canonicalize(tb testing.TB, data lokiapi.QueryResponseData) limitResult {
	tb.Helper()
	require.Equal(tb, lokiapi.StreamsResultQueryResponseData, data.Type)

	out := limitResult{}
	for _, s := range data.StreamsResult.Result {
		pairs := make([]string, 0, len(s.Stream.Value))
		for k, v := range s.Stream.Value {
			pairs = append(pairs, k+"="+v)
		}
		slices.Sort(pairs)
		key := strings.Join(pairs, ",")
		require.NotContains(tb, out, key, "duplicate stream in result")
		out[key] = s.Values
	}
	return out
}

// timestamps returns every entry's timestamp, sorted ascending.
func (r limitResult) timestamps() []uint64 {
	var out []uint64
	for _, values := range r {
		for _, v := range values {
			out = append(out, v.T)
		}
	}
	slices.Sort(out)
	return out
}

func (r limitResult) len() (n int) {
	for _, values := range r {
		n += len(values)
	}
	return n
}

// TestLogQLLimitPushdownEquivalence proves a limited log query returns the same entries whether or
// not the limit is pushed into the storage fetch.
//
// For each query it evaluates the unlimited form once as the reference, then every limit/direction
// combination, and asserts the limited result is exactly the reference cut to N in the query's
// order:
//
//   - it holds min(N, total) entries;
//   - each stream's entries are a contiguous end of that stream's reference entries — the newest
//     when backward, the oldest when forward;
//   - the kept timestamps are the N extreme timestamps of the reference, so no entry that should
//     have made the cut was dropped for one that should not have.
//
// The last point is what a storage-side limit gets wrong when a stage can still drop entries, and
// the per-stream check pins that nothing is reordered or substituted among timestamp ties (the
// corpus has many: every stream shares the same timestamp grid).
//
// The bare selector (and the selector plus a non-dropping parser stage) is where the pushdown is
// active; the line-filter query is where it must stay off.
func TestLogQLLimitPushdownEquivalence(t *testing.T) {
	corpus := goldenLogQLFixture(t)
	ctx := context.Background()

	// Number of entries a single service contributes to the corpus.
	const perService = goldenLogQLPerRound * (goldenLogQLParts + 1)

	for _, tc := range []struct {
		name   string
		query  string
		limits []int
	}{
		{
			// Pushdown active: nothing can drop an entry.
			name:   "bare_selector",
			query:  `{env="prod"}`,
			limits: []int{1, 7, 100, 999},
		},
		{
			// Pushdown active: a parser records failures as __error__, it never drops.
			name:   "non_dropping_stage",
			query:  `{service_name="svc-0"} | json`,
			limits: []int{1, 45, 1000},
		},
		{
			// Pushdown must stay off: the line filter drops entries after the fetch.
			name:   "line_filter",
			query:  `{service_name="svc-0"} |= "\"method\":\"GET\""`,
			limits: []int{1, 100, 900, 901},
		},
		{
			// Boundary: one short of, exactly, and more than the matching entry count.
			name:   "boundary",
			query:  `{service_name="svc-0"}`,
			limits: []int{perService - 1, perService, perService + 1, 10 * perService},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := corpus.engine.NewQuery(ctx, tc.query)
			require.NoError(t, err)

			for _, dir := range []logqlengine.Direction{logqlengine.DirectionForward, logqlengine.DirectionBackward} {
				t.Run(string(dir), func(t *testing.T) {
					eval := func(limit int) limitResult {
						t.Helper()
						data, err := q.Eval(ctx, logqlengine.EvalParams{
							Start:     corpus.start,
							End:       corpus.end,
							Direction: dir,
							Limit:     limit,
						})
						require.NoError(t, err)
						return canonicalize(t, data)
					}

					want := eval(-1)
					wantTS := want.timestamps()
					require.NotEmpty(t, wantTS)

					backward := dir == logqlengine.DirectionBackward
					for _, limit := range tc.limits {
						t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
							got := eval(limit)
							n := min(limit, len(wantTS))
							require.Equal(t, n, got.len())

							// The kept timestamps are the n extreme ones of the reference.
							cut := wantTS[:n]
							if backward {
								cut = wantTS[len(wantTS)-n:]
							}
							require.Equal(t, cut, got.timestamps())

							// Each stream keeps a contiguous end of its reference entries.
							for key, values := range got {
								ref, ok := want[key]
								require.Truef(t, ok, "stream %q is not in the unlimited result", key)
								if backward {
									require.Equal(t, ref[len(ref)-len(values):], values, "stream %q", key)
								} else {
									require.Equal(t, ref[:len(values)], values, "stream %q", key)
								}
							}
						})
					}
				})
			}
		})
	}
}
