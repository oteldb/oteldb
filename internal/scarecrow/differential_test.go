package scarecrow_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// diffCorpus exercises irregular spacing, counter resets, gaps and a staleness marker — the
// shapes where lookback boundaries and rate extrapolation actually differ between
// implementations.
const diffCorpus = `
load 10s
  counter{job="a"}   0 10 20 30 45 45 10 20 30 40
  counter{job="b"}   5 5 5 5 5 5 5 5 5 5
  gauge{job="a"}     1 2 3 -4 5 NaN 7 8 9 10
  sparse{job="a"}    1 _ _ _ 5 _ _ _ 9 _
  stale_metric       0 1 stale 3 4 5 6 7 8 9
`

// diffQueries are evaluated by both engines and compared. They stay within M1's surface:
// selectors and range-vector functions.
var diffQueries = []string{
	`counter`,
	`counter{job="a"}`,
	`gauge`,
	`sparse`,
	`stale_metric`,
	`counter offset 30s`,
	`sparse offset 1m`,
	`counter offset -30s`,
	`gauge offset -1m`,
	`rate(counter[30s])`,
	`rate(counter[1m])`,
	`rate(counter[90s])`,
	`increase(counter[1m])`,
	`increase(counter[30s])`,
	`delta(gauge[1m])`,
	`irate(counter[1m])`,
	`idelta(gauge[1m])`,
	`resets(counter[2m])`,
	`changes(gauge[2m])`,
	`count_over_time(counter[1m])`,
	`sum_over_time(gauge[1m])`,
	`avg_over_time(gauge[1m])`,
	`min_over_time(gauge[1m])`,
	`max_over_time(gauge[1m])`,
	`last_over_time(gauge[1m])`,
	`present_over_time(sparse[30s])`,
	`stddev_over_time(gauge[2m])`,
	`stdvar_over_time(gauge[2m])`,
	`count_over_time(stale_metric[1m])`,
	`rate(counter[1m] offset 30s)`,
	`sum_over_time(sparse[2m])`,

	// Aggregations.
	`sum(counter)`,
	`sum by (job) (counter)`,
	`sum without (job) (counter)`,
	`count(counter)`,
	`avg(gauge)`,
	`avg by (job) (gauge)`,
	`min(gauge)`,
	`max(gauge)`,
	`group(counter)`,
	`count_values("value", counter)`,
	`count_values by (job) ("value", counter)`,
	`count_values without (job) ("value", counter)`,
	`count_values("job", counter)`,
	`stddev(gauge)`,
	`stdvar(gauge)`,
	`sum by (job) (rate(counter[1m]))`,
	`sum(sparse)`,

	// Binary operators.
	`counter * 2`,
	`2 * counter`,
	`counter / 0`,
	`counter > 20`,
	`counter > bool 20`,
	`counter == 45`,
	`-counter`,
	`counter + counter`,
	`counter - counter`,
	`counter * on(job) counter`,
	`counter > counter`,
	`counter >= bool counter`,
	`counter and counter`,
	`counter unless counter`,
	`counter or gauge`,
	`sparse or counter`,
	`counter and on(job) gauge`,
	`counter unless on(job) sparse`,
	`sum by (job) (counter) / sum by (job) (gauge)`,
	`1 + 2 * 3`,
	`2 > bool 1`,

	// Subqueries.
	`sum_over_time(counter[1m:10s])`,
	`avg_over_time(gauge[2m:30s])`,
	`rate(counter[1m:10s])`,
	`max_over_time(rate(counter[30s])[1m:10s])`,
	`sum_over_time(sum(counter)[1m:10s])`,
	`count_over_time(sparse[2m:20s])`,
	`last_over_time(gauge[1m:15s])`,
	`sum_over_time(counter[1m:10s] offset 30s)`,
	`avg_over_time((counter * 2)[1m:20s])`,
	`sum_over_time(counter[90s:])`,
	`rate(sum_over_time(counter[30s:10s])[1m:10s])`,

	// Instant functions.
	`abs(-counter)`,
	`ceil(gauge / 3)`,
	`floor(gauge / 3)`,
	`exp(gauge / 100)`,
	`ln(counter)`,
	`log2(counter)`,
	`log10(counter)`,
	`sqrt(counter)`,
	`sgn(gauge)`,
	`round(gauge / 3)`,
	`round(gauge / 3, 0.5)`,
	`clamp(gauge, 2, 8)`,
	`clamp_min(gauge, 5)`,
	`clamp_max(gauge, 5)`,
	`clamp(gauge, 8, 2)`,
	`timestamp(counter)`,
	`scalar(sum(counter))`,
	`vector(42)`,
	`vector(scalar(sum(counter)))`,
	`label_replace(counter, "id", "$1", "job", "(.*)")`,
	`label_replace(counter, "job", "", "job", "a")`,
	`label_join(counter, "id", "-", "job", "__name__")`,
	`sin(gauge)`,
	`cos(gauge)`,
	`deg(gauge)`,
	`rad(gauge)`,

	// absent / absent_over_time (M13).
	`absent(counter)`,
	`absent(nonexistent)`,
	`absent(counter{job="a"})`,
	`absent(counter{job="c"})`,
	`absent(counter{job=~"a|b"})`,
	`absent(counter{job="a",job="b"})`,
	`absent(sum(counter))`,
	`absent(counter + counter)`,
	`absent_over_time(counter[1m])`,
	`absent_over_time(nonexistent[1m])`,
	`absent_over_time(counter{job="a"}[1m])`,
	`absent_over_time(sparse[15s])`,
	`absent_over_time(sum(counter)[1m:10s])`,
}

// upstreamEngine builds Prometheus' own engine, the reference implementation.
func upstreamEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		MaxSamples:           1e6,
		Timeout:              time.Minute,
		LookbackDelta:        5 * time.Minute,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		// Required for `foo[5m:]`; the upstream engine nil-derefs without it.
		NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
	})
}

func scarecrowEngine() *scarecrow.Engine {
	return scarecrow.NewEngine(scarecrow.Opts{
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})
}

// TestDifferentialInstant asserts scarecrow agrees with the upstream engine on every query at
// several evaluation times. This is the correctness lever the compliance corpus cannot give at
// case granularity: it pins rate extrapolation, lookback boundaries and staleness against the
// reference rather than against hand-computed expectations.
func TestDifferentialInstant(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, diffCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	ours, theirs := scarecrowEngine(), upstreamEngine()

	// Includes times before, inside and after the data, plus non-aligned instants.
	for _, at := range []time.Duration{0, 5 * time.Second, 35 * time.Second, 90 * time.Second, 5 * time.Minute} {
		for _, qs := range diffQueries {
			t.Run(qs+" at="+at.String(), func(t *testing.T) {
				t.Parallel()

				ts := time.Unix(0, 0).Add(at)

				want := execInstant(t, theirs, st, qs, ts)
				got := execInstant(t, ours, st, qs, ts)

				requireSameValue(t, want, got)
			})
		}
	}
}

// TestDifferentialRange is [TestDifferentialInstant] over a stepped range, which additionally
// exercises the two-pointer sliding window across steps.
func TestDifferentialRange(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, diffCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	ours, theirs := scarecrowEngine(), upstreamEngine()

	start, end := time.Unix(0, 0), time.Unix(120, 0)

	for _, step := range []time.Duration{7 * time.Second, 10 * time.Second, 30 * time.Second} {
		for _, qs := range diffQueries {
			t.Run(qs+" step="+step.String(), func(t *testing.T) {
				t.Parallel()

				want := execRange(t, theirs, st, qs, start, end, step)
				got := execRange(t, ours, st, qs, start, end, step)

				requireSameValue(t, want, got)
			})
		}
	}
}

// result is a query's rendered outcome. It is captured as text *before* Close, because the
// upstream engine pools its FPoint slices and Close returns them — reading res.Value afterwards
// is a use-after-free that only shows up once queries run concurrently.
type result struct {
	typ parser.ValueType
	str string
}

func render(t *testing.T, res *promql.Result, qs string) result {
	t.Helper()

	require.NoError(t, res.Err, "query %q", qs)

	v := res.Value

	// An instant vector has no defined element order in PromQL, and upstream's happens to be
	// nondeterministic. Sort a copy before rendering so the comparison comes down to content.
	// Matrices are already ordered by both engines.
	if vec, ok := v.(promql.Vector); ok {
		sorted := slices.Clone(vec)
		slices.SortFunc(sorted, func(a, b promql.Sample) int {
			return labels.Compare(a.Metric, b.Metric)
		})
		v = sorted
	}

	return result{typ: v.Type(), str: v.String()}
}

func execInstant(t *testing.T, e promql.QueryEngine, q storage.Queryable, qs string, ts time.Time) result {
	t.Helper()

	query, err := e.NewInstantQuery(context.Background(), q, nil, qs, ts)
	require.NoError(t, err)

	defer query.Close()

	return render(t, query.Exec(context.Background()), qs)
}

func execRange(
	t *testing.T, e promql.QueryEngine, q storage.Queryable, qs string, start, end time.Time, step time.Duration,
) result {
	t.Helper()

	query, err := e.NewRangeQuery(context.Background(), q, nil, qs, start, end, step)
	require.NoError(t, err)

	defer query.Close()

	return render(t, query.Exec(context.Background()), qs)
}

// requireSameValue compares two PromQL values, using String() so a difference reports as
// readable PromQL rather than as a struct dump. Float formatting is identical between engines
// because both round-trip through the same formatter.
func requireSameValue(t *testing.T, want, got result) {
	t.Helper()

	require.Equal(t, want.typ, got.typ, "result type")
	require.Equal(t, want.str, got.str)
}
