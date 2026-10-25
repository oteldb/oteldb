package scarecrow_test

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

const classicHistogramData = `
load 30s
  duration_bucket{job="a",le="1"} 1 2 3 4 5 6 7 8 9
  duration_bucket{job="a",le="2"} 2 4 6 8 10 12 14 16 18
  duration_bucket{job="a",le="+Inf"} 4 8 12 16 20 24 28 32 36
  duration_bucket{job="b",le="-2"} 1 2 3 4 5 6 7 8 9
  duration_bucket{job="b",le="0"} 2 4 6 8 10 12 14 16 18
  duration_bucket{job="b",le="+Inf"} 4 8 12 16 20 24 28 32 36
  duration_sum{job="a"} 4 8 12 16 20 24 28 32 36
  duration_count{job="a"} 4 8 12 16 20 24 28 32 36
  malformed_bucket{le="invalid"} 1 1 1
  malformed_bucket{le="1"} 1 1 1
  malformed_bucket{le="+Inf"} 2 2 2
  duplicate_bucket{le="1"} 1 2 3
  duplicate_bucket{le="1.0"} 1 2 3
  duplicate_bucket{le="+Inf"} 4 8 12
  missing_inf_bucket{le="1"} 1 2 3
  missing_inf_bucket{le="2"} 2 4 6
  inf_only_bucket{le="+Inf"} 1 2 3
  zero_bucket{le="1"} 0 0 0
  zero_bucket{le="+Inf"} 0 0 0
  nan_count_bucket{le="1"} NaN NaN NaN
  nan_count_bucket{le="+Inf"} 2 2 2
  nonmonotonic_bucket{le="1"} 10 10 10
  nonmonotonic_bucket{le="2"} 5 5 5
  nonmonotonic_bucket{le="+Inf"} 20 20 20
  precision_bucket{le="1"} 10 10 10
  precision_bucket{le="2"} 9.9999999999999 9.9999999999999 9.9999999999999
  precision_bucket{le="+Inf"} 20 20 20
  gaps_bucket{le="1"} 1 2 stale _ 5 6 stale _ 9
  gaps_bucket{le="+Inf"} 2 4 stale _ 10 12 stale _ 18
  other_bucket{job="a",le="1"} 1 2 3
  other_bucket{job="a",le="+Inf"} 2 4 6
  early_bucket{le="1"} 1 stale _ _ _
  early_bucket{le="+Inf"} 2 stale _ _ _
  late_bucket{le="1"} _ _ 3 4 5
  late_bucket{le="+Inf"} _ _ 6 8 10
  metadata_bucket{le="1",__type__="counter",__unit__="seconds"} 1 2 3
  metadata_bucket{le="+Inf",__type__="counter",__unit__="seconds"} 2 4 6
`

type annotatedResult struct {
	value           result
	err             string
	warnings, infos []string
}

func runAnnotatedQuery(t *testing.T, engine promql.QueryEngine, store storage.Queryable, text string, instant bool) annotatedResult {
	t.Helper()
	ctx := t.Context()
	var query promql.Query
	var err error
	if instant {
		query, err = engine.NewInstantQuery(ctx, store, nil, text, time.Unix(60, 0))
	} else {
		query, err = engine.NewRangeQuery(ctx, store, nil, text, time.Unix(0, 0), time.Unix(240, 0), 30*time.Second)
	}
	require.NoError(t, err)
	defer query.Close()
	res := query.Exec(ctx)
	out := annotatedResult{}
	out.warnings, out.infos = res.Warnings.AsStrings(text, 0, 0)
	slices.Sort(out.warnings)
	slices.Sort(out.infos)
	if res.Err != nil {
		out.err = res.Err.Error()
	} else {
		out.value = render(t, res, text)
	}
	return out
}

func TestClassicHistogramQuantileMatchesPrometheus(t *testing.T) {
	store := promqltest.LoadedStorage(t, classicHistogramData)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, query := range []string{
		`histogram_quantile(0.5, duration_bucket)`,
		`histogram_quantile(0, duration_bucket)`,
		`histogram_quantile(1, duration_bucket)`,
		`histogram_quantile(-0.5, duration_bucket)`,
		`histogram_quantile(1.5, duration_bucket)`,
		`histogram_quantile(0/0, duration_bucket)`,
		`histogram_quantile(time()/240, duration_bucket)`,
		`histogram_quantile(time()/60-1, duration_bucket)`,
		`histogram_quantile(0.9, missing)`,
		`histogram_quantile(-1, missing)`,
		`histogram_quantile(0.5, duplicate_bucket)`,
		`histogram_quantile(0.5, malformed_bucket)`,
		`histogram_quantile(0.5, missing_inf_bucket)`,
		`histogram_quantile(0.5, inf_only_bucket)`,
		`histogram_quantile(0.5, zero_bucket)`,
		`histogram_quantile(0.5, nan_count_bucket)`,
		`histogram_quantile(0.5, nonmonotonic_bucket)`,
		`histogram_quantile(0.5, precision_bucket)`,
		`histogram_quantile(0.5, gaps_bucket)`,
		`histogram_quantile(0.5, metadata_bucket)`,
		`histogram_quantile(0.5, {__name__=~"duration_.+"})`,
		`histogram_quantile(0.5, {__name__=~"duration_bucket|other_bucket"})`,
		`histogram_quantile(0.5, {__name__=~"early_bucket|late_bucket"})`,
		`histogram_quantile(0.5, sum by(job, le)(rate(duration_bucket[1m])))`,
		`sum(histogram_quantile(0.5, duration_bucket))`,
		`histogram_quantile(0.5, nonmonotonic_bucket)+histogram_quantile(-1, duration_bucket)`,
		`max_over_time(histogram_quantile(0.5, nonmonotonic_bucket)[1m:30s])`,
	} {
		t.Run(query, func(t *testing.T) {
			for _, instant := range []bool{false, true} {
				want := runAnnotatedQuery(t, upstreamEngine(), store, query, instant)
				for _, chunk := range []int{-1, 1, 3} {
					got := runAnnotatedQuery(t, scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true, EnableNegativeOffset: true, ChunkSteps: chunk}), store, query, instant)
					want.infos = withoutUpstreamRepairCount(want.infos)
					got.infos = withoutUpstreamRepairCount(got.infos)
					require.Equalf(t, want, got, "instant=%v chunk=%d", instant, chunk)
				}
			}
		})
	}
}

func TestClassicHistogramWarningsResetPerExecution(t *testing.T) {
	store := promqltest.LoadedStorage(t, classicHistogramData)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine := scarecrow.NewEngine(scarecrow.Opts{})
	query, err := engine.NewInstantQuery(context.Background(), store, nil, `histogram_quantile(0.5, nonmonotonic_bucket)`, time.Unix(60, 0))
	require.NoError(t, err)
	t.Cleanup(query.Close)
	first := query.Exec(t.Context())
	require.NoError(t, first.Err)
	second := query.Exec(t.Context())
	require.NoError(t, second.Err)
	firstWarn, firstInfo := first.Warnings.AsStrings(query.String(), 0, 0)
	secondWarn, secondInfo := second.Warnings.AsStrings(query.String(), 0, 0)
	require.Equal(t, firstWarn, secondWarn)
	require.Equal(t, firstInfo, secondInfo)
	require.Len(t, firstInfo, 1)
}

var repairCount = regexp.MustCompile(`over (\d+) samples from ([^ ]+) to ([^ ]+)`)

// The pinned upstream engine self-merges repair counts; all other annotation fields stay exact.
func withoutUpstreamRepairCount(infos []string) []string {
	out := slices.Clone(infos)
	for i, info := range out {
		if strings.HasPrefix(info, "PromQL info: input to histogram_quantile needed to be fixed for monotonicity") {
			match := repairCount.FindStringSubmatch(info)
			if len(match) == 4 && match[2] != match[3] {
				out[i] = repairCount.ReplaceAllString(info, "over <count> samples from ${2} to ${3}")
			}
		}
	}
	return out
}

func TestClassicHistogramRepairCount(t *testing.T) {
	store := promqltest.LoadedStorage(t, classicHistogramData)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, chunk := range []int{-1, 1, 3} {
		got := runAnnotatedQuery(t, scarecrow.NewEngine(scarecrow.Opts{ChunkSteps: chunk}), store, "histogram_quantile(0.5, nonmonotonic_bucket)", false)
		require.Empty(t, got.err)
		require.Len(t, got.infos, 1)
		require.Contains(t, got.infos[0], "over 9 samples from")
	}
}
