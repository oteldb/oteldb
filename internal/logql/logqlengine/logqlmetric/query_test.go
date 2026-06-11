package logqlmetric

import (
	"cmp"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

func testSampler(samples []SampledEntry) SampleSelector {
	return func(_ *logql.RangeAggregationExpr, _, _ time.Time) (iterators.Iterator[SampledEntry], error) {
		slices.SortStableFunc(samples, func(a, b SampledEntry) int {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		})
		return iterators.Slice(samples), nil
	}
}

func evaluateQuery(t *testing.T, samples []SampledEntry, query string, params EvalParams, instant bool) lokiapi.QueryResponseData {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Query: \n%s", query)
		}
	})

	expr, err := logql.Parse(query, logql.ParseOptions{})
	require.NoError(t, err)

	require.Implements(t, (*logql.MetricExpr)(nil), expr)
	metricExpr := expr.(logql.MetricExpr)

	agg, err := Build(metricExpr, testSampler(samples), params)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, agg.Close())
	}()

	data, err := ReadStepResponse(
		agg,
		instant,
	)
	require.NoError(t, err)

	return data
}

func TestInstantAggregation(t *testing.T) {
	var (
		testParams = EvalParams{
			Start: time.Unix(1700000004, 0),
			End:   time.Unix(1700000004, 0),
			Step:  0,
		}
		testSamples = []SampledEntry{
			// Would not be used.
			{Sample: 10000, Timestamp: 1700000002_000000000, Set: emptyLabels()},

			// Step 1.
			// 2s Window.
			{Sample: 2, Timestamp: 1700000003_000000000, Set: emptyLabels()},
			{Sample: 3, Timestamp: 1700000004_000000000, Set: emptyLabels()},
			// Window ends.

			// Would not be used.
			{Sample: 10000, Timestamp: 1700000005_000000000, Set: emptyLabels()},
		}
	)

	tests := []struct {
		query    string
		expected string
	}{
		// Range aggregation.
		{`count_over_time({} [2s])`, "2"},
		{`rate({} [2s])`, "1"},                // count per log range interval
		{`rate({} | unwrap foo [2s])`, "2.5"}, // sum per log range interval
		{`bytes_over_time({} [2s])`, "5"},     // same as sum
		{`bytes_rate({} [2s])`, "2.5"},        // sum per log range interval
		{`avg_over_time({} | unwrap foo [2s])`, "2.5"},
		{`sum_over_time({} | unwrap foo [2s])`, "5"},
		{`min_over_time({} | unwrap foo [2s])`, "2"},
		{`max_over_time({} | unwrap foo [2s])`, "3"},
		//
		// Mean is 2.5.
		// Count is 2.
		//
		// stdvar =
		// 	( (2-mean)^2 + (3-mean)^2 ) / count =>
		// 	( (2-2.5)^2 + (3-2.5)^2 ) / 2 =>
		//	(0.25 + 0.25) / 2 =>
		//	stdvar = 0.25
		//
		{`stdvar_over_time({} | unwrap foo [2s])`, "0.25"},
		//
		// stddev = sqrt(stdvar) =>
		// 	sqrt(0.25) =>
		//  stddev = 0.5
		//
		{`stddev_over_time({} | unwrap foo [2s])`, "0.5"},
		{`quantile_over_time(0.99, {} | unwrap foo [2s])`, "2.9899999999999998"},
		{`first_over_time({} | unwrap foo [2s])`, "2"},
		{`last_over_time({} | unwrap foo [2s])`, "3"},
		// Vector aggregation.
		{`count(count_over_time({} [2s]))`, "1"},
		{`sum(count_over_time({} [2s]))`, "2"},
		{`avg(count_over_time({} [2s]))`, "2"},

		// Vector function.
		{`vector(1)`, "1"},
		{`vector(3.14)`, "3.14"},

		// Binary operation.
		// With literal.
		// Addition.
		{`vector(2) + 2`, "4"},
		// Subtraction.
		{`vector(2) - 2`, "0"},
		// Multiplication.
		{`vector(2) * 2`, "4"},
		{`2 * vector(2)`, "4"},
		// Division.
		{`vector(4) / 2`, "2"},
		{`vector(4) / 0`, "NaN"},
		{`2 / vector(4)`, "0.5"},
		{`2 / vector(0)`, "NaN"},
		// Modular division.
		{`vector(4) % 2`, "0"},
		{`vector(3) % 2`, "1"},
		{`vector(4) % 0`, "NaN"},
		{`4 % vector(2)`, "0"},
		{`3 % vector(2)`, "1"},
		{`2 % vector(0)`, "NaN"},
		// Exponentiation.
		{`vector(2) ^ 4`, "16"},
		{`2 ^ vector(3)`, "8"},
		// Comparison operations.
		{`vector(2) == bool 2`, "1"},
		{`vector(2) == bool 1`, "0"},
		{`vector(2) != bool 1`, "1"},
		{`vector(2) != bool 2`, "0"},
		{`vector(2) > bool 1`, "1"},
		{`vector(2) > bool 2`, "0"},
		{`vector(2) >= bool 1`, "1"},
		{`vector(2) >= bool 2`, "1"},
		{`vector(2) >= bool 3`, "0"},
		{`vector(2) < bool 3`, "1"},
		{`vector(2) < bool 2`, "0"},
		{`vector(2) <= bool 3`, "1"},
		{`vector(2) <= bool 2`, "1"},
		{`vector(2) <= bool 1`, "0"},

		// Operations with range.
		{`count_over_time({} [2s]) * 2`, "4"},
		{`2 * count_over_time({} [2s])`, "4"},
		// Between vectors.
		{`vector(2) * vector(2)`, "4"},
		// Between ranges.
		{`count_over_time({} [2s]) - count_over_time({} [2s])`, "0"},
	}
	for i, tt := range tests {
		id := i + 1
		t.Run(fmt.Sprintf("Test%d", id), func(t *testing.T) {
			if id == 12 && runtime.GOARCH == "riscv64" {
				// TODO: Fix test failure on riscv64.
				t.Skip("https://github.com/oteldb/oteldb/issues/828")
			}

			data := evaluateQuery(t, testSamples, tt.query, testParams, true)

			v, ok := data.GetVectorResult()
			require.True(t, ok)
			require.NotEmpty(t, v.Result)
			sample := v.Result[0]
			require.Equal(t, tt.expected, sample.Value.V)
		})
	}
}

func TestRangeAggregationStep(t *testing.T) {
	var (
		ts = func(s, ns int64) pcommon.Timestamp {
			return pcommon.NewTimestampFromTime(time.Unix(1700000000+s, ns))
		}
		testSamples = []SampledEntry{
			{Timestamp: ts(2, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(5, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(6, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(10, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(10, 1), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(11, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(35, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(35, 1), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(40, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(100, 0), Set: emptyLabels(), Sample: 1.},
			{Timestamp: ts(100, 1), Set: emptyLabels(), Sample: 1.},
		}
	)

	tests := []struct {
		interval   time.Duration
		step       time.Duration
		expected   []lokiapi.FPoint
		start, end pcommon.Timestamp
	}{
		{
			5 * time.Second,
			30 * time.Second,
			[]lokiapi.FPoint{
				{
					T: getPrometheusTimestamp(ts(10, 0).AsTime()),
					V: "2",
				},
				{
					T: getPrometheusTimestamp(ts(40, 0).AsTime()),
					V: "2",
				},
				{
					T: getPrometheusTimestamp(ts(100, 0).AsTime()),
					V: "1",
				},
			},
			ts(10, 0), ts(110, 0),
		},
		{
			35 * time.Second, // will overlap by 5 sec
			30 * time.Second,
			[]lokiapi.FPoint{
				{
					T: getPrometheusTimestamp(ts(10, 0).AsTime()),
					V: "4",
				},
				{
					T: getPrometheusTimestamp(ts(40, 0).AsTime()),
					V: "7",
				},
				{
					T: getPrometheusTimestamp(ts(70, 0).AsTime()),
					V: "2",
				},
				{
					T: getPrometheusTimestamp(ts(100, 0).AsTime()),
					V: "1",
				},
			},
			ts(10, 0), ts(110, 0),
		},
		{
			50 * time.Second,
			10 * time.Second,
			[]lokiapi.FPoint{
				{
					T: getPrometheusTimestamp(ts(110, 0).AsTime()),
					V: "2",
				},
				{
					T: getPrometheusTimestamp(ts(120, 0).AsTime()),
					V: "2",
				},
			},
			ts(110, 0), ts(120, 0),
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var (
				query      = fmt.Sprintf(`count_over_time({} [%s])`, tt.interval)
				testParams = EvalParams{
					Start: tt.start.AsTime(),
					End:   tt.end.AsTime(),
					Step:  tt.step,
				}
			)
			data := evaluateQuery(t, testSamples, query, testParams, false)

			v, ok := data.GetMatrixResult()
			require.True(t, ok)
			require.Len(t, v.Result, 1)

			group := v.Result[0].Values
			require.Equal(t, tt.expected, group)
		})
	}
}

func TestRangeAggregation(t *testing.T) {
	var (
		testParams = EvalParams{
			Start: time.Unix(1700000000, 0),
			End:   time.Unix(1700000010, 0),
			Step:  3 * time.Second,
		}
		testSamples = []SampledEntry{
			// Step 1.
			// No samples.

			// Step 2.
			{Sample: 1, Timestamp: 1700000001_000000000, Set: emptyLabels()},
			// 2s window.
			{Sample: 2, Timestamp: 1700000002_000000000, Set: emptyLabels()},
			{Sample: 3, Timestamp: 1700000003_000000000, Set: emptyLabels()},
			// Window ends.

			// Step 3.
			{Sample: 4, Timestamp: 1700000004_000000000, Set: emptyLabels()},
			// 2s window.
			{Sample: 5, Timestamp: 1700000005_000000000, Set: emptyLabels()},
			{Sample: 6, Timestamp: 1700000006_000000000, Set: emptyLabels()},
			// Window ends.

			// Step 4.
			{Sample: 1, Timestamp: 1700000007_000000000, Set: emptyLabels()},
			// 2s window.
			{Sample: 2, Timestamp: 1700000008_000000000, Set: emptyLabels()},
			{Sample: 3, Timestamp: 1700000008_100000000, Set: emptyLabels()},
			{Sample: 4, Timestamp: 1700000009_000000000, Set: emptyLabels()},
			// Window ends.
		}
	)

	tests := []struct {
		query    string
		expected []string
	}{
		// Range aggregation.
		{`count_over_time({} [2s])`, []string{"2", "2", "3"}},
		{`rate({} [2s])`, []string{"1", "1", "1.5"}},                  // count per log range interval
		{`rate({} | unwrap foo [2s])`, []string{"2.5", "5.5", "4.5"}}, // sum per log range interval
		{`bytes_over_time({} [2s])`, []string{"5", "11", "9"}},        // same as sum
		{`bytes_rate({} [2s])`, []string{"2.5", "5.5", "4.5"}},        // sum per log range interval
		{`avg_over_time({} | unwrap foo [2s])`, []string{"2.5", "5.5", "3"}},
		{`sum_over_time({} | unwrap foo [2s])`, []string{"5", "11", "9"}},
		{`min_over_time({} | unwrap foo [2s])`, []string{"2", "5", "2"}},
		{`max_over_time({} | unwrap foo [2s])`, []string{"3", "6", "4"}},
		{`first_over_time({} | unwrap foo [2s])`, []string{"2", "5", "2"}},
		{`last_over_time({} | unwrap foo [2s])`, []string{"3", "6", "4"}},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data := evaluateQuery(t, testSamples, tt.query, testParams, false)

			v, ok := data.GetMatrixResult()
			require.True(t, ok)
			require.NotEmpty(t, v.Result)
			group := v.Result[0].Values

			samples := make([]string, 0, len(group))
			for _, v := range group {
				samples = append(samples, v.V)
			}
			require.Equal(t, tt.expected, samples)
		})
	}
}

func TestGroupedAggregation(t *testing.T) {
	var (
		testParams = EvalParams{
			Start: time.Unix(1700000000, 0),
			End:   time.Unix(1700000016, 0),
			Step:  4 * time.Second,
		}
		testSamples = []SampledEntry{
			// Step 1.
			// foo=a
			{Sample: 1, Timestamp: 1700000001_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "POST"})},
			{Sample: 2, Timestamp: 1700000002_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "GET"})},
			{Sample: 3, Timestamp: 1700000003_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "GET"})},
			// foo=b
			{Sample: 10, Timestamp: 1700000001_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "GET"})},
			{Sample: 20, Timestamp: 1700000002_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "POST"})},
			{Sample: 30, Timestamp: 1700000003_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "GET"})},

			// Step 2.
			// foo=a
			{Sample: 5, Timestamp: 1700000005_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "POST"})},
			{Sample: 6, Timestamp: 1700000006_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "POST"})},
			{Sample: 7, Timestamp: 1700000007_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "GET"})},
			// foo=b
			{Sample: 50, Timestamp: 1700000005_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "GET"})},
			{Sample: 60, Timestamp: 1700000006_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "GET"})},
			{Sample: 70, Timestamp: 1700000007_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "POST"})},

			// Step 3.
			// foo=a
			{Sample: 10, Timestamp: 1700000009_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "POST"})},
			{Sample: 20, Timestamp: 1700000010_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "GET"})},
			{Sample: 30, Timestamp: 1700000011_000000000, Set: mapLabels(map[string]string{"foo": "a", "method": "GET"})},
			// foo=b
			{Sample: 100, Timestamp: 1700000009_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "GET"})},
			{Sample: 200, Timestamp: 1700000010_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "POST"})},
			{Sample: 300, Timestamp: 1700000011_000000000, Set: mapLabels(map[string]string{"foo": "b", "method": "GET"})},
		}
	)

	type series struct {
		labels map[string]string
		data   []string
	}
	tests := []struct {
		query    string
		expected []series
	}{
		// Range aggregation.
		{
			`count_over_time({} [4s])`,
			[]series{
				{
					map[string]string{
						"foo": "a", "method": "GET",
					},
					[]string{"2", "1", "2"},
				},
				{
					map[string]string{
						"foo": "a", "method": "POST",
					},
					[]string{"1", "2", "1"},
				},
				{
					map[string]string{
						"foo": "b", "method": "GET",
					},
					[]string{"2", "2", "2"},
				},
				{
					map[string]string{
						"foo": "b", "method": "POST",
					},
					[]string{"1", "1", "1"},
				},
			},
		},
		{
			`avg_over_time({} | unwrap _ [4s])`,
			[]series{
				{
					map[string]string{
						"foo": "a", "method": "GET",
					},
					[]string{"2.5", "7", "25"},
				},
				{
					map[string]string{
						"foo": "a", "method": "POST",
					},
					[]string{"1", "5.5", "10"},
				},
				{
					map[string]string{
						"foo": "b", "method": "GET",
					},
					[]string{"20", "55", "200"},
				},
				{
					map[string]string{
						"foo": "b", "method": "POST",
					},
					[]string{"20", "70", "200"},
				},
			},
		},
		{
			`max_over_time({} | unwrap _ [4s]) by (foo)`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "7", "30"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"30", "70", "300"},
				},
			},
		},
		{
			`max_over_time({} | unwrap _ [4s]) without (method)`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "7", "30"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"30", "70", "300"},
				},
			},
		},

		// Vector aggregation.
		{
			`sum by (foo) ( count_over_time({} [4s]) )`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "3", "3"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"3", "3", "3"},
				},
			},
		},
		{
			`sum by (foo) ( count_over_time({} [4s]) ) * 2`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"6", "6", "6"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"6", "6", "6"},
				},
			},
		},
		{
			`sum without (method) ( count_over_time({} [4s]) )`,
			[]series{
				{
					map[string]string{
						"foo": "a",
					},
					[]string{"3", "3", "3"},
				},
				{
					map[string]string{
						"foo": "b",
					},
					[]string{"3", "3", "3"},
				},
			},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data := evaluateQuery(t, testSamples, tt.query, testParams, false)

			v, ok := data.GetMatrixResult()
			require.True(t, ok)

			var got []series
			for _, from := range v.Result {
				to := series{labels: from.Metric.Value}
				for _, p := range from.Values {
					to.data = append(to.data, p.V)
				}
				got = append(got, to)
			}
			slices.SortFunc(got, func(a, b series) int {
				akey := a.labels["foo"] + a.labels["method"]
				bkey := b.labels["foo"] + b.labels["method"]
				return cmp.Compare(akey, bkey)
			})
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestKHeapAggregation(t *testing.T) {
	var (
		testParams = EvalParams{
			Start: time.Unix(1700000000, 0),
			End:   time.Unix(1700000006, 0),
			Step:  6 * time.Second,
		}
		testSamples = []SampledEntry{
			{Sample: 4, Timestamp: 1700000001_000000000, Set: mapLabels(map[string]string{"key": "a", "sample": "1"})},
			{Sample: 5, Timestamp: 1700000002_000000000, Set: mapLabels(map[string]string{"key": "a", "sample": "2"})},
			{Sample: 6, Timestamp: 1700000003_000000000, Set: mapLabels(map[string]string{"key": "a", "sample": "3"})},
			{Sample: 3, Timestamp: 1700000004_000000000, Set: mapLabels(map[string]string{"key": "a", "sample": "4"})},
			{Sample: 2, Timestamp: 1700000005_000000000, Set: mapLabels(map[string]string{"key": "a", "sample": "5"})},
			{Sample: 1, Timestamp: 1700000006_000000000, Set: mapLabels(map[string]string{"key": "a", "sample": "6"})},
		}
	)

	tests := []struct {
		query    string
		expected []string
	}{
		{`topk by (key) (2, sum_over_time({} | unwrap _ [6s]))`, []string{"6", "5"}},
		{`topk by (key) (3, sum_over_time({} | unwrap _ [6s]))`, []string{"6", "5", "4"}},
		{`bottomk by (key) (2, sum_over_time({} | unwrap _ [6s]))`, []string{"1", "2"}},
		{`bottomk by (key) (3, sum_over_time({} | unwrap _ [6s]))`, []string{"1", "2", "3"}},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data := evaluateQuery(t, testSamples, tt.query, testParams, false)

			v, ok := data.GetMatrixResult()
			require.True(t, ok)

			matrix := v.Result
			require.NotEmpty(t, matrix)

			var result []string
			for _, s := range matrix {
				values := s.Values
				require.Len(t, s.Values, 1)
				result = append(result, values[0].V)
			}
			require.ElementsMatch(t, tt.expected, result)
		})
	}
}
