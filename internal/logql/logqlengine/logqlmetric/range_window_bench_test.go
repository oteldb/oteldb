package logqlmetric

import (
	"strconv"
	"testing"
	"time"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func BenchmarkLogRangeWindow(b *testing.B) {
	start := time.Unix(1_700_000_000, 0)
	const count = 40_000
	const span = 10 * time.Minute
	sets := make([]logqlabels.AggregatedLabels, 4)
	for i := range sets {
		sets[i] = logqlabels.AggregatedLabelsFromMap(map[string]string{"job": strconv.Itoa(i)})
	}
	samples := make([]SampledEntry, count)
	for i := range samples {
		samples[i] = SampledEntry{
			Timestamp: otelstorage.NewTimestampFromTime(start.Add(time.Duration(i) * (span / count))),
			Sample:    float64(i % 128),
			Set:       sets[i%len(sets)],
		}
	}
	for _, op := range []string{"count_over_time", "bytes_over_time"} {
		for _, window := range []string{"1s", "5m"} {
			b.Run(op+"/window="+window, func(b *testing.B) {
				expression, err := logql.Parse(op+"({}["+window+"])", logql.ParseOptions{})
				if err != nil {
					b.Fatal(err)
				}
				expr := expression.(*logql.RangeAggregationExpr)
				b.ReportAllocs()
				b.SetBytes(count * 16)
				for b.Loop() {
					it, err := RangeAggregation(iterators.Slice(samples), expr, start, start.Add(span), time.Second)
					if err != nil {
						b.Fatal(err)
					}
					steps := 0
					var step Step
					for it.Next(&step) {
						if len(step.Samples) > 0 {
							steps++
						}
					}
					if err := it.Err(); err != nil {
						b.Fatal(err)
					}
					if err := it.Close(); err != nil {
						b.Fatal(err)
					}
					if steps < 500 {
						b.Fatal("missing window samples")
					}
				}
			})
		}
	}
}
