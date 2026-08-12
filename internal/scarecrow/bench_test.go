package scarecrow_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// benchCorpus builds a load statement with the given series count, enough to make per-series
// work visible above fixed query overhead.
func benchCorpus(series int) string {
	var b strings.Builder

	b.WriteString("load 15s\n")

	for i := range series {
		fmt.Fprintf(&b, "  counter{instance=\"i%d\", job=\"j%d\"} 0+%dx40\n", i, i%8, i%5+1)
		fmt.Fprintf(&b, "  gauge{instance=\"i%d\", job=\"j%d\"} %d+1x40\n", i, i%8, i)
	}

	return b.String()
}

func benchQueries() []string {
	return []string{
		`counter`,
		`rate(counter[1m])`,
		`sum by (job) (rate(counter[1m]))`,
		`counter + gauge`,
		`sum by (job) (rate(counter[1m])) / sum by (job) (gauge)`,
		`avg_over_time(gauge[2m:30s])`,
	}
}

func BenchmarkRangeQuery(b *testing.B) {
	for _, series := range []int{10, 200} {
		st := promqltest.LoadedStorage(b, benchCorpus(series))
		b.Cleanup(func() { require.NoError(b, st.Close()) })

		e := scarecrow.NewEngine(scarecrow.Opts{})

		start, end := time.Unix(0, 0), time.Unix(600, 0)

		for _, qs := range benchQueries() {
			b.Run(fmt.Sprintf("series=%d/%s", series, qs), func(b *testing.B) {
				ctx := context.Background()

				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					q, err := e.NewRangeQuery(ctx, st, nil, qs, start, end, 15*time.Second)
					if err != nil {
						b.Fatal(err)
					}

					res := q.Exec(ctx)
					if res.Err != nil {
						b.Fatal(res.Err)
					}

					q.Close()
				}
			})
		}
	}
}
