package scarecrow_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func histogramBenchData(groups, steps int, gappy bool) string {
	var data strings.Builder
	data.WriteString("load 30s\n")
	for group := range groups {
		for bucket := range 12 {
			bound := strconv.Itoa(bucket + 1)
			if bucket == 11 {
				bound = "+Inf"
			}
			fmt.Fprintf(&data, " h_bucket{job=\"%d\",le=\"%s\"}", group, bound)
			for step := range steps {
				switch {
				case !gappy || step%8 == 0:
					fmt.Fprintf(&data, " %d", bucket+1)
				case step%8 == 1:
					data.WriteString(" stale")
				default:
					data.WriteString(" _")
				}
			}
			data.WriteByte('\n')
		}
	}
	return data.String()
}

func BenchmarkClassicHistogramQuantile(b *testing.B) {
	const steps = 241
	for _, layout := range []string{"dense", "gappy"} {
		store := promqltest.LoadedStorage(b, histogramBenchData(100, steps, layout == "gappy"))
		b.Cleanup(func() {
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
		})
		for _, engineName := range []string{"prometheus", "scarecrow"} {
			var engine promql.QueryEngine = upstreamEngine()
			if engineName == "scarecrow" {
				engine = scarecrow.NewEngine(scarecrow.Opts{})
			}
			b.Run("layout="+layout+"/engine="+engineName, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					query, err := engine.NewRangeQuery(context.Background(), store, nil, "histogram_quantile(0.9, h_bucket)", time.Unix(0, 0), time.Unix((steps-1)*30, 0), 30*time.Second)
					if err != nil {
						b.Fatal(err)
					}
					result := query.Exec(context.Background())
					if result.Err != nil {
						b.Fatal(result.Err)
					}
					if len(result.Value.(promql.Matrix)) != 100 {
						b.Fatal("missing histograms")
					}
					query.Close()
				}
			})
		}
	}
}

type histogramSupersetScanner struct {
	scarecrow.Scanner
	extra []labels.Labels
}

func (s histogramSupersetScanner) Series(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]labels.Labels, error) {
	series, err := s.Scanner.Series(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}
	return slices.Concat(series, s.extra), nil
}

func BenchmarkClassicHistogramSparseSchema(b *testing.B) {
	store := promqltest.LoadedStorage(b, histogramBenchData(1, 241, true))
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
	})
	for _, inactive := range []int{0, 10_000} {
		extra := make([]labels.Labels, inactive)
		for i := range extra {
			extra[i] = labels.FromStrings("__name__", "h_bucket", "job", "inactive"+strconv.Itoa(i), "le", "+Inf")
		}
		engine := scarecrow.NewEngine(scarecrow.Opts{NewScanner: func(q storage.Queryable) scarecrow.Scanner {
			return histogramSupersetScanner{Scanner: scarecrow.NewQueryableScanner(q), extra: extra}
		}})
		b.Run(fmt.Sprintf("inactive=%d", inactive), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				query, err := engine.NewRangeQuery(context.Background(), store, nil, "histogram_quantile(0.9, h_bucket)", time.Unix(0, 0), time.Unix(7200, 0), 30*time.Second)
				if err != nil {
					b.Fatal(err)
				}
				result := query.Exec(context.Background())
				if result.Err != nil {
					b.Fatal(result.Err)
				}
				if len(result.Value.(promql.Matrix)) != 1 {
					b.Fatal("missing histogram")
				}
				query.Close()
			}
		})
	}
}
