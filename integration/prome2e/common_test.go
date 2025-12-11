package prome2e_test

import (
	"context"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/integration/prome2e"
	"github.com/go-faster/oteldb/integration/requirex"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promhandler"
)

// MetricsConsumer is metrics consumer.
type MetricsConsumer interface {
	ConsumeMetrics(ctx context.Context, ld pmetric.Metrics) error
}

func readBatchSet(p string) (s prome2e.BatchSet, _ error) {
	f, err := os.Open(p)
	if err != nil {
		return s, err
	}
	defer func() {
		_ = f.Close()
	}()
	return prome2e.ParseBatchSet(f)
}

const exemplarMetric = "prometheus_build_info"

var (
	exemplarSpanID  = pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	exemplarTraceID = pcommon.TraceID{
		1, 2, 3, 4, 5, 6, 7, 8,
		1, 2, 3, 4, 5, 6, 7, 8,
	}
)

func tryGenerateExemplars(batch pmetric.Metrics) {
	var (
		resources = batch.ResourceMetrics()
		point     pmetric.NumberDataPoint
		found     bool
	)
findLoop:
	for resIdx := 0; resIdx < resources.Len(); resIdx++ {
		scopes := resources.At(resIdx).ScopeMetrics()
		if scopes.Len() == 0 {
			continue
		}
		for scopeIdx := 0; scopeIdx < scopes.Len(); scopeIdx++ {
			metrics := scopes.At(scopeIdx).Metrics()
			if metrics.Len() == 0 {
				continue
			}
			for metricIdx := 0; metricIdx < metrics.Len(); metricIdx++ {
				metric := metrics.At(metricIdx)
				if metric.Name() != exemplarMetric {
					continue
				}

				var points pmetric.NumberDataPointSlice
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					points = metric.Gauge().DataPoints()
				case pmetric.MetricTypeSum:
					points = metric.Sum().DataPoints()
				default:
					continue
				}

				if points.Len() != 0 {
					point = points.At(0)
					found = true
					break findLoop
				}
			}
		}
	}
	if !found {
		return
	}

	exemplar := point.Exemplars().AppendEmpty()
	exemplar.SetTimestamp(point.Timestamp())
	exemplar.SetIntValue(10)
	exemplar.SetSpanID(exemplarSpanID)
	exemplar.SetTraceID(exemplarTraceID)
	attrs := exemplar.FilteredAttributes()
	attrs.PutStr("foo", "bar")
	attrs.PutInt("code", 10)
}

func setupDB(
	ctx context.Context,
	t *testing.T,
	provider trace.TracerProvider,
	set prome2e.BatchSet,
	consumer MetricsConsumer,
	querier storage.Queryable,
	exemplarQuerier storage.ExemplarQueryable,
) *promapi.Client {
	if consumer != nil {
		for i, b := range set.Batches {
			tryGenerateExemplars(b)
			if err := consumer.ConsumeMetrics(ctx, b); err != nil {
				t.Fatalf("Send batch %d: %+v", i, err)
			}
		}
	}

	engine := promql.NewEngine(promql.EngineOpts{
		Timeout:              time.Minute,
		MaxSamples:           1_000_000,
		EnableNegativeOffset: true,
	})
	api := promhandler.NewPromAPI(engine, querier, exemplarQuerier, promhandler.PromAPIOptions{})
	promh, err := promapi.NewServer(api,
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	s := httptest.NewServer(promh)
	t.Cleanup(s.Close)

	c, err := promapi.NewClient(s.URL,
		promapi.WithClient(s.Client()),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)
	return c
}

func runTest(
	ctx context.Context,
	t *testing.T,
	provider trace.TracerProvider,
	consumer MetricsConsumer,
	querier storage.Queryable,
	exemplarQuerier storage.ExemplarQueryable,
) {
	set, err := readBatchSet("_testdata/metrics.json")
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Labels)
	c := setupDB(ctx, t, provider, set, consumer, querier, exemplarQuerier)

	t.Run("Labels", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetLabels(ctx, promapi.GetLabelsParams{})
			a.NoError(err)
			a.ElementsMatch(maps.Keys(set.Labels), []string(r.Data))
			requirex.Unique(t, r.Data)
			requirex.Sorted(t, r.Data)

			r2, err := c.PostLabels(ctx, &promapi.LabelsForm{})
			a.NoError(err)
			a.ElementsMatch(maps.Keys(set.Labels), []string(r2.Data))
		})
		for _, tt := range []struct {
			name  string
			match []string
		}{
			{
				"OneMatcher",
				[]string{
					`{handler="/api/v1/series"}`,
				},
			},
			{
				"NameMatcher",
				[]string{
					`prometheus_http_requests_total{}`,
				},
			},
			{
				"RegexMatcher",
				[]string{
					`{handler=~"/api/v1/(series|query)$"}`,
				},
			},
			{
				"MultipleMatchers",
				[]string{
					`{handler="/api/v1/series"}`,
					`{handler="/api/v1/query"}`,
				},
			},
			{
				"UnknownValue",
				[]string{
					`{handler="value_clearly_not_exist"}`,
				},
			},
			{
				"NoMatch",
				[]string{
					`{handler=~".+",clearly="not_exist"}`,
				},
			},
		} {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				a := require.New(t)
				a.NotEmpty(tt.match)

				r, err := c.GetLabels(ctx, promapi.GetLabelsParams{
					Match: tt.match,
				})
				a.NoError(err)

				series, err := set.MatchingSeries(tt.match)
				a.NoError(err)

				labels := map[string]struct{}{}
				for _, set := range series {
					for label := range set {
						labels[label] = struct{}{}
					}
				}
				a.ElementsMatch(maps.Keys(labels), []string(r.Data))
				requirex.Unique(t, r.Data)
				requirex.Sorted(t, r.Data)
			})
		}
	})
	t.Run("LabelValues", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			a := require.New(t)

			for labelName, valueSet := range set.Labels {
				r, err := c.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: labelName})
				a.NoError(err)

				var (
					expected = maps.Keys(valueSet)
					got      = []string(r.Data)
				)
				a.ElementsMatch(expected, got, "check label %q", labelName)
				requirex.Unique(t, got)
				requirex.Sorted(t, got)
			}
		})
		for _, tt := range []struct {
			name    string
			params  promapi.GetLabelValuesParams
			want    []string
			wantErr bool
		}{
			{
				"OneMatcher",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`{handler="/api/v1/series"}`,
					},
				},
				[]string{"/api/v1/series"},
				false,
			},
			{
				"NameMatcher",
				promapi.GetLabelValuesParams{
					Label: "__name__",
					Match: []string{
						`prometheus_http_requests_total{}`,
					},
				},
				[]string{"prometheus_http_requests_total"},
				false,
			},
			{
				"RegexMatcher",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`{handler=~"/api/v1/(series|query)$"}`,
					},
				},
				[]string{"/api/v1/series", "/api/v1/query"},
				false,
			},
			{
				"MultipleMatchers",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`{handler="/api/v1/series"}`,
						`{handler="/api/v1/query"}`,
					},
				},
				[]string{"/api/v1/series", "/api/v1/query"},
				false,
			},
			{
				"AnotherLabel",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`{handler="/api/v1/series",code="200"}`,
					},
				},
				[]string{"/api/v1/series"},
				false,
			},
			{
				"MatchWithName",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`prometheus_http_requests_total{handler="/api/v1/series"}`,
						`prometheus_http_requests_total{handler="/api/v1/query"}`,
					},
				},
				[]string{"/api/v1/series", "/api/v1/query"},
				false,
			},
			{
				"UnknownLabel",
				promapi.GetLabelValuesParams{
					Label: "label_clearly_not_exist",
				},
				nil,
				false,
			},
			{
				"UnknownValue",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`{handler="value_clearly_not_exist"}`,
					},
				},
				nil,
				false,
			},
			{
				"NoMatch",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`{handler=~".+",clearly="not_exist"}`,
					},
				},
				nil,
				false,
			},
			{
				"OutOfRange",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`prometheus_http_requests_total{handler="/api/v1/series"}`,
						`prometheus_http_requests_total{handler="/api/v1/query"}`,
					},
					Start: promapi.NewOptPrometheusTimestamp(getPromTS(10)),
					End:   promapi.NewOptPrometheusTimestamp(getPromTS(20)),
				},
				nil,
				false,
			},
			{
				"InvalidMatcher",
				promapi.GetLabelValuesParams{
					Label: "handler",
					Match: []string{
						`\{\}`,
					},
				},
				nil,
				true,
			},
		} {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				a := require.New(t)

				r, err := c.GetLabelValues(ctx, tt.params)
				if tt.wantErr {
					var gotErr *promapi.FailStatusCode
					a.ErrorAs(err, &gotErr)
					a.Equal("error", gotErr.Response.Status)
					return
				}
				a.NoError(err)

				requirex.Unique(t, r.Data)
				requirex.Sorted(t, r.Data)
				a.ElementsMatch(tt.want, r.Data)
			})
		}
	})
	t.Run("Series", func(t *testing.T) {
		testName := func(name string) func(t *testing.T) {
			return func(t *testing.T) {
				t.Helper()
				a := require.New(t)

				r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
					Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
					End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
					Match: []string{name + "{}"},
				})
				a.NoError(err)

				a.NotEmpty(r.Data)
				for _, labels := range r.Data {
					a.Equal(name, labels["__name__"])
				}
			}
		}
		t.Run("PointByName", testName(`prometheus_http_requests_total`))
		t.Run("HistogramByName", testName(`prometheus_http_request_duration_seconds_count`))
		t.Run("SummaryByName", testName(`go_gc_duration_seconds`))

		// TODO(ernado): support when parser support dots?
		// t.Run("PointByMappedName", testName(`process.runtime.go.gc.count`))

		t.Run("OneMatcher", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`prometheus_http_requests_total{
						code="200",
						handler=~"/api/v1.+",
						handler!="/api/v1/series",
						handler!~"/api/v1/query(_range)?"
					}`,
				},
			})
			a.NoError(err)

			a.NotEmpty(r.Data)
			for _, labels := range r.Data {
				a.Equal("200", labels["code"])

				handler := labels["handler"]
				// Check that handler=~"/api/v1.+" is satisfied.
				a.Contains(handler, "/api/v1")

				// Check that handler!="/api/v1/series" is satisfied.
				a.NotEqual("/api/v1/series", handler)

				// Check that handler!~"/api/v1/query(_range)?" is satisfied.
				a.NotEqual("/api/v1/query", handler)
				a.NotEqual("/api/v1/query_range", handler)
			}
		})
		t.Run("MultipleMatchers", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`prometheus_http_requests_total{
						handler="/api/v1/query"
					}`,
					`prometheus_http_requests_total{
						handler="/api/v1/series"
					}`,
				},
			})
			a.NoError(err)

			a.NotEmpty(r.Data)
			for _, labels := range r.Data {
				a.Contains([]string{
					"/api/v1/query",
					"/api/v1/series",
				}, labels["handler"])
			}
		})
		t.Run("OutOfRange", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1000000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1100000000.0`),
				Match: []string{
					`prometheus_http_requests_total{}`,
				},
			})
			a.NoError(err)
			a.Empty(r.Data)
		})
		t.Run("NoMatch", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`prometheus_http_requests_total{
						clearly="not_exist"
					}`,
				},
			})
			a.NoError(err)
			a.Empty(r.Data)
		})
		t.Run("InvalidTimestamp", func(t *testing.T) {
			a := require.New(t)

			_, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`abcd`),
				Match: []string{
					`prometheus_http_requests_total{}`,
				},
			})
			perr := new(promapi.FailStatusCode)
			a.ErrorAs(err, &perr)
			a.Equal(promapi.FailErrorTypeBadData, perr.Response.ErrorType)
		})
		t.Run("InvalidMatcher", func(t *testing.T) {
			a := require.New(t)

			_, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`\{\}`,
				},
			})
			perr := new(promapi.FailStatusCode)
			a.ErrorAs(err, &perr)
			a.Equal(promapi.FailErrorTypeBadData, perr.Response.ErrorType)
		})
	})
	t.Run("QueryExemplars", func(t *testing.T) {
		a := require.New(t)

		r, err := c.GetQueryExemplars(ctx, promapi.GetQueryExemplarsParams{
			Query: exemplarMetric + `{}`,
			Start: getPromTS(set.Start),
			End:   getPromTS(set.End),
		})
		a.NoError(err)
		a.Len(r.Data, 1)
		set := r.Data[0]

		a.Equal(exemplarMetric, set.SeriesLabels.Value["__name__"])
		for _, e := range set.Exemplars {
			a.Equal(promapi.LabelSet{
				"code":     "10",
				"foo":      "bar",
				"span_id":  exemplarSpanID.String(),
				"trace_id": exemplarTraceID.String(),
			}, e.Labels)
			a.Equal(10.0, e.Value)
		}
	})
	t.Run("QueryRange", func(t *testing.T) {
		t.Run("Points", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetQueryRange(ctx, promapi.GetQueryRangeParams{
				Query: `count(prometheus_http_requests_total{})`,
				Start: getPromTS(set.Start),
				End:   getPromTS(set.End),
				Step:  "5s",
			})
			a.NoError(err)

			data := r.Data
			a.Equal(promapi.MatrixData, data.Type)

			mat := data.Matrix.Result
			a.Len(mat, 1)
			values := mat[0].Values
			a.NotEmpty(values)

			for _, point := range values {
				a.Equal(float64(51), point.V)
			}
		})
		t.Run("Histogram", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetQueryRange(ctx, promapi.GetQueryRangeParams{
				Query: `prometheus_http_response_size_bytes_bucket{handler="/api/v1/write", le="+Inf"}`,
				Start: getPromTS(set.Start),
				End:   getPromTS(set.End),
				Step:  "5s",
			})
			a.NoError(err)

			data := r.Data
			a.Equal(promapi.MatrixData, data.Type)

			mat := data.Matrix.Result
			a.Len(mat, 1)

			a.Equal(mat[0].Metric["le"], "+Inf")
			values := mat[0].Values
			a.NotEmpty(values)
		})
	})
}

func getPromTS(ts pcommon.Timestamp) promapi.PrometheusTimestamp {
	v := strconv.FormatInt(ts.AsTime().Unix(), 10)
	return promapi.PrometheusTimestamp(v)
}
