package chstorage

import (
	"context"
	"testing"
	"time"

	singleflight "github.com/go-faster/sdk/singleflightx"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestPromQuerier_Offloading(t *testing.T) {
	hash := [16]byte{1}
	ts := map[[16]byte]labels.Labels{
		hash: labels.FromStrings("__name__", "test_metric"),
	}

	for _, tc := range []struct {
		name                    string
		disableRateOffloading   bool
		disableMetricOffloading bool
		function                string
		samplePoints            bool
		expectRateOffloaded     bool
		expectSampledOffloaded  bool
	}{
		{
			name:                    "default_rate",
			disableRateOffloading:   false,
			disableMetricOffloading: false,
			function:                "rate",
			samplePoints:            true,
			expectRateOffloaded:     true,
			expectSampledOffloaded:  false,
		},
		{
			name:                    "disable_rate",
			disableRateOffloading:   true,
			disableMetricOffloading: false,
			function:                "rate",
			samplePoints:            true,
			expectRateOffloaded:     false,
			expectSampledOffloaded:  false,
		},
		{
			name:                    "disable_metric_rate",
			disableRateOffloading:   false,
			disableMetricOffloading: true,
			function:                "rate",
			samplePoints:            true,
			expectRateOffloaded:     false,
			expectSampledOffloaded:  false,
		},
		{
			name:                    "default_sum_over_time",
			disableRateOffloading:   false,
			disableMetricOffloading: false,
			function:                "sum_over_time",
			samplePoints:            true,
			expectRateOffloaded:     false,
			expectSampledOffloaded:  true,
		},
		{
			name:                    "disable_metric_sum_over_time",
			disableRateOffloading:   false,
			disableMetricOffloading: true,
			function:                "sum_over_time",
			samplePoints:            true,
			expectRateOffloaded:     false,
			expectSampledOffloaded:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rateOffloadedCalled bool
			var sampledOffloadedCalled bool
			var rawPointsCalled bool

			p := &promQuerier{
				tables:                  DefaultTables(),
				disableRateOffloading:   tc.disableRateOffloading,
				disableMetricOffloading: tc.disableMetricOffloading,
				tracer:                  noop.NewTracerProvider().Tracer("test"),
				metricsSg:               new(singleflight.Group[xxh3.Uint128, metricSelectResult]),
				queryTimeseries: func(ctx context.Context, mint, maxt time.Time, matchers [][]*labels.Matcher) (map[[16]byte]labels.Labels, error) {
					return ts, nil
				},
				do: func(ctx context.Context, s selectQuery) error {
					switch s.Type {
					case "QueryRatePoints", "QueryInstantPoints":
						rateOffloadedCalled = true
					case "QuerySampledPoints":
						sampledOffloadedCalled = true
					case "QueryPoints":
						rawPointsCalled = true
					}
					return nil
				},
			}

			p.queryPointsFunc = p.queryPoints
			p.querySampledPointsPerSeriesFunc = p.querySampledPointsPerSeries
			p.queryRatePointsByHashFunc = p.queryRatePointsByHash

			_, err := p.querySeriesSingleflight(
				context.Background(),
				tc.samplePoints,
				metricSelectParams{
					Function: tc.function,
					Start:    time.Unix(60, 0),
					End:      time.Unix(120, 0),
					Step:     time.Minute,
					Range:    5 * time.Minute,
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			if tc.expectRateOffloaded != rateOffloadedCalled {
				t.Errorf("expected rate offloaded called to be %v, got %v", tc.expectRateOffloaded, rateOffloadedCalled)
			}
			if tc.expectSampledOffloaded != sampledOffloadedCalled {
				t.Errorf("expected sampled offloaded called to be %v, got %v", tc.expectSampledOffloaded, sampledOffloadedCalled)
			}
			if !tc.expectRateOffloaded && !tc.expectSampledOffloaded && !rawPointsCalled {
				t.Errorf("expected raw points to be queried when not offloaded")
			}
		})
	}
}
