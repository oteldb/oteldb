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

func TestDecodeUnicodeLabel(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no_prefix",
			input: "normal_label",
			want:  "normal_label",
		},
		{
			name:  "decode_dot",
			input: "U__k8s_2e_node_2e_name",
			want:  "k8s.node.name",
		},
		{
			name:  "decode_dash",
			input: "U__my_2d_label",
			want:  "my-label",
		},
		{
			name:  "decode_slash",
			input: "U__path_2f_to_2f_resource",
			want:  "path/to/resource",
		},
		{
			name:  "mixed_encodings",
			input: "U__k8s_2e_io_2f_app_2d_name",
			want:  "k8s.io/app-name",
		},
		{
			name:  "underscore_not_encoded",
			input: "U__some_label_name",
			want:  "some_label_name",
		},
		{
			name:  "partial_encoding",
			input: "U__test_2x_value",
			want:  "test_2x_value",
		},
		{
			name:  "empty_after_prefix",
			input: "U__",
			want:  "",
		},
		{
			name:  "only_dots",
			input: "U___2e__2e__2e_",
			want:  "...",
		},
		{
			name:  "trailing_underscore_number",
			input: "U__label_2",
			want:  "label_2",
		},
		{
			name:  "incomplete_encoding_at_end",
			input: "U__label_2e",
			want:  "label_2e",
		},
		{
			name:  "decode_colon",
			input: "U__service_3a_name",
			want:  "service:name",
		},
		{
			name:  "decode_space",
			input: "U__hello_20_world",
			want:  "hello world",
		},
		{
			name:  "decode_at_sign",
			input: "U__user_40_domain",
			want:  "user@domain",
		},
		{
			name:  "decode_uppercase_hex",
			input: "U__test_2E_value",
			want:  "test.value",
		},
		{
			name:  "invalid_hex_single_digit",
			input: "U__test_2_value",
			want:  "test_2_value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodeUnicodeLabel(tt.input)
			if got != tt.want {
				t.Errorf("DecodeUnicodeLabel(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

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
