package promrw_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/promrw"
)

func label(name, value string) prompb.Label {
	return prompb.Label{Name: []byte(name), Value: []byte(value)}
}

// TestClassify asserts unit and counter-ness are inferred from the name suffixes, the only type
// information remote write carries.
func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name string
		want string
	}{
		{"go_goroutines", "unit= kind=0 temporality=0 monotonic=false"},
		{"http_requests_total", "unit= kind=1 temporality=2 monotonic=true"},
		{"http_duration_seconds_sum", "unit=seconds kind=1 temporality=2 monotonic=true"},
		{"http_duration_seconds_count", "unit=seconds kind=1 temporality=2 monotonic=true"},
		{"http_duration_seconds_max", "unit=seconds kind=0 temporality=0 monotonic=false"},
		{"process_heap_bytes", "unit=bytes kind=0 temporality=0 monotonic=false"},
		{"one_two", "unit= kind=0 temporality=0 monotonic=false"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, dropped := convertOne(t, prompb.TimeSeries{
				Labels:  []prompb.Label{label("__name__", tt.name)},
				Samples: []prompb.Sample{{Timestamp: 1000, Value: 1}},
			})

			require.Contains(t, got, "metric "+tt.name+" "+tt.want)
			require.Zero(t, dropped)
		})
	}
}

// TestDropsOldPoints asserts points older than the threshold are dropped and counted, and that a
// series left with none contributes no metric at all.
func TestDropsOldPoints(t *testing.T) {
	now := time.Unix(3600, 0)

	var conv promrw.Converter
	got, dropped, err := conv.Convert([]prompb.TimeSeries{
		{
			Labels: []prompb.Label{label("__name__", "old")},
			Samples: []prompb.Sample{
				{Timestamp: now.Add(-2 * time.Minute).UnixMilli(), Value: 1},
			},
		},
		{
			Labels: []prompb.Label{label("__name__", "mixed")},
			Samples: []prompb.Sample{
				{Timestamp: now.Add(-2 * time.Minute).UnixMilli(), Value: 1},
				{Timestamp: now.Add(-30 * time.Second).UnixMilli(), Value: 2},
			},
		},
	}, promrw.Options{TimeThreshold: time.Minute, Now: now})
	require.NoError(t, err)
	require.Equal(t, 2, dropped)

	dumped := dump(got)
	require.NotContains(t, dumped, "metric old ")
	require.Contains(t, dumped, "metric mixed ")
	require.Contains(t, dumped, "value=2 {}")
}

// TestMissingName asserts a nameless series fails the whole request, as the pdata translator did:
// remote write has no way to store it.
func TestMissingName(t *testing.T) {
	var conv promrw.Converter
	_, _, err := conv.Convert([]prompb.TimeSeries{
		{
			Labels:  []prompb.Label{label("job", "api")},
			Samples: []prompb.Sample{{Timestamp: 1000, Value: 1}},
		},
	}, promrw.Options{TimeThreshold: wideThreshold})

	require.ErrorIs(t, err, promrw.ErrNoName)
}

// TestAttributesAliasRequest asserts the batch references the request's bytes instead of copying
// them, which is what lets the ingest path stay allocation-free.
func TestAttributesAliasRequest(t *testing.T) {
	labels := []prompb.Label{label("__name__", "m"), label("job", "api")}

	var conv promrw.Converter
	got, _, err := conv.Convert([]prompb.TimeSeries{
		{Labels: labels, Samples: []prompb.Sample{{Timestamp: 1000, Value: 1}}},
	}, promrw.Options{TimeThreshold: wideThreshold})
	require.NoError(t, err)

	mt := got.Resources[0].Scopes[0].Metrics[0]
	require.Same(t, unsafeFirst(labels[0].Value), unsafeFirst(mt.Name))
	require.Same(t, unsafeFirst(labels[1].Value), unsafeFirst(mt.Points[0].Attributes[0].Value.Str()))
}

func unsafeFirst(b []byte) *byte { return &b[0] }

// TestAttributesSorted asserts attributes reach the engine sorted by key, which the series hash
// assumes.
func TestAttributesSorted(t *testing.T) {
	got, _ := convertOne(t, prompb.TimeSeries{
		Labels: []prompb.Label{
			label("zone", "b"),
			label("__name__", "m"),
			label("app", "a"),
		},
		Samples: []prompb.Sample{{Timestamp: 1000, Value: 1}},
	})

	require.Contains(t, got, "{app=a,zone=b}")
}
