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
		// A single separator still carries a type: these used to be classified as gauges purely
		// because the name had one component fewer than the same metric namespaced.
		{"requests_total", "unit= kind=1 temporality=2 monotonic=true"},
		{"latency_seconds", "unit=seconds kind=0 temporality=0 monotonic=false"},
		{"seconds_total", "unit=seconds kind=1 temporality=2 monotonic=true"},
		// No separator at all: `total` is the whole name, not a suffix.
		{"total", "unit= kind=0 temporality=0 monotonic=false"},
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
	got, rej := conv.Convert([]prompb.TimeSeries{
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
	require.Equal(t, 2, rej.Old)
	require.Equal(t, 2, rej.Total())

	dumped := dump(got)
	require.NotContains(t, dumped, "metric old ")
	require.Contains(t, dumped, "metric mixed ")
	require.Contains(t, dumped, "value=2 {}")
}

// TestSkipsInvalidSeries asserts a series whose labels cannot be stored is skipped and counted
// while the rest of the batch is still ingested. Failing the request instead would cost a sender
// every other series in it, and since it would retry the same bytes, wedge its queue for good.
func TestSkipsInvalidSeries(t *testing.T) {
	sample := []prompb.Sample{{Timestamp: 1000, Value: 1}}

	for _, tt := range []struct {
		name   string
		labels []prompb.Label
	}{
		{"NoName", []prompb.Label{label("job", "api")}},
		{"EmptyName", []prompb.Label{label("__name__", "")}},
		{"EmptyLabelName", []prompb.Label{label("__name__", "m"), label("", "v")}},
		{"DuplicateName", []prompb.Label{label("__name__", "a"), label("__name__", "b")}},
		{"DuplicateLabel", []prompb.Label{
			label("__name__", "m"), label("job", "a"), label("job", "b"),
		}},
		{"InvalidUTF8Name", []prompb.Label{
			label("__name__", "m"), {Name: []byte{0xff}, Value: []byte("v")},
		}},
		{"InvalidUTF8Value", []prompb.Label{
			label("__name__", "m"), {Name: []byte("k"), Value: []byte{0xff}},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var conv promrw.Converter
			got, rej := conv.Convert([]prompb.TimeSeries{
				{Labels: tt.labels, Samples: sample},
				{Labels: []prompb.Label{label("__name__", "good")}, Samples: sample},
			}, promrw.Options{TimeThreshold: wideThreshold})

			require.Equal(t, 1, rej.Invalid)
			require.Equal(t, 1, rej.Total(), "nothing else was rejected")

			dumped := dump(got)
			require.Contains(t, dumped, "metric good ", "the valid series is still ingested")
		})
	}
}

// TestAttributesAliasRequest asserts the batch references the request's bytes instead of copying
// them, which is what lets the ingest path stay allocation-free.
func TestAttributesAliasRequest(t *testing.T) {
	labels := []prompb.Label{label("__name__", "m"), label("job", "api")}

	var conv promrw.Converter
	got, _ := conv.Convert([]prompb.TimeSeries{
		{Labels: labels, Samples: []prompb.Sample{{Timestamp: 1000, Value: 1}}},
	}, promrw.Options{TimeThreshold: wideThreshold})

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
