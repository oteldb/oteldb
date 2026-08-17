package promrw_test

import (
	"net/http"
	"testing"
	"time"

	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/prompb"
	"github.com/oteldb/oteldb/internal/promrw"
)

// symbolizer builds a 2.0 request the way a sender does: strings are interned into one table whose
// first element is empty, since an unset ref decodes as 0.
type symbolizer struct {
	symbols []string
	index   map[string]uint32
}

func newSymbolizer() *symbolizer {
	return &symbolizer{symbols: []string{""}, index: map[string]uint32{"": 0}}
}

func (s *symbolizer) ref(v string) uint32 {
	if ref, ok := s.index[v]; ok {
		return ref
	}

	ref := uint32(len(s.symbols))
	s.symbols = append(s.symbols, v)
	s.index[v] = ref

	return ref
}

// refs interns label pairs and returns their flat ref list, sorted by name as a sender sends them.
func (s *symbolizer) refs(pairs ...string) []uint32 {
	out := make([]uint32, 0, len(pairs))
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, s.ref(pairs[i]), s.ref(pairs[i+1]))
	}

	return out
}

// convertV2 runs one 2.0 request through the converter.
func convertV2(t *testing.T, sym *symbolizer, tss ...writev2.TimeSeries) (_ string, _ promrw.Counts) {
	t.Helper()

	native := writev2.Request{Symbols: sym.symbols, Timeseries: tss}
	data, err := native.Marshal()
	require.NoError(t, err)

	var req prompb.WriteRequestV2
	require.NoError(t, req.Unmarshal(data))

	var conv promrw.Converter
	got, counts := conv.ConvertV2(&req, promrw.Options{
		TimeThreshold: wideThreshold,
		Now:           time.Unix(0, 0).Add(wideThreshold / 2),
	})

	return dump(got), counts
}

// TestConvertV2MetadataType asserts the declared type decides the metric's kind where it describes
// one series, and defers to the name suffix where it describes a family. A histogram's `_bucket`,
// `_sum` and `_count` are separate series here, and only the suffix says which one arrived — so
// those land on the identity a 1.0 sender would produce for the same series.
func TestConvertV2MetadataType(t *testing.T) {
	for _, tt := range []struct {
		name    string
		metric  string
		typ     writev2.Metadata_MetricType
		want    string
		comment string
	}{
		{
			name: "Counter", metric: "requests",
			typ:  writev2.Metadata_METRIC_TYPE_COUNTER,
			want: "unit= kind=1 temporality=2 monotonic=true",
		},
		{
			name: "Gauge", metric: "queue_depth",
			typ:  writev2.Metadata_METRIC_TYPE_GAUGE,
			want: "unit= kind=0 temporality=0 monotonic=false",
		},
		{
			// A counter name the suffix would not have caught, so the declared type is doing the work.
			name: "CounterWithoutSuffix", metric: "cache_hits",
			typ:  writev2.Metadata_METRIC_TYPE_COUNTER,
			want: "unit= kind=1 temporality=2 monotonic=true",
		},
		{
			// A gauge named like a counter: the declared type must win over the suffix.
			name: "GaugeNamedTotal", metric: "queue_total",
			typ:  writev2.Metadata_METRIC_TYPE_GAUGE,
			want: "unit= kind=0 temporality=0 monotonic=false",
		},
		{
			// Info and stateset always report 1, so they accumulate without counting up.
			name: "Info", metric: "build_info",
			typ:  writev2.Metadata_METRIC_TYPE_INFO,
			want: "unit= kind=1 temporality=2 monotonic=false",
		},
		{
			name: "StateSet", metric: "feature_enabled",
			typ:  writev2.Metadata_METRIC_TYPE_STATESET,
			want: "unit= kind=1 temporality=2 monotonic=false",
		},
		{
			// A histogram family member: the suffix says which series this is.
			name: "HistogramCount", metric: "http_duration_seconds_count",
			typ:  writev2.Metadata_METRIC_TYPE_HISTOGRAM,
			want: "unit=seconds kind=1 temporality=2 monotonic=true",
		},
		{
			// No declared type at all, which is what a sender without metadata sends: the suffix is
			// all there is, exactly as over 1.0.
			name: "Unspecified", metric: "errors_total",
			typ:  writev2.Metadata_METRIC_TYPE_UNSPECIFIED,
			want: "unit= kind=1 temporality=2 monotonic=true",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sym := newSymbolizer()
			got, counts := convertV2(t, sym, writev2.TimeSeries{
				LabelsRefs: sym.refs("__name__", tt.metric),
				Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
				Metadata:   writev2.Metadata{Type: tt.typ},
			})

			require.Contains(t, got, "metric "+tt.metric+" "+tt.want)
			require.Equal(t, 1, counts.Samples)
			require.Zero(t, counts.Rejected.Total())
		})
	}
}

// TestConvertV2Unit asserts the declared unit is used when the sender gives one, and the name
// suffix otherwise. The declared unit is a Prometheus unit word, the same vocabulary the suffix
// inference produces, so a series sent over either protocol lands on one identity.
func TestConvertV2Unit(t *testing.T) {
	t.Run("Declared", func(t *testing.T) {
		sym := newSymbolizer()
		unit := sym.ref("bytes")

		got, _ := convertV2(t, sym, writev2.TimeSeries{
			LabelsRefs: sym.refs("__name__", "heap_used"),
			Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
			Metadata: writev2.Metadata{
				Type:    writev2.Metadata_METRIC_TYPE_GAUGE,
				UnitRef: unit,
			},
		})

		require.Contains(t, got, "metric heap_used unit=bytes")
	})
	t.Run("FromSuffix", func(t *testing.T) {
		sym := newSymbolizer()

		got, _ := convertV2(t, sym, writev2.TimeSeries{
			LabelsRefs: sym.refs("__name__", "heap_used_bytes"),
			Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
			Metadata:   writev2.Metadata{Type: writev2.Metadata_METRIC_TYPE_GAUGE},
		})

		require.Contains(t, got, "metric heap_used_bytes unit=bytes")
	})
}

// TestConvertV2StartTimestamp asserts the start timestamp is carried through. It is the time the
// counter began counting, which 1.0 had no way to express, and which lets a rate over the first
// point of a series be computed instead of skipped.
func TestConvertV2StartTimestamp(t *testing.T) {
	sym := newSymbolizer()

	got, _ := convertV2(t, sym, writev2.TimeSeries{
		LabelsRefs: sym.refs("__name__", "requests_total"),
		Samples:    []writev2.Sample{{Value: 5, Timestamp: 2000, StartTimestamp: 1000}},
		Metadata:   writev2.Metadata{Type: writev2.Metadata_METRIC_TYPE_COUNTER},
	})

	require.Contains(t, got, "point start=1000000000 ts=2000000000 value=5")
}

// TestConvertV2HistogramStartTimestamp asserts a native histogram's start timestamp reaches every
// series it decomposes into.
func TestConvertV2HistogramStartTimestamp(t *testing.T) {
	sym := newSymbolizer()

	got, counts := convertV2(t, sym, writev2.TimeSeries{
		LabelsRefs: sym.refs("__name__", "h"),
		Histograms: []writev2.Histogram{{
			Count:          &writev2.Histogram_CountInt{CountInt: 3},
			Sum:            6,
			Schema:         0,
			PositiveSpans:  []writev2.BucketSpan{{Offset: 0, Length: 1}},
			PositiveDeltas: []int64{3},
			Timestamp:      2000,
			StartTimestamp: 1000,
		}},
		Metadata: writev2.Metadata{Type: writev2.Metadata_METRIC_TYPE_HISTOGRAM},
	})

	require.Contains(t, got, "metric h_count")
	require.NotContains(t, got, "start=0 ", "every decomposed series carries the start timestamp")
	require.Equal(t, 1, counts.Histograms, "one histogram was sent, however many series it became")
	require.Zero(t, counts.Samples)
}

// TestConvertV2Symbols asserts labels are resolved through the symbol table, and that a request
// whose refs the table cannot satisfy is skipped rather than read out of bounds.
func TestConvertV2Symbols(t *testing.T) {
	t.Run("Resolved", func(t *testing.T) {
		sym := newSymbolizer()

		got, _ := convertV2(t, sym, writev2.TimeSeries{
			LabelsRefs: sym.refs("__name__", "m", "job", "api", "zone", "b"),
			Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
		})

		require.Contains(t, got, "{job=api,zone=b}")
	})
	t.Run("RefPastTable", func(t *testing.T) {
		sym := newSymbolizer()
		refs := sym.refs("__name__", "m")

		_, counts := convertV2(t, sym, writev2.TimeSeries{
			LabelsRefs: append(refs, 999, 999),
			Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
		})

		require.Equal(t, 1, counts.Rejected.Invalid)
		require.Zero(t, counts.Samples)
	})
	t.Run("OddRefs", func(t *testing.T) {
		sym := newSymbolizer()

		_, counts := convertV2(t, sym, writev2.TimeSeries{
			LabelsRefs: []uint32{sym.ref("__name__")},
			Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
		})

		require.Equal(t, 1, counts.Rejected.Invalid)
	})
	t.Run("UnitRefPastTable", func(t *testing.T) {
		sym := newSymbolizer()

		_, counts := convertV2(t, sym, writev2.TimeSeries{
			LabelsRefs: sym.refs("__name__", "m"),
			Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
			Metadata:   writev2.Metadata{UnitRef: 999},
		})

		require.Equal(t, 1, counts.Rejected.Invalid)
	})
}

// TestHandlerV2 drives a 2.0 request through the handler and asserts the written-stats headers a
// 2.0 sender reads to decide whether a partial write needs resending.
func TestHandlerV2(t *testing.T) {
	sym := newSymbolizer()
	native := writev2.Request{
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: sym.refs("__name__", "requests_total", "job", "api"),
				Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}, {Value: 2, Timestamp: 2000}},
				Metadata:   writev2.Metadata{Type: writev2.Metadata_METRIC_TYPE_COUNTER},
			},
			{
				LabelsRefs: sym.refs("__name__", "h"),
				Histograms: []writev2.Histogram{{
					Count:          &writev2.Histogram_CountInt{CountInt: 3},
					PositiveSpans:  []writev2.BucketSpan{{Offset: 0, Length: 1}},
					PositiveDeltas: []int64{3},
					Timestamp:      1000,
				}},
			},
		},
	}
	native.Symbols = sym.symbols

	raw, err := native.Marshal()
	require.NoError(t, err)

	s := &sink{}
	h := promrw.NewHandler(s, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: wideThreshold},
	})

	// Twice, so the second request runs on recycled buffers.
	for range 2 {
		rec := postRaw(t, h, snappyEncode(raw), promrw.MessageV2.ContentType())
		require.Equal(t, http.StatusNoContent, rec.Code, rec.Body)

		require.Equal(t, "2", rec.Header().Get("X-Prometheus-Remote-Write-Samples-Written"))
		require.Equal(t, "1", rec.Header().Get("X-Prometheus-Remote-Write-Histograms-Written"))
		require.Equal(t, "0", rec.Header().Get("X-Prometheus-Remote-Write-Exemplars-Written"))
	}

	require.Equal(t, s.dumps[0], s.dumps[1])
	require.Contains(t, s.last(), "metric requests_total unit= kind=1 temporality=2 monotonic=true")
	require.Contains(t, s.last(), "metric h_bucket")
}

// TestHandlerV1SetsNoWrittenHeaders asserts a 1.0 response carries none of the written-stats
// headers. Their absence is how a sender tells a 1.0 receiver from a 2.0 one, so claiming them
// would promise a guarantee 1.0 does not make.
func TestHandlerV1SetsNoWrittenHeaders(t *testing.T) {
	h := promrw.NewHandler(&sink{}, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: wideThreshold},
	})

	rec := post(t, h, readCorpus(t))
	require.Equal(t, http.StatusNoContent, rec.Code)

	for _, header := range []string{
		"X-Prometheus-Remote-Write-Samples-Written",
		"X-Prometheus-Remote-Write-Histograms-Written",
		"X-Prometheus-Remote-Write-Exemplars-Written",
	} {
		require.Empty(t, rec.Header().Get(header), header)
	}
}

// TestHandlerV2WriteFailureReportsNothingWritten asserts a failed write claims no writes: the
// sender has to resend all of it, and a non-zero count would tell it not to.
func TestHandlerV2WriteFailureReportsNothingWritten(t *testing.T) {
	sym := newSymbolizer()
	native := writev2.Request{
		Timeseries: []writev2.TimeSeries{{
			LabelsRefs: sym.refs("__name__", "m"),
			Samples:    []writev2.Sample{{Value: 1, Timestamp: 1000}},
		}},
	}
	native.Symbols = sym.symbols

	raw, err := native.Marshal()
	require.NoError(t, err)

	h := promrw.NewHandler(&sink{failWr: errStorageDown}, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: wideThreshold},
	})

	rec := postRaw(t, h, snappyEncode(raw), promrw.MessageV2.ContentType())
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, "0", rec.Header().Get("X-Prometheus-Remote-Write-Samples-Written"))
}

func FuzzHandlerV2(f *testing.F) {
	for _, req := range []writev2.Request{{}, {Symbols: []string{""}}} {
		data, err := req.Marshal()
		require.NoError(f, err)

		f.Add(snappyEncode(data))
	}
	f.Add([]byte{})
	f.Add([]byte("not snappy"))

	h := promrw.NewHandler(nopSink{}, promrw.HandlerConfig{MaxBodyBytes: 1 << 20})

	f.Fuzz(func(t *testing.T, body []byte) {
		rec := postRaw(t, h, body, promrw.MessageV2.ContentType())

		switch code := rec.Code; code {
		case http.StatusNoContent, http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		default:
			t.Fatalf("unexpected code %d", code)
		}
	})
}
