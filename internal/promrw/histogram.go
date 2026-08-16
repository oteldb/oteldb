package promrw

import (
	"math"
	"slices"
	"strconv"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/prompb"
)

// The engine stores only float series, so a native histogram is ingested by classic
// decomposition: `<name>_count`, `<name>_sum` and cumulative `<name>_bucket{le=…}` series, the
// same shape a Prometheus classic histogram is scraped as, so histogram_quantile works over it
// unchanged.
//
// Bounds come from the sparse bucket indices: for schema s the base is 2^(2^-s) and bucket index
// i covers (base^(i-1), base^i], so its `le` is base^i. Negative buckets mirror that around zero
// and the zero bucket's `le` is the zero threshold.
//
// The bound is computed as 2^(i·2^-s) rather than from Prometheus' per-schema table of exact
// fractional bounds, so a bound can land one ulp off the value Prometheus prints (√2 renders as
// 1.414213562373095, not 1.4142135623730951). Since `le` is a label, that is a different series
// than the same bucket scraped classically — it is consistent within this ingest path, but a
// histogram fed in both ways would not merge.

var leLabel = []byte("le")

// posInf is the `le` value of the catch-all overflow bucket, which carries the total count.
const posInf = "+Inf"

// appendHistograms decomposes the series' in-window native histograms, returning how many were
// dropped as out of window.
func (c *Converter) appendHistograms(
	sm *metric.ScopeMetrics,
	ts *prompb.TimeSeries,
	name []byte,
	attrs signal.Attributes,
	cutoff int64,
) (dropped int) {
	for i := range ts.Histograms {
		h := &ts.Histograms[i]

		tsNano := msToNano(h.Timestamp)
		if tsNano < cutoff {
			dropped++
			continue
		}
		c.appendHistogram(sm, h, name, attrs, tsNano)
	}
	return dropped
}

func (c *Converter) appendHistogram(
	sm *metric.ScopeMetrics,
	h *prompb.Histogram,
	name []byte,
	attrs signal.Attributes,
	tsNano int64,
) {
	count := histogramCount(h)

	c.addCounter(sm, c.suffixed(name, "_count"), attrs, tsNano, count, true)
	c.addCounter(sm, c.suffixed(name, "_sum"), attrs, tsNano, h.Sum, false)

	bucketName := c.suffixed(name, "_bucket")

	var cum float64
	emit := func(le float64) {
		c.addCounter(sm, bucketName, c.withLabel(attrs, leLabel, c.formatBound(le)), tsNano, cum, true)
	}

	// Negative buckets hold observations below zero: index i covers [-base^i, -base^(i-1)), so its
	// upper bound is -base^(i-1) and descending indices give ascending bounds.
	neg := collectBuckets(h.NegativeSpans, h.NegativeDeltas, h.NegativeCounts)
	for _, b := range slices.Backward(neg) {
		cum += b.count
		emit(-bound(b.index-1, h.Schema))
	}

	if zero := zeroCount(h); zero > 0 {
		cum += zero
		emit(h.ZeroThreshold)
	}

	for _, b := range collectBuckets(h.PositiveSpans, h.PositiveDeltas, h.PositiveCounts) {
		cum += b.count
		emit(bound(b.index, h.Schema))
	}

	// The overflow bucket carries the reported total, which also covers observations no bucket
	// counted (NaN, and anything lost to a truncated bucket set).
	c.addCounter(sm, bucketName, c.withLabel(attrs, leLabel, []byte(posInf)), tsNano, count, true)
}

// addCounter appends a one-point cumulative sum series.
func (c *Converter) addCounter(
	sm *metric.ScopeMetrics,
	name []byte,
	attrs signal.Attributes,
	tsNano int64,
	value float64,
	monotonic bool,
) {
	mt := sm.AddMetric()
	mt.Name = name
	mt.Kind = metric.KindSum
	mt.Temporality = metric.TemporalityCumulative
	mt.Monotonic = monotonic

	p := mt.AddPoint()
	p.Attributes = attrs
	p.Ts = tsNano
	p.Value = value
}

// bucket is one populated sparse bucket: its index and its absolute count.
type bucket struct {
	index int32
	count float64
}

// collectBuckets expands the span-encoded buckets of one histogram side into absolute-count
// buckets in ascending index order. Integer histograms carry delta-encoded counts in deltas,
// float histograms absolute ones in counts; a histogram uses one or the other.
func collectBuckets(spans []prompb.BucketSpan, deltas []int64, counts []float64) []bucket {
	// A malformed request may carry both, or fewer counts than the spans claim; the spans are
	// followed only as far as the counts actually go.
	n := len(counts)
	if len(deltas) > 0 {
		n = len(deltas)
	}
	if n = min(n, totalSpanLength(spans)); n == 0 {
		return nil
	}

	out := make([]bucket, 0, n)

	var (
		idx     int32
		delta   int64
		emitted int
	)
	for si, span := range spans {
		if si == 0 {
			idx = span.Offset
		} else {
			// A span's offset is the gap from the previous span's last index.
			idx += span.Offset + 1
		}

		for j := uint32(0); j < span.Length; j++ {
			if emitted == n {
				return out
			}

			var count float64
			if len(deltas) > 0 {
				delta += deltas[emitted]
				count = float64(delta)
			} else {
				count = counts[emitted]
			}
			if count > 0 {
				out = append(out, bucket{index: idx, count: count})
			}

			idx++
			emitted++
		}
		idx--
	}
	return out
}

func totalSpanLength(spans []prompb.BucketSpan) (n int) {
	for _, s := range spans {
		n += int(s.Length)
	}
	return n
}

// bound returns the upper bound of bucket index for the given schema: base^index, with
// base = 2^(2^-schema).
func bound(index, schema int32) float64 {
	return math.Exp2(float64(index) * math.Exp2(-float64(schema)))
}

// formatBound formats a bucket bound as its `le` label value, carved from the name arena.
func (c *Converter) formatBound(f float64) []byte {
	return strconv.AppendFloat(c.names.Alloc(32), f, 'g', -1, 64)
}

func histogramCount(h *prompb.Histogram) float64 {
	if v, ok := h.Count.AsUint64(); ok {
		return float64(v)
	}
	v, _ := h.Count.AsFloat64()
	return v
}

func zeroCount(h *prompb.Histogram) float64 {
	if v, ok := h.ZeroCount.AsUint64(); ok {
		return float64(v)
	}
	v, _ := h.ZeroCount.AsFloat64()
	return v
}
