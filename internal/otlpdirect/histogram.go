package otlpdirect

import (
	"math"
	"strconv"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// Histogram, exponential-histogram and summary points are stored by classic decomposition into
// ordinary float series — `_count`, `_sum` and cumulative `_bucket{le=…}` for histograms, and
// `_count`/`_sum`/`{quantile=…}` for summaries. The engine has no histogram code; this is where the
// shape is chosen, and it must match otlp/pdataconv exactly, since the two paths ingest the same
// requests into the same series.

// posInf is the Prometheus `le` value of the catch-all overflow bucket.
var posInf = []byte("+Inf")

var (
	leKey       = []byte("le")
	quantileKey = []byte("quantile")
)

func (c *MetricsConverter) histogram(sm *metric.ScopeMetrics, name, unit, src []byte) error {
	var (
		fc   easyproto.FieldContext
		temp metric.Temporality
		err  error
	)

	points := c.dataPoints[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read histogram field")
		}

		switch fc.FieldNum {
		case fieldDataPoints:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read histogram data point")
			}

			points = append(points, data)
		case fieldTemporality:
			v, ok := fc.Enum()
			if !ok {
				return errors.New("read histogram temporality")
			}

			temp = temporalityOf(v)
		}
	}

	c.dataPoints = points

	for _, data := range points {
		if err := c.histogramPoint(sm, name, unit, temp, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *MetricsConverter) histogramPoint(
	sm *metric.ScopeMetrics, name, unit []byte, temp metric.Temporality, src []byte,
) error {
	var (
		fc        easyproto.FieldContext
		start, ts int64
		count     uint64
		sum       float64
		hasSum    bool
		err       error
	)

	kvs := c.pointAttrs[:0]
	bounds := c.bounds[:0]
	counts := c.counts[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read histogram data point field")
		}

		switch fc.FieldNum {
		case fieldHistStart:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read histogram start")
			}

			start = int64(v)
		case fieldHistTime:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read histogram time")
			}

			ts = int64(v)
		case fieldHistCount:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read histogram count")
			}

			count = v
		case fieldHistSum:
			v, ok := fc.Double()
			if !ok {
				return errors.New("read histogram sum")
			}

			// sum is an optional scalar, so it is on the wire only when set — which is exactly
			// what pdata's HasSum reports, and what decides whether a _sum series exists.
			sum, hasSum = v, true
		case fieldHistBuckets:
			// A packed repeated field may arrive as several occurrences, so each appends.
			v, ok := fc.UnpackFixed64s(counts)
			if !ok {
				return errors.New("read histogram bucket counts")
			}

			counts = v
		case fieldHistBounds:
			v, ok := fc.UnpackDoubles(bounds)
			if !ok {
				return errors.New("read histogram explicit bounds")
			}

			bounds = v
		case fieldHistAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read histogram attribute")
			}

			kvs = append(kvs, data)
		}
	}

	c.pointAttrs, c.bounds, c.counts = kvs, bounds, counts

	base, err := c.dec.attributes(kvs)
	if err != nil {
		return err
	}

	cumulative := temp == metric.TemporalityCumulative

	c.addSeries(sm, c.suffix(name, "_count"), unit, temp, cumulative, base, start, ts, float64(count))

	if hasSum {
		c.addSeries(sm, c.suffix(name, "_sum"), unit, temp, false, base, start, ts, sum)
	}

	bucketName := c.suffix(name, "_bucket")

	var cum uint64

	for b, n := range counts {
		cum += n

		le := posInf
		if b < len(bounds) {
			le = c.formatBound(bounds[b])
		}

		c.addSeries(sm, bucketName, unit, temp, cumulative,
			c.withLabel(base, leKey, le), start, ts, float64(cum))
	}

	return nil
}

func (c *MetricsConverter) summary(sm *metric.ScopeMetrics, name, unit, src []byte) error {
	points, err := collectInto(c.dataPoints[:0], src, fieldDataPoints, "summary data point")
	if err != nil {
		return err
	}

	c.dataPoints = points

	for _, data := range points {
		if err := c.summaryPoint(sm, name, unit, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *MetricsConverter) summaryPoint(sm *metric.ScopeMetrics, name, unit, src []byte) error {
	var (
		fc        easyproto.FieldContext
		start, ts int64
		count     uint64
		sum       float64
		quantiles [][]byte
		err       error
	)

	kvs := c.pointAttrs[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read summary data point field")
		}

		switch fc.FieldNum {
		case fieldSummaryStart:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read summary start")
			}

			start = int64(v)
		case fieldSummaryTime:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read summary time")
			}

			ts = int64(v)
		case fieldSummaryCount:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read summary count")
			}

			count = v
		case fieldSummarySum:
			v, ok := fc.Double()
			if !ok {
				return errors.New("read summary sum")
			}

			sum = v
		case fieldSummaryQuantiles:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read summary quantile")
			}

			quantiles = append(quantiles, data)
		case fieldSummaryAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read summary attribute")
			}

			kvs = append(kvs, data)
		}
	}

	c.pointAttrs = kvs

	base, err := c.dec.attributes(kvs)
	if err != nil {
		return err
	}

	// A summary has no temporality; its count and sum are cumulative counters.
	c.addSeries(sm, c.suffix(name, "_count"), unit, metric.TemporalityCumulative, true, base, start, ts, float64(count))
	c.addSeries(sm, c.suffix(name, "_sum"), unit, metric.TemporalityCumulative, false, base, start, ts, sum)

	for _, data := range quantiles {
		q, v, err := quantileOf(data)
		if err != nil {
			return err
		}

		// The estimate is an instantaneous gauge under the base name with a quantile label.
		mt := sm.AddMetric()
		mt.Name, mt.Unit, mt.Kind = name, unit, metric.KindGauge

		p := mt.AddPoint()
		p.Attributes = c.withLabel(base, quantileKey, c.formatBound(q))
		p.StartTs, p.Ts, p.Value = start, ts, v
	}

	return nil
}

func quantileOf(src []byte) (quantile, value float64, _ error) {
	var (
		fc  easyproto.FieldContext
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return 0, 0, errors.Wrap(err, "read quantile field")
		}

		switch fc.FieldNum {
		case fieldQuantileQuantile:
			v, ok := fc.Double()
			if !ok {
				return 0, 0, errors.New("read quantile")
			}

			quantile = v
		case fieldQuantileValue:
			v, ok := fc.Double()
			if !ok {
				return 0, 0, errors.New("read quantile value")
			}

			value = v
		}
	}

	return quantile, value, nil
}

func (c *MetricsConverter) expHistogram(sm *metric.ScopeMetrics, name, unit, src []byte) error {
	var (
		fc   easyproto.FieldContext
		temp metric.Temporality
		err  error
	)

	points := c.dataPoints[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read exponential histogram field")
		}

		switch fc.FieldNum {
		case fieldDataPoints:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read exponential histogram data point")
			}

			points = append(points, data)
		case fieldTemporality:
			v, ok := fc.Enum()
			if !ok {
				return errors.New("read exponential histogram temporality")
			}

			temp = temporalityOf(v)
		}
	}

	c.dataPoints = points

	for _, data := range points {
		if err := c.expHistogramPoint(sm, name, unit, temp, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *MetricsConverter) expHistogramPoint(
	sm *metric.ScopeMetrics, name, unit []byte, temp metric.Temporality, src []byte,
) error {
	var (
		fc           easyproto.FieldContext
		start, ts    int64
		count        uint64
		sum          float64
		hasSum       bool
		scale        int32
		zeroCount    uint64
		positiveData []byte
		negativeData []byte
		err          error
	)

	kvs := c.pointAttrs[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read exponential histogram point field")
		}

		switch fc.FieldNum {
		case fieldExpStart:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read exponential histogram start")
			}

			start = int64(v)
		case fieldExpTime:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read exponential histogram time")
			}

			ts = int64(v)
		case fieldExpCount:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read exponential histogram count")
			}

			count = v
		case fieldExpSum:
			v, ok := fc.Double()
			if !ok {
				return errors.New("read exponential histogram sum")
			}

			sum, hasSum = v, true
		case fieldExpScale:
			v, ok := fc.Sint32()
			if !ok {
				return errors.New("read exponential histogram scale")
			}

			scale = v
		case fieldExpZeroCount:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read exponential histogram zero count")
			}

			zeroCount = v
		case fieldExpPositive:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read exponential histogram positive buckets")
			}

			positiveData = data
		case fieldExpNegative:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read exponential histogram negative buckets")
			}

			negativeData = data
		case fieldExpAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read exponential histogram attribute")
			}

			kvs = append(kvs, data)
		}
	}

	c.pointAttrs = kvs

	base, err := c.dec.attributes(kvs)
	if err != nil {
		return err
	}

	cumulative := temp == metric.TemporalityCumulative

	c.addSeries(sm, c.suffix(name, "_count"), unit, temp, cumulative, base, start, ts, float64(count))

	if hasSum {
		c.addSeries(sm, c.suffix(name, "_sum"), unit, temp, false, base, start, ts, sum)
	}

	buckets, err := c.expBuckets(scale, zeroCount, count, negativeData, positiveData)
	if err != nil {
		return err
	}

	bucketName := c.suffix(name, "_bucket")

	for _, b := range buckets {
		le := posInf
		if !math.IsInf(b.le, 1) {
			le = c.formatBound(b.le)
		}

		c.addSeries(sm, bucketName, unit, temp, cumulative,
			c.withLabel(base, leKey, le), start, ts, float64(b.cumulative))
	}

	return nil
}

// expBucket is a derived classic bucket: an upper bound and the cumulative count at or below it.
type expBucket struct {
	le         float64
	cumulative uint64
}

// expBuckets converts a point's negative/zero/positive buckets into classic `le` buckets in
// ascending bound order with cumulative counts, ending at +Inf carrying the full count.
func (c *MetricsConverter) expBuckets(
	scale int32, zeroCount, total uint64, negativeData, positiveData []byte,
) ([]expBucket, error) {
	factor := math.Exp2(math.Exp2(-float64(scale)))
	bound := func(index int) float64 { return math.Pow(factor, float64(index)) }

	negOffset, negCounts, err := bucketsOf(negativeData, c.deltas[:0])
	if err != nil {
		return nil, err
	}

	c.deltas = negCounts

	// The negative counts must be read out before the positive ones reuse the scratch.
	out := make([]expBucket, 0, len(negCounts)+2)

	var cum uint64

	// A negative bucket at index i holds values in [-base^(i+1), -base^i); its upper bound, the one
	// closest to zero, is -base^i.
	for k, n := range negCounts {
		if n == 0 {
			continue
		}

		cum += n
		out = append(out, expBucket{le: -bound(int(negOffset) + k), cumulative: cum})
	}

	if zeroCount > 0 {
		cum += zeroCount
		out = append(out, expBucket{le: 0, cumulative: cum})
	}

	posOffset, posCounts, err := bucketsOf(positiveData, nil)
	if err != nil {
		return nil, err
	}

	// A positive bucket at index i holds values in (base^i, base^(i+1)]; its upper bound is
	// base^(i+1).
	for k, n := range posCounts {
		if n == 0 {
			continue
		}

		cum += n
		out = append(out, expBucket{le: bound(int(posOffset) + k + 1), cumulative: cum})
	}

	// The +Inf bucket carries the full count, covering anything the buckets did not (a NaN
	// observation, say).
	return append(out, expBucket{le: math.Inf(1), cumulative: total}), nil
}

func bucketsOf(src []byte, dst []uint64) (offset int32, counts []uint64, _ error) {
	var (
		fc  easyproto.FieldContext
		err error
	)

	counts = dst

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return 0, nil, errors.Wrap(err, "read buckets field")
		}

		switch fc.FieldNum {
		case fieldBucketsOffset:
			v, ok := fc.Sint32()
			if !ok {
				return 0, nil, errors.New("read buckets offset")
			}

			offset = v
		case fieldBucketsCounts:
			v, ok := fc.UnpackUint64s(counts)
			if !ok {
				return 0, nil, errors.New("read buckets counts")
			}

			counts = v
		}
	}

	return offset, counts, nil
}

// addSeries appends a one-point synthetic sum series — a decomposed _count/_sum/_bucket.
func (c *MetricsConverter) addSeries(
	sm *metric.ScopeMetrics, name, unit []byte, temp metric.Temporality, monotonic bool,
	attrs signal.Attributes, startTS, ts int64, value float64,
) {
	mt := sm.AddMetric()
	mt.Name, mt.Unit = name, unit
	mt.Kind, mt.Temporality, mt.Monotonic = metric.KindSum, temp, monotonic

	p := mt.AddPoint()
	p.Attributes = attrs
	p.StartTs, p.Ts, p.Value = startTS, ts, value
}

// withLabel returns base plus one string label, re-sorted by key. The result is carved from the
// arena, so the decomposed series do not allocate per bucket.
func (c *MetricsConverter) withLabel(base signal.Attributes, key, value []byte) signal.Attributes {
	kvs := c.dec.attrs.Alloc(len(base) + 1)
	kvs = append(kvs, base...)
	kvs = append(kvs, signal.KeyValue{Key: key, Value: signal.StringValue(value)})

	return signal.NewAttributes(kvs...)
}

func (c *MetricsConverter) suffix(name []byte, s string) []byte {
	return c.dec.scratch.Concat(name, []byte(s))
}

func (c *MetricsConverter) formatBound(f float64) []byte {
	return strconv.AppendFloat(c.dec.scratch.Alloc(32), f, 'g', -1, 64)
}
