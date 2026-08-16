package otlpdirect

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// Field numbers of metrics.proto and collector/metrics/v1/metrics_service.proto.
const (
	// opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest
	fieldExportResourceMetrics = 1

	// opentelemetry.proto.metrics.v1.ResourceMetrics
	fieldResourceMetricsResource  = 1
	fieldResourceMetricsScope     = 2
	fieldResourceMetricsSchemaURL = 3

	// opentelemetry.proto.metrics.v1.ScopeMetrics
	fieldScopeMetricsScope     = 1
	fieldScopeMetricsMetrics   = 2
	fieldScopeMetricsSchemaURL = 3

	// opentelemetry.proto.metrics.v1.Metric — 4 (data) is the oneof the five below inhabit.
	fieldMetricName    = 1
	fieldMetricUnit    = 3
	fieldMetricGauge   = 5
	fieldMetricSum     = 7
	fieldMetricHist    = 9
	fieldMetricExpHist = 10
	fieldMetricSummary = 11

	// opentelemetry.proto.metrics.v1.{Gauge,Sum,Histogram,ExponentialHistogram,Summary}
	fieldDataPoints     = 1
	fieldTemporality    = 2
	fieldSumIsMonotonic = 3

	// opentelemetry.proto.metrics.v1.NumberDataPoint
	fieldNumberStart      = 2
	fieldNumberTime       = 3
	fieldNumberAsDouble   = 4
	fieldNumberAsInt      = 6
	fieldNumberAttributes = 7

	// opentelemetry.proto.metrics.v1.HistogramDataPoint
	fieldHistStart      = 2
	fieldHistTime       = 3
	fieldHistCount      = 4
	fieldHistSum        = 5
	fieldHistBuckets    = 6
	fieldHistBounds     = 7
	fieldHistAttributes = 9

	// opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint
	fieldExpAttributes = 1
	fieldExpStart      = 2
	fieldExpTime       = 3
	fieldExpCount      = 4
	fieldExpSum        = 5
	fieldExpScale      = 6
	fieldExpZeroCount  = 7
	fieldExpPositive   = 8
	fieldExpNegative   = 9

	// opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets
	fieldBucketsOffset = 1
	fieldBucketsCounts = 2

	// opentelemetry.proto.metrics.v1.SummaryDataPoint
	fieldSummaryStart      = 2
	fieldSummaryTime       = 3
	fieldSummaryCount      = 4
	fieldSummarySum        = 5
	fieldSummaryQuantiles  = 6
	fieldSummaryAttributes = 7

	// opentelemetry.proto.metrics.v1.SummaryDataPoint.ValueAtQuantile
	fieldQuantileQuantile = 1
	fieldQuantileValue    = 2
)

// OTLP aggregation temporality, which the model spells with its own constants.
const (
	otlpTemporalityDelta      = 1
	otlpTemporalityCumulative = 2
)

// MetricsConverter decodes an OTLP ExportMetricsServiceRequest into [metric.Metrics]. It retains
// the batch and the scratch it is built from, so a converter reused across requests allocates
// nothing in steady state. It is not safe for concurrent use; pool one per in-flight request.
type MetricsConverter struct {
	batch metric.Metrics
	dec   decoder

	// Scratch reused across the data points of a request; each is consumed before the next point
	// reaches it.
	pointAttrs [][]byte
	dataPoints [][]byte
	bounds     []float64
	counts     []uint64
	deltas     []uint64
}

// Convert decodes a serialized ExportMetricsServiceRequest, returning how many points it could not
// represent (a number point carrying no value — the only unrepresentable case).
//
// The returned batch aliases src: every attribute key, string value, metric name and unit is a
// sub-slice of it. It stays valid until the next Convert on this converter, and src must not be
// recycled until the write consuming the batch has returned.
func (c *MetricsConverter) Convert(src []byte) (_ *metric.Metrics, dropped int, _ error) {
	c.batch.Reset()
	c.dec.reset()

	resources, err := collect(src, fieldExportResourceMetrics, "resource metrics")
	if err != nil {
		return nil, 0, err
	}

	for _, data := range resources {
		n, err := c.resourceMetrics(data)
		if err != nil {
			return nil, 0, err
		}

		dropped += n
	}

	return &c.batch, dropped, nil
}

func (c *MetricsConverter) resourceMetrics(src []byte) (dropped int, _ error) {
	var (
		fc     easyproto.FieldContext
		res    signal.Resource
		scopes [][]byte
		err    error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return 0, errors.Wrap(err, "read resource metrics field")
		}

		switch fc.FieldNum {
		case fieldResourceMetricsResource:
			data, ok := fc.MessageData()
			if !ok {
				return 0, errors.New("read resource")
			}

			if res.Attributes, err = c.dec.resource(data); err != nil {
				return 0, err
			}
		case fieldResourceMetricsScope:
			data, ok := fc.MessageData()
			if !ok {
				return 0, errors.New("read scope metrics")
			}

			scopes = append(scopes, data)
		case fieldResourceMetricsSchemaURL:
			v, ok := fc.Bytes()
			if !ok {
				return 0, errors.New("read resource schema url")
			}

			res.SchemaURL = v
		}
	}

	rm := c.batch.AddResource()
	rm.Resource = res

	for _, data := range scopes {
		n, err := c.scopeMetrics(rm, data)
		if err != nil {
			return dropped, err
		}

		dropped += n
	}

	return dropped, nil
}

func (c *MetricsConverter) scopeMetrics(rm *metric.ResourceMetrics, src []byte) (dropped int, _ error) {
	var (
		fc        easyproto.FieldContext
		scopeData []byte
		schemaURL []byte
		metrics   [][]byte
		err       error
	)

	// Field order is the producer's choice — pdata writes them descending, so schema_url arrives
	// before scope. The scope submessage is decoded after the walk, never during it: decoding in
	// place would overwrite a schema_url already read.
	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return 0, errors.Wrap(err, "read scope metrics field")
		}

		switch fc.FieldNum {
		case fieldScopeMetricsScope:
			data, ok := fc.MessageData()
			if !ok {
				return 0, errors.New("read scope")
			}

			scopeData = data
		case fieldScopeMetricsMetrics:
			data, ok := fc.MessageData()
			if !ok {
				return 0, errors.New("read metric")
			}

			metrics = append(metrics, data)
		case fieldScopeMetricsSchemaURL:
			v, ok := fc.Bytes()
			if !ok {
				return 0, errors.New("read scope schema url")
			}

			schemaURL = v
		}
	}

	sc, err := c.dec.scope(scopeData)
	if err != nil {
		return 0, err
	}

	sc.SchemaURL = schemaURL

	sm := rm.AddScope()
	sm.Scope = sc

	for _, data := range metrics {
		n, err := c.metric(sm, data)
		if err != nil {
			return dropped, err
		}

		dropped += n
	}

	return dropped, nil
}

// metric dispatches on which arm of the Metric `data` oneof is present. The name and unit are
// siblings of that arm and may arrive on either side of it, so the arm is decoded after the walk.
func (c *MetricsConverter) metric(sm *metric.ScopeMetrics, src []byte) (dropped int, _ error) {
	var (
		fc       easyproto.FieldContext
		name     []byte
		unit     []byte
		body     []byte
		bodyKind uint32
		err      error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return 0, errors.Wrap(err, "read metric field")
		}

		switch fc.FieldNum {
		case fieldMetricName:
			if name, err = takeBytes(&fc, "metric name"); err != nil {
				return 0, err
			}
		case fieldMetricUnit:
			if unit, err = takeBytes(&fc, "metric unit"); err != nil {
				return 0, err
			}
		case fieldMetricGauge, fieldMetricSum, fieldMetricHist, fieldMetricExpHist, fieldMetricSummary:
			data, ok := fc.MessageData()
			if !ok {
				return 0, errors.New("read metric data")
			}

			body, bodyKind = data, fc.FieldNum
		}
	}

	switch bodyKind {
	case fieldMetricGauge:
		return c.numbers(sm, name, unit, body, metric.KindGauge, 0, false)
	case fieldMetricSum:
		return c.sum(sm, name, unit, body)
	case fieldMetricHist:
		return 0, c.histogram(sm, name, unit, body)
	case fieldMetricExpHist:
		return 0, c.expHistogram(sm, name, unit, body)
	case fieldMetricSummary:
		return 0, c.summary(sm, name, unit, body)
	default: // a metric with no data arm carries no points
		return 0, nil
	}
}

// sum reads the temporality and monotonicity that qualify a sum's identity, then its points.
func (c *MetricsConverter) sum(sm *metric.ScopeMetrics, name, unit, src []byte) (dropped int, _ error) {
	var (
		fc        easyproto.FieldContext
		temp      metric.Temporality
		monotonic bool
		err       error
	)

	points := c.dataPoints[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return 0, errors.Wrap(err, "read sum field")
		}

		switch fc.FieldNum {
		case fieldDataPoints:
			data, ok := fc.MessageData()
			if !ok {
				return 0, errors.New("read sum data point")
			}

			points = append(points, data)
		case fieldTemporality:
			v, ok := fc.Enum()
			if !ok {
				return 0, errors.New("read sum temporality")
			}

			temp = temporalityOf(v)
		case fieldSumIsMonotonic:
			v, ok := fc.Bool()
			if !ok {
				return 0, errors.New("read sum monotonicity")
			}

			monotonic = v
		}
	}

	c.dataPoints = points

	mt := sm.AddMetric()
	mt.Name, mt.Unit = name, unit
	mt.Kind, mt.Temporality, mt.Monotonic = metric.KindSum, temp, monotonic

	for _, data := range points {
		n, err := c.numberPoint(mt, data)
		if err != nil {
			return dropped, err
		}

		dropped += n
	}

	return dropped, nil
}

// numbers reads a gauge's points under one metric entry.
func (c *MetricsConverter) numbers(
	sm *metric.ScopeMetrics, name, unit, src []byte,
	kind metric.PointKind, temp metric.Temporality, monotonic bool,
) (dropped int, _ error) {
	points, err := collectInto(c.dataPoints[:0], src, fieldDataPoints, "number data point")
	if err != nil {
		return 0, err
	}

	c.dataPoints = points

	mt := sm.AddMetric()
	mt.Name, mt.Unit = name, unit
	mt.Kind, mt.Temporality, mt.Monotonic = kind, temp, monotonic

	for _, data := range points {
		n, err := c.numberPoint(mt, data)
		if err != nil {
			return dropped, err
		}

		dropped += n
	}

	return dropped, nil
}

// numberPoint appends one gauge/sum point. A point carrying neither as_double nor as_int has no
// value to store, so it is dropped and counted rather than stored as zero.
func (c *MetricsConverter) numberPoint(mt *metric.Metric, src []byte) (dropped int, _ error) {
	var (
		fc        easyproto.FieldContext
		start, ts int64
		value     float64
		hasValue  bool
		err       error
	)

	kvs := c.pointAttrs[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return 0, errors.Wrap(err, "read number data point field")
		}

		switch fc.FieldNum {
		case fieldNumberStart:
			v, ok := fc.Fixed64()
			if !ok {
				return 0, errors.New("read point start")
			}

			start = int64(v)
		case fieldNumberTime:
			v, ok := fc.Fixed64()
			if !ok {
				return 0, errors.New("read point time")
			}

			ts = int64(v)
		case fieldNumberAsDouble:
			v, ok := fc.Double()
			if !ok {
				return 0, errors.New("read point double value")
			}

			value, hasValue = v, true
		case fieldNumberAsInt:
			v, ok := fc.Sfixed64()
			if !ok {
				return 0, errors.New("read point int value")
			}

			value, hasValue = float64(v), true
		case fieldNumberAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return 0, errors.New("read point attribute")
			}

			kvs = append(kvs, data)
		}
	}

	c.pointAttrs = kvs

	if !hasValue {
		return 1, nil
	}

	attrs, err := c.dec.attributes(kvs)
	if err != nil {
		return 0, err
	}

	p := mt.AddPoint()
	p.Attributes = attrs
	p.StartTs, p.Ts, p.Value = start, ts, value

	return 0, nil
}

func temporalityOf(v int32) metric.Temporality {
	switch v {
	case otlpTemporalityDelta:
		return metric.TemporalityDelta
	case otlpTemporalityCumulative:
		return metric.TemporalityCumulative
	default:
		return metric.TemporalityUnspecified
	}
}
