package prompb

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// Remote write 2.0 (`io.prometheus.write.v2.Request`) differs from 1.0 in three ways that matter
// to a decoder.
//
// Labels are interned: a series carries pairs of indices into one per-request string table instead
// of its own name/value bytes, which is most of why a 2.0 body is smaller. Refs are resolved with
// [AppendLabels].
//
// Each series carries its own [Metadata] — the metric's type, and refs to its help text and unit —
// so a receiver no longer has to guess a type from the name.
//
// Samples and histograms carry a start timestamp, the time the counter began counting, which 1.0
// had no way to express.
//
// The two schemas are deliberately wire-incompatible: 2.0 reserves fields 1 to 3, which is where
// 1.0 puts its timeseries, so reading a body with the wrong schema yields an empty message rather
// than plausible nonsense. Histogram, BucketSpan and the count oneofs are identical between the
// two, so they share this package's types and decoder.

// WriteRequestV2 is a remote write 2.0 request.
type WriteRequestV2 struct {
	// Symbols is the request's string table. Every ref in the request indexes it. By convention
	// element 0 is the empty string, since an unset ref decodes as 0.
	Symbols [][]byte
	// Timeseries are the request's series.
	Timeseries []TimeSeriesV2

	pools *poolsV2
}

// Unmarshal unmarshals WriteRequestV2 from src. The result aliases src.
func (req *WriteRequestV2) Unmarshal(src []byte) (err error) {
	if req.pools == nil {
		req.pools = &poolsV2{}
		req.pools.init()
	}
	p := req.pools

	// Field order is the sender's choice — the reference marshaler emits a series' exemplars
	// (field 4) before its histograms (field 3) — and the symbols a series' refs resolve against
	// may arrive after the series itself. So the series bodies are collected on this pass and
	// decoded once the whole table is known.
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 4:
			symbol, ok := fc.Bytes()
			if !ok {
				return errors.Errorf("read symbols (field %d)", fc.FieldNum)
			}
			p.Symbols.Push(symbol)
		case 5:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read timeseries data")
			}
			p.TimeseriesData.Push(data)
		}
	}
	req.Symbols = p.Symbols.Cut()

	for _, data := range p.TimeseriesData.Cut() {
		ts := p.Timeseries.GetNext()
		if err := ts.Unmarshal(p, data); err != nil {
			return errors.Wrap(err, "read timeseries")
		}
	}
	req.Timeseries = p.Timeseries.Cut()

	return nil
}

// Symbol returns the symbol at ref and whether ref is within the table.
func (req *WriteRequestV2) Symbol(ref uint32) ([]byte, bool) {
	if int(ref) >= len(req.Symbols) {
		return nil, false
	}

	return req.Symbols[ref], true
}

// AppendLabels resolves refs against symbols and appends the labels to dst.
//
// refs is a flat list of name/value index pairs, so its length must be even, and every index must
// be within the table.
func AppendLabels(dst []Label, refs []uint32, symbols [][]byte) ([]Label, error) {
	if len(refs)%2 != 0 {
		return dst, errors.Errorf("label refs length %d is not even", len(refs))
	}

	for i := 0; i < len(refs); i += 2 {
		nameRef, valueRef := refs[i], refs[i+1]
		if int(nameRef) >= len(symbols) || int(valueRef) >= len(symbols) {
			return dst, errors.Errorf("label refs %d/%d are outside the %d symbol table",
				nameRef, valueRef, len(symbols))
		}
		dst = append(dst, Label{Name: symbols[nameRef], Value: symbols[valueRef]})
	}

	return dst, nil
}

// TimeSeriesV2 is a remote write 2.0 series.
type TimeSeriesV2 struct {
	// LabelsRefs is a flat list of name/value index pairs into [WriteRequestV2.Symbols].
	LabelsRefs []uint32
	Samples    []SampleV2
	Histograms []Histogram
	Exemplars  []ExemplarV2
	Metadata   Metadata
}

// Unmarshal unmarshals TimeSeriesV2 from src.
func (ts *TimeSeriesV2) Unmarshal(p *poolsV2, src []byte) (err error) {
	var (
		refPool       = p.LabelsRefs
		samplePool    = p.Samples
		histogramPool = p.Histograms
		exemplarPool  = p.Exemplars
	)

	var (
		fc easyproto.FieldContext
		ok bool
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			refPool.pool, ok = fc.UnpackUint32s(refPool.pool)
			if !ok {
				return errors.Errorf("read labels_refs (field %d)", fc.FieldNum)
			}
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read samples data")
			}
			var sample SampleV2
			if err := sample.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read samples (field %d)", fc.FieldNum)
			}
			samplePool.Push(sample)
		case 3:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read histograms data")
			}
			histogram := histogramPool.GetNext()
			if err := histogram.Unmarshal(p.histograms, data); err != nil {
				return errors.Wrapf(err, "read histograms (field %d)", fc.FieldNum)
			}
		case 4:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read exemplars data")
			}
			exemplar := exemplarPool.GetNext()
			if err := exemplar.Unmarshal(p, data); err != nil {
				return errors.Wrapf(err, "read exemplars (field %d)", fc.FieldNum)
			}
		case 5:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read metadata data")
			}
			if err := ts.Metadata.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read metadata (field %d)", fc.FieldNum)
			}
		}
	}
	ts.LabelsRefs = refPool.Cut()
	ts.Samples = samplePool.Cut()
	ts.Histograms = histogramPool.Cut()
	ts.Exemplars = exemplarPool.Cut()

	return nil
}

// SampleV2 is a remote write 2.0 sample: a 1.0 sample plus the time its counter started.
type SampleV2 struct {
	Value     float64
	Timestamp int64
	// StartTimestamp is when the series started counting, in ms. Zero means unset.
	StartTimestamp int64
}

// Unmarshal unmarshals SampleV2 from src.
func (s *SampleV2) Unmarshal(src []byte) (err error) {
	var (
		fc easyproto.FieldContext
		ok bool
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			s.Value, ok = fc.Double()
			if !ok {
				return errors.Errorf("read value (field %d)", fc.FieldNum)
			}
		case 2:
			s.Timestamp, ok = fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
		case 3:
			s.StartTimestamp, ok = fc.Int64()
			if !ok {
				return errors.Errorf("read start_timestamp (field %d)", fc.FieldNum)
			}
		}
	}

	return nil
}

// ExemplarV2 is a remote write 2.0 exemplar, whose labels are interned like a series'.
type ExemplarV2 struct {
	LabelsRefs []uint32
	Value      float64
	Timestamp  int64
}

// Unmarshal unmarshals ExemplarV2 from src.
func (e *ExemplarV2) Unmarshal(p *poolsV2, src []byte) (err error) {
	refPool := p.ExemplarLabelsRefs

	var (
		fc easyproto.FieldContext
		ok bool
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			refPool.pool, ok = fc.UnpackUint32s(refPool.pool)
			if !ok {
				return errors.Errorf("read labels_refs (field %d)", fc.FieldNum)
			}
		case 2:
			e.Value, ok = fc.Double()
			if !ok {
				return errors.Errorf("read value (field %d)", fc.FieldNum)
			}
		case 3:
			e.Timestamp, ok = fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
		}
	}
	e.LabelsRefs = refPool.Cut()

	return nil
}

// MetricType is a series' declared type.
type MetricType int32

// The metric types remote write 2.0 defines.
const (
	MetricTypeUnspecified MetricType = 0
	MetricTypeCounter     MetricType = 1
	MetricTypeGauge       MetricType = 2
	// MetricTypeHistogram is a cumulative histogram, classic or native.
	MetricTypeHistogram MetricType = 3
	// MetricTypeGaugeHistogram is a histogram whose buckets may go down.
	MetricTypeGaugeHistogram MetricType = 4
	MetricTypeSummary        MetricType = 5
	// MetricTypeInfo and MetricTypeStateSet are OpenMetrics types carrying state in their labels;
	// their value is always 1.
	MetricTypeInfo     MetricType = 6
	MetricTypeStateSet MetricType = 7
)

// Metadata is a series' declared type, help text and unit. Help and unit are refs into
// [WriteRequestV2.Symbols]; an unset one is 0, which by convention is the empty string.
type Metadata struct {
	Type    MetricType
	HelpRef uint32
	UnitRef uint32
}

// Unmarshal unmarshals Metadata from src.
func (m *Metadata) Unmarshal(src []byte) (err error) {
	var (
		fc easyproto.FieldContext
		ok bool
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		// Field 2 does not exist: the schema skips it.
		switch fc.FieldNum {
		case 1:
			var typ int32
			typ, ok = fc.Int32()
			if !ok {
				return errors.Errorf("read type (field %d)", fc.FieldNum)
			}
			m.Type = MetricType(typ)
		case 3:
			m.HelpRef, ok = fc.Uint32()
			if !ok {
				return errors.Errorf("read help_ref (field %d)", fc.FieldNum)
			}
		case 4:
			m.UnitRef, ok = fc.Uint32()
			if !ok {
				return errors.Errorf("read unit_ref (field %d)", fc.FieldNum)
			}
		}
	}

	return nil
}
