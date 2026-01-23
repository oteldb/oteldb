package prompb

import (
	"math"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
)

// TimeSeries is a timeseries.
type TimeSeries struct {
	Labels     []Label
	Samples    []Sample
	Exemplars  []Exemplar
	Histograms []Histogram
}

// Unmarshal unmarshals TimeSeries from src.
func (ts *TimeSeries) Unmarshal(p *pools, src []byte) (err error) {
	var (
		labelPool     = p.Labels
		samplePool    = p.Samples
		exemplarPool  = p.Exemplars
		histogramPool = p.Histograms
	)

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read labels data")
			}
			var label Label
			if err := label.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read labels (field %d)", fc.FieldNum)
			}
			labelPool.Push(label)
		case 2:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read samples data")
			}
			var sample Sample
			if err := sample.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read samples (field %d)", fc.FieldNum)
			}
			samplePool.Push(sample)
		case 3:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read exemplars data")
			}
			exemplar := exemplarPool.GetNext()
			if err := exemplar.Unmarshal(p, data); err != nil {
				return errors.Wrapf(err, "read exemplars (field %d)", fc.FieldNum)
			}
		case 4:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read histograms data")
			}
			histogram := histogramPool.GetNext()
			if err := histogram.Unmarshal(p, data); err != nil {
				return errors.Wrapf(err, "read histograms (field %d)", fc.FieldNum)
			}
		}
	}
	ts.Labels = labelPool.Cut()
	ts.Samples = samplePool.Cut()
	ts.Exemplars = exemplarPool.Cut()
	ts.Histograms = histogramPool.Cut()
	return nil
}

// Label is a timeseries label
type Label struct {
	Name  []byte
	Value []byte
}

// Unmarshal unmarshals Label from src.
func (m *Label) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			name, ok := fc.Bytes()
			if !ok {
				return errors.Errorf("read name (field %d)", fc.FieldNum)
			}
			m.Name = name
		case 2:
			value, ok := fc.Bytes()
			if !ok {
				return errors.Errorf("read value (field %d)", fc.FieldNum)
			}
			m.Value = value
		}
	}
	return nil
}

// Sample is a timeseries sample.
type Sample struct {
	Value     float64
	Timestamp int64
}

// Unmarshal unmarshals Sample from src.
func (s *Sample) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			value, ok := fc.Double()
			if !ok {
				return errors.Errorf("read value (field %d)", fc.FieldNum)
			}
			s.Value = value
		case 2:
			timestamp, ok := fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
			s.Timestamp = timestamp
		}
	}
	return nil
}

type Exemplar struct {
	Labels    []Label
	Value     float64
	Timestamp int64
}

// Unmarshal unmarshals Exemplar from src.
func (e *Exemplar) Unmarshal(p *pools, src []byte) (err error) {
	labelPool := p.ExemplarLabels

	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read labels data")
			}
			var label Label
			if err := label.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read labels (field %d)", fc.FieldNum)
			}
			labelPool.Push(label)
		case 2:
			value, ok := fc.Double()
			if !ok {
				return errors.Errorf("read value (field %d)", fc.FieldNum)
			}
			e.Value = value
		case 3:
			timestamp, ok := fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
			e.Timestamp = timestamp
		}
	}
	e.Labels = labelPool.Cut()
	return nil
}

type Histogram struct {
	Count         HistogramCount
	Sum           float64
	Schema        int32
	ZeroThreshold float64
	ZeroCount     HistogramZeroCount

	NegativeSpans  []BucketSpan
	NegativeDeltas []int64
	NegativeCounts []float64

	PositiveSpans  []BucketSpan
	PositiveDeltas []int64
	PositiveCounts []float64

	ResetHint HistogramResetHint
	Timestamp int64
}

// Unmarshal unmarshals BucketSpan from src.
func (h *Histogram) Unmarshal(p *pools, src []byte) (err error) {
	var (
		negativeSpansPool  = p.HistogramNegativeSpans
		negativeDeltasPool = p.HistogramNegativeDeltas
		negativeCountsPool = p.HistogramNegativeCounts
		positiveSpansPool  = p.HistogramPositiveSpans
		positiveDeltasPool = p.HistogramPositiveDeltas
		positiveCountsPool = p.HistogramPositiveCounts
	)

	var (
		fc   easyproto.FieldContext
		data []byte
		ok   bool
	)
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			var count uint64
			count, ok = fc.Uint64()
			if !ok {
				return errors.Errorf("read count_int (field %d)", fc.FieldNum)
			}
			h.Count.SetInt(count)
		case 2:
			var count float64
			count, ok = fc.Double()
			if !ok {
				return errors.Errorf("read count_float (field %d)", fc.FieldNum)
			}
			h.Count.SetFloat(count)
		case 3:
			h.Sum, ok = fc.Double()
			if !ok {
				return errors.Errorf("read sum (field %d)", fc.FieldNum)
			}
		case 4:
			h.Schema, ok = fc.Sint32()
			if !ok {
				return errors.Errorf("read schema (field %d)", fc.FieldNum)
			}
		case 5:
			h.ZeroThreshold, ok = fc.Double()
			if !ok {
				return errors.Errorf("read zero_threshold (field %d)", fc.FieldNum)
			}
		case 6:
			var zeroCount uint64
			zeroCount, ok = fc.Uint64()
			if !ok {
				return errors.Errorf("read zero_count_int (field %d)", fc.FieldNum)
			}
			h.ZeroCount.SetInt(zeroCount)
		case 7:
			var zeroCount float64
			zeroCount, ok = fc.Double()
			if !ok {
				return errors.Errorf("read zero_count_float (field %d)", fc.FieldNum)
			}
			h.ZeroCount.SetFloat(zeroCount)
		case 8:
			data, ok = fc.MessageData()
			if !ok {
				return errors.New("read negative_spans data")
			}
			var span BucketSpan
			if err := span.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read negative_spans (field %d)", fc.FieldNum)
			}
			negativeSpansPool.Push(span)
		case 9:
			negativeDeltasPool.pool, ok = fc.UnpackSint64s(negativeDeltasPool.pool)
			if !ok {
				return errors.Errorf("read negative_deltas (field %d)", fc.FieldNum)
			}
		case 10:
			negativeCountsPool.pool, ok = fc.UnpackDoubles(negativeCountsPool.pool)
			if !ok {
				return errors.Errorf("read negative_counts (field %d)", fc.FieldNum)
			}
		case 11:
			data, ok = fc.MessageData()
			if !ok {
				return errors.New("read positive_spans data")
			}
			var span BucketSpan
			if err := span.Unmarshal(data); err != nil {
				return errors.Wrapf(err, "read positive_spans (field %d)", fc.FieldNum)
			}
			positiveSpansPool.Push(span)
		case 12:
			positiveDeltasPool.pool, ok = fc.UnpackSint64s(positiveDeltasPool.pool)
			if !ok {
				return errors.Errorf("read positive_deltas (field %d)", fc.FieldNum)
			}
		case 13:
			positiveCountsPool.pool, ok = fc.UnpackDoubles(positiveCountsPool.pool)
			if !ok {
				return errors.Errorf("read positive_counts (field %d)", fc.FieldNum)
			}
		case 14:
			var hint int32
			hint, ok = fc.Int32()
			if !ok {
				return errors.Errorf("read hint (field %d)", fc.FieldNum)
			}
			h.ResetHint = HistogramResetHint(hint)
		case 15:
			h.Timestamp, ok = fc.Int64()
			if !ok {
				return errors.Errorf("read timestamp (field %d)", fc.FieldNum)
			}
		}
	}
	h.NegativeSpans = negativeSpansPool.Cut()
	h.NegativeDeltas = negativeDeltasPool.Cut()
	h.NegativeCounts = negativeCountsPool.Cut()
	h.PositiveSpans = positiveSpansPool.Cut()
	h.PositiveDeltas = positiveDeltasPool.Cut()
	h.PositiveCounts = positiveCountsPool.Cut()
	return nil
}

type (
	HistogramCount     = intOrFloat
	HistogramZeroCount = intOrFloat
)

type intOrFloat struct {
	data  uint64
	isInt bool
}

func (h *intOrFloat) SetInt(data uint64) {
	h.data = data
	h.isInt = true
}

func (h *intOrFloat) SetFloat(data float64) {
	h.data = math.Float64bits(data)
	h.isInt = false
}

func (h intOrFloat) AsUint64() (uint64, bool) {
	return h.data, h.isInt
}

func (h intOrFloat) AsFloat64() (float64, bool) {
	return math.Float64frombits(h.data), !h.isInt
}

type BucketSpan struct {
	Offset int32
	Length uint32
}

// Unmarshal unmarshals BucketSpan from src.
func (s *BucketSpan) Unmarshal(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return err
		}
		switch fc.FieldNum {
		case 1:
			offset, ok := fc.Sint32()
			if !ok {
				return errors.Errorf("read offset (field %d)", fc.FieldNum)
			}
			s.Offset = offset
		case 2:
			length, ok := fc.Uint32()
			if !ok {
				return errors.Errorf("read length (field %d)", fc.FieldNum)
			}
			s.Length = length
		}
	}
	return nil
}

type HistogramResetHint int32

const (
	HistogramResentHintUNKNOWN HistogramResetHint = 0
	HistogramResentHintYES     HistogramResetHint = 1
	HistogramResentHintNO      HistogramResetHint = 2
	HistogramResentHintGAUGE   HistogramResetHint = 3
)
