package logqlmetric

import (
	"math"
	"time"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

type additiveSeries struct {
	data       []FPoint
	set        logqlabels.AggregatedLabels
	head, size int
	bytes      int64
	bytesValid bool
}

func (s *additiveSeries) add(point FPoint) {
	if s.bytesValid {
		v := point.Value
		if v < 0 || v >= 1<<63 || math.Trunc(v) != v {
			s.bytesValid = false
		} else if value := int64(v); value > math.MaxInt64-s.bytes {
			s.bytesValid = false
		} else {
			s.bytes += value
		}
	}
	if s.size == len(s.data) {
		data := make([]FPoint, max(8, 2*len(s.data)))
		n := copy(data, s.data[s.head:])
		copy(data[n:], s.data[:s.head])
		s.data, s.head = data, 0
	}
	tail := s.head + s.size
	if tail >= len(s.data) {
		tail -= len(s.data)
	}
	s.data[tail] = point
	s.size++
}

func (s *additiveSeries) expire(start time.Time) {
	for s.size > 0 && !s.data[s.head].Timestamp.AsTime().After(start) {
		if s.bytesValid {
			s.bytes -= int64(s.data[s.head].Value)
		}
		s.head++
		if s.head == len(s.data) {
			s.head = 0
		}
		s.size--
	}
}

func (s *additiveSeries) sum() float64 {
	var sum SumAggregator
	index := s.head
	for range s.size {
		sum.Apply(s.data[index].Value)
		index++
		if index == len(s.data) {
			index = 0
		}
	}
	return sum.Result()
}

type additiveRangeIterator struct {
	*rangeAggIterator

	series  map[logqlabels.GroupingKey]*additiveSeries
	counts  bool
	divisor float64
}

func newAdditiveRangeIterator(base *rangeAggIterator, expr *logql.RangeAggregationExpr) *additiveRangeIterator {
	if expr.Range.Unwrap != nil {
		return nil
	}
	counts := false
	switch expr.Op {
	case logql.RangeOpCount, logql.RangeOpRate:
		counts = true
	case logql.RangeOpBytes, logql.RangeOpBytesRate:
	default:
		return nil
	}
	divisor := 1.0
	if expr.Op == logql.RangeOpRate || expr.Op == logql.RangeOpBytesRate {
		divisor = expr.Range.Range.Seconds()
	}
	return &additiveRangeIterator{
		rangeAggIterator: base,
		series:           map[logqlabels.GroupingKey]*additiveSeries{},
		counts:           counts,
		divisor:          divisor,
	}
}

func (i *additiveRangeIterator) Next(result *Step) bool {
	current, ok := i.stepper.next()
	if !ok {
		return false
	}
	start := current.Add(-i.interval)
	for key, series := range i.series {
		series.expire(start)
		if series.size == 0 {
			delete(i.series, key)
		}
	}
	i.fill(start, current)
	result.Timestamp = otelstorage.NewTimestampFromTime(current)
	result.Samples = result.Samples[:0]
	for _, series := range i.series {
		var total float64
		switch {
		case i.counts:
			total = float64(series.size) / i.divisor
		case series.bytesValid:
			total = float64(series.bytes) / i.divisor
		default:
			total = series.sum() / i.divisor
		}
		result.Samples = append(result.Samples, Sample{Data: total, Set: series.set})
	}
	return true
}

func (i *additiveRangeIterator) fill(start, end time.Time) {
	for {
		if !i.buffered {
			if !i.iter.Next(&i.entry) {
				return
			}
		} else {
			i.buffered = false
		}
		entry := i.entry
		ts := entry.Timestamp.AsTime()
		if ts.After(end) {
			i.buffered = true
			return
		}
		if !ts.After(start) {
			continue
		}
		labels := i.grouper(entry.Set, i.groupLabels...)
		key := labels.Key()
		series := i.series[key]
		if series == nil {
			series = &additiveSeries{set: labels, bytesValid: !i.counts}
			i.series[key] = series
		}
		series.add(FPoint{Timestamp: entry.Timestamp, Value: entry.Sample})
	}
}
