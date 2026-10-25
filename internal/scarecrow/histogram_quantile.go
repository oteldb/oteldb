package scarecrow

import (
	"context"
	"math"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
)

type histogramGroup struct {
	slots []int
}

type histogramQuantile struct {
	input, param       Operator
	ec                 *EvalContext
	paramPos, inputPos posrange.PositionRange

	schema       *Schema
	slotOf       []int
	badBucket    map[SeriesRef]error
	bounds       []float64
	groupOf      []int
	groups       []histogramGroup
	outputGroups [][]int

	columnOf  []int
	columns   []histogramBucketValues
	quantiles []float64
	buckets   promql.Buckets
	loaded    bool
	next      int
	out       Column
}

func newHistogramQuantile(input, param Operator, call *parser.Call, ec *EvalContext) *histogramQuantile {
	return &histogramQuantile{input: input, param: param, ec: ec, paramPos: call.Args[0].PositionRange(), inputPos: call.Args[1].PositionRange()}
}

func (*histogramQuantile) String() string         { return "HistogramQuantile" }
func (o *histogramQuantile) Children() []Operator { return []Operator{o.input, o.param} }
func (o *histogramQuantile) Close() error         { return errors.Join(o.input.Close(), o.param.Close()) }

func (o *histogramQuantile) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}
	input, err := o.input.Schema(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.slotOf = make([]int, input.Len())

	byInput := make(map[string]int)
	byOutput := make(map[string]int)
	var outputs []labels.Labels
	var inputKey, outputKey []byte
	for ref, ls := range input.Series {
		if ref&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		o.slotOf[ref] = -1
		bound, err := strconv.ParseFloat(ls.Get(model.BucketLabel), 64)
		if err != nil {
			if o.badBucket == nil {
				o.badBucket = make(map[SeriesRef]error)
			}
			o.badBucket[SeriesRef(ref)] = annotations.NewBadBucketLabelWarning("", ls.Get(model.BucketLabel), o.inputPos)
			continue
		}
		inputKey = ls.BytesWithoutLabels(inputKey, model.BucketLabel)
		group, ok := byInput[string(inputKey)]
		if !ok {
			group = len(o.groups)
			byInput[string(inputKey)] = group
			o.groups = append(o.groups, histogramGroup{})
			outLabels := labels.NewBuilder(ls).Del(model.BucketLabel, model.MetricNameLabel, model.MetricTypeLabel, model.MetricUnitLabel).Labels()
			outputKey = outLabels.Bytes(outputKey)
			output, ok := byOutput[string(outputKey)]
			if !ok {
				output = len(outputs)
				byOutput[string(outputKey)] = output
				outputs = append(outputs, outLabels)
				o.outputGroups = append(o.outputGroups, nil)
			}
			o.outputGroups[output] = append(o.outputGroups[output], group)
		}
		slot := len(o.bounds)
		o.bounds = append(o.bounds, bound)
		o.slotOf[ref] = slot
		o.groupOf = append(o.groupOf, group)
	}
	o.schema = NewSchema(outputs)
	return o.schema, nil
}

func histogramBufferSize(current, samples int, budget *sampleBudget) (int, error) {
	if samples > int(^uint(0)>>1)/8-current {
		return 0, promql.ErrTooManySamples("histogram bucket values")
	}
	n := current + samples
	if budget != nil && budget.max > 0 && int64(n) > budget.max {
		return 0, promql.ErrTooManySamples("histogram bucket values")
	}
	return n, nil
}

func (o *histogramQuantile) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true
	if _, err := o.Schema(ctx); err != nil {
		return err
	}
	steps := o.ec.NumSteps()
	o.columnOf = make([]int, len(o.bounds))
	for i := range o.columnOf {
		o.columnOf[i] = -1
	}
	var err error
	o.quantiles, err = scalarValues(ctx, o.param, math.NaN(), steps)
	if err != nil {
		return err
	}
	for _, q := range o.quantiles {
		if math.IsNaN(q) || q < 0 || q > 1 {
			o.ec.annotations.add(annotations.NewInvalidQuantileWarning(q, o.paramPos))
		}
	}
	stored := 0
	for {
		column, err := o.input.Next(ctx)
		if err != nil {
			return err
		}
		if column == nil {
			return nil
		}
		if column.Empty() {
			continue
		}
		slot := o.slotOf[column.Ref]
		if slot < 0 {
			o.ec.annotations.add(o.badBucket[column.Ref])
			continue
		}
		if o.columnOf[slot] >= 0 {
			return errDuplicateLabelSet
		}
		count := column.Count()
		if stored, err = histogramBufferSize(stored, count, o.ec.Budget); err != nil {
			return err
		}
		values, err := copyHistogramBucket(ctx, column, count)
		if err != nil {
			return err
		}
		o.columnOf[slot] = len(o.columns)
		o.columns = append(o.columns, values)
		group := o.groupOf[slot]
		o.groups[group].slots = append(o.groups[group].slots, slot)
	}
}

func (o *histogramQuantile) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := o.load(ctx); err != nil {
		return nil, err
	}
	steps := o.ec.NumSteps()
	for o.next < o.schema.Len() {
		output := o.next
		o.next++
		active := false
		for _, group := range o.outputGroups[output] {
			active = active || len(o.groups[group].slots) > 0
		}
		if !active {
			continue
		}
		o.out.Resize(SeriesRef(output), steps)
		for _, group := range o.outputGroups[output] {
			if len(o.groups[group].slots) == 0 {
				continue
			}
			for step := range steps {
				if step&63 == 0 {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
				o.buckets = o.buckets[:0]
				for i, slot := range o.groups[group].slots {
					if i&4095 == 0 && i > 0 {
						if err := ctx.Err(); err != nil {
							return nil, err
						}
					}
					index := o.columnOf[slot]
					if index < 0 {
						continue
					}
					if value, ok := o.columns[index].valueAt(step); ok {
						o.buckets = append(o.buckets, promql.Bucket{UpperBound: o.bounds[slot], Count: value})
					}
				}

				if len(o.buckets) == 0 {
					continue
				}
				if o.out.IsSet(step) {
					return nil, errDuplicateLabelSet
				}
				value, forced, _, minBucket, maxBucket, maxDiff := promql.BucketQuantile(o.quantiles[step], o.buckets)
				if forced {
					o.ec.annotations.add(annotations.NewHistogramQuantileForcedMonotonicityInfo("", o.inputPos, o.ec.Steps[step], minBucket, maxBucket, maxDiff))
				}
				o.out.Set(step, value)
			}
		}
		if !o.out.Empty() {
			return &o.out, nil
		}
	}
	return nil, nil
}
