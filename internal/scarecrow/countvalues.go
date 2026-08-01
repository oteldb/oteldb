package scarecrow

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// countValuesAgg implements count_values: for each step, groups the input by (grouping labels,
// observed value) and counts how many series landed in each group.
//
// Unlike every other aggregation, an input series does not belong to one fixed output group —
// its value, and therefore its group, can change from step to step. So there is no per-series
// groupOf to precompute the way [aggregate] does; the group a series belongs to at step k is
// only known once its value at k is read. That in turn means the output schema is a genuine
// function of the data (§4.4), not a subset of it the way a survivor set is for [limitAgg]: load
// drains the whole input once, synthesizing and counting groups as it goes, before Schema can
// answer at all.
type countValuesAgg struct {
	input      Operator
	valueLabel string
	grouping   []string
	without    bool
	ec         *EvalContext

	schema *Schema
	// byKey resolves an output label set's canonical string form to its ref, so the same group
	// seen at a later step (or a later series) reuses the row rather than duplicating it.
	byKey map[string]SeriesRef
	// counts[ref] holds one row of per-step counts, 0 meaning "not observed at this step" — a
	// real count is never 0, since a group only exists because of an observation.
	counts [][]float64

	loaded bool
	cursor int
	out    Column
}

func newCountValuesAgg(input Operator, valueLabel string, e *parser.AggregateExpr, ec *EvalContext) (Operator, error) {
	if !model.UTF8Validation.IsValidLabelName(valueLabel) {
		return nil, errors.Errorf("invalid label name %q", valueLabel)
	}

	return &countValuesAgg{
		input:      input,
		valueLabel: valueLabel,
		grouping:   e.Grouping,
		without:    e.Without,
		ec:         ec,
	}, nil
}

func (o *countValuesAgg) String() string {
	return fmt.Sprintf("CountValues(%q, by=%v, without=%v)", o.valueLabel, o.grouping, o.without)
}

func (o *countValuesAgg) Children() []Operator { return []Operator{o.input} }

func (o *countValuesAgg) Close() error { return o.input.Close() }

// Schema runs the whole grouping — see the type doc for why this can't be deferred.
func (o *countValuesAgg) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	if err := o.load(ctx); err != nil {
		return nil, err
	}

	return o.schema, nil
}

func (o *countValuesAgg) load(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	o.loaded = true

	in, err := o.input.Schema(ctx)
	if err != nil {
		return err
	}

	steps := o.ec.NumSteps()
	o.byKey = make(map[string]SeriesRef, in.Len())

	var series []labels.Labels

	for {
		col, err := o.input.Next(ctx)
		if err != nil {
			return err
		}

		if col == nil {
			break
		}

		ls := in.At(col.Ref)

		for k := range steps {
			if !col.IsSet(k) {
				continue
			}

			out := countValuesGroupLabels(ls, o.valueLabel, col.V[k], o.grouping, o.without)
			key := out.String()

			ref, ok := o.byKey[key]
			if !ok {
				ref = SeriesRef(len(series))
				o.byKey[key] = ref
				series = append(series, out)
				o.counts = append(o.counts, make([]float64, steps))
			}

			o.counts[ref][k]++
		}
	}

	o.schema = NewSchema(series)

	return nil
}

// countValuesGroupLabels is the output identity for one (series, step): ls with valueLabel set
// to the observed value's formatted string, then reduced by grouping/without exactly as any
// other aggregation would — except that unlike a plain aggregation, `by` must keep valueLabel
// even though it is not itself one of the grouping labels named in the query (upstream adds it
// to the keep-list implicitly), while `without` already keeps it for free since it only ever
// deletes names, and valueLabel was never one of them.
func countValuesGroupLabels(ls labels.Labels, valueLabel string, v float64, grouping []string, without bool) labels.Labels {
	lb := labels.NewBuilder(ls)
	lb.Set(valueLabel, strconv.FormatFloat(v, 'f', -1, 64))
	withValue := lb.Labels()

	keep := grouping
	if !without {
		keep = append(append(make([]string, 0, len(grouping)+1), grouping...), valueLabel)
	}

	return groupLabels(withValue, keep, without)
}

func (o *countValuesAgg) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}

	for o.cursor < o.schema.Len() {
		ref := o.cursor
		o.cursor++

		o.out.Resize(SeriesRef(ref), o.ec.NumSteps())

		for k, c := range o.counts[ref] {
			if c > 0 {
				o.out.Set(k, c)
			}
		}

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}

	return nil, nil
}
