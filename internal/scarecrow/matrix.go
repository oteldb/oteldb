package scarecrow

import (
	"context"
	"fmt"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// matrixFold applies a range-vector function over each step's window (t-range, t].
//
// The matrix selector and its function call are one fused operator, never two. There is no
// matrix-shaped value in this engine and no operator emits a range vector: raw samples go in,
// one [Column] comes out. That is what keeps [Samples] from ever crossing an operator boundary.
type matrixFold struct {
	scanner  Scanner
	matchers []*labels.Matcher
	fnName   string
	fn       rangeFunc
	rng      time.Duration
	offset   time.Duration
	// at pins the evaluation timestamp (the @ modifier); see [vectorSelect].
	at *int64
	ec *EvalContext

	schema *Schema
	byHash map[uint64][]SeriesRef

	iter SeriesIterator
	out  Column

	// Scratch buffers for the per-step window, reused across steps and series.
	wt []int64
	wv []float64
	ww []float64
}

func newMatrixFold(
	sc Scanner, matchers []*labels.Matcher, fnName string, fn rangeFunc,
	rng, offset time.Duration, at *int64, ec *EvalContext,
) *matrixFold {
	return &matrixFold{
		scanner:  sc,
		matchers: matchers,
		fnName:   fnName,
		fn:       fn,
		rng:      rng,
		offset:   offset,
		at:       at,
		ec:       ec,
	}
}

func (o *matrixFold) String() string {
	return fmt.Sprintf("MatrixFold(%s, %s[%s])", o.fnName, matchersString(o.matchers), o.rng)
}

func (o *matrixFold) Children() []Operator { return nil }

func (o *matrixFold) Close() error {
	if o.iter == nil {
		return nil
	}

	err := o.iter.Close()
	o.iter = nil

	return err
}

// window returns the fetch bounds covering every step's range window, shifted by the offset.
func (o *matrixFold) window() (mint, maxt int64) {
	refs := refTimes(o.ec.Steps, o.offset, o.at)

	return refs[0] - o.rng.Milliseconds(), refs[len(refs)-1]
}

func (o *matrixFold) Schema(ctx context.Context) (*Schema, error) {
	if o.schema != nil {
		return o.schema, nil
	}

	mint, maxt := o.window()

	series, err := o.scanner.Series(ctx, mint, maxt, o.matchers)
	if err != nil {
		return nil, errors.Wrap(err, "enumerate series")
	}

	// A range-vector function's output is no longer the input metric, so __name__ is dropped
	// unless the function is one that retains it.
	if !keepsMetricName[o.fnName] {
		for i, ls := range series {
			series[i] = dropMetricName(ls)
		}
	}

	o.schema = NewSchema(series)
	o.byHash = indexByHash(o.schema)

	return o.schema, nil
}

func (o *matrixFold) Next(ctx context.Context) (*Column, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := o.Schema(ctx); err != nil {
		return nil, err
	}

	if o.iter == nil {
		mint, maxt := o.window()

		it, err := o.scanner.Scan(ctx, mint, maxt, o.matchers)
		if err != nil {
			return nil, errors.Wrap(err, "scan series")
		}
		o.iter = it
	}

	for {
		s, err := o.iter.Next(ctx)
		if err != nil {
			return nil, err
		}

		if s == nil {
			return nil, nil
		}

		ref, ok := o.refFor(s.Labels)
		if !ok {
			return nil, errors.Errorf("series %s absent from resolved schema", s.Labels)
		}

		o.out.Resize(ref, o.ec.NumSteps())
		o.fold(s)

		if o.out.Empty() {
			continue
		}

		return &o.out, nil
	}
}

// refFor resolves a scanned series to its schema ref, re-dropping __name__ so the lookup keys
// match the schema this operator published.
func (o *matrixFold) refFor(ls labels.Labels) (SeriesRef, bool) {
	if !keepsMetricName[o.fnName] {
		ls = dropMetricName(ls)
	}

	return lookupRef(o.schema, o.byHash, ls)
}

// dropMetricName returns ls without __name__.
func dropMetricName(ls labels.Labels) labels.Labels {
	return labels.NewBuilder(ls).Del(model.MetricNameLabel).Labels()
}

// fold walks the step grid and the sample list together with two pointers. Both are ascending,
// so the whole series folds in one forward pass: O(samples + steps), not O(steps × window).
func (o *matrixFold) fold(s *Samples) {
	var (
		rngMs  = o.rng.Milliseconds()
		refs   = refTimes(o.ec.Steps, o.offset, o.at)
		lo, hi int
	)

	for k, rangeEnd := range refs {
		rangeStart := rangeEnd - rngMs

		// The range is left-open: a sample exactly at rangeStart is outside the window.
		for lo < len(s.T) && s.T[lo] <= rangeStart {
			lo++
		}

		if hi < lo {
			hi = lo
		}

		for hi < len(s.T) && s.T[hi] <= rangeEnd {
			hi++
		}

		if lo >= hi {
			continue
		}

		w := o.buildWindow(s, lo, hi, rangeStart, rangeEnd, rngMs)
		if w.Len() == 0 {
			continue
		}

		if v, ok := o.fn(w); ok {
			o.out.Set(k, v)
		}
	}
}

// buildWindow copies samples [lo,hi) into the reusable scratch window, dropping staleness
// markers, which range functions never fold.
func (o *matrixFold) buildWindow(s *Samples, lo, hi int, rangeStart, rangeEnd, rngMs int64) *window {
	o.wt = o.wt[:0]
	o.wv = o.wv[:0]
	o.ww = o.ww[:0]

	for i := lo; i < hi; i++ {
		if isStale(s.V[i]) {
			continue
		}

		o.wt = append(o.wt, s.T[i])
		o.wv = append(o.wv, s.V[i])

		if s.Weights != nil {
			o.ww = append(o.ww, s.Weights[i])
		}
	}

	w := &window{
		T:          o.wt,
		V:          o.wv,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
		RangeMs:    rngMs,
	}
	if s.Weights != nil {
		w.W = o.ww
	}

	return w
}
