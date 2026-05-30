package logqlmetric

import (
	"regexp"

	"github.com/oteldb/oteldb/internal/logql"
)

// LabelReplace returns new step iterator performing label replacement.
func LabelReplace(iter StepIterator, expr *logql.LabelReplaceExpr) (StepIterator, error) {
	return &labelReplaceIterator{
		iter:     iter,
		dstLabel: expr.DstLabel,
		pattern:  expr.Replacement,
		srcLabel: expr.SrcLabel,
		re:       expr.Re,
	}, nil
}

type labelReplaceIterator struct {
	iter StepIterator

	dstLabel string
	pattern  string
	srcLabel string
	re       *regexp.Regexp
}

func (i *labelReplaceIterator) Next(r *Step) bool {
	if !i.iter.Next(r) {
		return false
	}

	for idx := range r.Samples {
		r.Samples[idx].Set = r.Samples[idx].Set.Replace(
			i.dstLabel,
			i.pattern,
			i.srcLabel,
			i.re,
		)
	}
	return true
}

func (i *labelReplaceIterator) Err() error {
	return i.iter.Err()
}

func (i *labelReplaceIterator) Close() error {
	return i.iter.Close()
}
