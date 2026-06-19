package logqlmetric

// DivideStep returns a new step iterator dividing every sample's value by a
// constant. It is used to turn a raw per-step sum (as produced by a SQL
// bucketed aggregation pushdown) into a per-second rate, mirroring what
// Rate[A].Aggregate (aggregator.go) does for the non-offloaded path.
func DivideStep(iter StepIterator, by float64) StepIterator {
	return &divideStepIterator{iter: iter, by: by}
}

type divideStepIterator struct {
	iter StepIterator
	by   float64
}

func (i *divideStepIterator) Next(r *Step) bool {
	if !i.iter.Next(r) {
		return false
	}
	for idx := range r.Samples {
		r.Samples[idx].Data /= i.by
	}
	return true
}

func (i *divideStepIterator) Err() error {
	return i.iter.Err()
}

func (i *divideStepIterator) Close() error {
	return i.iter.Close()
}
