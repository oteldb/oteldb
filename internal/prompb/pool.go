package prompb

// Reset resets wr.
func (wr *WriteRequest) Reset() {
	for i := range wr.Timeseries {
		wr.Timeseries[i] = TimeSeries{}
	}
	wr.Timeseries = wr.Timeseries[:0]
	wr.pools.Reset()
}

type pools struct {
	Labels  *slicepool[Label]
	Samples *slicepool[Sample]

	// [Exemplar] fields pools.
	Exemplars      *slicepool[Exemplar]
	ExemplarLabels *slicepool[Label]

	// [Histogram] fields pools.
	Histograms *slicepool[Histogram]
	// Negative spans.
	HistogramNegativeSpans  *slicepool[BucketSpan]
	HistogramNegativeDeltas *slicepool[int64]
	HistogramNegativeCounts *slicepool[float64]
	// Positive spans.
	HistogramPositiveSpans  *slicepool[BucketSpan]
	HistogramPositiveDeltas *slicepool[int64]
	HistogramPositiveCounts *slicepool[float64]
	// Custom bucket bounds.
	HistogramCustomValues *slicepool[float64]
}

func (p *pools) init() {
	if p.Labels == nil {
		p.Labels = new(slicepool[Label])
	}
	if p.Samples == nil {
		p.Samples = new(slicepool[Sample])
	}
	if p.Exemplars == nil {
		p.Exemplars = new(slicepool[Exemplar])
	}
	if p.ExemplarLabels == nil {
		p.ExemplarLabels = new(slicepool[Label])
	}
	if p.Histograms == nil {
		p.Histograms = new(slicepool[Histogram])
	}
	if p.HistogramNegativeSpans == nil {
		p.HistogramNegativeSpans = new(slicepool[BucketSpan])
	}
	if p.HistogramNegativeDeltas == nil {
		p.HistogramNegativeDeltas = new(slicepool[int64])
	}
	if p.HistogramNegativeCounts == nil {
		p.HistogramNegativeCounts = new(slicepool[float64])
	}
	if p.HistogramPositiveSpans == nil {
		p.HistogramPositiveSpans = new(slicepool[BucketSpan])
	}
	if p.HistogramPositiveDeltas == nil {
		p.HistogramPositiveDeltas = new(slicepool[int64])
	}
	if p.HistogramPositiveCounts == nil {
		p.HistogramPositiveCounts = new(slicepool[float64])
	}
	if p.HistogramCustomValues == nil {
		p.HistogramCustomValues = new(slicepool[float64])
	}
}

func (p *pools) Reset() {
	if p == nil {
		return
	}
	p.Labels.Reset()
	p.Samples.Reset()

	p.Exemplars.Reset()
	p.ExemplarLabels.Reset()

	p.Histograms.Reset()
	p.HistogramNegativeSpans.Reset()
	p.HistogramNegativeDeltas.Reset()
	p.HistogramNegativeCounts.Reset()
	p.HistogramPositiveSpans.Reset()
	p.HistogramPositiveDeltas.Reset()
	p.HistogramPositiveCounts.Reset()
	p.HistogramCustomValues.Reset()
}

type slicepool[T any] struct {
	pool   []T
	offset int
}

func (p *slicepool[T]) Reset() {
	var zero T
	for i := range p.pool {
		p.pool[i] = zero
	}
	p.pool = p.pool[:0]
	p.offset = 0
}

func (p *slicepool[T]) Push(v T) {
	p.pool = append(p.pool, v)
}

func (p *slicepool[T]) GetNext() *T {
	if len(p.pool)+1 < cap(p.pool) {
		// Re-use existing item.
		p.pool = p.pool[:len(p.pool)+1]
	} else {
		// Allocate a new one.
		var zero T
		p.pool = append(p.pool, zero)
	}
	return &p.pool[len(p.pool)-1]
}

func (p *slicepool[T]) Cut() []T {
	cut := p.pool[p.offset:len(p.pool):len(p.pool)]
	if len(cut) == 0 {
		return nil
	}
	p.offset = len(p.pool)
	return cut
}
