package prompb

// Reset resets req so it can decode another request, keeping the buffers it grew.
func (req *WriteRequestV2) Reset() {
	for i := range req.Timeseries {
		req.Timeseries[i] = TimeSeriesV2{}
	}
	req.Timeseries = req.Timeseries[:0]
	req.Symbols = nil
	req.pools.Reset()
}

// poolsV2 holds the slice pools a [WriteRequestV2] is decoded into. The histogram field pools are
// the 1.0 ones, since the Histogram message is identical in both schemas.
type poolsV2 struct {
	Symbols *slicepool[[]byte]
	// TimeseriesData holds each series' undecoded body, collected on the first pass so the symbol
	// table is complete before any ref is resolved.
	TimeseriesData *slicepool[[]byte]
	Timeseries     *slicepool[TimeSeriesV2]

	LabelsRefs *slicepool[uint32]
	Samples    *slicepool[SampleV2]

	Exemplars          *slicepool[ExemplarV2]
	ExemplarLabelsRefs *slicepool[uint32]

	Histograms *slicepool[Histogram]
	histograms *pools
}

func (p *poolsV2) init() {
	if p.Symbols == nil {
		p.Symbols = new(slicepool[[]byte])
	}
	if p.TimeseriesData == nil {
		p.TimeseriesData = new(slicepool[[]byte])
	}
	if p.Timeseries == nil {
		p.Timeseries = new(slicepool[TimeSeriesV2])
	}
	if p.LabelsRefs == nil {
		p.LabelsRefs = new(slicepool[uint32])
	}
	if p.Samples == nil {
		p.Samples = new(slicepool[SampleV2])
	}
	if p.Exemplars == nil {
		p.Exemplars = new(slicepool[ExemplarV2])
	}
	if p.ExemplarLabelsRefs == nil {
		p.ExemplarLabelsRefs = new(slicepool[uint32])
	}
	if p.Histograms == nil {
		p.Histograms = new(slicepool[Histogram])
	}
	if p.histograms == nil {
		p.histograms = &pools{}
		p.histograms.init()
	}
}

func (p *poolsV2) Reset() {
	if p == nil {
		return
	}
	p.Symbols.Reset()
	p.TimeseriesData.Reset()
	p.Timeseries.Reset()

	p.LabelsRefs.Reset()
	p.Samples.Reset()

	p.Exemplars.Reset()
	p.ExemplarLabelsRefs.Reset()

	p.Histograms.Reset()
	p.histograms.Reset()
}
