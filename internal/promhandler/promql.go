package promhandler

import (
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/oteldb/oteldb/internal/promapi"
)

func mapResult(query string, r *promql.Result) (*promapi.QueryResponse, error) {
	if err := r.Err; err != nil {
		return nil, executionErr("query", err)
	}

	warnings, infos := r.Warnings.AsStrings(query, 0, 0)
	resp := &promapi.QueryResponse{
		Status:   "success",
		Warnings: warnings,
		Infos:    infos,
	}
	switch r := r.Value.(type) {
	case promql.Matrix:
		mat := promapi.Matrix{
			Result: make([]promapi.MatrixResultItem, len(r)),
		}
		for i, e := range r {
			var values []promapi.FPoint
			if len(e.Floats) > 0 {
				values = make([]promapi.FPoint, len(e.Floats))
				for i, val := range e.Floats {
					values[i] = mapFPoint(val.T, val.F)
				}
			}
			var histograms []promapi.HPoint
			if len(e.Histograms) > 0 {
				histograms = make([]promapi.HPoint, len(e.Histograms))
				for i, val := range e.Histograms {
					histograms[i] = mapHPoint(val.T, val.H)
				}
			}
			mat.Result[i] = promapi.MatrixResultItem{
				Metric:     e.Metric.Map(),
				Values:     values,
				Histograms: histograms,
			}
		}
		resp.Data.SetMatrix(mat)
	case promql.Vector:
		vec := promapi.Vector{
			Result: make([]promapi.VectorResultItem, len(r)),
		}
		for i, e := range r {
			vec.Result[i] = promapi.VectorResultItem{
				Metric: e.Metric.Map(),
				Value:  mapSample(e),
			}
		}
		resp.Data.SetVector(vec)
	case promql.Scalar:
		resp.Data.SetScalar(promapi.Scalar{
			Result: mapFPoint(r.T, r.V),
		})
	case promql.String:
		resp.Data.SetString(promapi.String{
			Result: promapi.StringValue{
				T: apiTimestamp(r.T),
				V: r.V,
			},
		})
	default:
		return nil, errors.Errorf("unknown value %T", r)
	}

	return resp, nil
}

func mapSample(s promql.Sample) promapi.Sample {
	var val promapi.HistogramOrValue
	if h := s.H; h != nil {
		val.SetHistogram(mapHistogram(h))
	} else {
		val.SetStringFloat64(s.F)
	}
	return promapi.Sample{
		T:                apiTimestamp(s.T),
		HistogramOrValue: val,
	}
}

func mapFPoint(t int64, v float64) promapi.FPoint {
	return promapi.FPoint{
		T: apiTimestamp(t),
		V: v,
	}
}

func mapHPoint(t int64, h *histogram.FloatHistogram) promapi.HPoint {
	return promapi.HPoint{
		T:  apiTimestamp(t),
		V1: mapHistogram(h),
	}
}

func mapHistogram(h *histogram.FloatHistogram) promapi.Histogram {
	var (
		buckets []promapi.Bucket
		iter    = h.AllBucketIterator()
	)
	for iter.Next() {
		bucket := iter.At()
		if bucket.Count == 0 {
			continue // No need to expose empty buckets in JSON.
		}

		boundaries := 2 // Exclusive on both sides AKA open interval.
		if bucket.LowerInclusive {
			if bucket.UpperInclusive {
				boundaries = 3 // Inclusive on both sides AKA closed interval.
			} else {
				boundaries = 1 // Inclusive only on lower end AKA right open.
			}
		} else {
			if bucket.UpperInclusive {
				boundaries = 0 // Inclusive only on upper end AKA left open.
			}
		}
		buckets = append(buckets, promapi.Bucket{
			BoundaryType: boundaries,
			Lower:        bucket.Lower,
			Upper:        bucket.Upper,
			Count:        bucket.Count,
		})
	}
	return promapi.Histogram{
		Count:   h.Count,
		Sum:     h.Sum,
		Buckets: buckets,
	}
}

func apiTimestamp(t int64) float64 {
	return float64(t) / 1000
}
