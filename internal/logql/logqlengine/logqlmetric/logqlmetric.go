// Package logqlmetric provides metric queries implementation.
package logqlmetric

import (
	"strconv"
	"time"

	"golang.org/x/exp/maps"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Step represents a one query range step i
type Step struct {
	Timestamp otelstorage.Timestamp
	Samples   []Sample
}

// StepIterator is a one query range step iterator.
type StepIterator = iterators.Iterator[Step]

// ReadStepResponse reads aggregation result into API structure.
func ReadStepResponse(iter StepIterator, instant bool) (s lokiapi.QueryResponseData, _ error) {
	var (
		agg          Step
		matrixSeries map[logqlabels.GroupingKey]lokiapi.Series
	)
	for iter.Next(&agg) {
		if instant {
			if err := iter.Err(); err != nil {
				return s, err
			}

			var vector lokiapi.Vector
			for _, s := range agg.Samples {
				vector = append(vector, lokiapi.Sample{
					Metric: lokiapi.NewOptLabelSet(s.Set.AsLokiAPI()),
					Value: lokiapi.FPoint{
						T: getPrometheusTimestamp(agg.Timestamp.AsTime()),
						V: strconv.FormatFloat(s.Data, 'f', -1, 64),
					},
				})
			}

			s.SetVectorResult(lokiapi.VectorResult{
				Result: vector,
			})
			return s, nil
		}

		if matrixSeries == nil {
			matrixSeries = map[logqlabels.GroupingKey]lokiapi.Series{}
		}
		for _, s := range agg.Samples {
			key := s.Set.Key()
			ser, ok := matrixSeries[key]
			if !ok {
				ser.Metric.SetTo(s.Set.AsLokiAPI())
			}

			ser.Values = append(ser.Values, lokiapi.FPoint{
				T: getPrometheusTimestamp(agg.Timestamp.AsTime()),
				V: strconv.FormatFloat(s.Data, 'f', -1, 64),
			})
			matrixSeries[key] = ser
		}
	}
	if err := iter.Err(); err != nil {
		return s, err
	}

	s.SetMatrixResult(lokiapi.MatrixResult{
		Result: maps.Values(matrixSeries),
	})
	return s, nil
}

func getPrometheusTimestamp(t time.Time) float64 {
	// Pass milliseconds as fraction part.
	return float64(t.UnixMilli()) / 1000
}
