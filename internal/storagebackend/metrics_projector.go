package storagebackend

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal"
)

var metricsProjectorPool = sync.Pool{New: func() any { return new(pdataconv.MetricsProjector) }}

func (b *Backend) consumeProjectedMetrics(ctx context.Context, md pmetric.Metrics) (bool, error) {
	projector := metricsProjectorPool.Get().(*pdataconv.MetricsProjector)
	defer func() {
		projector.Reset()
		metricsProjectorPool.Put(projector)
	}()
	if !projector.Prepare(md) {
		return false, nil
	}
	_, err := b.store.WriteProjectedMetrics(ctx, projector.Project)
	if errors.Is(err, storage.ErrMetricProjectionUnsupported) {
		return false, nil
	}
	dropped := projector.Dropped()
	b.countDropped(ctx, signal.Metric, reasonNoValue, dropped.Points)
	b.countDropped(ctx, signal.Metric, reasonExemplar, dropped.Exemplars)
	if err != nil {
		return true, errors.Wrap(err, "write metrics")
	}
	return true, nil
}
