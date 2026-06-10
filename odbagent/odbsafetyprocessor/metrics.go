package odbsafetyprocessor

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type safetyMetrics struct {
	dropped        metric.Int64Counter
	sampled        metric.Int64Counter
	compacted      metric.Int64Counter
	collapsedCount metric.Int64Counter
	attrs          metric.MeasurementOption
}

func newSafetyMetrics(meter metric.Meter, workload, namespace string) safetyMetrics {
	m := safetyMetrics{
		attrs: metric.WithAttributes(
			attribute.String("workload", workload),
			attribute.String("namespace", namespace),
		),
	}
	m.dropped, _ = meter.Int64Counter(
		"oteldb_safety_dropped_records_total",
		metric.WithDescription("Volume lost to safety limiting."),
		metric.WithUnit("{records}"),
	)
	m.sampled, _ = meter.Int64Counter(
		"oteldb_safety_sampled_records_total",
		metric.WithDescription("Volume reduced by safety sampling."),
		metric.WithUnit("{records}"),
	)
	m.compacted, _ = meter.Int64Counter(
		"oteldb_safety_compacted_records_total",
		metric.WithDescription("Records compacted by safety limiting."),
		metric.WithUnit("{records}"),
	)
	m.collapsedCount, _ = meter.Int64Counter(
		"oteldb_safety_collapsed_count_total",
		metric.WithDescription("Original records represented by compacted records."),
		metric.WithUnit("{records}"),
	)
	return m
}

func (m safetyMetrics) addDropped(ctx context.Context, n int64, reason string) {
	if m.dropped == nil || n == 0 {
		return
	}
	m.dropped.Add(ctx, n, m.attrs, metric.WithAttributes(attribute.String("reason", reason)))
}

// Dropped implements the shared safety metrics interface.
func (m safetyMetrics) Dropped(ctx context.Context, reason string) {
	m.addDropped(ctx, 1, reason)
}

func (m safetyMetrics) addSampled(ctx context.Context, n int64) {
	if m.sampled == nil || n == 0 {
		return
	}
	m.sampled.Add(ctx, n, m.attrs)
}

// Sampled implements the shared safety metrics interface.
func (m safetyMetrics) Sampled(ctx context.Context) {
	m.addSampled(ctx, 1)
}

func (m safetyMetrics) addCompacted(ctx context.Context, n int64) {
	if m.compacted == nil || n == 0 {
		return
	}
	m.compacted.Add(ctx, n, m.attrs)
}

// Compacted implements the shared safety metrics interface.
func (m safetyMetrics) Compacted(ctx context.Context) {
	m.addCompacted(ctx, 1)
}

func (m safetyMetrics) addCollapsedCount(ctx context.Context, n int64) {
	if m.collapsedCount == nil || n == 0 {
		return
	}
	m.collapsedCount.Add(ctx, n, m.attrs)
}

// Collapsed implements the shared safety metrics interface.
func (m safetyMetrics) Collapsed(ctx context.Context) {
	m.addCollapsedCount(ctx, 1)
}
