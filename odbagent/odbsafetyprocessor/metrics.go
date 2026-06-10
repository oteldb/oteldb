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
	bucketPressure metric.Float64Gauge
	maxBuckets     int
	attrs          metric.MeasurementOption
}

func mustInstrument[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func newSafetyMetrics(meter metric.Meter, workload, namespace string, maxBuckets int) safetyMetrics {
	m := safetyMetrics{
		maxBuckets: maxBuckets,
		attrs: metric.WithAttributes(
			attribute.String("workload", workload),
			attribute.String("namespace", namespace),
		),
	}
	m.dropped = mustInstrument(meter.Int64Counter(
		"oteldb_safety_dropped_records_total",
		metric.WithDescription("Volume lost to safety limiting."),
		metric.WithUnit("{records}"),
	))
	m.sampled = mustInstrument(meter.Int64Counter(
		"oteldb_safety_sampled_records_total",
		metric.WithDescription("Volume reduced by safety sampling."),
		metric.WithUnit("{records}"),
	))
	m.compacted = mustInstrument(meter.Int64Counter(
		"oteldb_safety_compacted_records_total",
		metric.WithDescription("Records compacted by safety limiting."),
		metric.WithUnit("{records}"),
	))
	m.collapsedCount = mustInstrument(meter.Int64Counter(
		"oteldb_safety_collapsed_count_total",
		metric.WithDescription("Original records represented by compacted records."),
		metric.WithUnit("{records}"),
	))
	m.bucketPressure = mustInstrument(meter.Float64Gauge(
		"oteldb_safety_compact_bucket_pressure",
		metric.WithDescription("Fraction of compactMaxBuckets in use; near 1.0 signals LRU churn."),
		metric.WithUnit("1"),
	))
	return m
}

// Dropped implements the shared safety metrics interface.
func (m safetyMetrics) Dropped(ctx context.Context, reason string) {
	m.dropped.Add(ctx, 1, m.attrs, metric.WithAttributes(attribute.String("reason", reason)))
}

// Sampled implements the shared safety metrics interface.
func (m safetyMetrics) Sampled(ctx context.Context) {
	m.sampled.Add(ctx, 1, m.attrs)
}

// Compacted implements the shared safety metrics interface.
func (m safetyMetrics) Compacted(ctx context.Context) {
	m.compacted.Add(ctx, 1, m.attrs)
}

// Collapsed implements the shared safety metrics interface.
func (m safetyMetrics) Collapsed(ctx context.Context) {
	m.collapsedCount.Add(ctx, 1, m.attrs)
}

// RecordBucketPressure records the fraction of compactMaxBuckets currently in use.
func (m safetyMetrics) RecordBucketPressure(ctx context.Context, count int) {
	if m.maxBuckets == 0 {
		return
	}
	m.bucketPressure.Record(ctx, float64(count)/float64(m.maxBuckets), m.attrs)
}
