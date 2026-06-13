package odbsafetyprocessor

import (
	"context"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/oteldb/oteldb/internal/odbsafety"
)

type logsProcessor struct {
	next consumer.Logs

	maxRatePerSecond int
	redactFields     []string
	handler          *odbsafety.Handler[plog.LogRecord]
	metrics          safetyMetrics
	now              func() time.Time

	windowStart time.Time
	windowCount int
}

var _ processor.Logs = (*logsProcessor)(nil)

func newLogsProcessor(settings processor.Settings, cfg *Config, next consumer.Logs) *logsProcessor {
	if settings.MeterProvider == nil {
		settings.MeterProvider = metricnoop.NewMeterProvider()
	}
	meter := settings.MeterProvider.Meter("odbagent/odbsafetyprocessor")
	p := &logsProcessor{
		next:             next,
		maxRatePerSecond: cfg.MaxRatePerSecond,
		redactFields:     cfg.RedactFields,
		metrics:          newSafetyMetrics(meter, cfg.Workload, cfg.Namespace, cfg.CompactMaxBuckets),
		now:              time.Now,
	}

	sampler := func() bool { return rand.Float64() < cfg.SampleRate } //#nosec G404
	p.handler = odbsafety.NewHandler[plog.LogRecord](cfg.Config, sampler, p.metrics)
	return p
}

func (p *logsProcessor) Start(context.Context, component.Host) error { return nil }

func (p *logsProcessor) Shutdown(context.Context) error { return nil }

func (p *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if len(p.redactFields) == 0 && (p.maxRatePerSecond == 0 || p.handler == nil) {
		return p.next.ConsumeLogs(ctx, ld)
	}

	batch := newProcessBatch(ctx, p.handler)
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			records := sl.LogRecords()
			records.RemoveIf(func(record plog.LogRecord) bool {
				if isSafetyRecord(record) {
					return false
				}
				p.redact(record)
				if !p.exceeded() {
					return false
				}
				return batch.handle(records, record)
			})
			return records.Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	err := p.next.ConsumeLogs(ctx, ld)
	p.metrics.RecordBucketPressure(ctx, p.handler.BucketCount())
	return err
}

func isSafetyRecord(record plog.LogRecord) bool {
	attrs := record.Attributes()
	if _, ok := attrs.Get("oteldb.collapsed_count"); ok {
		return true
	}
	if _, ok := attrs.Get("oteldb.truncated_count"); ok {
		return true
	}
	return false
}

func (p *logsProcessor) redact(record plog.LogRecord) {
	attrs := record.Attributes()
	for _, field := range p.redactFields {
		if value, ok := attrs.Get(field); ok {
			value.SetStr("<redacted>")
		}
	}
}

func (p *logsProcessor) exceeded() bool {
	if p.maxRatePerSecond <= 0 {
		return false
	}

	now := p.now()
	if p.windowStart.IsZero() || now.Sub(p.windowStart) >= time.Second || now.Before(p.windowStart) {
		p.windowStart = now.Truncate(time.Second)
		p.windowCount = 0
	}
	p.windowCount++
	return p.windowCount > p.maxRatePerSecond
}
