package odbsafetyprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/multierr"

	"github.com/oteldb/oteldb/internal/odbsafety"
)

type logsProcessor struct {
	next consumer.Logs

	cfg          *Config
	redactFields []string
	handler      *odbsafety.Handler[plog.LogRecord]
	metrics      safetyMetrics
	now          func() time.Time

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
		next:         next,
		cfg:          cfg,
		redactFields: cfg.RedactFields,
		metrics:      newSafetyMetrics(meter, cfg.Workload, cfg.Namespace, cfg.CompactMaxBuckets),
		now:          time.Now,
	}

	sampler := odbsafety.NewSampler(cfg.Config)
	p.handler = odbsafety.NewHandler[plog.LogRecord](cfg.Config, sampler, p.metrics)
	return p
}

func (p *logsProcessor) Start(context.Context, component.Host) error { return nil }

func (p *logsProcessor) Shutdown(context.Context) error { return nil }

func (p *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if len(p.redactFields) == 0 && p.cfg.SoftLimit() <= 0 && p.cfg.HardLimit() <= 0 {
		return p.next.ConsumeLogs(ctx, ld)
	}

	batch := newProcessBatch(ctx, p.handler)
	sawScope := false
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			records := sl.LogRecords()
			batch.records = records
			sawScope = true
			records.RemoveIf(func(record plog.LogRecord) bool {
				if isSafetyRecord(record) {
					return false
				}
				p.redact(record)
				mode := p.excessMode()
				if mode == "" {
					return false
				}
				return batch.handle(mode, records, record)
			})
			return records.Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	// If the incoming batch carried no scope, batch.records has no backing
	// LogRecordSlice to flush synthetic records into. Give it a standalone
	// one and ship it separately, rather than appending into ld blindly.
	var flushed plog.Logs
	if p.handler != nil {
		if !sawScope {
			flushed = plog.NewLogs()
			batch.records = flushed.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		}
		p.handler.Flush(ctx, batch)
	}

	var errs error
	if !sawScope && batch.records.Len() > 0 {
		errs = multierr.Append(errs, p.next.ConsumeLogs(ctx, flushed))
	}
	errs = multierr.Append(errs, p.next.ConsumeLogs(ctx, ld))
	p.metrics.RecordBucketPressure(ctx, p.handler.BucketCount())
	return errs
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

func (p *logsProcessor) excessMode() string {
	if p.cfg.SoftLimit() <= 0 && p.cfg.HardLimit() <= 0 {
		return ""
	}

	now := p.now()
	if p.windowStart.IsZero() || now.Sub(p.windowStart) >= time.Second || now.Before(p.windowStart) {
		p.windowStart = now.Truncate(time.Second)
		p.windowCount = 0
	}
	p.windowCount++

	hard := p.cfg.HardLimit()
	if hard > 0 && p.windowCount > hard {
		return p.cfg.HardMode()
	}
	soft := p.cfg.SoftLimit()
	if soft > 0 && p.windowCount > soft {
		return p.cfg.Mode()
	}
	return ""
}
