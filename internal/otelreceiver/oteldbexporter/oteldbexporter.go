// Package oteldbexporter contains oteldb exporter factory.
package oteldbexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logparser"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

const (
	typeStr   = "oteldbexporter"
	stability = component.StabilityLevelDevelopment
)

var typ = component.MustNewType(typeStr)

// MetricsSink ingests OTLP metrics batches. When set on the factory (see
// [WithMetricsSink]) the metrics exporter writes to it instead of dialing ClickHouse,
// letting the embedded storage engine serve the metrics signal.
type MetricsSink interface {
	ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesSink ingests OTLP traces batches. When set on the factory (see [WithTracesSink]) the
// traces exporter writes to it instead of dialing ClickHouse, letting the embedded storage
// engine serve the traces signal.
type TracesSink interface {
	ConsumeTraces(ctx context.Context, td ptrace.Traces) error
}

// LogsSink ingests OTLP logs batches. When set on the factory (see [WithLogsSink]) the logs
// exporter writes to it instead of dialing ClickHouse, letting the embedded storage engine
// serve the logs signal.
type LogsSink interface {
	ConsumeLogs(ctx context.Context, ld plog.Logs) error
}

// ProfilesSink ingests OTLP profiles batches. Profiles have no ClickHouse implementation, so the
// exporter's profiles signal is only available when a sink is set (see [WithProfilesSink]); the
// embedded storage engine then serves the profiles signal.
type ProfilesSink interface {
	ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error
}

// Option configures the exporter factory.
type Option func(*factory)

// WithMetricsSink routes ingested metrics to sink instead of ClickHouse.
func WithMetricsSink(sink MetricsSink) Option {
	return func(f *factory) { f.metricsSink = sink }
}

// WithTracesSink routes ingested traces to sink instead of ClickHouse.
func WithTracesSink(sink TracesSink) Option {
	return func(f *factory) { f.tracesSink = sink }
}

// WithLogsSink routes ingested logs to sink instead of ClickHouse.
func WithLogsSink(sink LogsSink) Option {
	return func(f *factory) { f.logsSink = sink }
}

// WithProfilesSink enables the exporter's profiles signal, routing ingested profiles to sink (the
// embedded storage engine). Without it the exporter does not support the profiles signal.
func WithProfilesSink(sink ProfilesSink) Option {
	return func(f *factory) { f.profilesSink = sink }
}

type factory struct {
	metricsSink  MetricsSink
	tracesSink   TracesSink
	logsSink     LogsSink
	profilesSink ProfilesSink
}

// NewFactory creates new factory of [Exporter]. It always supports traces, metrics, and logs; the
// experimental profiles signal is additionally supported when a profiles sink is configured.
func NewFactory(opts ...Option) exporter.Factory {
	f := &factory{}
	for _, o := range opts {
		o(f)
	}
	xopts := []xexporter.FactoryOption{
		xexporter.WithTraces(f.createTracesExporter, stability),
		xexporter.WithMetrics(f.createMetricsExporter, stability),
		xexporter.WithLogs(f.createLogsExporter, stability),
	}
	if f.profilesSink != nil {
		xopts = append(xopts, xexporter.WithProfiles(f.createProfilesExporter, stability))
	}
	return xexporter.NewFactory(typ, createDefaultConfig, xopts...)
}

// createProfilesExporter builds the profiles exporter that writes to the configured profiles sink.
func (f *factory) createProfilesExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (xexporter.Profiles, error) {
	if f.profilesSink == nil {
		return nil, errors.New("profiles ingestion requires the storage backend (set profiles_backend: storage)")
	}
	settings.Logger.Info("Routing profiles to storage backend sink")
	return xexporterhelper.NewProfiles(ctx, settings, cfg, f.profilesSink.ConsumeProfiles)
}

func createDefaultConfig() component.Config {
	return &Config{
		DSN: "clickhouse://localhost:9000",
	}
}

func (f *factory) createTracesExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	ecfg := cfg.(*Config)

	// Route traces to the configured sink (the embedded storage engine) when set, otherwise
	// fall back to ClickHouse.
	var consume func(context.Context, ptrace.Traces) error
	if f.tracesSink != nil {
		settings.Logger.Info("Routing traces to storage backend sink")
		consume = f.tracesSink.ConsumeTraces
	} else {
		inserter, err := ecfg.connect(ctx, settings)
		if err != nil {
			return nil, err
		}
		consume = tracestorage.NewConsumer(inserter).ConsumeTraces
	}

	if ecfg.Traces.Spans.enabled() {
		spansCfg := ecfg.Traces.Spans
		settings.Logger.Info("Span sampling enabled",
			zap.Bool("drop", spansCfg.Drop),
			zap.Float64("rate", spansCfg.Rate),
		)
		inner := consume
		consume = func(ctx context.Context, td ptrace.Traces) error {
			sampleSpans(td, spansCfg)
			return inner(ctx, td)
		}
	}
	return exporterhelper.NewTraces(ctx, settings, cfg, consume)
}

func (f *factory) createMetricsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	ecfg := cfg.(*Config)

	// Route metrics to the configured sink (the embedded storage engine) when set,
	// otherwise fall back to ClickHouse. Traces and logs always use ClickHouse.
	var consume func(context.Context, pmetric.Metrics) error
	if f.metricsSink != nil {
		settings.Logger.Info("Routing metrics to storage backend sink")
		consume = f.metricsSink.ConsumeMetrics
	} else {
		inserter, err := ecfg.connect(ctx, settings)
		if err != nil {
			return nil, err
		}
		consume = inserter.ConsumeMetrics
	}

	if ecfg.Metrics.Exemplars.enabled() {
		exemplarsCfg := ecfg.Metrics.Exemplars
		settings.Logger.Info("Exemplar sampling enabled",
			zap.Bool("drop", exemplarsCfg.Drop),
			zap.Float64("rate", exemplarsCfg.Rate),
		)
		inner := consume
		consume = func(ctx context.Context, md pmetric.Metrics) error {
			sampleExemplars(md, exemplarsCfg)
			return inner(ctx, md)
		}
	}
	return exporterhelper.NewMetrics(ctx, settings, cfg, consume)
}

func (f *factory) createLogsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	ecfg := cfg.(*Config)

	lg := settings.Logger

	// Route logs to the configured sink (the embedded storage engine) when set, bypassing the
	// ClickHouse log-processing consumer. Record sampling still applies.
	if f.logsSink != nil {
		lg.Info("Routing logs to storage backend sink")
		consume := f.logsSink.ConsumeLogs
		if ecfg.Logs.Records.enabled() {
			recordsCfg := ecfg.Logs.Records
			inner := consume
			consume = func(ctx context.Context, ld plog.Logs) error {
				sampleLogRecords(ld, recordsCfg)
				return inner(ctx, ld)
			}
		}
		return exporterhelper.NewLogs(ctx, settings, cfg, consume)
	}

	procCfg := ecfg.Logs.Processing
	triggerAttrs := parseAttributeRefs(procCfg.TriggerAttributes, "trigger_attributes", lg)
	formatAttrs := parseAttributeRefs(procCfg.FormatAttributes, "format_attributes", lg)
	formats := make([]logparser.Parser, 0, len(procCfg.DetectFormats))
	for _, raw := range procCfg.DetectFormats {
		p, ok := logparser.LookupFormat(raw)
		if !ok {
			lg.Warn("Unknown format", zap.String("format", raw))
			continue
		}
		formats = append(formats, p)
	}
	lg.Info("Creating logs consumer",
		zap.Int("trigger_attributes", len(triggerAttrs)),
		zap.Int("format_attributes", len(triggerAttrs)),
		zap.Stringers("format", formats),
	)

	inserter, err := ecfg.connect(ctx, settings)
	if err != nil {
		return nil, errors.Wrap(err, "connect to ClickHouse")
	}
	consumer, err := logstorage.NewConsumer(inserter, logstorage.ConsumerOptions{
		TriggerAttributes: triggerAttrs,
		FormatAttributes:  formatAttrs,
		DetectFormats:     formats,
		MeterProvider:     settings.MeterProvider,
		TracerProvider:    settings.TracerProvider,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create consumer")
	}

	consume := consumer.ConsumeLogs
	if ecfg.Logs.Records.enabled() {
		recordsCfg := ecfg.Logs.Records
		lg.Info("Log record sampling enabled",
			zap.Bool("drop", recordsCfg.Drop),
			zap.Float64("rate", recordsCfg.Rate),
		)
		consume = func(ctx context.Context, ld plog.Logs) error {
			sampleLogRecords(ld, recordsCfg)
			return consumer.ConsumeLogs(ctx, ld)
		}
	}
	return exporterhelper.NewLogs(ctx, settings, cfg, consume)
}

func parseAttributeRefs(refs []string, field string, lg *zap.Logger) []logstorage.AttributeRef {
	result := make([]logstorage.AttributeRef, 0, len(refs))
	for _, raw := range refs {
		ref, err := logstorage.ParseAttributeRef(raw)
		if err != nil {
			lg.Warn("Invalid attribute ref, ignoring", zap.String("field", field), zap.String("ref", raw))
			continue
		}
		result = append(result, ref)
	}
	return result
}
