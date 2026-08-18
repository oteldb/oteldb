package queryapi

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/ogen-go/ogen/ogenerrors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/lokihandler"
)

// LokiOptions configures the Loki API server.
type LokiOptions struct {
	// Config is the Loki API config. Defaults are applied to a copy of it.
	Config config.Loki
	// Querier serves the API.
	Querier LogQuerier
	// Optimizers are appended to [logqlengine.DefaultOptimizers], to let the caller add the ones
	// its storage backend understands.
	Optimizers []logqlengine.Optimizer
	// TracerProvider and MeterProvider instrument the server and the engine.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// NewLoki builds the Loki API server.
func NewLoki(opts LokiOptions) (*lokiapi.Server, error) {
	cfg := opts.Config
	cfg.SetDefaults()

	optimizers := logqlengine.DefaultOptimizers()
	optimizers = append(optimizers, opts.Optimizers...)

	engine, err := logqlengine.NewEngine(opts.Querier, logqlengine.Options{
		ParseOptions: logql.ParseOptions{
			AllowDots: true,
		},
		LookbackDuration: cfg.LookbackDelta,
		Optimizers:       optimizers,
		MeterProvider:    opts.MeterProvider,
		TracerProvider:   opts.TracerProvider,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create LogQL engine")
	}

	handler := lokihandler.NewLokiAPI(opts.Querier, engine, lokihandler.LokiAPIOptions{
		DrilldownEnabled: cfg.DrilldownEnabled,
	})

	return lokiapi.NewServer(handler,
		lokiapi.WithAttributes(attribute.String("oteldb.api", "loki")),
		lokiapi.WithTracerProvider(opts.TracerProvider),
		lokiapi.WithMeterProvider(opts.MeterProvider),
		lokiapi.WithErrorHandler(writeLokiError),
	)
}

// writeLokiError renders an error as the bare JSON string the Loki API returns.
func writeLokiError(_ context.Context, w http.ResponseWriter, _ *http.Request, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(ogenerrors.ErrorCode(err))

	e := jx.GetEncoder()
	defer jx.PutEncoder(e)

	if err != nil {
		e.Str(err.Error())
	} else {
		e.Str("<nil>")
	}

	_, _ = w.Write(e.Bytes())
}
