package storagebackend

import "go.opentelemetry.io/otel/trace"

// WithTracerProvider traces query adaptation through tp. Nil leaves tracing disabled.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(b *Backend) {
		if tp != nil {
			b.tracer = tp.Tracer(meterName)
		}
	}
}
