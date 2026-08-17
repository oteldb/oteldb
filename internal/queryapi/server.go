// Package queryapi builds the query APIs (PromQL, LogQL, TraceQL, ProfileQL) oteldb binaries
// serve.
//
// Each builder returns a constructed ogen server, and [HTTPServer] wraps one into an
// [http.Server] with the standard middleware stack. Serving it, resolving its address and any
// authentication are left to the caller, since those differ per binary.
package queryapi

import (
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

// ServerOptions configures the HTTP server an ogen server is wrapped into.
type ServerOptions struct {
	// Name identifies the API in logs and metrics.
	Name string
	// Addr is the listen address.
	Addr string
	// Logger is injected into the request context.
	Logger *zap.Logger
	// Metrics instruments the server.
	Metrics httpmiddleware.Metrics
	// ReadHeaderTimeout bounds how long a client may take to send its headers. Zero ⇒ 15s.
	ReadHeaderTimeout time.Duration
}

// HTTPServer wraps an ogen server into an HTTP server with the standard middleware stack: logger
// injection, instrumentation, request logging and query explanation, followed by extra.
func HTTPServer[
	R httpmiddleware.OgenRoute,
	S interface {
		httpmiddleware.OgenServer[R]
		http.Handler
	},
](opts ServerOptions, server S, extra ...httpmiddleware.Middleware) *http.Server {
	routeFinder := httpmiddleware.MakeRouteFinder(server)

	middlewares := []httpmiddleware.Middleware{
		httpmiddleware.InjectLogger(opts.Logger),
		httpmiddleware.Instrument(opts.Addr, opts.Name, routeFinder, opts.Metrics),
		httpmiddleware.LogRequests(routeFinder),
		httpmiddleware.Explain(),
	}
	middlewares = append(middlewares, extra...)

	readHeaderTimeout := opts.ReadHeaderTimeout
	if readHeaderTimeout == 0 {
		readHeaderTimeout = 15 * time.Second
	}

	return &http.Server{
		Addr:              opts.Addr,
		Handler:           httpmiddleware.Wrap(server, middlewares...),
		ReadHeaderTimeout: readHeaderTimeout,
	}
}
