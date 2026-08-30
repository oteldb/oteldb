package main

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/metric"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/signal/trace"
)

// clusterSink writes a batch into the cluster by routing each shard to its ring primary. odbingest
// holds no data of its own: it frames, routes, and reports what the primaries said.
type clusterSink struct {
	router   *router.Router
	tenantOf tenantFuncOf

	accepted metric.Int64Counter
	rejected metric.Int64Counter
}

// tenantFuncOf derives the routing callback for one write from its request context, so a tenant
// named per request (a header) and one named per resource (an attribute) reach framing the same
// way. A nil tenantFuncOf, or one returning nil, routes to [cluster.DefaultTenant].
type tenantFuncOf func(context.Context) cluster.TenantFunc

func newClusterSink(r *router.Router, tenantOf tenantFuncOf, mp metric.MeterProvider) (*clusterSink, error) {
	meter := mp.Meter("github.com/oteldb/oteldb/cmd/odbingest")

	accepted, err := meter.Int64Counter("odbingest.cluster.accepted_points",
		metric.WithDescription("Points the shard primaries accepted."),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create accepted counter")
	}

	rejected, err := meter.Int64Counter("odbingest.cluster.rejected_points",
		metric.WithDescription("Points the shard primaries rejected, by reason."),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create rejected counter")
	}

	return &clusterSink{router: r, tenantOf: tenantOf, accepted: accepted, rejected: rejected}, nil
}

// WriteMetrics implements promrw.Sink and otlpdirect.Sink.
//
// A rejection is not an error: the primaries admitted what they could and said why they refused
// the rest, which is a success with the counters moving. Only a routing or transport failure fails
// the request, so the sender retries a write that may not have landed and leaves one that did.
func (s *clusterSink) WriteMetrics(ctx context.Context, batch sigmetric.Metrics) error {
	res, err := s.router.WriteMetrics(ctx, batch, s.routing(ctx))

	return s.record(ctx, signal.Metric, res, err)
}

func (s *clusterSink) WriteLogs(ctx context.Context, batch log.Logs) error {
	res, err := s.router.WriteLogs(ctx, batch, s.routing(ctx))

	return s.record(ctx, signal.Log, res, err)
}

func (s *clusterSink) WriteTraces(ctx context.Context, batch trace.Traces) error {
	res, err := s.router.WriteTraces(ctx, batch, s.routing(ctx))

	return s.record(ctx, signal.Trace, res, err)
}

func (s *clusterSink) WriteProfiles(ctx context.Context, batch *profile.Profiles) error {
	res, err := s.router.WriteProfiles(ctx, batch, s.routing(ctx))

	return s.record(ctx, signal.Profile, res, err)
}

func (s *clusterSink) routing(ctx context.Context) cluster.TenantFunc {
	if s.tenantOf == nil {
		return nil
	}

	return s.tenantOf(ctx)
}

// record meters what the primaries said and turns a routing failure into the caller's error.
func (s *clusterSink) record(ctx context.Context, sig signal.Signal, res router.Written, err error) error {
	attr := metric.WithAttributes(signalKey.String(sig.String()))

	s.accepted.Add(ctx, int64(res.Accepted), attr)
	s.observeRejects(ctx, sig, res.Rejected)

	if err != nil {
		return errors.Wrap(err, "write to cluster")
	}

	return nil
}

func (s *clusterSink) observeRejects(ctx context.Context, sig signal.Signal, rej cluster.Reject) {
	for reason, n := range map[string]int{
		"out_of_order":  rej.OOO,
		"max_series":    rej.Cardinality,
		"max_in_flight": rej.InFlight,
	} {
		if n > 0 {
			s.rejected.Add(ctx, int64(n),
				metric.WithAttributes(signalKey.String(sig.String()), reasonKey.String(reason)))
		}
	}
}
