package main

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/metric"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/router"
	sigmetric "github.com/oteldb/storage/signal/metric"
)

// clusterSink writes a batch into the cluster by routing each shard to its ring primary. odbingest
// holds no data of its own: it frames, routes, and reports what the primaries said.
type clusterSink struct {
	router   *router.Router
	tenantOf cluster.TenantFunc

	accepted metric.Int64Counter
	rejected metric.Int64Counter
}

func newClusterSink(r *router.Router, tenantOf cluster.TenantFunc, mp metric.MeterProvider) (*clusterSink, error) {
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

// WriteMetrics implements promrw.Sink.
//
// A rejection is not an error: the primaries admitted what they could and said why they refused
// the rest, which is a 202 with the counters moving. Only a routing or transport failure fails the
// request, so the sender retries a write that may not have landed and leaves one that did.
func (s *clusterSink) WriteMetrics(ctx context.Context, batch sigmetric.Metrics) error {
	res, err := s.router.WriteMetrics(ctx, batch, s.tenantOf)

	s.accepted.Add(ctx, int64(res.Accepted))
	s.observeRejects(ctx, res.Rejected)

	if err != nil {
		return errors.Wrap(err, "write to cluster")
	}

	return nil
}

func (s *clusterSink) observeRejects(ctx context.Context, rej cluster.Reject) {
	for reason, n := range map[string]int{
		"out_of_order":  rej.OOO,
		"max_series":    rej.Cardinality,
		"max_in_flight": rej.InFlight,
	} {
		if n > 0 {
			s.rejected.Add(ctx, int64(n), metric.WithAttributes(reasonKey.String(reason)))
		}
	}
}
