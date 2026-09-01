package clusteradmin

import (
	"context"
	"slices"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// forward answers a request addressed to one named member, by asking that member and returning its
// answer as it stands. It is the counterpart to [fanout]: same peers, same per-node timeout, but no
// aggregation, so what comes back is exactly one node's own report.
//
// A failure here is the whole answer's failure, unlike a fan-out's, where a missing node only
// subtracts its share: a request that named a node has no useful partial form.
func forward[T any](
	ctx context.Context, a *Aggregator, name, node string, call func(context.Context, Peer) (T, error),
) (_ T, rerr error) {
	ctx, span := a.tracer.Start(ctx, "clusteradmin.forward",
		trace.WithAttributes(
			attribute.String("clusteradmin.op", name),
			attribute.String("clusteradmin.node", node),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
			span.SetStatus(codes.Error, rerr.Error())
		}
		span.End()
	}()

	var zero T

	peer, err := a.peer(node)
	if err != nil {
		return zero, err
	}
	span.SetAttributes(attribute.String("clusteradmin.addr", peer.Addr))

	ctx, cancel := context.WithTimeout(ctx, a.opts.Timeout)
	defer cancel()

	v, err := call(ctx, peer)
	if err != nil {
		return zero, errors.Wrapf(err, "node %s", node)
	}

	return v, nil
}

// peer resolves a member by its ring id. An unknown id names the members that do exist: the caller
// picked from a list that may have changed under it, and the current list is the answer to that.
func (a *Aggregator) peer(node string) (Peer, error) {
	peers, err := a.opts.Peers.Peers()
	if err != nil {
		return Peer{}, errors.Wrap(err, "resolve cluster members")
	}

	for _, p := range peers {
		if p.Node == node {
			return p, nil
		}
	}

	known := make([]string, 0, len(peers))
	for _, p := range peers {
		known = append(known, p.Node)
	}
	slices.Sort(known)

	if len(known) == 0 {
		return Peer{}, errors.Errorf("node %q is not a member: the ring is empty", node)
	}

	return Peer{}, errors.Errorf("node %q is not a member: known members are %s", node, strings.Join(known, ", "))
}

// GetClusterNodes implements getClusterNodes operation.
//
// Membership plus reachability, which is what a client needs to offer a node to address. The probe
// is /api/v1/info: a node's cheapest call, and one whose failure means the same thing an operator
// wants the selector to show — that picking this member would not answer.
func (a *Aggregator) GetClusterNodes(ctx context.Context) (*adminapi.ClusterNodes, error) {
	answers, err := fanout(ctx, a, "cluster/nodes",
		func(ctx context.Context, p Peer) (*adminapi.InstanceInfo, error) {
			return p.Client.GetInfo(ctx)
		},
	)
	if err != nil {
		return nil, err
	}

	return &adminapi.ClusterNodes{Nodes: nodeStatuses(answers)}, nil
}
