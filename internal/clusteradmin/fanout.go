package clusteradmin

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// nodeAnswer is what one member contributed to a fan-out: its value, or why it has none.
type nodeAnswer[T any] struct {
	Peer  Peer
	Value T
	Err   error
	Took  time.Duration
}

// ok reports whether the node answered.
func (r nodeAnswer[T]) ok() bool { return r.Err == nil }

// fanout asks every peer concurrently, bounding each node separately so one that never answers
// costs the report its own share and nothing more. Failures are collected rather than returned:
// an admin view of a degraded cluster is exactly when a node is missing, so a whole-response error
// would hide the state the operator opened the page to see.
//
// Answers come back in peer order, so an aggregate built from them does not depend on which node
// replied first.
//
// Every node gets a span of its own. The response already carries each node's duration and status,
// so the spans are not there to repeat them: they place the calls on one timeline, showing how much
// of the fan-out actually overlapped and how a node's answer split between the aggregator's wait and
// the node's own work, which the node's server span continues under the same trace.
func fanout[T any](
	ctx context.Context, a *Aggregator, name string, call func(context.Context, Peer) (T, error),
) (_ []nodeAnswer[T], rerr error) {
	ctx, span := a.tracer.Start(ctx, "clusteradmin.fanout",
		trace.WithAttributes(attribute.String("clusteradmin.op", name)),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
			span.SetStatus(codes.Error, rerr.Error())
		}
		span.End()
	}()

	peers, err := a.opts.Peers.Peers()
	if err != nil {
		return nil, errors.Wrap(err, "resolve cluster members")
	}

	sort.Slice(peers, func(i, j int) bool { return peers[i].Node < peers[j].Node })

	span.SetAttributes(attribute.Int("clusteradmin.peers", len(peers)))

	out := make([]nodeAnswer[T], len(peers))

	var wg sync.WaitGroup
	for i, p := range peers {
		wg.Go(func() {
			nodeCtx, cancel := context.WithTimeout(ctx, a.opts.Timeout)
			defer cancel()

			nodeCtx, nodeSpan := a.tracer.Start(nodeCtx, "clusteradmin.fanout.node",
				trace.WithAttributes(
					attribute.String("clusteradmin.op", name),
					attribute.String("clusteradmin.node", p.Node),
					attribute.String("clusteradmin.addr", p.Addr),
				),
			)
			defer nodeSpan.End()

			started := time.Now()
			v, err := call(nodeCtx, p)
			out[i] = nodeAnswer[T]{Peer: p, Value: v, Err: err, Took: time.Since(started)}

			if err != nil {
				nodeSpan.RecordError(err)
				nodeSpan.SetStatus(codes.Error, err.Error())

				a.opts.Logger.Warn("Node did not answer",
					zap.String("op", name), zap.String("node", p.Node), zap.String("addr", p.Addr),
					zap.Error(err),
				)
			}
		})
	}

	wg.Wait()

	return out, nil
}

// nodeStatuses renders a fan-out's per-node outcome, so a partial answer is visibly partial.
func nodeStatuses[T any](answers []nodeAnswer[T]) []adminapi.ClusterNodeStatus {
	out := make([]adminapi.ClusterNodeStatus, 0, len(answers))
	for _, r := range answers {
		s := adminapi.ClusterNodeStatus{
			Node:            r.Peer.Node,
			Status:          adminapi.ClusterNodeStateOk,
			DurationSeconds: adminapi.NewOptFloat64(r.Took.Seconds()),
		}
		if r.Peer.Addr != "" {
			s.Addr = adminapi.NewOptString(r.Peer.Addr)
		}
		if r.Err != nil {
			s.Status = adminapi.ClusterNodeStateUnreachable
			s.Error = adminapi.NewOptString(r.Err.Error())
		}
		out = append(out, s)
	}

	return out
}
