// Package clusteradmin answers the admin API for a whole storage cluster by fanning out to every
// member node's own admin API and folding the answers together.
//
// It is the aggregation half of cmd/odbadmin. It holds no data and joins no ring: every number it
// reports comes from a member node, so a node that is slow or down subtracts from the answer rather
// than failing it. Where a figure cannot be summed honestly — stored bytes, which replication counts
// once per copy — the report carries both readings instead of picking one; see
// [Aggregator.GetClusterStorage].
//
// A request can also name one member (the `node` query parameter, listed by
// [Aggregator.GetClusterNodes]), which is forwarded to that member and answered verbatim. That is
// what makes the per-node-only operations reachable here at all: they are refused as a fan-out and
// ordinary once addressed.
package clusteradmin

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// NodeClient is the part of a member node's admin API the aggregator reads. *adminapi.Client
// implements it; a test supplies a fake.
//
// The last two are never fanned out — see [Aggregator.GetStreamCosts] and [Aggregator.RunAction] —
// and are here only to forward a request that named a single node.
type NodeClient interface {
	GetInfo(ctx context.Context) (*adminapi.InstanceInfo, error)
	GetHealth(ctx context.Context, params adminapi.GetHealthParams) (*adminapi.HealthReport, error)
	GetRuntime(ctx context.Context, params adminapi.GetRuntimeParams) (*adminapi.RuntimeStats, error)
	GetStorage(ctx context.Context, params adminapi.GetStorageParams) (*adminapi.StorageStats, error)
	GetEfficiency(ctx context.Context, params adminapi.GetEfficiencyParams) (*adminapi.EfficiencyStats, error)
	GetStreamCosts(ctx context.Context, params adminapi.GetStreamCostsParams) (*adminapi.StreamCosts, error)
	RunAction(ctx context.Context, params adminapi.RunActionParams) (*adminapi.ActionResult, error)
}

// Peer is one member node and the admin API endpoint the aggregator reaches it on.
type Peer struct {
	// Node is the member's ring id.
	Node string
	// Addr is the base URL of its admin API, reported so a partial answer names the target that
	// failed rather than only the node that owns it.
	Addr   string
	Client NodeClient
}

// PeerSet resolves the cluster's current members. It is re-read per request rather than captured at
// startup: membership changes while the process runs, and a stale peer list would report a departed
// node as unreachable forever.
type PeerSet interface {
	Peers() ([]Peer, error)
}

// BuildInfo is static build information about the running aggregator.
type BuildInfo struct {
	Version   string
	Commit    string
	GoVersion string
}

// Options configures [Aggregator].
type Options struct {
	// Peers resolves the member nodes to fan out to. Required.
	Peers PeerSet
	// Info is build information about the aggregator process itself.
	Info BuildInfo
	// StartTime is the aggregator's start time, used to compute uptime.
	StartTime time.Time
	// ReplicationFactor is the cluster's configured replication factor, echoed in the storage report
	// so its two byte figures read on their own.
	ReplicationFactor int
	// Timeout bounds one node's answer. Zero ⇒ 10s. It is per node rather than per request: one
	// unresponsive node must cost the report its own share and nothing more.
	Timeout time.Duration
	// Logger records per-node failures, which the response reports but does not explain.
	Logger *zap.Logger
	// TracerProvider provides the OpenTelemetry tracer for the fan-out. Nil selects the global
	// provider, which is a noop unless the process configures one.
	TracerProvider trace.TracerProvider
}

// Aggregator implements adminapi.Handler over a cluster's member nodes.
type Aggregator struct {
	opts   Options
	tracer trace.Tracer
}

var _ adminapi.Handler = (*Aggregator)(nil)

// defaultTimeout bounds one node's answer when none is configured.
const defaultTimeout = 10 * time.Second

// New creates an aggregating admin API handler.
func New(opts Options) (*Aggregator, error) {
	if opts.Peers == nil {
		return nil, errors.New("peers is required: the aggregator has no data of its own to report")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	if opts.StartTime.IsZero() {
		opts.StartTime = time.Now()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}

	return &Aggregator{
		opts:   opts,
		tracer: opts.TracerProvider.Tracer("clusteradmin.Aggregator"),
	}, nil
}

// GetStreamCosts implements getStreamCosts operation. Stream cost attribution decodes every
// accounted column of every live part, so it stays a per-node drill-down: fanned across the cluster
// it would decode each replicated part once per replica to answer one question.
//
// Addressed to one node it is that drill-down, so it is forwarded rather than refused.
func (a *Aggregator) GetStreamCosts(
	ctx context.Context, params adminapi.GetStreamCostsParams,
) (*adminapi.StreamCosts, error) {
	node, ok := params.Node.Get()
	if !ok {
		return nil, errors.New("stream costs are a per-node drill-down: name a node with ?node=, or ask a storage node's admin API directly")
	}

	params.Node.Reset()

	return forward(ctx, a, "storage/stream-costs", node,
		func(ctx context.Context, p Peer) (*adminapi.StreamCosts, error) {
			return p.Client.GetStreamCosts(ctx, params)
		},
	)
}

// RunAction implements runAction operation. Every action mutates the node it runs on, and one that
// half succeeds across a cluster needs a partial-failure contract this API does not have.
//
// That is an argument about the fan-out, not about the action: addressed to one named node there is
// no partial failure to contract for — the action either ran on that node or reports why it did
// not — so a request naming a node is forwarded and only an unaddressed one is refused.
func (a *Aggregator) RunAction(
	ctx context.Context, params adminapi.RunActionParams,
) (*adminapi.ActionResult, error) {
	node, ok := params.Node.Get()
	if !ok {
		return nil, errors.Errorf("action %q does not run cluster-wide: name a node with ?node=, or ask a storage node's admin API directly", params.Action)
	}

	params.Node.Reset()

	return forward(ctx, a, "actions/"+string(params.Action), node,
		func(ctx context.Context, p Peer) (*adminapi.ActionResult, error) {
			return p.Client.RunAction(ctx, params)
		},
	)
}

// NewError creates *ErrorStatusCode from error returned by handler.
func (a *Aggregator) NewError(_ context.Context, err error) *adminapi.ErrorStatusCode {
	return &adminapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   adminapi.Error{ErrorMessage: err.Error()},
	}
}
