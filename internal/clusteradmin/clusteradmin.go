// Package clusteradmin answers the admin API for a whole storage cluster by fanning out to every
// member node's own admin API and folding the answers together.
//
// It is the aggregation half of cmd/odbadmin. It holds no data and joins no ring: every number it
// reports comes from a member node, so a node that is slow or down subtracts from the answer rather
// than failing it. Where a figure cannot be summed honestly — stored bytes, which replication counts
// once per copy — the report carries both readings instead of picking one; see
// [Aggregator.GetClusterStorage].
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
type NodeClient interface {
	GetInfo(ctx context.Context) (*adminapi.InstanceInfo, error)
	GetHealth(ctx context.Context) (*adminapi.HealthReport, error)
	GetRuntime(ctx context.Context) (*adminapi.RuntimeStats, error)
	GetStorage(ctx context.Context) (*adminapi.StorageStats, error)
	GetEfficiency(ctx context.Context, params adminapi.GetEfficiencyParams) (*adminapi.EfficiencyStats, error)
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
func (a *Aggregator) GetStreamCosts(context.Context, adminapi.GetStreamCostsParams) (*adminapi.StreamCosts, error) {
	return nil, errors.New("stream costs are a per-node drill-down: ask a storage node's admin API directly")
}

// RunAction implements runAction operation. Every action mutates the node it runs on, and one that
// half succeeds across a cluster needs a partial-failure contract this API does not have.
func (a *Aggregator) RunAction(_ context.Context, params adminapi.RunActionParams) (*adminapi.ActionResult, error) {
	return nil, errors.Errorf("action %q does not run cluster-wide: ask a storage node's admin API directly", params.Action)
}

// NewError creates *ErrorStatusCode from error returned by handler.
func (a *Aggregator) NewError(_ context.Context, err error) *adminapi.ErrorStatusCode {
	return &adminapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   adminapi.Error{ErrorMessage: err.Error()},
	}
}
