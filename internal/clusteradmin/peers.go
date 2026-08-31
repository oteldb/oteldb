package clusteradmin

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/cluster/etcd"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// Membership is the live ring view peers are resolved from. *router.Router implements it.
type Membership interface {
	Members() []etcd.Member
}

// RingPeers resolves the ring's members to their admin APIs.
//
// A member advertises the address its peers reach it on for cluster RPCs, not for its admin API, so
// the admin endpoint is that address's host with the configured admin port. This assumes every node
// serves its admin API on the same port, which is what a homogeneous storage deployment does.
//
// Clients are cached per node: an ogen client owns a connection pool, and rebuilding it per request
// would open a fresh connection to every node on every poll.
type RingPeers struct {
	members Membership
	scheme  string
	port    int

	tracerProvider trace.TracerProvider
	// httpClient is shared by every node's client so the outbound call carries the trace context to
	// the node, whose server span then continues it.
	httpClient *http.Client

	mu      sync.Mutex
	clients map[string]NodeClient
}

var _ PeerSet = (*RingPeers)(nil)

// RingPeersOptions configures [RingPeers].
type RingPeersOptions struct {
	// Members is the live ring view. Required.
	Members Membership
	// Scheme is the admin API scheme, "http" or "https". Required.
	Scheme string
	// Port is the admin API port every node serves on. Required.
	Port int
	// TracerProvider instruments the calls to the nodes. Nil selects the global provider, which is a
	// noop unless the process configures one.
	TracerProvider trace.TracerProvider
}

// NewRingPeers resolves ring members to admin APIs at scheme://<member host>:port.
func NewRingPeers(opts RingPeersOptions) (*RingPeers, error) {
	if opts.Members == nil {
		return nil, errors.New("membership is required")
	}
	if opts.Port <= 0 || opts.Port > 65535 {
		return nil, errors.Errorf("admin port %d is out of range", opts.Port)
	}
	switch opts.Scheme {
	case "http", "https":
	default:
		return nil, errors.Errorf("unsupported scheme %q", opts.Scheme)
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}

	return &RingPeers{
		members:        opts.Members,
		scheme:         opts.Scheme,
		port:           opts.Port,
		tracerProvider: opts.TracerProvider,
		httpClient: &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport,
				otelhttp.WithTracerProvider(opts.TracerProvider),
			),
		},
		clients: map[string]NodeClient{},
	}, nil
}

// Peers implements [PeerSet].
func (r *RingPeers) Peers() ([]Peer, error) {
	members := r.members.Members()

	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]Peer, 0, len(members))
	live := make(map[string]struct{}, len(members))

	for _, m := range members {
		addr, err := r.endpoint(m)
		if err != nil {
			return nil, err
		}

		client, ok := r.clients[addr]
		if !ok {
			client, err = adminapi.NewClient(addr,
				adminapi.WithTracerProvider(r.tracerProvider),
				adminapi.WithClient(r.httpClient),
			)
			if err != nil {
				return nil, errors.Wrapf(err, "admin client for %s", m.ID)
			}
			r.clients[addr] = client
		}

		live[addr] = struct{}{}
		out = append(out, Peer{Node: m.ID, Addr: addr, Client: client})
	}

	// A node that leaves the ring must not keep its connection pool alive for the rest of the
	// process's life.
	for addr := range r.clients {
		if _, ok := live[addr]; !ok {
			delete(r.clients, addr)
		}
	}

	return out, nil
}

// endpoint is the member's admin API base URL. A member with no advertised address falls back to
// its ring id, which is the hostname in every deployment that derives one from the other.
func (r *RingPeers) endpoint(m etcd.Member) (string, error) {
	host := m.Addr
	if host == "" {
		host = m.ID
	}
	if host == "" {
		return "", errors.New("ring member has neither an address nor an id")
	}

	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	u := url.URL{Scheme: r.scheme, Host: net.JoinHostPort(host, strconv.Itoa(r.port))}

	return u.String(), nil
}
