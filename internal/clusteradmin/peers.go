package clusteradmin

import (
	"net"
	"net/url"
	"strconv"
	"sync"

	"github.com/go-faster/errors"

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

	mu      sync.Mutex
	clients map[string]NodeClient
}

var _ PeerSet = (*RingPeers)(nil)

// NewRingPeers resolves ring members to admin APIs at scheme://<member host>:port.
func NewRingPeers(members Membership, scheme string, port int) (*RingPeers, error) {
	if members == nil {
		return nil, errors.New("membership is required")
	}
	if port <= 0 || port > 65535 {
		return nil, errors.Errorf("admin port %d is out of range", port)
	}
	switch scheme {
	case "http", "https":
	default:
		return nil, errors.Errorf("unsupported scheme %q", scheme)
	}

	return &RingPeers{
		members: members,
		scheme:  scheme,
		port:    port,
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
			client, err = adminapi.NewClient(addr)
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
