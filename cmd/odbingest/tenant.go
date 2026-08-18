package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-faster/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
)

// HeaderScopeOrgID is the header Grafana-stack senders (Loki, Mimir, their Grafana datasources)
// put the tenant in, and the value to configure [TenantConfig.Header] with to accept them.
const HeaderScopeOrgID = "X-Scope-OrgID"

// maxTenantLen bounds a tenant id. A tenant id becomes a shard key, which becomes a backend path
// segment and an etcd key component, so an unbounded one from a request header is a way to make
// keys nobody can address.
const maxTenantLen = 150

// tenantCtxKey carries the tenant a request's headers named. Header tenancy is per request while
// [cluster.TenantFunc] is per resource, so the request-scoped value rides the context the handler
// already passes to the sink, and the sink turns it into a constant TenantFunc for that write.
type tenantCtxKey struct{}

func withRequestTenant(ctx context.Context, tid signal.TenantID) context.Context {
	return context.WithValue(ctx, tenantCtxKey{}, tid)
}

func requestTenant(ctx context.Context) signal.TenantID {
	tid, _ := ctx.Value(tenantCtxKey{}).(signal.TenantID)

	return tid
}

// tenantResolver decides which tenant a write routes to.
//
// The sources compose by scope, narrowest first: a header names the tenant of a whole request, a
// resource attribute names the tenant of one resource within it, and the configured default backs
// both. A header therefore wins — it is set by the gateway that authenticated the sender, whereas
// an attribute is whatever the sender put in its payload.
type tenantResolver struct {
	def      signal.TenantID
	header   string
	attrs    [][]byte
	required bool
}

// newTenantResolver builds the resolver cfg describes, or nil if cfg configures nothing — a nil
// resolver routes exactly like the unconfigured ingest path, to [cluster.DefaultTenant].
func newTenantResolver(cfg TenantConfig) (*tenantResolver, error) {
	r := &tenantResolver{
		header:   strings.TrimSpace(cfg.Header),
		required: cfg.Require,
	}

	if cfg.Default != "" {
		tid, err := parseTenantID(cfg.Default)
		if err != nil {
			return nil, errors.Wrap(err, "tenant.default")
		}

		r.def = tid
	}

	for _, key := range cfg.ResourceAttributes {
		if key == "" {
			return nil, errors.New("tenant.resource_attributes: empty attribute key")
		}

		r.attrs = append(r.attrs, []byte(key))
	}

	if r.required && r.header == "" {
		return nil, errors.New("tenant.require needs tenant.header")
	}

	if r.def == "" && r.header == "" && len(r.attrs) == 0 {
		return nil, nil
	}

	return r, nil
}

// defaultTenant is where an unresolved write lands.
func (r *tenantResolver) defaultTenant() signal.TenantID {
	if r == nil || r.def == "" {
		return cluster.DefaultTenant
	}

	return r.def
}

// tenantFunc returns the routing callback for one write, given the request context the header
// middleware annotated. A nil result routes to [cluster.DefaultTenant].
func (r *tenantResolver) tenantFunc(ctx context.Context) cluster.TenantFunc {
	if r == nil {
		return nil
	}

	if tid := requestTenant(ctx); tid != "" {
		return func(signal.Resource, signal.Scope) signal.TenantID { return tid }
	}

	if len(r.attrs) == 0 {
		if r.def == "" {
			return nil
		}

		def := r.def

		return func(signal.Resource, signal.Scope) signal.TenantID { return def }
	}

	return r.fromResource
}

// fromResource reads the tenant off the resource, first configured key wins. A missing, non-string
// or malformed value falls back to the default rather than failing the batch: framing is past the
// point where a request can be answered with an error, and shedding a resource silently would be
// worse than putting it where an unconfigured deployment puts it.
func (r *tenantResolver) fromResource(res signal.Resource, _ signal.Scope) signal.TenantID {
	for _, key := range r.attrs {
		v, ok := res.Attributes.Get(key)
		if !ok {
			continue
		}

		s := v.Str()
		if len(s) == 0 {
			continue
		}

		tid, err := parseTenantID(string(s))
		if err != nil {
			continue
		}

		return tid
	}

	return r.def
}

// Middleware resolves the header tenant onto the request context, refusing a request that names an
// unusable tenant instead of quietly routing it to the default.
func (r *tenantResolver) Middleware(next http.Handler) http.Handler {
	if r == nil || (r.header == "" && !r.required) {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var raw string
		if r.header != "" {
			raw = req.Header.Get(r.header)
		}

		tid, err := r.resolveHeader(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		if tid != "" {
			req = req.WithContext(withRequestTenant(req.Context(), tid))
		}

		next.ServeHTTP(w, req)
	})
}

// UnaryInterceptor is [tenantResolver.Middleware] for OTLP/gRPC, reading the same header out of
// the request metadata.
func (r *tenantResolver) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		if r == nil || (r.header == "" && !r.required) {
			return handler(ctx, req)
		}

		var raw string
		if r.header != "" {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if vs := md.Get(r.header); len(vs) > 0 {
					raw = vs[0]
				}
			}
		}

		tid, err := r.resolveHeader(raw)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		if tid != "" {
			ctx = withRequestTenant(ctx, tid)
		}

		return handler(ctx, req)
	}
}

// resolveHeader validates a header value. An absent one is not an error unless the deployment
// requires the sender to name its tenant, in which case routing it to a shared default would mix
// two senders' data under one tenant.
func (r *tenantResolver) resolveHeader(raw string) (signal.TenantID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if r.required {
			return "", errors.Errorf("missing %s header", r.header)
		}

		return "", nil
	}

	tid, err := parseTenantID(raw)
	if err != nil {
		return "", errors.Wrapf(err, "%s header", r.header)
	}

	return tid, nil
}

// parseTenantID validates a tenant id. The character set is what stays safe as a backend path
// segment and an etcd key, and excludes [cluster.ShardSep] so a tenant id can never be mistaken
// for the shard key derived from it.
func parseTenantID(s string) (signal.TenantID, error) {
	s = strings.TrimSpace(s)

	switch {
	case s == "":
		return "", errors.New("empty tenant id")
	case len(s) > maxTenantLen:
		return "", errors.Errorf("tenant id longer than %d bytes", maxTenantLen)
	case s == "." || s == "..":
		return "", errors.Errorf("invalid tenant id %q", s)
	}

	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
		default:
			return "", errors.Errorf("invalid character %q in tenant id", string(c))
		}
	}

	return signal.TenantID(s), nil
}
