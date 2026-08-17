package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/oteldb/storage/signal"
)

type passthrough struct{}

func (passthrough) ServeHTTP(http.ResponseWriter, *http.Request) {}

func resourceWith(kvs ...[2]string) signal.Resource {
	attrs := make([]signal.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		attrs = append(attrs, signal.KeyValue{Key: []byte(kv[0]), Value: signal.StringValue([]byte(kv[1]))})
	}

	return signal.Resource{Attributes: signal.NewAttributes(attrs...)}
}

func TestParseTenantID(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		in    string
		want  signal.TenantID
		error bool
	}{
		{name: "Simple", in: "acme", want: "acme"},
		{name: "Trimmed", in: "  acme  ", want: "acme"},
		{name: "Punctuation", in: "acme-prod_1.eu", want: "acme-prod_1.eu"},
		{name: "Empty", in: "", error: true},
		{name: "Blank", in: "   ", error: true},
		{name: "Dot", in: ".", error: true},
		{name: "DotDot", in: "..", error: true},
		{name: "Slash", in: "acme/prod", error: true},
		{name: "ShardMarker", in: "acme/_s1", error: true},
		{name: "Pipe", in: "acme|other", error: true},
		{name: "Space", in: "acme prod", error: true},
		{name: "TooLong", in: strings.Repeat("a", maxTenantLen+1), error: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseTenantID(tt.in)
			if tt.error {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestNewTenantResolver(t *testing.T) {
	t.Parallel()

	t.Run("ZeroConfigIsNil", func(t *testing.T) {
		t.Parallel()

		r, err := newTenantResolver(TenantConfig{})
		require.NoError(t, err)
		require.Nil(t, r, "zero config must route exactly like no tenancy at all")
		require.Nil(t, r.tenantFunc(context.Background()))
	})

	t.Run("RejectsBadDefault", func(t *testing.T) {
		t.Parallel()

		_, err := newTenantResolver(TenantConfig{Default: "bad/tenant"})
		require.Error(t, err)
	})

	t.Run("RejectsEmptyAttributeKey", func(t *testing.T) {
		t.Parallel()

		_, err := newTenantResolver(TenantConfig{ResourceAttributes: []string{""}})
		require.Error(t, err)
	})

	t.Run("RejectsRequireWithoutHeader", func(t *testing.T) {
		t.Parallel()

		_, err := newTenantResolver(TenantConfig{Require: true})
		require.Error(t, err)
	})
}

func TestTenantResolverTenantFunc(t *testing.T) {
	t.Parallel()

	const attrKey = "service.namespace"

	for _, tt := range []struct {
		name   string
		cfg    TenantConfig
		header signal.TenantID
		res    signal.Resource
		want   signal.TenantID
	}{
		{
			name: "DefaultOnly",
			cfg:  TenantConfig{Default: "shared"},
			res:  resourceWith([2]string{attrKey, "acme"}),
			want: "shared",
		},
		{
			name: "Attribute",
			cfg:  TenantConfig{ResourceAttributes: []string{attrKey}},
			res:  resourceWith([2]string{attrKey, "acme"}),
			want: "acme",
		},
		{
			name: "AttributeFallsBackToDefault",
			cfg:  TenantConfig{Default: "shared", ResourceAttributes: []string{attrKey}},
			res:  resourceWith([2]string{"service.name", "api"}),
			want: "shared",
		},
		{
			name: "MalformedAttributeFallsBackToDefault",
			cfg:  TenantConfig{Default: "shared", ResourceAttributes: []string{attrKey}},
			res:  resourceWith([2]string{attrKey, "acme/prod"}),
			want: "shared",
		},
		{
			name: "FirstConfiguredAttributeWins",
			cfg:  TenantConfig{ResourceAttributes: []string{"tenant", attrKey}},
			res:  resourceWith([2]string{"tenant", "one"}, [2]string{attrKey, "two"}),
			want: "one",
		},
		{
			name:   "HeaderBeatsAttribute",
			cfg:    TenantConfig{Header: HeaderScopeOrgID, ResourceAttributes: []string{attrKey}},
			header: "acme",
			res:    resourceWith([2]string{attrKey, "other"}),
			want:   "acme",
		},
		{
			name:   "HeaderBeatsDefault",
			cfg:    TenantConfig{Header: HeaderScopeOrgID, Default: "shared"},
			header: "acme",
			want:   "acme",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := newTenantResolver(tt.cfg)
			require.NoError(t, err)
			require.NotNil(t, r)

			ctx := context.Background()
			if tt.header != "" {
				ctx = withRequestTenant(ctx, tt.header)
			}

			fn := r.tenantFunc(ctx)
			require.NotNil(t, fn)
			require.Equal(t, tt.want, fn(tt.res, signal.Scope{}))
		})
	}
}

// TestTenantResolverUnresolvedRoutesToDefault pins the compatibility guarantee: with tenancy
// configured but nothing naming a tenant, framing must see a nil callback and put the write where
// an unconfigured deployment puts it.
func TestTenantResolverUnresolvedRoutesToDefault(t *testing.T) {
	t.Parallel()

	r, err := newTenantResolver(TenantConfig{Header: HeaderScopeOrgID})
	require.NoError(t, err)
	require.Nil(t, r.tenantFunc(context.Background()))
}

func TestTenantResolverMiddleware(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		cfg    TenantConfig
		header string
		code   int
		want   signal.TenantID
	}{
		{
			name:   "Resolves",
			cfg:    TenantConfig{Header: HeaderScopeOrgID},
			header: "acme",
			code:   http.StatusOK,
			want:   "acme",
		},
		{
			name: "MissingIsAllowed",
			cfg:  TenantConfig{Header: HeaderScopeOrgID},
			code: http.StatusOK,
		},
		{
			name: "MissingIsRefusedWhenRequired",
			cfg:  TenantConfig{Header: HeaderScopeOrgID, Require: true},
			code: http.StatusBadRequest,
		},
		{
			name:   "MalformedIsRefused",
			cfg:    TenantConfig{Header: HeaderScopeOrgID},
			header: "acme/prod",
			code:   http.StatusBadRequest,
		},
		{
			name:   "HeaderIgnoredWhenNotConfigured",
			cfg:    TenantConfig{Default: "shared"},
			header: "acme",
			code:   http.StatusOK,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := newTenantResolver(tt.cfg)
			require.NoError(t, err)

			var seen signal.TenantID

			h := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				seen = requestTenant(req.Context())
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodPost, "/v1/metrics", http.NoBody)
			if tt.header != "" {
				req.Header.Set(HeaderScopeOrgID, tt.header)
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, tt.code, rec.Code, rec.Body)
			assert.Equal(t, tt.want, seen)
		})
	}
}

// TestTenantMiddlewareNilResolver pins that unconfigured tenancy adds nothing to the chain.
func TestTenantMiddlewareNilResolver(t *testing.T) {
	t.Parallel()

	var r *tenantResolver

	next := passthrough{}
	require.Equal(t, http.Handler(next), r.Middleware(next))
}

func TestTenantResolverUnaryInterceptor(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		cfg  TenantConfig
		md   metadata.MD
		code codes.Code
		want signal.TenantID
	}{
		{
			name: "Resolves",
			cfg:  TenantConfig{Header: HeaderScopeOrgID},
			md:   metadata.Pairs(HeaderScopeOrgID, "acme"),
			want: "acme",
		},
		{
			name: "MissingIsAllowed",
			cfg:  TenantConfig{Header: HeaderScopeOrgID},
			md:   metadata.MD{},
		},
		{
			name: "MissingIsRefusedWhenRequired",
			cfg:  TenantConfig{Header: HeaderScopeOrgID, Require: true},
			md:   metadata.MD{},
			code: codes.InvalidArgument,
		},
		{
			name: "MalformedIsRefused",
			cfg:  TenantConfig{Header: HeaderScopeOrgID},
			md:   metadata.Pairs(HeaderScopeOrgID, "acme/prod"),
			code: codes.InvalidArgument,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := newTenantResolver(tt.cfg)
			require.NoError(t, err)

			var seen signal.TenantID

			ctx := metadata.NewIncomingContext(t.Context(), tt.md)
			_, err = r.UnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{},
				func(ctx context.Context, _ any) (any, error) {
					seen = requestTenant(ctx)

					return nil, nil
				})

			require.Equal(t, tt.code, status.Code(err))
			assert.Equal(t, tt.want, seen)
		})
	}
}
