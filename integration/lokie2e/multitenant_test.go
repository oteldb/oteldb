package lokie2e_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"net/http/httptest"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokihandler"
	"github.com/go-faster/oteldb/internal/multitenancy"
	"github.com/go-faster/oteldb/internal/multitenancy/static"
)

type authRoundTripper struct {
	next  http.RoundTripper
	token string
}

func (rt *authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+rt.token)
	if rt.next == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return rt.next.RoundTrip(req)
}

func TestMultitenancy(t *testing.T) {
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")
	)
	_, c, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "lokie2e-mt",
		TablePrefix:    tablePrefix,
		TracerProvider: provider,
	})

	mapper := multitenancy.NewTenantMapper(multitenancy.TenantMapperConfig{
		DefaultTenant: "100",
	})
	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
		TenantMapper:   mapper,
	})
	require.NoError(t, err)

	set := loadTestData(ctx, t, inserter)

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	ctx = zctx.Base(ctx, integration.Logger(t))

	var optimizers []logqlengine.Optimizer
	optimizers = append(optimizers, logqlengine.DefaultOptimizers()...)
	optimizers = append(optimizers, &chstorage.ClickhouseOptimizer{})
	engine, err := logqlengine.NewEngine(querier, logqlengine.Options{
		ParseOptions:   logql.ParseOptions{AllowDots: true},
		Optimizers:     optimizers,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	api := lokihandler.NewLokiAPI(querier, engine, lokihandler.LokiAPIOptions{})
	lokih, err := lokiapi.NewServer(api,
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	resolver := static.NewResolver(static.Config{
		ReadDecisions: map[string]multitenancy.Decision{
			"100": {Enabled: true, TenantIDs: []string{"100"}},
			"200": {Enabled: true, TenantIDs: []string{"200"}},
		},
	})
	mw := multitenancy.NewMiddleware(multitenancy.MiddlewareConfig{
		Resolver: resolver,
	})

	s := httptest.NewServer(mw(lokih))
	t.Cleanup(s.Close)

	c100, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(&http.Client{
			Transport: &authRoundTripper{next: http.DefaultTransport, token: "100"},
		}),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c200, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(&http.Client{
			Transport: &authRoundTripper{next: http.DefaultTransport, token: "200"},
		}),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	t.Run("Tenant100", func(t *testing.T) {
		r, err := c100.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: `{service_name=~".+"}`,
			Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
			End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
		})
		require.NoError(t, err)
		
		streams, ok := r.Data.GetStreamsResult()
		require.True(t, ok)
		require.NotEmpty(t, streams.Result)
	})

	t.Run("Tenant200", func(t *testing.T) {
		r, err := c200.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: `{service_name=~".+"}`,
			Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
			End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
		})
		require.NoError(t, err)

		streams, ok := r.Data.GetStreamsResult()
		require.True(t, ok)
		require.Empty(t, streams.Result)
	})
}
