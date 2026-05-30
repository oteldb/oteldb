package prome2e_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/httpmiddleware"
	"github.com/go-faster/oteldb/internal/multitenancy"
	"github.com/go-faster/oteldb/internal/multitenancy/static"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promhandler"
	"github.com/go-faster/oteldb/internal/promql"
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
		Name:           "prome2e-mt",
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

	_ = loadTestData(ctx, t, inserter)

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	engine, err := promql.New(querier, promql.EngineOpts{
		Timeout:              time.Minute,
		MaxSamples:           1_000_000,
		EnableNegativeOffset: true,
	})
	require.NoError(t, err)
	api := promhandler.NewPromAPI(engine, querier, querier, querier, promhandler.PromAPIOptions{})
	promh, err := promapi.NewServer(api,
		promapi.WithTracerProvider(provider),
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

	s := httptest.NewServer(mw(httpmiddleware.Wrap(promh, promhandler.PatchForm)))
	t.Cleanup(s.Close)

	c100, err := promapi.NewClient(s.URL,
		promapi.WithClient(&http.Client{
			Transport: &authRoundTripper{next: http.DefaultTransport, token: "100"},
		}),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c200, err := promapi.NewClient(s.URL,
		promapi.WithClient(&http.Client{
			Transport: &authRoundTripper{next: http.DefaultTransport, token: "200"},
		}),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	t.Run("Tenant100", func(t *testing.T) {
		r, err := c100.GetLabels(ctx, promapi.GetLabelsParams{})
		require.NoError(t, err)

		require.NotEmpty(t, r.Data)
	})

	t.Run("Tenant200", func(t *testing.T) {
		r, err := c200.GetLabels(ctx, promapi.GetLabelsParams{})
		require.NoError(t, err)

		require.Empty(t, r.Data)
	})
}
