package tempoe2e_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/multitenancy"
	"github.com/go-faster/oteldb/internal/multitenancy/static"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempohandler"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
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
		Name:           "tempoe2e-mt",
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

	engine := traceqlengine.NewEngine(querier, traceqlengine.Options{
		TracerProvider: provider,
	})
	api := tempohandler.NewTempoAPI(querier, engine, tempohandler.TempoAPIOptions{
		EnableAutocompleteQuery: true,
	})
	tempoh, err := tempoapi.NewServer(api,
		tempoapi.WithTracerProvider(provider),
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

	s := httptest.NewServer(mw(tempoh))
	t.Cleanup(s.Close)

	c100, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(&http.Client{
			Transport: &authRoundTripper{next: http.DefaultTransport, token: "100"},
		}),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c200, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(&http.Client{
			Transport: &authRoundTripper{next: http.DefaultTransport, token: "200"},
		}),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	t.Run("Tenant100", func(t *testing.T) {
		r, err := c100.SearchTags(ctx, tempoapi.SearchTagsParams{})
		require.NoError(t, err)
		require.NotEmpty(t, r.TagNames)
	})

	t.Run("Tenant200", func(t *testing.T) {
		r, err := c200.SearchTags(ctx, tempoapi.SearchTagsParams{})
		require.NoError(t, err)
		require.Empty(t, r.TagNames)
	})
	
	_ = set
}
