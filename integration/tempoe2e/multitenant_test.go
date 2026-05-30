package tempoe2e_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/integration/tempoe2e"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/multitenancy"
	"github.com/go-faster/oteldb/internal/multitenancy/static"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempohandler"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
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
		Tenants: []multitenancy.TenantRule{
			{ID: "tenant2", KeyAttributes: map[string]string{"tenant": "tenant2"}},
		},
		DefaultTenant: "tenant1",
	})
	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
		TenantMapper:   mapper,
	})
	require.NoError(t, err)

	set := loadTestData(ctx, t, inserter)

	// Consume again as tenant2.
	consumer := tracestorage.NewConsumer(inserter)
	for _, b := range set.Batches {
		resSpans := b.ResourceSpans()
		for j := 0; j < resSpans.Len(); j++ {
			resSpans.At(j).Resource().Attributes().PutStr("tenant", "tenant2")
		}
		require.NoError(t, consumer.ConsumeTraces(ctx, b))
	}

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
			"tenant1":           {Enabled: true, TenantIDs: []string{"tenant1"}},
			"tenant2":           {Enabled: true, TenantIDs: []string{"tenant2"}},
			"tenant_none":       {Enabled: true, TenantIDs: []string{"tenant_none"}},
			"tenant1_2":         {Enabled: true, TenantIDs: []string{"tenant1", "tenant2"}},
			"tenant_superadmin": {Enabled: false},
		},
	})
	mw := multitenancy.NewMiddleware(multitenancy.MiddlewareConfig{
		Resolver: resolver,
	})

	s := httptest.NewServer(mw(tempoh))
	t.Cleanup(s.Close)

	c1, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant1"}}),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c2, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant2"}}),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	cNone, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant_none"}}),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	cBoth, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant1_2"}}),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	cSuperadmin, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant_superadmin"}}),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	t.Run("Tenant1", func(t *testing.T) {
		checkTempoTenant(ctx, t, c1, set, true, false)
	})

	t.Run("Tenant2", func(t *testing.T) {
		checkTempoTenant(ctx, t, c2, set, false, true)
	})

	t.Run("TenantNone", func(t *testing.T) {
		checkTempoTenant(ctx, t, cNone, set, false, false)
	})

	t.Run("TenantBoth", func(t *testing.T) {
		checkTempoTenant(ctx, t, cBoth, set, true, true)
	})

	t.Run("TenantSuperadmin", func(t *testing.T) {
		checkTempoTenant(ctx, t, cSuperadmin, set, true, true)
	})
}

func checkTempoTenant(
	ctx context.Context,
	t *testing.T,
	c *tempoapi.Client,
	set tempoe2e.BatchSet,
	hasTenant1, hasTenant2 bool,
) {
	// 1. SearchTags.
	r, err := c.SearchTags(ctx, tempoapi.SearchTagsParams{
		Start: tempoTime(set.Start.AsTime()),
		End:   tempoTime(set.End.AsTime()),
	})
	require.NoError(t, err)

	if !hasTenant1 && !hasTenant2 {
		require.Empty(t, r.TagNames)
	} else {
		expectedTags := maps.Keys(set.Tags)
		if hasTenant2 {
			expectedTags = append(expectedTags, "tenant")
		}
		slices.Sort(expectedTags)
		gotTags := r.TagNames
		slices.Sort(gotTags)
		require.Equal(t, expectedTags, gotTags)

		// 2. SearchTagValues.
		for _, tagName := range gotTags {
			valRes, err := c.SearchTagValues(ctx, tempoapi.SearchTagValuesParams{
				TagName: tagName,
				Start:   tempoTime(set.Start.AsTime()),
				End:     tempoTime(set.End.AsTime()),
			})
			require.NoError(t, err)

			var expectedValues []string
			if tagName == "tenant" {
				expectedValues = []string{"tenant2"}
			} else {
				tags := set.Tags[tagName]
				valMap := make(map[string]struct{})
				for _, tag := range tags {
					valMap[tag.Value] = struct{}{}
				}
				expectedValues = maps.Keys(valMap)
			}
			slices.Sort(expectedValues)
			gotValues := valRes.TagValues
			slices.Sort(gotValues)
			require.Equal(t, expectedValues, gotValues, "values for tag %q", tagName)
		}
	}

	// 3. Search.
	searchRes, err := c.Search(ctx, tempoapi.SearchParams{
		Q:     tempoapi.NewOptString(`{}`),
		Start: tempoTime(set.Start.AsTime()),
		End:   tempoTime(set.End.AsTime()),
		Limit: tempoapi.NewOptInt(100),
	})
	require.NoError(t, err)

	var expectedTracesCount int
	if hasTenant1 || hasTenant2 {
		expectedTracesCount = len(set.Traces)
	}

	if !hasTenant1 && !hasTenant2 {
		require.Empty(t, searchRes.Traces)
	} else {
		require.Len(t, searchRes.Traces, expectedTracesCount)
		for _, traceMetadata := range searchRes.Traces {
			traceID := pcommon.TraceID(uuid.MustParse(traceMetadata.TraceID))
			_, exists := set.Traces[traceID]
			require.True(t, exists, "trace %s must exist in test data", traceMetadata.TraceID)
		}
	}

	// 4. TraceByID.
	for traceID, expectedTrace := range set.Traces {
		tid := otelstorage.TraceID(traceID).Hex()
		r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{
			TraceID: tid,
			Start:   tempoTime(set.Start.AsTime()),
			End:     tempoTime(set.End.AsTime()),
		})
		require.NoError(t, err)

		if !hasTenant1 && !hasTenant2 {
			require.IsType(t, &tempoapi.TraceByIDNotFound{}, r)
		} else {
			require.IsType(t, &tempoapi.TraceByID{}, r)
			data, err := io.ReadAll(r.(*tempoapi.TraceByID))
			require.NoError(t, err)

			var u ptrace.ProtoUnmarshaler
			resp, err := u.UnmarshalTraces(data)
			require.NoError(t, err)

			var expectedSpansCount int
			if hasTenant1 {
				expectedSpansCount += len(expectedTrace.Spanset)
			}
			if hasTenant2 {
				expectedSpansCount += len(expectedTrace.Spanset)
			}
			require.Equal(t, expectedSpansCount, resp.SpanCount(), "span count for trace %s", tid)
		}
	}
}
