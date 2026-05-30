package prome2e_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/integration/prome2e"
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
	for _, b := range set.Batches {
		resMetrics := b.ResourceMetrics()
		for j := 0; j < resMetrics.Len(); j++ {
			resMetrics.At(j).Resource().Attributes().PutStr("tenant", "tenant2")
		}
		require.NoError(t, inserter.ConsumeMetrics(ctx, b))
	}

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

	s := httptest.NewServer(mw(httpmiddleware.Wrap(promh, promhandler.PatchForm)))
	t.Cleanup(s.Close)

	c1, err := promapi.NewClient(s.URL,
		promapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant1"}}),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c2, err := promapi.NewClient(s.URL,
		promapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant2"}}),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	cNone, err := promapi.NewClient(s.URL,
		promapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant_none"}}),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c1_2, err := promapi.NewClient(s.URL,
		promapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant1_2"}}),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	cSuperadmin, err := promapi.NewClient(s.URL,
		promapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant_superadmin"}}),
		promapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	t.Run("Tenant1", func(t *testing.T) {
		checkPromTenant(ctx, t, c1, set, true, false)
	})

	t.Run("Tenant2", func(t *testing.T) {
		checkPromTenant(ctx, t, c2, set, false, true)
	})

	t.Run("TenantNone", func(t *testing.T) {
		checkPromTenant(ctx, t, cNone, set, false, false)
	})

	t.Run("Tenant1_2", func(t *testing.T) {
		checkPromTenant(ctx, t, c1_2, set, true, true)
	})

	t.Run("TenantSuperadmin", func(t *testing.T) {
		checkPromTenant(ctx, t, cSuperadmin, set, true, true)
	})
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func checkPromTenant(
	ctx context.Context,
	t *testing.T,
	c *promapi.Client,
	set prome2e.BatchSet,
	hasTenant1, hasTenant2 bool,
) {
	// 1. Check Labels
	r, err := c.GetLabels(ctx, promapi.GetLabelsParams{})
	require.NoError(t, err)

	if !hasTenant1 && !hasTenant2 {
		require.Empty(t, r.Data)
	} else {
		expectedLabels := maps.Keys(set.Labels)
		if hasTenant2 {
			expectedLabels = append(expectedLabels, "tenant")
		}
		slices.Sort(expectedLabels)
		gotLabels := []string(r.Data)
		slices.Sort(gotLabels)
		require.Equal(t, expectedLabels, gotLabels)

		// 2. Check Label Values
		for _, labelName := range gotLabels {
			valRes, err := c.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: labelName})
			require.NoError(t, err)

			var expectedValues []string
			if labelName == "tenant" {
				expectedValues = []string{"tenant2"}
			} else {
				expectedValues = maps.Keys(set.Labels[labelName])
			}
			slices.Sort(expectedValues)
			gotValues := []string(valRes.Data)
			slices.Sort(gotValues)
			require.Equal(t, expectedValues, gotValues, "values for label %q", labelName)
		}
	}

	// 3. Check data (Query)
	queryRes, err := c.GetQuery(ctx, promapi.GetQueryParams{
		Query: "prometheus_http_requests_total{}",
		Time:  promapi.NewOptPrometheusTimestamp(getPromTS(set.End)),
	})
	require.NoError(t, err)

	vec, ok := queryRes.Data.GetVector()
	require.True(t, ok)

	var expectedCount int
	if hasTenant1 {
		expectedCount += 51
	}
	if hasTenant2 {
		expectedCount += 51
	}

	if expectedCount == 0 {
		require.Empty(t, vec.Result)
	} else {
		require.Len(t, vec.Result, expectedCount)
		for _, vr := range vec.Result {
			assert.Equal(t, "prometheus_http_requests_total", vr.Metric["__name__"])
			tenantVal, hasTenant := vr.Metric["tenant"]

			// Build a map of labels to compare with set.Series
			labelsMap := make(map[string]string)
			for k, v := range vr.Metric {
				if k != "tenant" {
					labelsMap[k] = v
				}
			}

			if hasTenant {
				require.True(t, hasTenant2)
				require.Equal(t, "tenant2", tenantVal)
			} else {
				require.True(t, hasTenant1)
			}

			// Verify labelsMap matches one of set.Series
			found := false
			for _, expectedSeries := range set.Series {
				if mapsEqual(expectedSeries, labelsMap) {
					found = true
					break
				}
			}
			require.True(t, found, "metric %+v not found in expected series", vr.Metric)
			assert.GreaterOrEqual(t, vr.Value.HistogramOrValue.StringFloat64, 0.0)
		}
	}

	// 4. Check exemplars
	exemplarRes, err := c.GetQueryExemplars(ctx, promapi.GetQueryExemplarsParams{
		Query: exemplarMetric + `{}`,
		Start: getPromTS(set.Start),
		End:   getPromTS(set.End),
	})
	require.NoError(t, err)

	var expectedExemplarSeriesCount int
	if hasTenant1 {
		expectedExemplarSeriesCount++
	}
	if hasTenant2 {
		expectedExemplarSeriesCount++
	}

	require.Len(t, exemplarRes.Data, expectedExemplarSeriesCount)
	for _, series := range exemplarRes.Data {
		assert.Equal(t, exemplarMetric, series.SeriesLabels.Value["__name__"])
		tenantVal, hasTenant := series.SeriesLabels.Value["tenant"]
		if hasTenant {
			require.True(t, hasTenant2)
			require.Equal(t, "tenant2", tenantVal)
		} else {
			require.True(t, hasTenant1)
		}

		require.NotEmpty(t, series.Exemplars)
		for _, e := range series.Exemplars {
			assert.Equal(t, promapi.LabelSet{
				"code":     "10",
				"foo":      "bar",
				"span_id":  exemplarSpanID.String(),
				"trace_id": exemplarTraceID.String(),
			}, e.Labels)
			assert.Equal(t, 10.0, e.Value)
		}
	}
}
