package lokie2e_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/integration/lokie2e"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
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
	consumer, err := logstorage.NewConsumer(inserter, logstorage.ConsumerOptions{})
	require.NoError(t, err)
	for _, b := range set.Batches {
		resLogs := b.ResourceLogs()
		for j := 0; j < resLogs.Len(); j++ {
			resLogs.At(j).Resource().Attributes().PutStr("tenant", "tenant2")
		}
		require.NoError(t, consumer.ConsumeLogs(ctx, b))
	}

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

	s := httptest.NewServer(mw(lokih))
	t.Cleanup(s.Close)

	c1, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant1"}}),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c2, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant2"}}),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	cNone, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant_none"}}),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	c1_2, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant1_2"}}),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	cSuperadmin, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(&http.Client{Transport: &authRoundTripper{next: http.DefaultTransport, token: "tenant_superadmin"}}),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	t.Run("Tenant1", func(t *testing.T) {
		checkLokiTenant(ctx, t, c1, set, true, false)
	})

	t.Run("Tenant2", func(t *testing.T) {
		checkLokiTenant(ctx, t, c2, set, false, true)
	})

	t.Run("TenantNone", func(t *testing.T) {
		checkLokiTenant(ctx, t, cNone, set, false, false)
	})

	t.Run("Tenant1_2", func(t *testing.T) {
		checkLokiTenant(ctx, t, c1_2, set, true, true)
	})

	t.Run("TenantSuperadmin", func(t *testing.T) {
		checkLokiTenant(ctx, t, cSuperadmin, set, true, true)
	})
}

func checkLokiTenant(
	ctx context.Context,
	t *testing.T,
	c *lokiapi.Client,
	set *lokie2e.BatchSet,
	hasTenant1, hasTenant2 bool,
) {
	// 1. Check Labels
	r, err := c.Labels(ctx, lokiapi.LabelsParams{
		Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
		End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
	})
	require.NoError(t, err)

	var expectedLabels []string
	if !hasTenant1 && !hasTenant2 {
		expectedLabels = []string{
			"detected_level", "level", "msg", "service_instance_id",
			"service_name", "service_namespace", "span_id", "trace_id",
		}
	} else {
		expectedLabels = maps.Keys(set.Labels)
		if hasTenant2 {
			expectedLabels = append(expectedLabels, "tenant")
		}
	}
	slices.Sort(expectedLabels)
	gotLabels := r.Data
	slices.Sort(gotLabels)
	require.Equal(t, expectedLabels, gotLabels)

	// 2. Check Label Values
	for _, labelName := range gotLabels {
		valRes, err := c.LabelValues(ctx, lokiapi.LabelValuesParams{
			Name:  labelName,
			Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
			End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
		})
		require.NoError(t, err)

		var expectedValues []string
		switch {
		case !hasTenant1 && !hasTenant2:
			if labelName == "level" || labelName == "detected_level" {
				unique := map[string]struct{}{}
				for _, label := range set.Labels[labelName] {
					unique[label.Value] = struct{}{}
				}
				expectedValues = maps.Keys(unique)
			} else {
				expectedValues = []string{}
			}
		case labelName == "tenant":
			expectedValues = []string{"tenant2"}
		default:
			unique := map[string]struct{}{}
			for _, label := range set.Labels[labelName] {
				unique[label.Value] = struct{}{}
			}
			expectedValues = maps.Keys(unique)
		}
		slices.Sort(expectedValues)
		gotValues := valRes.Data
		slices.Sort(gotValues)
		require.Equal(t, expectedValues, gotValues, "values for label %q", labelName)
	}

	// 3. Check data (QueryRange)
	qRangeRes, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
		Query: `{service_name=~".+"}`,
		Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
		End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
		Limit: lokiapi.NewOptInt(1000),
	})
	require.NoError(t, err)

	streams, ok := qRangeRes.Data.GetStreamsResult()
	require.True(t, ok)

	var expectedCount int
	if hasTenant1 {
		expectedCount += len(set.Records)
	}
	if hasTenant2 {
		expectedCount += len(set.Records)
	}

	if expectedCount == 0 {
		require.Empty(t, streams.Result)
	} else {
		actualEntries := 0
		for _, stream := range streams.Result {
			for _, entry := range stream.Values {
				actualEntries++
				// Check that the record exists and matches.
				record, ok := set.Records[pcommon.Timestamp(entry.T)]
				require.Truef(t, ok, "can't find log record %d", entry.T)
				require.Equal(t, record.Body().AsString(), entry.V)
				labelSetHasAttrs(t, stream.Stream.Value, record.Attributes())

				tenantVal, ok := stream.Stream.Value["tenant"]
				if ok {
					require.True(t, hasTenant2)
					require.Equal(t, "tenant2", tenantVal)
				}
			}
		}
		require.Equal(t, expectedCount, actualEntries)
	}
}
