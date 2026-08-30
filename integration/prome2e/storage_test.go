package prome2e_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestStorageBackend exercises the embedded github.com/oteldb/storage metrics backend
// end-to-end: real OTLP metric batches are ingested through the storage ingestion sink
// (ConsumeMetrics), then queried back over the full Prometheus HTTP API through the PromQL
// engine and the storage fetch seam.
//
// Unlike TestCH this needs no ClickHouse or Docker — the storage engine runs fully in
// memory — so it is not gated behind E2E and runs as a normal unit test.
func TestStorageBackend(t *testing.T) {
	t.Parallel()

	var (
		ctx      = t.Context()
		provider = integration.TraceProvider(t)
	)

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	backend := storagebackend.New(store)

	// Ingest the shared Prometheus test batches through the OTLP ingestion sink. Histogram,
	// summary, and value-less points are not representable in the storage engine yet and are
	// silently dropped by the conversion; the gauge/counter series used below survive.
	set := loadTestData(ctx, t, backend)

	_, c := setupDB(t, provider, backend)

	// Each case is a range query over the populated window whose value must equal count at
	// every step. prometheus_http_requests_total is a counter (sum) with 51 series.
	for _, tt := range []struct {
		name  string
		query string
		count float64
		empty bool
	}{
		{"CountAll", `count(prometheus_http_requests_total{})`, 51, false},
		{"CountRegexFilter", `count(prometheus_http_requests_total{handler=~".+"})`, 51, false},
		{"CountSelectFilter", `count(prometheus_http_requests_total{handler="/api/v1/query"})`, 1, false},
		{"CountExcludeFilter", `count(prometheus_http_requests_total{handler!="/api/v1/query"})`, 50, false},
		{"CountGroupByName", `count by (__name__) (prometheus_http_requests_total)`, 51, false},
		{"CountEmpty", `count(prometheus_http_requests_total{handler="clearly-not-exist"})`, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := require.New(t)

			r, err := c.GetQueryRange(ctx, promapi.GetQueryRangeParams{
				Query: tt.query,
				Start: getPromTS(set.Start),
				End:   getPromTS(set.End),
				Step:  promapi.NewOptString("5s"),
			})
			a.NoError(err)
			a.Equal(promapi.MatrixData, r.Data.Type)

			mat := r.Data.Matrix.Result
			if tt.empty {
				a.Empty(mat)
				return
			}
			a.Len(mat, 1)
			a.NotEmpty(mat[0].Values)
			for _, point := range mat[0].Values {
				a.Equal(tt.count, point.V)
			}
		})
	}

	// Grafana value-encodes every label name it puts in a URL path or a selector, so a store
	// keyed by the decoded name matches nothing unless the handler decodes it first. Both
	// positions are escaped here; "handler" carries no special characters, so the encoding is
	// the bare U__ prefix and the expected values are the plain ones.
	t.Run("ValueEncodedLabelName", func(t *testing.T) {
		t.Parallel()
		a := require.New(t)

		plain, err := c.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: "handler"})
		a.NoError(err)
		a.NotEmpty(plain.Data, "plain label name resolves")

		escaped, err := c.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: "U__handler"})
		a.NoError(err)
		a.Equal([]string(plain.Data), []string(escaped.Data))

		matched, err := c.GetLabelValues(ctx, promapi.GetLabelValuesParams{
			Label: "U__handler",
			Match: []string{`{U__handler="/api/v1/query"}`},
		})
		a.NoError(err)
		a.Equal([]string{"/api/v1/query"}, []string(matched.Data))
	})

	// An instant query over the gauge build-info metric must return its single series.
	t.Run("InstantBuildInfo", func(t *testing.T) {
		a := require.New(t)

		r, err := c.GetQuery(ctx, promapi.GetQueryParams{
			Query: `prometheus_build_info`,
			Time:  promapi.NewOptPrometheusTimestamp(getPromTS(set.End)),
		})
		a.NoError(err)
		a.Equal(promapi.VectorData, r.Data.Type)

		vec := r.Data.Vector.Result
		a.NotEmpty(vec)
		for _, s := range vec {
			a.Equal("prometheus_build_info", s.Metric["__name__"])
		}
	})
}
