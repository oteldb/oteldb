package tempoe2e_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/oteldb/storage"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
)

func checkMetricsInstant(t *testing.T, client *tempoapi.Client, base time.Time) {
	t.Helper()
	for _, tt := range []struct {
		name, query string
		want        map[string]float64
	}{
		{"instant count", `{} | count_over_time() by(resource.service.name)`, map[string]float64{"checkout": 6, "payments": 5}},
		{"instant rate", `{} | rate() by(resource.service.name)`, map[string]float64{"checkout": 6.0 / 75, "payments": 5.0 / 75}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := client.Query(t.Context(), tempoapi.QueryParams{Q: tt.query, Start: tempoTime(base), End: tempoTime(base.Add(75 * time.Second))})
			require.NoError(t, err)
			require.Len(t, result.Series, len(tt.want))
			for _, series := range result.Series {
				name := ""
				for _, label := range series.Labels {
					if label.Key == "resource.service.name" {
						name = label.Value.StringValue.StringValue
					}
				}
				require.Contains(t, tt.want, name)
				require.True(t, series.Value.Set)
				require.InDelta(t, tt.want[name], series.Value.Value, 1e-12)
			}
		})
	}
}

func TestMetricsHTTPErrors(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	q := storagebackend.New(store).Traces()
	client := setupDB(t, integration.TraceProvider(t), q, q)
	base := time.Unix(1_700_000_010, 0).UTC()
	t.Run("defaults", func(t *testing.T) {
		result, err := client.QueryRange(t.Context(), tempoapi.QueryRangeParams{Q: `{} | count_over_time()`})
		require.NoError(t, err)
		require.Len(t, result.Series, 1)
		require.Greater(t, len(result.Series[0].Samples), 1)
		for i, sample := range result.Series[0].Samples {
			require.True(t, sample.Value.Set)
			require.Zero(t, sample.Value.Value)
			if i > 0 {
				require.Equal(t, 15_000, sample.TimestampMs.Value-result.Series[0].Samples[i-1].TimestampMs.Value)
			}
		}
		instant, err := client.Query(t.Context(), tempoapi.QueryParams{Q: `{} | count_over_time()`})
		require.NoError(t, err)
		require.Len(t, instant.Series, 1)
		require.Zero(t, instant.Series[0].Value.Value)
	})
	for _, tt := range []struct {
		name, query, step string
		reverse           bool
		code              int
	}{
		{"invalid range", `{} | rate()`, "15s", true, http.StatusBadRequest},
		{"zero step", `{} | rate()`, "0s", false, http.StatusBadRequest},
		{"invalid step", `{} | rate()`, "invalid", false, http.StatusBadRequest},
		{"submillisecond step", `{} | rate()`, "0.0001s", false, http.StatusBadRequest},
		{"too many steps", `{} | rate()`, "1ms", false, http.StatusBadRequest},
		{"not metrics", `{}`, "15s", false, http.StatusBadRequest},
		{"syntax", `{} | rate(`, "15s", false, http.StatusBadRequest},
		{"duration aggregation", `{} | avg_over_time(duration)`, "15s", false, http.StatusNotImplemented},
		{"compare", `{} | compare({status=error})`, "15s", false, http.StatusNotImplemented},
		{"sampling", `{} | rate() with(sample=true)`, "15s", false, http.StatusNotImplemented},
	} {
		t.Run(tt.name, func(t *testing.T) {
			start, end := base, base.Add(time.Minute)
			if tt.reverse {
				start, end = end, start
			}
			_, err := client.QueryRange(t.Context(), tempoapi.QueryRangeParams{Q: tt.query, Start: tempoTime(start), End: tempoTime(end), Step: tempoapi.NewOptPrometheusDuration(tempoapi.PrometheusDuration(tt.step))})
			var apiErr *tempoapi.ErrorStatusCode
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, tt.code, apiErr.StatusCode)
			require.NotEmpty(t, apiErr.Response)
		})
	}
}
