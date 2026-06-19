package lokie2e_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// TestSampleRowsLimit verifies that a LogQL sample query (count_over_time)
// fails with a clear error instead of buffering an unbounded number of rows
// once it would exceed the configured MaxSampleRows safety cap.
func TestSampleRowsLimit(t *testing.T) {
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")
	)
	_, c, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "lokie2e-sample-rows-limit",
		TablePrefix:    tablePrefix,
		TracerProvider: provider,
	})

	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)
	set := loadTestData(ctx, t, inserter)
	ctx = zctx.Base(ctx, integration.Logger(t))

	const query = `count_over_time({http_method=~".+"} [30s])`
	queryRangeParams := lokiapi.QueryRangeParams{
		Query: query,
		// Query all data (123 rows) in a single step.
		Start: lokiapi.NewOptLokiTime(asLokiTime(set.End)),
		End:   lokiapi.NewOptLokiTime(asLokiTime(set.End + otelstorage.Timestamp(10*time.Second))),
		Step:  lokiapi.NewOptPrometheusDuration("30s"),
		Limit: lokiapi.NewOptInt(1000),
	}

	t.Run("ExceedsLimit", func(t *testing.T) {
		querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
			Tables:         tables,
			TracerProvider: provider,
			// Set lower than the 123 rows produced by loadTestData.
			MaxSampleRows: 50,
		})
		require.NoError(t, err)

		client := setupDB(t, provider, querier, querier)
		_, err = client.QueryRange(ctx, queryRangeParams)

		var gotErr *lokiapi.ErrorStatusCode
		require.ErrorAs(t, err, &gotErr)
		require.Equal(t, http.StatusBadRequest, gotErr.StatusCode)
	})
	t.Run("UnderLimit", func(t *testing.T) {
		querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
			Tables:         tables,
			TracerProvider: provider,
			MaxSampleRows:  1000,
		})
		require.NoError(t, err)

		client := setupDB(t, provider, querier, querier)
		resp, err := client.QueryRange(ctx, queryRangeParams)
		require.NoError(t, err)

		data, ok := resp.Data.GetMatrixResult()
		require.True(t, ok)
		require.NotEmpty(t, data.Result)
	})
}
