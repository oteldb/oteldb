package prome2e

import (
	"context"
	_ "embed"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainerslog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/internal/promapi"
)

type jxEncode interface {
	Encode(e *jx.Encoder)
}

func printJSON(t *testing.T, v jxEncode, name string) {
	t.Helper()

	var e jx.Encoder
	e.SetIdent(2)
	v.Encode(&e)

	t.Logf("%s:\n%s", name, e.String())
}

//go:embed prometheus.yml
var prometheusConfig []byte

func TestPrometheusOAS(t *testing.T) {
	integration.Skip(t)
	ctx := context.Background()

	// Provide config to prometheus testcontainer.
	dir := t.TempDir()
	configPath := filepath.Join(dir, "prometheus.yml")
	require.NoError(t, os.WriteFile(configPath, prometheusConfig, 0o644))

	req := testcontainers.ContainerRequest{
		Name:         "oteldb-e2e-prom",
		Image:        "prom/prometheus:v2.44.0",
		ExposedPorts: []string{"9090/tcp"},
		Cmd: []string{
			"--log.level=debug",
			"--config.file=/etc/prometheus/prometheus.yml",
			"--storage.tsdb.path=/prometheus",
			"--web.console.libraries=/usr/share/prometheus/console_libraries",
			"--web.console.templates=/usr/share/prometheus/consoles",
		},
		WaitingFor: wait.ForLog("Server is ready to receive web requests"),
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      configPath,
				ContainerFilePath: "/etc/prometheus/prometheus.yml",
				FileMode:          644,
			},
		},
	}
	promContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainerslog.TestLogger(t),
		Reuse:            true,
	})
	require.NoError(t, err, "container start")

	endpoint, err := promContainer.Endpoint(ctx, "")
	require.NoError(t, err, "container endpoint")

	u := url.URL{
		Scheme: "http",
		Host:   endpoint,
	}

	api, err := promapi.NewClient(u.String())
	require.NoError(t, err, "api client")

	{
		bo := backoff.WithContext(backoff.NewConstantBackOff(time.Millisecond*100), ctx)
		v, err := backoff.RetryWithData(func() (v promapi.Vector, _ error) {
			res, err := api.GetQuery(ctx, promapi.GetQueryParams{
				Query: "go_info{}",
			})
			if err != nil {
				return v, errors.Wrap(err, "api query")
			}
			v, ok := res.Data.GetVector()
			if !ok {
				return v, backoff.Permanent(errors.New("not a vector"))
			}
			if len(v.Result) == 0 {
				// Waiting to be scraped.
				return v, errors.New("empty vector")
			}
			return v, nil
		}, bo)
		require.NoError(t, err, "api query")
		require.Len(t, v.Result, 1, "should be one result")
		printJSON(t, &v, "query result")

		vr := v.Result[0]
		assert.Equal(t, "go_info", vr.Metric["__name__"], "metric name")
		assert.Greater(t, vr.Value.T, 0.0, "value")
		assert.Equal(t, vr.Value.HistogramOrValue.StringFloat64, 1.0)
	}

	var (
		now   = time.Now()
		start = promapi.PrometheusTimestamp(now.Add(-15 * time.Minute).Format(time.RFC3339))
		end   = promapi.PrometheusTimestamp(now.Add(+15 * time.Minute).Format(time.RFC3339))
	)
	t.Run("QueryRange", func(t *testing.T) {
		t.Parallel()

		res, err := api.GetQueryRange(ctx, promapi.GetQueryRangeParams{
			Query: "go_info{}",
			Start: start,
			End:   end,
			Step:  promapi.NewOptString("15s"),
		})
		require.NoError(t, err)
		m, ok := res.Data.GetMatrix()
		require.True(t, ok, "not a matrix")

		require.NotEmpty(t, m.Result)
		mr := m.Result[0]
		assert.Equal(t, "go_info", mr.Metric["__name__"], "metric name")
	})
	t.Run("QueryExemplars", func(t *testing.T) {
		t.Parallel()

		// By default, Prometheus does not store any exemplars.
		//
		// But Grafana may query it, so make sure that request is properly encoded
		// and we able to parse response.
		//
		// See https://prometheus.io/docs/prometheus/latest/feature_flags/#exemplars-storage for more
		// info about exemplars.
		res, err := api.GetQueryExemplars(ctx, promapi.GetQueryExemplarsParams{
			Query: "go_goroutines{}",
			Start: start,
			End:   end,
		})
		require.NoError(t, err)
		require.Equal(t, "success", res.Status)
	})
	t.Run("Series", func(t *testing.T) {
		t.Parallel()

		res, err := api.GetSeries(ctx, promapi.GetSeriesParams{
			Start: promapi.NewOptPrometheusTimestamp(start),
			End:   promapi.NewOptPrometheusTimestamp(end),
			Match: []string{`go_info{job="prometheus"}`},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res.Data)
		printJSON(t, res.Data, "series")

		m := res.Data[0]
		assert.Equal(t, "go_info", m["__name__"])
		assert.Equal(t, "prometheus", m["job"])
	})
	t.Run("Labels", func(t *testing.T) {
		t.Parallel()

		labels, err := api.GetLabels(ctx, promapi.GetLabelsParams{})
		require.NoError(t, err)
		require.NotEmpty(t, labels.Data)
		printJSON(t, labels, "labels")

		label := labels.Data[0]
		values, err := api.GetLabelValues(ctx, promapi.GetLabelValuesParams{
			Label: label,
		})
		require.NoError(t, err)
		require.NotEmpty(t, values.Data)
		printJSON(t, values, "label values")
	})
	t.Run("Metadata", func(t *testing.T) {
		t.Parallel()

		res, err := api.GetMetadata(ctx, promapi.GetMetadataParams{})
		require.NoError(t, err)
		require.NotEmpty(t, res.Data)
		printJSON(t, res.Data, "metadata")

		m := res.Data
		assert.NotEmpty(t, m["go_info"])
	})
}
