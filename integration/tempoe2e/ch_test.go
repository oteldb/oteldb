package tempoe2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainerslog "github.com/testcontainers/testcontainers-go/log"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/internal/chstorage"
)

func TestCH(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	ctx := context.Background()
	provider := integration.TraceProvider(t)

	req := testcontainers.ContainerRequest{
		Name:         "oteldb-tempoe2e-clickhouse",
		Image:        "clickhouse/clickhouse-server:25.9",
		ExposedPorts: []string{"8123/tcp", "9000/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_PASSWORD": "default",
		},
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainerslog.TestLogger(t),
		Reuse:            true,
	})
	require.NoError(t, err, "container start")

	endpoint, err := chContainer.PortEndpoint(ctx, "9000", "")
	require.NoError(t, err, "container endpoint")

	opts := ch.Options{
		Address:  endpoint,
		Database: "default",
		User:     "default",
		Password: "default",

		OpenTelemetryInstrumentation: true,
		TracerProvider:               provider,
	}

	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute
	c, err := backoff.RetryWithData(func() (*chpool.Pool, error) {
		c, err := chpool.Dial(ctx, chpool.Options{
			ClientOptions: opts,
		})
		if err != nil {
			return nil, errors.Wrap(err, "dial")
		}
		return c, nil
	}, connectBackoff)
	if err != nil {
		t.Fatal(err)
	}

	prefix := strings.ReplaceAll(uuid.NewString(), "-", "")
	tables := chstorage.DefaultTables()
	tables.Each(func(name *string) error {
		old := *name
		*name = prefix + "_" + old
		return nil
	})
	t.Logf("Test tables prefix: %s", prefix)
	require.NoError(t, tables.Create(ctx, c))

	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	ctx = zctx.Base(ctx, integration.Logger(t))
	runTest(ctx, t, provider, inserter, querier, querier)
}
