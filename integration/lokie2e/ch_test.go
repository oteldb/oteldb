package lokie2e_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainerslog "github.com/testcontainers/testcontainers-go/log"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/internal/chstorage"
)

func randomPrefix() string {
	var data [6]byte
	_, _ = rand.Read(data[:])
	return fmt.Sprintf("%x", data[:])
}

func setupCH(t *testing.T, name string, provider trace.TracerProvider) (chstorage.ClickHouseClient, chstorage.Tables) {
	t.Helper()
	ctx := t.Context()

	req := testcontainers.ContainerRequest{
		Name:         fmt.Sprintf("oteldb-%s-clickhouse", name),
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

	prefix := randomPrefix()
	tables := chstorage.DefaultTables()
	tables.TTL = time.Since(time.Date(2010, 1, 1, 1, 1, 1, 1, time.UTC))
	tables.Each(func(name *string) error {
		old := *name
		*name = prefix + "_" + old
		return nil
	})
	t.Logf("Test tables prefix: %s", prefix)
	require.NoError(t, tables.Create(ctx, c))

	return c, tables
}

func TestCH(t *testing.T) {
	integration.Skip(t)
	ctx := t.Context()
	provider := integration.TraceProvider(t)

	c, tables := setupCH(t, "lokie2e", provider)

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

func TestCHBackup(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	var (
		ctx       = t.Context()
		backupDir = t.TempDir()
		provider  = integration.TraceProvider(t)
	)

	// Create backup.
	{
		client, tables := setupCH(t, "lokie2e-backup", provider)

		inserter, err := chstorage.NewInserter(client, chstorage.InserterOptions{
			Tables:         tables,
			TracerProvider: provider,
		})
		require.NoError(t, err)

		querier, err := chstorage.NewQuerier(client, chstorage.QuerierOptions{
			Tables:         tables,
			TracerProvider: provider,
		})
		require.NoError(t, err)

		ctx = zctx.Base(ctx, integration.Logger(t))
		runTest(ctx, t, provider, inserter, querier, querier)

		b := chstorage.NewBackup(client, tables, integration.Logger(t).Named("backup"))
		require.NoError(t, b.Create(ctx, backupDir))
	}

	// Restore into destination
	{
		client, tables := setupCH(t, "lokie2e-restore", provider)

		r := chstorage.NewRestore(client, tables, integration.Logger(t).Named("restore"))
		require.NoError(t, r.Restore(ctx, backupDir))

		querier, err := chstorage.NewQuerier(client, chstorage.QuerierOptions{
			Tables:         tables,
			TracerProvider: provider,
		})
		require.NoError(t, err)

		ctx = zctx.Base(ctx, integration.Logger(t))
		runTest(ctx, t, provider, nil, querier, querier)
	}
}
