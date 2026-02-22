package prome2e_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainerslog "github.com/testcontainers/testcontainers-go/log"
	testcontainersnetwork "github.com/testcontainers/testcontainers-go/network"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/integration/prome2e"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/promapi"
)

func TestCH(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")
	)
	_, c, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "prome2e",
		TablePrefix:    tablePrefix,
		TracerProvider: provider,
	})

	inserter, err := chstorage.NewInserter(c, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)
	set := loadTestData(ctx, t, inserter)

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	ctx = zctx.Base(ctx, integration.Logger(t))
	runTest(ctx, t, provider, set, querier, false)
}

func TestCHBackup(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")

		backupDir = t.TempDir()
		set       prome2e.BatchSet
	)

	// Create backup.
	{
		_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
			Name:           "prome2e-backup",
			TablePrefix:    tablePrefix,
			TracerProvider: provider,
		})

		inserter, err := chstorage.NewInserter(client, chstorage.InserterOptions{
			Tables:         tables,
			TracerProvider: provider,
		})
		require.NoError(t, err)
		set = loadTestData(ctx, t, inserter)

		b := chstorage.NewBackup(client, tables, integration.Logger(t).Named("backup"))
		require.NoError(t, b.Create(ctx, backupDir))
	}

	// Restore from backup and check.
	{
		_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
			Name:           "prome2e-restore",
			TablePrefix:    tablePrefix,
			TracerProvider: provider,
		})

		r := chstorage.NewRestore(client, tables, integration.Logger(t).Named("restore"))
		require.NoError(t, r.Restore(ctx, backupDir))

		querier, err := chstorage.NewQuerier(client, chstorage.QuerierOptions{
			Tables:         tables,
			TracerProvider: provider,
		})
		require.NoError(t, err)

		ctx = zctx.Base(ctx, integration.Logger(t))
		runTest(ctx, t, provider, set, querier, false)
	}
}

func TestCHBackupOldOteldb(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")

		backupDir = t.TempDir()
		set       prome2e.BatchSet
	)

	// Create backup.
	{
		net, err := testcontainersnetwork.New(ctx)
		require.NoError(t, err)

		ch, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
			Name:           "prome2e-old-backup",
			TablePrefix:    tablePrefix,
			Networks:       []string{net.Name},
			SkipMigrate:    true,
			TracerProvider: provider,
		})

		promClient, consumer := setupOtelDBInserter(t, net.Name, ch)
		set = loadTestData(ctx, t, consumer)

		// Wait for oteldb to be available.
		dataWaitBackoff := backoff.NewExponentialBackOff()
		dataWaitBackoff.InitialInterval = 2 * time.Second
		dataWaitBackoff.MaxElapsedTime = time.Minute
		if err := backoff.Retry(func() error {
			labels, err := promClient.GetLabels(ctx, promapi.GetLabelsParams{})
			if err != nil {
				return err
			}
			if len(labels.Data) < 1 {
				return errors.New("empty labels response")
			}
			return nil
		}, dataWaitBackoff); err != nil {
			t.Fatal(err)
		}

		b := chstorage.NewBackup(client, tables, integration.Logger(t).Named("backup"))
		require.NoError(t, b.Create(ctx, backupDir))
	}

	// Restore from backup and check.
	{
		_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
			Name:           "prome2e-new-restore",
			TablePrefix:    tablePrefix,
			TracerProvider: provider,
		})

		r := chstorage.NewRestore(client, tables, integration.Logger(t).Named("restore"))
		require.NoError(t, r.Restore(ctx, backupDir))

		querier, err := chstorage.NewQuerier(client, chstorage.QuerierOptions{
			Tables:         tables,
			TracerProvider: provider,
		})
		require.NoError(t, err)

		ctx = zctx.Base(ctx, integration.Logger(t))
		runTest(ctx, t, provider, set, querier, true)
	}
}

func setupOtelDBInserter(t *testing.T, network string, chContainer testcontainers.Container) (promapi.Invoker, MetricsConsumer) {
	ctx := t.Context()

	resp, err := chContainer.Inspect(ctx)
	require.NoError(t, err)
	chName := strings.TrimPrefix(resp.Name, "/")

	req := testcontainers.ContainerRequest{
		Name:         "old-oteldb",
		Image:        "ghcr.io/oteldb/oteldb:v0.30.0",
		ExposedPorts: []string{"4317/tcp", "9090/tcp"},
		Env: map[string]string{
			"OTELDB_STORAGE":        "ch",
			"CH_DSN":                fmt.Sprintf("clickhouse://default:default@%s:9000", chName),
			"OTEL_LOG_LEVEL":        "info",
			"OTEL_METRICS_EXPORTER": "none",
			"OTEL_LOGS_EXPORTER":    "none",
			"OTEL_TRACES_EXPORTER":  "none",
		},
		Networks: []string{network},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainerslog.TestLogger(t),
		Reuse:            true,
	})
	require.NoError(t, err, "container start")

	promEndpoint, err := container.PortEndpoint(ctx, "9090", "http")
	require.NoError(t, err, "prometheus endpoint")

	promClient, err := promapi.NewClient(promEndpoint)
	require.NoError(t, err, "prometheus client")

	// Wait for oteldb to be available.
	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute
	if err := backoff.Retry(func() error {
		_, err := promClient.GetLabels(ctx, promapi.GetLabelsParams{})
		return err
	}, connectBackoff); err != nil {
		t.Fatal(err)
	}

	otlpEndpoint, err := container.PortEndpoint(ctx, "4317", "")
	require.NoError(t, err, "otlp endpoint")

	otlpConn, err := grpc.NewClient(otlpEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "otlp client")
	otlpClient := pmetricotlp.NewGRPCClient(otlpConn)

	var consumer MetricsConsumerFunc = func(ctx context.Context, ld pmetric.Metrics) error {
		_, err := otlpClient.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(ld))
		return err
	}
	return promClient, consumer
}
