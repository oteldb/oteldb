package lokie2e_test

import (
	"strings"
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/integration/lokie2e"
	"github.com/go-faster/oteldb/internal/chstorage"
)

func TestCH(t *testing.T) {
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")
	)
	_, c, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "lokie2e",
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
	runTest(ctx, t, provider, set, querier, querier)
}

func TestCHBackup(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	var (
		ctx         = t.Context()
		provider    = integration.TraceProvider(t)
		tablePrefix = strings.ReplaceAll(uuid.NewString(), "-", "")

		backupDir = t.TempDir()
		set       *lokie2e.BatchSet
	)

	// Create backup.
	{
		_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
			Name:           "lokie2e-backup",
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
			Name:           "lokie2e-restore",
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
		runTest(ctx, t, provider, set, querier, querier)
	}
}
