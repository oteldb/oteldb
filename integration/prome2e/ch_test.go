package prome2e_test

import (
	"testing"

	"github.com/go-faster/sdk/zctx"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/integration/prome2e"
	"github.com/go-faster/oteldb/internal/chstorage"
)

func TestCH(t *testing.T) {
	t.Parallel()
	integration.Skip(t)
	var (
		ctx      = t.Context()
		provider = integration.TraceProvider(t)
	)
	c, tables := integration.SetupCH(t, "prome2e", provider)

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
		ctx       = t.Context()
		backupDir = t.TempDir()
		provider  = integration.TraceProvider(t)

		set prome2e.BatchSet
	)

	// Create backup.
	{
		client, tables := integration.SetupCH(t, "prome2e-backup", provider)

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
		client, tables := integration.SetupCH(t, "prome2e-restore", provider)

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
