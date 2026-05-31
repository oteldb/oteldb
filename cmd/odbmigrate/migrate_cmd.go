package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/sdk/zctx"
)

func tableExists(ctx context.Context, client chstorage.ClickHouseClient, table string) (bool, error) {
	var count proto.ColUInt64
	err := client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '%s'", table),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	})
	if err != nil {
		return false, err
	}
	if count.Rows() == 0 {
		return false, nil
	}
	return count.Row(0) > 0, nil
}

func getBackupTables(ctx context.Context, client chstorage.ClickHouseClient) ([]string, error) {
	var names proto.ColStr
	err := client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '%\\_backup'",
		Result: proto.Results{
			{Name: "name", Data: &names},
		},
	})
	if err != nil {
		return nil, err
	}
	var tables []string
	for i := 0; i < names.Rows(); i++ {
		tables = append(tables, names.Row(i))
	}
	return tables, nil
}

func getPartitions(ctx context.Context, client chstorage.ClickHouseClient, table string) ([]string, error) {
	var partIDs proto.ColStr
	err := client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = '%s' AND active = 1 ORDER BY partition_id", table),
		Result: proto.Results{
			{Name: "partition_id", Data: &partIDs},
		},
	})
	if err != nil {
		return nil, err
	}
	var partitions []string
	for i := 0; i < partIDs.Rows(); i++ {
		partitions = append(partitions, partIDs.Row(i))
	}
	return partitions, nil
}

func getPartitionRowCount(ctx context.Context, client chstorage.ClickHouseClient, table, partitionID string) (uint64, error) {
	var count proto.ColUInt64
	err := client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT count() FROM `%s` WHERE _partition_id = '%s'", table, partitionID),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	})
	if err != nil {
		return 0, err
	}
	if count.Rows() == 0 {
		return 0, nil
	}
	return count.Row(0), nil
}

func targetPartitionExists(ctx context.Context, client chstorage.ClickHouseClient, table, partitionID string) (bool, error) {
	var count proto.ColUInt64
	err := client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '%s' AND partition_id = '%s' AND active = 1", table, partitionID),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	})
	if err != nil {
		return false, err
	}
	if count.Rows() == 0 {
		return false, nil
	}
	return count.Row(0) > 0, nil
}

func newMigrateCmd() *cobra.Command {
	setChFlag, dial := chFlag()
	var opts chstorage.MigratorOptions
	var yes bool
	var defaultTenant string

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate ClickHouse schema (e.g. to multitenancy).",
		Long:  "Migrate existing oteldb schema to new schema versions. This may rename tables, recreate them, and copy data.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes {
				return errors.New("migration requires --yes flag to confirm")
			}

			ctx := cmd.Context()
			client, err := dial(ctx)
			if err != nil {
				return errors.Wrap(err, "dial clickhouse")
			}

			migrator := chstorage.NewMigrator(client, opts)

			diffs, err := migrator.Diff(ctx)
			if err != nil {
				return fmt.Errorf("diff: %w", err)
			}

			backupTables, err := getBackupTables(ctx, client)
			if err != nil {
				return errors.Wrap(err, "find backup tables")
			}

			pendingSet := make(map[string]bool)
			for _, t := range backupTables {
				baseTable := strings.TrimSuffix(t, "_backup")
				pendingSet[baseTable] = true
			}
			for _, d := range diffs {
				if d.Status == chstorage.MigrationUpgrade {
					pendingSet[d.Table] = true
				}
			}

			var upgrades []string
			for table := range pendingSet {
				upgrades = append(upgrades, table)
			}

			if len(upgrades) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "migrate: no tables need upgrade")
				return migrator.Create(ctx)
			}

			for _, table := range upgrades {
				oldTable := table + "_backup"
				exists, err := tableExists(ctx, client, oldTable)
				if err != nil {
					return errors.Wrapf(err, "check if %s exists", oldTable)
				}
				if !exists {
					fmt.Fprintf(cmd.OutOrStdout(), "Migrating %s...\n", table)
					var renameSb strings.Builder
					fmt.Fprintf(&renameSb, "RENAME TABLE `%s` TO `%s`", table, oldTable)
					if cluster := opts.Cluster; cluster != "" && opts.Replicated {
						fmt.Fprintf(&renameSb, " ON CLUSTER '%s'", cluster)
					}
					if err := client.Do(ctx, ch.Query{
						Logger: zctx.From(ctx).Named("ch"),
						Body:   renameSb.String(),
					}); err != nil {
						return errors.Wrapf(err, "rename %s", table)
					}
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Backup table %s already exists, resuming migration.\n", oldTable)
				}
			}

			if err := migrator.ClearHashes(ctx, upgrades); err != nil {
				return errors.Wrap(err, "clear migration hashes")
			}

			if err := migrator.Create(ctx); err != nil {
				return errors.Wrap(err, "create new schema")
			}

			for _, table := range upgrades {
				oldTable := table + "_backup"

				partitions, err := getPartitions(ctx, client, oldTable)
				if err != nil {
					return errors.Wrapf(err, "get partitions for %s", oldTable)
				}

				var oldCols proto.ColStr
				if err := client.Do(ctx, ch.Query{
					Logger: zctx.From(ctx).Named("ch"),
					Body:   fmt.Sprintf("SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '%s' ORDER BY position", oldTable),
					Result: proto.Results{
						{Name: "name", Data: &oldCols},
					},
				}); err != nil {
					return errors.Wrapf(err, "get columns for %s", oldTable)
				}

				if oldCols.Rows() == 0 {
					continue
				}

				cols := make([]string, 0, oldCols.Rows())
				hasTenantID := false
				for i := 0; i < oldCols.Rows(); i++ {
					c := oldCols.Row(i)
					cols = append(cols, c)
					if c == "tenant_id" {
						hasTenantID = true
					}
				}

				fmt.Fprintf(cmd.OutOrStdout(), "Copying data for %s (%d partitions)...\n", table, len(partitions))

				for _, partID := range partitions {
					oldCount, err := getPartitionRowCount(ctx, client, oldTable, partID)
					if err != nil {
						return errors.Wrapf(err, "get old partition row count for %s part %s", oldTable, partID)
					}

					exists, err := targetPartitionExists(ctx, client, table, partID)
					if err != nil {
						return errors.Wrapf(err, "check if target partition exists for %s part %s", table, partID)
					}
					if exists {
						var dropSb strings.Builder
						fmt.Fprintf(&dropSb, "ALTER TABLE `%s`", table)
						if cluster := opts.Cluster; cluster != "" && opts.Replicated {
							fmt.Fprintf(&dropSb, " ON CLUSTER '%s'", cluster)
						}
						fmt.Fprintf(&dropSb, " DROP PARTITION ID '%s'", partID)

						if err := client.Do(ctx, ch.Query{
							Logger: zctx.From(ctx).Named("ch"),
							Body:   dropSb.String(),
						}); err != nil {
							return errors.Wrapf(err, "drop partition %s of %s", partID, table)
						}
					}

					var insertQuery string
					if !hasTenantID {
						escapedTenant := strings.ReplaceAll(defaultTenant, "'", "''")
						insertQuery = fmt.Sprintf("INSERT INTO `%s` SELECT '%s' AS tenant_id, %s FROM `%s` WHERE _partition_id = '%s'", table, escapedTenant, strings.Join(cols, ", "), oldTable, partID)
					} else {
						insertQuery = fmt.Sprintf("INSERT INTO `%s` SELECT %s FROM `%s` WHERE _partition_id = '%s'", table, strings.Join(cols, ", "), oldTable, partID)
					}

					if err := client.Do(ctx, ch.Query{
						Logger: zctx.From(ctx).Named("ch"),
						Body:   insertQuery,
						Settings: []ch.Setting{
							{
								Key:   "max_execution_time",
								Value: "0",
							},
						},
					}); err != nil {
						return errors.Wrapf(err, "migrate data for %s partition %s", table, partID)
					}

					if cluster := opts.Cluster; cluster != "" && opts.Replicated {
						_ = client.Do(ctx, ch.Query{
							Logger: zctx.From(ctx).Named("ch"),
							Body:   fmt.Sprintf("SYSTEM SYNC REPLICA `%s`", table),
						})
					}

					newCount, err := getPartitionRowCount(ctx, client, table, partID)
					if err != nil {
						return errors.Wrapf(err, "get new partition row count for %s part %s", table, partID)
					}

					if newCount != oldCount {
						return errors.Errorf("row count mismatch for %s partition %s: expected %d, got %d", table, partID, oldCount, newCount)
					}

					fmt.Fprintf(cmd.OutOrStdout(), "  Migrated partition %s (%d rows)\n", partID, newCount)
				}

				fmt.Fprintf(cmd.OutOrStdout(), "Successfully migrated %s\n", table)
			}

			return nil
		},
	}
	opts.AddFlags(cmd.Flags())
	setChFlag(cmd)
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm migration")
	cmd.Flags().StringVar(&defaultTenant, "default-tenant", "", "Default tenant ID for migrated records")
	return cmd
}
