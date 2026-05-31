package chstorage

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
)

// MigrateOptions configures a Migrate operation.
type MigrateOptions struct {
	// DefaultTenant is the tenant_id value written to rows migrated from tables
	// that do not yet have a tenant_id column. Defaults to empty string.
	DefaultTenant string
}

// BackupInfo describes a backup table left by a migrate run.
type BackupInfo struct {
	// Name is the full backup table name (e.g. "logs_backup").
	Name string
	// Rows is the current row count.
	Rows uint64
}

// Migrate performs a schema upgrade for all tables in MigrationUpgrade state.
//
// For each such table it renames the existing table to <table>_backup (safe to
// re-run — skipped if the backup already exists), clears stale schema-hash
// entries, creates the new table via Create, and copies data partition-by-partition
// from the backup. Partition data already present in the target is dropped before
// re-insertion so the operation is idempotent.
//
// logf, if non-nil, receives formatted progress messages.
func (m *Migrator) Migrate(ctx context.Context, opts MigrateOptions, logf func(format string, args ...any)) error {
	if logf == nil {
		logf = func(string, ...any) {}
	}

	diffs, err := m.Diff(ctx)
	if err != nil {
		return errors.Wrap(err, "diff")
	}

	existingBackups, err := m.backupTables(ctx)
	if err != nil {
		return errors.Wrap(err, "find backup tables")
	}

	// Build the set of tables that need upgrading from both the schema diff
	// and any leftover backup tables (handles crash-after-rename resume).
	pendingSet := make(map[string]bool)
	for _, t := range existingBackups {
		pendingSet[strings.TrimSuffix(t, "_backup")] = true
	}
	for _, d := range diffs {
		if d.Status == MigrationUpgrade {
			pendingSet[d.Table] = true
		}
	}

	upgrades := make([]string, 0, len(pendingSet))
	for table := range pendingSet {
		upgrades = append(upgrades, table)
	}
	slices.Sort(upgrades)

	if len(upgrades) == 0 {
		logf("migrate: no tables need upgrade")
		return m.Create(ctx)
	}

	// Phase 1: Rename existing tables to _backup.
	for _, table := range upgrades {
		oldTable := table + "_backup"
		exists, err := m.tableExists(ctx, oldTable)
		if err != nil {
			return errors.Wrapf(err, "check if %s exists", oldTable)
		}
		if !exists {
			logf("Migrating %s...", table)
			var sb strings.Builder
			fmt.Fprintf(&sb, "RENAME TABLE `%s` TO `%s`", table, oldTable)
			if cluster := m.opts.Cluster; cluster != "" && m.opts.Replicated {
				fmt.Fprintf(&sb, " ON CLUSTER '%s'", cluster)
			}
			if err := m.client.Do(ctx, ch.Query{
				Logger: zctx.From(ctx).Named("ch"),
				Body:   sb.String(),
			}); err != nil {
				return errors.Wrapf(err, "rename %s", table)
			}
		} else {
			logf("Backup table %s already exists, resuming migration.", oldTable)
		}
	}

	// Phase 2: Clear stale hash entries so Create does not see incompatible schema.
	if err := m.ClearHashes(ctx, upgrades); err != nil {
		return errors.Wrap(err, "clear migration hashes")
	}

	// Phase 3: Create new tables with the current schema.
	if err := m.Create(ctx); err != nil {
		return errors.Wrap(err, "create new schema")
	}

	// Phase 4: Copy data partition-by-partition from each backup.
	for _, table := range upgrades {
		oldTable := table + "_backup"

		cols, hasTenantID, err := m.getColumns(ctx, oldTable)
		if err != nil {
			return errors.Wrapf(err, "get columns for %s", oldTable)
		}
		if len(cols) == 0 {
			continue
		}

		partitions, err := m.getPartitions(ctx, oldTable)
		if err != nil {
			return errors.Wrapf(err, "get partitions for %s", oldTable)
		}

		logf("Copying data for %s (%d partitions)...", table, len(partitions))

		quotedCols := make([]string, len(cols))
		for i, c := range cols {
			quotedCols[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
		}
		colList := strings.Join(quotedCols, ", ")

		for _, partID := range partitions {
			if err := m.copyPartition(ctx, opts, table, oldTable, partID, colList, hasTenantID); err != nil {
				return err
			}
			logf("  Migrated partition %s", partID)
		}

		logf("Successfully migrated %s", table)
	}

	return nil
}

func (m *Migrator) copyPartition(
	ctx context.Context,
	opts MigrateOptions,
	table, oldTable, partID, colList string,
	hasTenantID bool,
) error {
	escapedPartID := strings.ReplaceAll(partID, "'", "''")

	oldCount, err := m.getPartitionRowCount(ctx, oldTable, escapedPartID)
	if err != nil {
		return errors.Wrapf(err, "get old partition row count for %s part %s", oldTable, partID)
	}

	// Drop any partial data already in the target partition for idempotency.
	exists, err := m.targetPartitionExists(ctx, table, escapedPartID)
	if err != nil {
		return errors.Wrapf(err, "check if target partition exists for %s part %s", table, partID)
	}
	if exists {
		var sb strings.Builder
		fmt.Fprintf(&sb, "ALTER TABLE `%s`", table)
		if cluster := m.opts.Cluster; cluster != "" && m.opts.Replicated {
			fmt.Fprintf(&sb, " ON CLUSTER '%s'", m.opts.Cluster)
		}
		fmt.Fprintf(&sb, " DROP PARTITION ID '%s'", escapedPartID)
		if err := m.client.Do(ctx, ch.Query{
			Logger: zctx.From(ctx).Named("ch"),
			Body:   sb.String(),
		}); err != nil {
			return errors.Wrapf(err, "drop partition %s of %s", partID, table)
		}
	}

	var insertQuery string
	if !hasTenantID {
		escapedTenant := strings.ReplaceAll(opts.DefaultTenant, "'", "''")
		insertQuery = fmt.Sprintf(
			"INSERT INTO `%s` SELECT '%s' AS tenant_id, %s FROM `%s` WHERE _partition_id = '%s'",
			table, escapedTenant, colList, oldTable, escapedPartID,
		)
	} else {
		insertQuery = fmt.Sprintf(
			"INSERT INTO `%s` SELECT %s FROM `%s` WHERE _partition_id = '%s'",
			table, colList, oldTable, escapedPartID,
		)
	}

	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   insertQuery,
		Settings: []ch.Setting{
			{Key: "max_execution_time", Value: "0"},
		},
	}); err != nil {
		return errors.Wrapf(err, "migrate data for %s partition %s", table, partID)
	}

	if cluster := m.opts.Cluster; cluster != "" && m.opts.Replicated {
		lg := zctx.From(ctx)
		if err := m.client.Do(ctx, ch.Query{
			Logger: lg.Named("ch"),
			Body:   fmt.Sprintf("SYSTEM SYNC REPLICA `%s`", table),
		}); err != nil {
			lg.Warn("SYSTEM SYNC REPLICA failed",
				zap.String("table", table),
				zap.String("partition", partID),
				zap.Error(err),
			)
		}
	}

	newCount, err := m.getPartitionRowCount(ctx, table, escapedPartID)
	if err != nil {
		return errors.Wrapf(err, "get new partition row count for %s part %s", table, partID)
	}
	if newCount != oldCount {
		return errors.Errorf("row count mismatch for %s partition %s: expected %d, got %d",
			table, partID, oldCount, newCount,
		)
	}
	return nil
}

// ListBackups returns the backup tables left by a previous migrate run,
// along with their row counts. Only known oteldb backup table names are checked.
func (m *Migrator) ListBackups(ctx context.Context) ([]BackupInfo, error) {
	candidates := m.backupCandidates()
	var result []BackupInfo
	for _, candidate := range candidates {
		exists, err := m.tableExists(ctx, candidate)
		if err != nil {
			return nil, errors.Wrapf(err, "check %s", candidate)
		}
		if !exists {
			continue
		}
		rows, err := m.rowCount(ctx, candidate)
		if err != nil {
			return nil, errors.Wrapf(err, "count rows in %s", candidate)
		}
		result = append(result, BackupInfo{Name: candidate, Rows: rows})
	}
	return result, nil
}

// backupCandidates returns the set of backup table names for all known oteldb
// data tables. Only data tables (IsData=true) are included; the migration
// tracking table is never backed up.
func (m *Migrator) backupCandidates() []string {
	var candidates []string
	for t := range m.opts.Tables.each() {
		if !t.IsData {
			continue
		}
		base := *t.Name
		if base == "" {
			continue
		}
		candidates = append(candidates, base+"_backup")
		if m.opts.Replicated {
			candidates = append(candidates, base+"_replicated_backup", base+"_distributed_backup")
		}
	}
	return candidates
}

// backupTables returns the names of existing backup tables for known oteldb
// data tables, querying system.tables with an explicit IN list rather than a
// LIKE pattern to avoid matching unrelated tables.
func (m *Migrator) backupTables(ctx context.Context) ([]string, error) {
	candidates := m.backupCandidates()
	if len(candidates) == 0 {
		return nil, nil
	}

	quotedNames := make([]string, len(candidates))
	for i, n := range candidates {
		quotedNames[i] = "'" + strings.ReplaceAll(n, "'", "''") + "'"
	}

	var names proto.ColStr
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body: fmt.Sprintf(
			"SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN (%s)",
			strings.Join(quotedNames, ", "),
		),
		Result: proto.Results{
			{Name: "name", Data: &names},
		},
	}); err != nil {
		return nil, err
	}

	tables := make([]string, 0, names.Rows())
	for i := 0; i < names.Rows(); i++ {
		tables = append(tables, names.Row(i))
	}
	return tables, nil
}

func (m *Migrator) tableExists(ctx context.Context, table string) (bool, error) {
	var count proto.ColUInt64
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body: fmt.Sprintf(
			"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '%s'",
			strings.ReplaceAll(table, "'", "''"),
		),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	}); err != nil {
		return false, err
	}
	if count.Rows() == 0 {
		return false, nil
	}
	return count.Row(0) > 0, nil
}

func (m *Migrator) getPartitions(ctx context.Context, table string) ([]string, error) {
	var partIDs proto.ColStr
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body: fmt.Sprintf(
			"SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = '%s' AND active = 1 ORDER BY partition_id",
			strings.ReplaceAll(table, "'", "''"),
		),
		Result: proto.Results{
			{Name: "partition_id", Data: &partIDs},
		},
	}); err != nil {
		return nil, err
	}
	partitions := make([]string, 0, partIDs.Rows())
	for i := 0; i < partIDs.Rows(); i++ {
		partitions = append(partitions, partIDs.Row(i))
	}
	return partitions, nil
}

func (m *Migrator) getPartitionRowCount(ctx context.Context, table, escapedPartitionID string) (uint64, error) {
	var count proto.ColUInt64
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body: fmt.Sprintf(
			"SELECT count() FROM `%s` WHERE _partition_id = '%s'",
			table, escapedPartitionID,
		),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	}); err != nil {
		return 0, err
	}
	if count.Rows() == 0 {
		return 0, nil
	}
	return count.Row(0), nil
}

func (m *Migrator) targetPartitionExists(ctx context.Context, table, escapedPartitionID string) (bool, error) {
	var count proto.ColUInt64
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body: fmt.Sprintf(
			"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '%s' AND partition_id = '%s' AND active = 1",
			strings.ReplaceAll(table, "'", "''"),
			escapedPartitionID,
		),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	}); err != nil {
		return false, err
	}
	if count.Rows() == 0 {
		return false, nil
	}
	return count.Row(0) > 0, nil
}

// getColumns returns column names of a table in position order, and whether
// tenant_id is one of them.
func (m *Migrator) getColumns(ctx context.Context, table string) (cols []string, hasTenantID bool, _ error) {
	var nameCol proto.ColStr
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body: fmt.Sprintf(
			"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '%s' ORDER BY position",
			strings.ReplaceAll(table, "'", "''"),
		),
		Result: proto.Results{
			{Name: "name", Data: &nameCol},
		},
	}); err != nil {
		return nil, false, err
	}
	cols = make([]string, 0, nameCol.Rows())
	for i := 0; i < nameCol.Rows(); i++ {
		c := nameCol.Row(i)
		cols = append(cols, c)
		if c == "tenant_id" {
			hasTenantID = true
		}
	}
	return cols, hasTenantID, nil
}

// rowCount returns the total number of rows in a table.
func (m *Migrator) rowCount(ctx context.Context, table string) (uint64, error) {
	var count proto.ColUInt64
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT count() FROM `%s`", table),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	}); err != nil {
		return 0, err
	}
	if count.Rows() == 0 {
		return 0, nil
	}
	return count.Row(0), nil
}
