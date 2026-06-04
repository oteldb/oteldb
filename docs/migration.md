# Schema Migration Operations

This document describes the schema migration procedure for oteldb, focusing on the multitenancy migration that prepends `tenant_id` to the ORDER BY / PRIMARY KEY of all signal tables.

## Overview

ClickHouse MergeTree ORDER BY cannot be altered in place. Migrating to a new ordering key requires:

1. Rename existing table to a backup name.
2. Create new table with the updated schema.
3. Copy data from the backup table to the new table.

`odbmigrate migrate` automates this sequence. The backup tables are retained after migration and must be dropped manually once data integrity is verified.

## Prerequisites

**Stop all writers before running `odbmigrate migrate`.**

During migration the target tables are temporarily absent (between rename and re-creation), and then contain no data (between creation and copy). Any writes that land during this window are lost.

Stop the `oteldb` process (or scale its replicas to zero) before running the migration, and restart it only after `odbmigrate status` confirms all tables are `OK`.

## Commands reference

| Command | Description |
|---|---|
| `odbmigrate status` | Show live engine type, `tenant_id` presence, and DDL status per table. |
| `odbmigrate status --diff` | Same, plus print SQL diff for tables in `UPGRADE` state. |
| `odbmigrate migrate --yes` | Rename → recreate → copy data for tables that need upgrading. |
| `odbmigrate cleanup` | Dry-run: list `_backup` tables and their row counts. |
| `odbmigrate cleanup --yes` | Drop `_backup` tables left by a previous `migrate` run. |
| `odbmigrate create` | Create schema for new deployments (no migration). |
| `odbmigrate drop` | Drop all oteldb tables (destructive; development use). |

## Running the migration

```bash
# 1. Check which tables need upgrading.
# Shows engine type, whether tenant_id is present, and DDL status per table.
odbmigrate status --dsn "$CH_DSN"
# Example output:
#
# Engine:  MergeTree (standalone)
#
# TABLE                           ENGINE                  TENANT_ID   STATUS
# ------------------------------  ----------------------  ----------  ------
# logs                            MergeTree               no          UPGRADE
# traces_spans                    MergeTree               no          UPGRADE
# metrics_points                  MergeTree               no          UPGRADE
# ...

# 2. Inspect the exact SQL changes for any UPGRADE tables
odbmigrate status --diff --dsn "$CH_DSN"

# 3. Run the migration (--yes required; stop oteldb first)
odbmigrate migrate --yes --dsn "$CH_DSN"

# 4. Confirm all tables are OK
odbmigrate status --dsn "$CH_DSN"

# 5. After verifying row counts, drop the backup tables
odbmigrate cleanup --dsn "$CH_DSN"   # dry-run first
odbmigrate cleanup --yes --dsn "$CH_DSN"
```

The `migrate` command:
1. Calls `Diff()` to identify tables in `UPGRADE` state, plus any `<table>_backup` tables left by a previous run.
2. For each such table, renames it to `<table>_backup` (skipped if the backup already exists — safe to re-run after any crash).
3. Clears the stale schema-hash entries from the migration tracking table so `Create()` sees the tables as new.
4. Creates new tables with the current schema (including `tenant_id`) via `Migrator.Create()`.
5. For each backup table, queries `system.columns` to discover the old column list (column names are backtick-quoted in the generated SQL).
6. Copies data partition-by-partition: any partition already present in the target is dropped first, then re-inserted from the backup — making the copy step fully idempotent. If the old table has no `tenant_id` column, `'<default-tenant>'` is prepended as the first selected value.
7. Validates the row count of each partition after copying.
8. Reports success per table.

### Available flags

| Flag | Default | Description |
|---|---|---|
| `--dsn` | `$CH_DSN` | ClickHouse DSN |
| `--yes` | | Required: confirms the destructive rename step |
| `--default-tenant` | `""` (empty string) | Tenant ID assigned to all migrated rows |
| `--cluster` | `{cluster}` | Cluster name for `ON CLUSTER` DDL |
| `--replicated` | false | Enable replicated table mode |
| `--keeper-path-prefix` | | ZooKeeper/Keeper path prefix for replicated tables |
| `--ttl` | | TTL for tables (e.g. `30d`, `1y`) |

## Failure modes and recovery

### Crash after rename, before CREATE

**Symptom:** `odbmigrate status` shows tables as `CREATE` (missing); `<table>_backup` tables exist.

**Recovery:** Re-run `odbmigrate migrate --yes`. The command checks for existing `_backup` tables and skips the rename step, then proceeds with CREATE and data copy.

### Crash after CREATE, before copy completes

**Symptom:** `odbmigrate status` shows tables as `OK`; `<table>_backup` exists; new table has fewer rows than expected (row count mismatch error on the previous run).

**Recovery:** Re-run `odbmigrate migrate --yes` directly — no manual truncation needed.

```bash
odbmigrate migrate --yes --dsn "$CH_DSN"
```

The command detects the existing `_backup` tables, skips the rename step, and resumes the data copy. Each partition is idempotent: any partition that already exists in the target is dropped before being re-inserted, so partial writes from the crashed run are cleaned up automatically.

### Row count mismatch

The command validates row counts after each table copy and returns an error if they differ. A mismatch indicates a partial copy (e.g. CH insert error mid-stream). Follow the "Crash after CREATE" recovery steps above.

## Post-migration cleanup

Backup tables are intentionally retained. Once you have verified data integrity (row counts, spot-check queries), drop them with `odbmigrate cleanup`:

```bash
# Dry-run: shows backup tables and row counts
odbmigrate cleanup --dsn "$CH_DSN"

# Actually drop them
odbmigrate cleanup --yes --dsn "$CH_DSN"
```

`cleanup` checks for backup tables by name against the known set of oteldb tables (it does not scan for arbitrary `_backup` tables in the database). In replicated mode it also checks for `_replicated_backup` and `_distributed_backup` variants. It uses cluster-aware `DROP TABLE IF EXISTS ... ON CLUSTER` when `--replicated` is set.

## Replicated deployments

When `--replicated` is set, `odbmigrate` operates on `<table>_replicated` and `<table>_distributed` table variants. The rename uses `RENAME TABLE ... ON CLUSTER '<cluster>'` to apply the rename cluster-wide. The Replicated engine propagates the schema change to all replicas automatically.

Confirm all replicas have applied the new schema before restarting writers.

## Multitenancy-specific notes

The multitenancy upgrade adds `tenant_id LowCardinality(String)` as the first column and first component of ORDER BY / PRIMARY KEY on all signal tables. See `docs/multitenancy.md` for the full schema change list.

After migration:
- All pre-migration rows receive `tenant_id = '<default-tenant>'` (empty string by default).
- New rows receive `tenant_id` from the configured tenant mapping (`key_attributes` map or `tenant_id_template`).
- When `multitenancy.enabled: false` (single-tenant mode), `decisionFilters()` returns no predicate and all rows are visible regardless of `tenant_id`. No data fixup is needed for single-tenant deployments.
- When multi-tenancy is enabled, queries will filter by authorized `tenant_id` values. Migrated rows with `tenant_id = ''` are only visible to credentials that include `''` in their `TenantIDs`. Use `--default-tenant` to assign a meaningful value at migration time, or patch the data afterward:

```sql
ALTER TABLE logs UPDATE tenant_id = 'myapp-prod' WHERE tenant_id = '';
```

## Migration checklist

- [ ] Stop the `oteldb` process (or scale replicas to 0).
- [ ] `odbmigrate status --diff` — confirm which tables show `UPGRADE` and inspect the SQL changes.
- [ ] `odbmigrate migrate --yes [--default-tenant <id>]` — run the migration. If interrupted, re-run the same command; it resumes safely from whatever point it reached.
- [ ] `odbmigrate status` — confirm all tables are `OK`.
- [ ] Restart the `oteldb` process (or scale replicas back up).
- [ ] `odbmigrate cleanup` — review backup tables and row counts.
- [ ] `odbmigrate cleanup --yes` — drop backup tables after verifying data integrity.
