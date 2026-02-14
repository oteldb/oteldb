package chstorage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/common/model"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/ddl"
)

// Migrator provides migration tool for oteldb.
type Migrator struct {
	client ClickHouseClient
	opts   MigratorOptions
}

// MigratorOptions defines migration options.
type MigratorOptions struct {
	Tables           Tables
	Cluster          string
	KeeperPathPrefix string
	TTL              time.Duration
	Replicated       bool
}

func (o *MigratorOptions) setDefaults() {
	if o.Tables == (Tables{}) {
		o.Tables = DefaultTables()
	}
	if o.Cluster == "" {
		o.Cluster = "{cluster}"
	}
}

// AddFlags registers command-line flags for migrator options.
func (o *MigratorOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Cluster, "cluster", o.Cluster, "ClickHouse cluster name for distributed tables")
	fs.StringVar(&o.KeeperPathPrefix, "keeper-path-prefix", o.KeeperPathPrefix, "Clickhouse Keeper path prefix for replicated tables")
	fs.Func("ttl", "TTL for tables, e.g. 15d", func(s string) error {
		if s == "" {
			o.TTL = 0
			return nil
		}
		// Support durations in Prometheus format, e.g. 15d, 1y, etc.
		d, err := model.ParseDuration(s)
		if err != nil {
			return err
		}
		o.TTL = time.Duration(d)
		return nil
	})
	fs.BoolVar(&o.Replicated, "replicated", o.Replicated, "Enable replicated tables")
}

// NewMigrator creates new [Migrator].
func NewMigrator(client ClickHouseClient, opts MigratorOptions) *Migrator {
	opts.setDefaults()

	return &Migrator{
		client: client,
		opts:   opts,
	}
}

// Create creates schema.
func (m *Migrator) Create(ctx context.Context) error {
	t := m.opts.Tables

	if err := t.Validate(); err != nil {
		return errors.Wrap(err, "validate")
	}

	database, err := queryCurrentDatabase(ctx, m.client)
	if err != nil {
		return errors.Wrap(err, "get current database")
	}

	{
		table := m.opts.Tables.migrationDDL()
		name := t.Migration
		if m.opts.Replicated {
			name += "_replicated"
			rep, _ := table.Engine.Replicated(m.opts.KeeperPathPrefix, name)
			table.Engine = rep
		}
		query, err := m.generateQuery(name, table, true)
		if err != nil {
			return errors.Wrap(err, "generate migration table ddl")
		}
		if err := m.createTable(ctx, query); err != nil {
			return errors.Wrapf(err, "create %q", name)
		}
	}

	existingHashes, err := m.getHashes(ctx)
	if err != nil {
		return errors.Wrap(err, "get hashes")
	}
	type change struct {
		name string
		ddl  string
		hash string
	}
	var changes []change

	// Verify that all tables are in expected state before applying any changes.
	{
		for s := range m.opts.Tables.each() {
			name := *s.Name
			{
				tableName, schema, err := m.tableSchema(name, s.DDL)
				if err != nil {
					return errors.Wrapf(err, "get schema for %q", name)
				}
				name = tableName
				query, err := m.generateQuery(name, schema, true)
				if err != nil {
					return errors.Wrapf(err, "generate %q", name)
				}
				target := m.ddlQueryHash(query)
				if current, ok := existingHashes[name]; ok && current.DDLHash != target {
					return &IncompatibleSchemaError{Table: name}
				}
				changes = append(changes, change{
					name: name,
					ddl:  query,
					hash: target,
				})
			}

			if m.opts.Replicated {
				name, schema, err := m.distributedTableSchema(database, *s.Name, s.DDL)
				if err != nil {
					return errors.Wrapf(err, "get distributed schema for %q", name)
				}
				query, err := m.generateQuery(name, schema, false)
				if err != nil {
					return errors.Wrapf(err, "generate %q", name)
				}
				target := fmt.Sprintf("%x", sha256.Sum256([]byte(query)))
				if current, ok := existingHashes[name]; ok && current.DDLHash != target {
					return &IncompatibleSchemaError{Table: name}
				}
				changes = append(changes, change{
					name: name,
					ddl:  query,
					hash: target,
				})
			}
		}
	}

	newTables := make(map[string]migration, len(changes))
	for _, c := range changes {
		if err := m.createTable(ctx, c.ddl); err != nil {
			return errors.Wrapf(err, "create %q", c.name)
		}
		newTables[c.name] = migration{
			DDL:     c.ddl,
			DDLHash: c.hash,
		}
	}
	if err := m.saveHashes(ctx, newTables); err != nil {
		return errors.Wrap(err, "save new hashes")
	}

	return nil
}

// IncompatibleSchemaError is returned when there is an existing table with incompatible schema.
type IncompatibleSchemaError struct {
	Table string
}

var _ error = (*IncompatibleSchemaError)(nil)

// IncompatibleSchemaError implements [error].
func (e *IncompatibleSchemaError) Error() string {
	return fmt.Sprintf("table %q has different schema, backup data and perform the upgrade", e.Table)
}

func (m *Migrator) tableSchema(name string, schema ddl.Table) (string, ddl.Table, error) {
	if m.opts.Replicated {
		name += "_replicated"
		rep, ok := schema.Engine.Replicated(m.opts.KeeperPathPrefix, name)
		if !ok {
			return name, schema, errors.Errorf(
				"table %q (engine: %q) does not support replication, but Replicated is enabled",
				name, schema.Engine.Type,
			)
		}
		schema.Engine = rep
	}
	return name, schema, nil
}

func (m *Migrator) distributedTableSchema(database, name string, schema ddl.Table) (string, ddl.Table, error) {
	if !m.opts.Replicated {
		return name, schema, errors.New("distributed tables are only supported when Replicated Mode is enabled")
	}
	name += "_distributed"
	schema.Indexes = nil
	schema.OrderBy = nil
	schema.PrimaryKey = nil
	schema.PartitionBy = ""
	schema.Engine = ddl.Engine{
		Type: "Distributed",
		Args: []string{fmt.Sprintf("'%s'", m.opts.Cluster), database, name},
	}
	return name, schema, nil
}

// Drop drops all known tables. This will remove data.
func (m *Migrator) Drop(ctx context.Context, log func(database, table string)) error {
	return m.drop(ctx, false, log)
}

// DropIfExists drops all known existing tables. This will remove data.
func (m *Migrator) DropIfExists(ctx context.Context, log func(database, table string)) error {
	return m.drop(ctx, true, log)
}

func (m *Migrator) drop(ctx context.Context, ifExists bool, log func(database, table string)) error {
	database, err := queryCurrentDatabase(ctx, m.client)
	if err != nil {
		return errors.Wrap(err, "query current database")
	}

	existingHashes, err := m.getHashes(ctx)
	if err != nil {
		return errors.Wrap(err, "get existing hashes")
	}

	if len(existingHashes) == 0 {
		return errors.New("no tables to drop")
	}

	drop := func(table string) error {
		if log != nil {
			log(database, table)
		}

		if err := m.dropTable(ctx, database, table, ifExists); err != nil {
			if ifExists {
				return errors.Wrapf(err, "drop (if exists) %q", table)
			}
			return errors.Wrapf(err, "drop %q", table)
		}
		return nil
	}

	migrationTable := "migration"
	delete(existingHashes, migrationTable)
	for table := range existingHashes {
		if err := drop(table); err != nil {
			return err
		}
	}
	if err := drop(migrationTable); err != nil {
		return err
	}

	return nil
}

// Validate performs validation of existing schema without applying any changes.
//
// This can be used in health checks to verify that schema is compatible with the current version of oteldb.
func (m *Migrator) Validate(ctx context.Context) error {
	diff, err := m.Diff(ctx)
	if err != nil {
		return errors.Wrap(err, "query difference")
	}
	for _, d := range diff {
		if d.Status != MigrationOK {
			return &IncompatibleSchemaError{Table: d.Table}
		}
	}
	return nil
}

// Diff returns a difference of current schema and latest schema.
func (m *Migrator) Diff(ctx context.Context) ([]MigrationDiff, error) {
	database, err := queryCurrentDatabase(ctx, m.client)
	if err != nil {
		return nil, errors.Wrap(err, "get current database")
	}

	existingHashes, err := m.getHashes(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get existing hashes")
	}

	latestHashes := map[string]migration{}
	for t := range m.opts.Tables.each() {
		{
			tableName, schema, err := m.tableSchema(*t.Name, t.DDL)
			if err != nil {
				return nil, errors.Wrapf(err, "get schema for %q", *t.Name)
			}
			q, err := m.generateQuery(tableName, schema, true)
			if err != nil {
				return nil, errors.Wrapf(err, "generate ddl for %q", tableName)
			}
			latestHashes[tableName] = migration{
				DDL:     q,
				DDLHash: m.ddlQueryHash(q),
			}
		}

		if m.opts.Replicated {
			tableName, schema, err := m.distributedTableSchema(database, *t.Name, t.DDL)
			if err != nil {
				return nil, errors.Wrapf(err, "get distributed schema for %q", *t.Name)
			}
			q, err := m.generateQuery(tableName, schema, false)
			if err != nil {
				return nil, errors.Wrapf(err, "generate ddl for %q", tableName)
			}
			latestHashes[tableName] = migration{
				DDL:     q,
				DDLHash: m.ddlQueryHash(q),
			}
		}
	}

	var tables []string
	for table := range existingHashes {
		tables = append(tables, table)
	}
	for table := range latestHashes {
		tables = append(tables, table)
	}
	slices.Sort(tables)
	tables = slices.Compact(tables)

	r := make([]MigrationDiff, 0, len(existingHashes))
	for _, table := range tables {
		latest, ok := latestHashes[table]
		if !ok {
			r = append(r, MigrationDiff{Table: table, Status: MigrationDelete})
			continue
		}
		existing, ok := existingHashes[table]
		if !ok {
			r = append(r, MigrationDiff{Table: table, Status: MigrationCreate})
			continue
		}
		if latest.DDLHash != existing.DDLHash {
			dmp := diffmatchpatch.New()
			diffs := dmp.DiffMain(latest.DDL, existing.DDL, false)
			fmt.Println(dmp.DiffPrettyText(diffs))

			r = append(r, MigrationDiff{
				Table:  table,
				Diff:   dmp.DiffPrettyText(diffs),
				Status: MigrationUpgrade,
			})
			continue
		}
		r = append(r, MigrationDiff{Table: table, Status: MigrationOK})
	}

	return r, nil
}

// MigrationDiff is result of comparison of existing table and latest schema version.
type MigrationDiff struct {
	Table  string
	Diff   string
	Status MigrationStatus
}

// MigrationStatus defines status of table.
type MigrationStatus uint8

const (
	// MigrationUnknown is an unknown migration status.
	MigrationUnknown = MigrationStatus(iota)
	// MigrationDelete indicates that table should be removed in the new version.
	MigrationDelete
	// MigrationCreate indicates that table should be created in the new version.
	MigrationCreate
	// MigrationUpgrade indicates that table should be changed in the new version.
	MigrationUpgrade
	// MigrationOK indicates that table does not require any changes.
	MigrationOK
)

// String implements [fmt.Stringer].
func (s MigrationStatus) String() string {
	switch s {
	case MigrationUnknown:
		return "UNKNOWN"
	case MigrationDelete:
		return "DELETE"
	case MigrationCreate:
		return "CREATE"
	case MigrationUpgrade:
		return "UPGRADE"
	case MigrationOK:
		return "OK"
	default:
		return fmt.Sprintf("MigrationStatus(%d)", uint8(s))
	}
}

// ColorString returns colored string representation.
func (s MigrationStatus) ColorString() string {
	c := fmt.Sprintf
	switch s {
	case MigrationUnknown:
	case MigrationDelete, MigrationUpgrade:
		c = color.RedString
	case MigrationCreate:
		c = color.YellowString
	case MigrationOK:
		c = color.GreenString
	}
	return c("%s", s.String())
}

func (m *Migrator) generateQuery(name string, schema ddl.Table, ifNotExists bool) (string, error) {
	schema.Name = name

	if c := m.opts.Cluster; c != "" && m.opts.Replicated {
		schema.Cluster = c
	}
	if m.opts.TTL > 0 && schema.TTL.Field != "" {
		schema.TTL.Delta = m.opts.TTL
	}

	if ifNotExists {
		return ddl.CreateIfNotExists(schema)
	}
	return ddl.Create(schema)
}

func (m *Migrator) getHashes(ctx context.Context) (map[string]migration, error) {
	col := newMigrationColumns()
	if err := m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT table, ddl, ddl_hash FROM %s FINAL", m.migrationTableName()),
		Result: col.Result(),
	}); err != nil {
		// Gracefully handle case when migration table does not exist, which means that no migrations have been applied yet.
		if exp, ok := ch.AsException(err); ok && exp.Code == proto.ErrUnknownTable {
			return map[string]migration{}, nil
		}
		return nil, err
	}
	return col.Mapping(), nil
}

func (m *Migrator) saveHashes(ctx context.Context, hashes map[string]migration) error {
	col := newMigrationColumns()
	col.Save(hashes)

	return m.client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Input:  col.Input(),
		Body:   col.Input().Into(m.migrationTableName()),
	})
}

func (m *Migrator) migrationTableName() string {
	if m.opts.Replicated {
		return m.opts.Tables.Migration + "_replicated"
	}
	return m.opts.Tables.Migration
}

func (m *Migrator) createTable(ctx context.Context, query string) error {
	lg := zctx.From(ctx)
	q := ch.Query{
		Logger: lg.Named("ch"),
		Body:   query,
	}
	if m.opts.Replicated {
		progress := new(clusterDDLResult)
		q.Result = progress.Result()
		q.OnResult = progress.Reporter(lg)
	}
	return m.client.Do(ctx, q)
}

func (m *Migrator) dropTable(ctx context.Context, database, name string, ifExists bool) error {
	var sb strings.Builder
	sb.WriteString("DROP TABLE")
	if ifExists {
		sb.WriteString(" IF EXISTS")
	}
	sb.WriteString(" ")
	if database != "" {
		sb.WriteString(database)
		sb.WriteString(".")
	}
	sb.WriteString(name)
	if cluster := m.opts.Cluster; cluster != "" && m.opts.Replicated {
		sb.WriteString(fmt.Sprintf(" ON CLUSTER '%s' SYNC", cluster))
	}
	lg := zctx.From(ctx)
	q := ch.Query{
		Logger: lg.Named("ch"),
		Body:   sb.String(),
	}
	if m.opts.Replicated {
		progress := new(clusterDDLResult)
		q.Result = progress.Result()
		q.OnResult = progress.Reporter(lg)
	}
	return m.client.Do(ctx, q)
}

type clusterDDLResult struct {
	Host           proto.ColStr
	Port           proto.ColUInt16
	Status         proto.ColInt64
	Error          proto.ColStr
	HostsRemaining proto.ColUInt64
	HostsActive    proto.ColUInt64
}

func (r *clusterDDLResult) Reporter(lg *zap.Logger) func(ctx context.Context, block proto.Block) error {
	return func(ctx context.Context, block proto.Block) error {
		for i := range r.Status.Rows() {
			var (
				host           = r.Host.Row(i)
				port           = r.Port.Row(i)
				status         = r.Status.Row(i)
				err            = r.Error.Row(i)
				hostsRemaining = r.HostsRemaining.Row(i)
				hostsActive    = r.HostsActive.Row(i)

				node = net.JoinHostPort(
					host,
					strconv.FormatUint(uint64(port), 10),
				)
			)
			errField := zap.Skip()
			if err != "" {
				errField = zap.String("error", err)
			}
			lg.Debug("Cluster DDL progress",
				zap.String("node", node),
				zap.Int64("status", status),
				errField,
				zap.Uint64("remaining", hostsRemaining),
				zap.Uint64("active", hostsActive),
			)
		}
		return nil
	}
}

func (r *clusterDDLResult) Result() proto.Results {
	return proto.Results{
		{Name: "host", Data: &r.Host},
		{Name: "port", Data: &r.Port},
		{Name: "status", Data: &r.Status},
		{Name: "error", Data: &r.Error},
		{Name: "num_hosts_remaining", Data: &r.HostsRemaining},
		{Name: "num_hosts_active", Data: &r.HostsActive},
	}
}

func (m *Migrator) ddlQueryHash(query string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(query)))
}

func queryCurrentDatabase(ctx context.Context, c ClickHouseClient) (string, error) {
	var col proto.ColStr

	if err := c.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   "SELECT currentDatabase() AS database",
		Result: proto.Results{
			{Name: "database", Data: &col},
		},
	}); err != nil {
		return "", err
	}

	if col.Rows() < 1 {
		return "", errors.New("currentDatabase() returned empty result")
	}
	return col.First(), nil
}
