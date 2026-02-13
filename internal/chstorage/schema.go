package chstorage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/ddl"
)

// Tables define table names.
type Tables struct {
	Spans string
	Tags  string

	Points        string
	Timeseries    string
	ExpHistograms string
	Exemplars     string
	Labels        string

	Logs     string
	LogAttrs string

	Migration string

	TTL        time.Duration
	Cluster    string
	Replicated bool
}

// Validate checks table names
func (t *Tables) Validate() error {
	return t.Each(func(name *string) error {
		if *name == "" {
			return errors.New("table name must be non-empty")
		}
		return nil
	})
}

// Each calls given callback for each table.
func (t *Tables) Each(cb func(name *string) error) error {
	for _, table := range []struct {
		field     *string
		fieldName string
	}{
		{&t.Spans, "Spans"},
		{&t.Tags, "Tags"},

		{&t.Points, "Points"},
		{&t.Timeseries, "Timeseries"},
		{&t.ExpHistograms, "ExpHistograms"},
		{&t.Exemplars, "Exemplars"},
		{&t.Labels, "Labels"},

		{&t.Logs, "Logs"},
		{&t.LogAttrs, "LogAttrs"},

		{&t.Migration, "Migration"},
	} {
		if err := cb(table.field); err != nil {
			return errors.Wrapf(err, "table %s", table.fieldName)
		}
	}
	return nil
}

// DefaultTables returns default tables.
func DefaultTables() Tables {
	return Tables{
		Spans: "traces_spans",
		Tags:  "traces_tags",

		Points:        "metrics_points",
		Timeseries:    "metrics_timeseries",
		ExpHistograms: "metrics_exp_histograms",
		Exemplars:     "metrics_exemplars",
		Labels:        "metrics_labels",

		Logs:     "logs",
		LogAttrs: "logs_attrs",

		Migration: "migration",
	}
}

func (t Tables) getHashes(ctx context.Context, c ClickHouseClient) (map[string]string, error) {
	col := newMigrationColumns()
	if err := c.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT table, ddl FROM %s FINAL", t.Migration),
		Result: col.Result(),
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}
	return col.Mapping(), nil
}

func (t Tables) saveHashes(ctx context.Context, c ClickHouseClient, m map[string]string) error {
	col := newMigrationColumns()
	col.Save(m)
	if err := c.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Input:  col.Input(),
		Body:   col.Input().Into(t.Migration),
	}); err != nil {
		return errors.Wrap(err, "query")
	}
	return nil
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

type generateOptions struct {
	Name string
	DDL  ddl.Table
}

func (t Tables) generateQuery(opts generateOptions) (string, error) {
	d := opts.DDL
	d.Name = opts.Name
	if t.Cluster != "" {
		d.Cluster = t.Cluster
	}
	if t.TTL > 0 && d.TTL.Field != "" {
		d.TTL.Delta = t.TTL
	}
	s, err := ddl.Generate(d)
	if err != nil {
		return "", errors.Wrap(err, "generate")
	}
	return s, nil
}

// Create creates tables.
func (t Tables) Create(ctx context.Context, c ClickHouseClient) error {
	return t.create(ctx, c, true)
}

// CreateNonDestructive creates tables, but does not re-create them if schema mismatches.
func (t Tables) CreateNonDestructive(ctx context.Context, c ClickHouseClient) error {
	return t.create(ctx, c, false)
}

func (t Tables) create(ctx context.Context, c ClickHouseClient, destructive bool) error {
	lg := zctx.From(ctx)

	if err := t.Validate(); err != nil {
		return errors.Wrap(err, "validate")
	}
	database, err := queryCurrentDatabase(ctx, c)
	if err != nil {
		return errors.Wrap(err, "get current database")
	}

	{
		table := ddl.Table{
			Engine: ddl.Engine{
				Type: "ReplacingMergeTree",
				Args: []string{"ts"},
			},
			OrderBy: []string{"table"},
			Columns: []ddl.Column{
				{Name: "table", Type: "String"},
				{Name: "ddl", Type: "String"},
				{Name: "ts", Type: "DateTime", Default: "now()"},
			},
		}
		if t.Replicated {
			rep, _ := table.Engine.Replicated(t.Migration)
			table.Engine = rep
		}
		query, err := t.generateQuery(generateOptions{
			Name: t.Migration,
			DDL:  table,
		})
		if err != nil {
			return errors.Wrap(err, "generate migration table ddl")
		}
		if err := t.createTable(ctx, c, query); err != nil {
			return errors.Wrapf(err, "create %q", t.Migration)
		}
	}

	hashes, err := t.getHashes(ctx, c)
	if err != nil {
		return errors.Wrap(err, "get hashes")
	}

	for _, s := range []generateOptions{
		{Name: t.Spans, DDL: newSpanColumns().DDL()},
		{Name: t.Tags, DDL: newTracesTagsDDL()},
		{Name: t.Points, DDL: newPointColumns().DDL()},
		{Name: t.Timeseries, DDL: newTimeseriesColumns().DDL()},
		{Name: t.ExpHistograms, DDL: newExpHistogramColumns().DDL()},
		{Name: t.Exemplars, DDL: newExemplarColumns().DDL()},
		{Name: t.Labels, DDL: newLabelsColumns().DDL()},
		{Name: t.Logs, DDL: newLogColumns().DDL()},
		{Name: t.LogAttrs, DDL: newLogAttrMapColumns().DDL()},
	} {
		baseName := s.Name
		distributedName := s.Name
		supportsReplication := t.Replicated
		if t.Replicated {
			baseName += "_replicated"
			rep, ok := s.DDL.Engine.Replicated(distributedName)
			if ok {
				s.DDL.Engine = rep
			} else {
				supportsReplication = false
				lg.Warn("Table does not support replication", zap.String("table", s.Name))
			}
		}

		query, err := t.generateQuery(s)
		if err != nil {
			return errors.Wrapf(err, "generate %q", s.Name)
		}
		target := fmt.Sprintf("%x", sha256.Sum256([]byte(query)))
		if current, ok := hashes[s.Name]; ok && current != target {
			if !destructive {
				return errors.Errorf("unexpected %q table schema, changing schema is not allowed", baseName)
			}

			// HACK: this will DROP all data in the table
			// TODO: implement ALTER TABLE
			lg.Warn("DROPPING TABLE (schema changed!)",
				zap.String("table", baseName),
				zap.String("current", current),
				zap.String("target", target),
			)
			if err := t.dropTable(ctx, c, baseName); err != nil {
				return errors.Wrapf(err, "drop %q", baseName)
			}

			// Update Distributed engine as well.
			if t.Replicated {
				lg.Info("DROPPING Distributed TABLE", zap.String("name", distributedName))
				if err := t.dropTable(ctx, c, distributedName); err != nil {
					return errors.Wrapf(err, "drop %q", distributedName)
				}
			}
		}
		hashes[baseName] = target
		if err := t.createTable(ctx, c, query); err != nil {
			return errors.Wrapf(err, "create %q", baseName)
		}

		if supportsReplication {
			// Distributed tables support neither of these things.
			s.DDL.Indexes = nil
			s.DDL.OrderBy = nil
			s.DDL.PrimaryKey = nil
			s.DDL.PartitionBy = ""
			s.DDL.Engine = ddl.Engine{
				Type: "Distributed",
				Args: []string{fmt.Sprintf("'%s'", t.Cluster), database, baseName},
			}
			query, err := t.generateQuery(s)
			if err != nil {
				return errors.Wrapf(err, "generate %q", distributedName)
			}
			if err := t.createTable(ctx, c, query); err != nil {
				return errors.Wrapf(err, "create %q", distributedName)
			}
		}
	}
	if err := t.saveHashes(ctx, c, hashes); err != nil {
		return errors.Wrap(err, "save hashes")
	}

	return nil
}

func (t Tables) createTable(ctx context.Context, c ClickHouseClient, query string) error {
	lg := zctx.From(ctx)
	q := ch.Query{
		Logger: lg.Named("ch"),
		Body:   query,
	}
	if t.Replicated {
		progress := new(clusterDDLResult)
		q.Result = progress.Result()
		q.OnResult = progress.Reporter(lg)
	}
	return c.Do(ctx, q)
}

func (t Tables) dropTable(ctx context.Context, c ClickHouseClient, name string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
	if t.Cluster != "" {
		query += fmt.Sprintf(" ON CLUSTER '%s' SYNC", t.Cluster)
	}
	lg := zctx.From(ctx)
	q := ch.Query{
		Logger: lg.Named("ch"),
		Body:   query,
	}
	if t.Replicated {
		progress := new(clusterDDLResult)
		q.Result = progress.Result()
		q.OnResult = progress.Reporter(lg)
	}
	return c.Do(ctx, q)
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
