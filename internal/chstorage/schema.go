package chstorage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
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
	ExpHistograms string
	Exemplars     string
	Labels        string

	Logs     string
	LogAttrs string

	Migration string

	TTL     time.Duration
	Cluster string
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
	if err := t.Validate(); err != nil {
		return errors.Wrap(err, "validate")
	}
	{
		q, err := t.generateQuery(generateOptions{
			Name: t.Migration,
			DDL: ddl.Table{
				Engine:  "ReplacingMergeTree(ts)",
				OrderBy: []string{"table"},
				Columns: []ddl.Column{
					{Name: "table", Type: "String"},
					{Name: "ddl", Type: "String"},
					{Name: "ts", Type: "DateTime", Default: "now()"},
				},
			},
		})
		if err != nil {
			return errors.Wrap(err, "generate migration table ddl")
		}
		if err := c.Do(ctx, ch.Query{
			Logger: zctx.From(ctx).Named("ch"),
			Body:   q,
		}); err != nil {
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
		{Name: t.ExpHistograms, DDL: newExpHistogramColumns().DDL()},
		{Name: t.Exemplars, DDL: newExemplarColumns().DDL()},
		{Name: t.Labels, DDL: newLabelsColumns().DDL()},
		{Name: t.Logs, DDL: newLogColumns().DDL()},
		{Name: t.LogAttrs, DDL: newLogAttrMapColumns().DDL()},
	} {
		query, err := t.generateQuery(s)
		if err != nil {
			return errors.Wrapf(err, "generate %q", s.Name)
		}
		name := s.Name
		target := fmt.Sprintf("%x", sha256.Sum256([]byte(query)))
		if current, ok := hashes[s.Name]; ok && current != target {
			// HACK: this will DROP all data in the table
			// TODO: implement ALTER TABLE
			zctx.From(ctx).Warn("DROPPING TABLE (schema changed!)",
				zap.String("table", name),
				zap.String("current", current),
				zap.String("target", target),
			)
			if err := c.Do(ctx, ch.Query{
				Logger: zctx.From(ctx).Named("ch"),
				Body:   fmt.Sprintf("DROP TABLE IF EXISTS %s", name),
			}); err != nil {
				return errors.Wrapf(err, "drop %q", name)
			}
		}
		hashes[name] = target
		if err := c.Do(ctx, ch.Query{
			Logger: zctx.From(ctx).Named("ch"),
			Body:   query,
		}); err != nil {
			return errors.Wrapf(err, "create %q", name)
		}
	}
	if err := t.saveHashes(ctx, c, hashes); err != nil {
		return errors.Wrap(err, "save hashes")
	}

	return nil
}
