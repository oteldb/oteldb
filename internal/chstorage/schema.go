package chstorage

import (
	"iter"

	"github.com/go-faster/errors"

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
	for table := range t.each() {
		if err := cb(table.Name); err != nil {
			return errors.Wrapf(err, "table %q", *table.Name)
		}
	}
	return nil
}

func (t *Tables) each() iter.Seq[oteldbTable] {
	return func(yield func(oteldbTable) bool) {
		for _, table := range []struct {
			field     *string
			fieldName string
			data      bool
			ddl       ddl.Table
		}{
			{&t.Spans, "Spans", true, newSpanColumns().DDL()},
			{&t.Tags, "Tags", true, newTracesTagsDDL()},

			{&t.Points, "Points", true, newPointColumns().DDL()},
			{&t.Timeseries, "Timeseries", true, newTimeseriesColumns().DDL()},
			{&t.ExpHistograms, "ExpHistograms", true, newExpHistogramColumns().DDL()},
			{&t.Exemplars, "Exemplars", true, newExemplarColumns().DDL()},
			{&t.Labels, "Labels", true, newLabelsColumns().DDL()},

			{&t.Logs, "Logs", true, newLogColumns().DDL()},
			{&t.LogAttrs, "LogAttrs", true, newLogAttrMapColumns().DDL()},

			{&t.Migration, "Migration", false, t.migrationDDL()},
		} {
			t := oteldbTable{
				Name:   table.field,
				DDL:    table.ddl,
				IsData: table.data,
			}
			if !yield(t) {
				return
			}
		}
	}
}

type oteldbTable struct {
	Name   *string
	DDL    ddl.Table
	IsData bool
}

func (t *Tables) migrationDDL() ddl.Table {
	return newMigrationColumns().DDL()
}
