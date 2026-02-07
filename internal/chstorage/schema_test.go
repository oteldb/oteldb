package chstorage

import (
	"testing"
	"time"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/ddl"
)

func TestGenerateDDL(t *testing.T) {
	tables := DefaultTables()
	m := NewMigrator(nil, MigratorOptions{
		Tables: tables,
		TTL:    72 * time.Hour,
	})

	for _, tt := range []struct {
		name string
		ddl  ddl.Table
	}{
		{tables.Spans, newSpanColumns().DDL()},
		{tables.Tags, newTracesTagsDDL()},
		{tables.Points, newPointColumns().DDL()},
		{tables.Timeseries, newTimeseriesColumns().DDL()},
		{tables.ExpHistograms, newExpHistogramColumns().DDL()},
		{tables.Exemplars, newExemplarColumns().DDL()},
		{tables.Labels, newLabelsColumns().DDL()},
		{tables.Logs, newLogColumns().DDL()},
		{tables.LogAttrs, newLogAttrMapColumns().DDL()},
		{tables.Migration, newMigrationColumns().DDL()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out, err := m.generateQuery(tt.name, tt.ddl, true)
			require.NoError(t, err)
			gold.Str(t, out, "schema."+tt.name+".sql")
		})
	}
}
