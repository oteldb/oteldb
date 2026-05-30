package chstorage

import (
	"strings"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
)

// Column is a column with name and data that can be used in INSERT or SELECT query.
type Column struct {
	Name string
	Data proto.Column
}

// MergeColumns merges multiple sets of columns into one.
func MergeColumns(sets ...Columns) Columns {
	var cols Columns
	for _, set := range sets {
		cols = append(cols, set...)
	}
	return cols
}

// Columns is a set of Columns.
type Columns []Column

// Names returns a slice of column names.
func (c Columns) Names() []string {
	names := make([]string, len(c))
	for i, col := range c {
		names[i] = col.Name
	}
	return names
}

// All returns comma-separated column names for using in SELECT query
// instead of `SELECT *`.
func (c Columns) All() string {
	return strings.Join(c.Names(), ", ")
}

// Input returns columns for using in INSERT query.
func (c Columns) Input() proto.Input {
	var cols proto.Input
	for _, col := range c {
		cols = append(cols, proto.InputColumn{
			Name: col.Name,
			Data: col.Data,
		})
	}
	return cols
}

// Result returns columns for using in SELECT query.
func (c Columns) Result() proto.Results {
	var cols proto.Results
	for _, col := range c {
		cols = append(cols, proto.ResultColumn{
			Name: col.Name,
			Data: col.Data,
		})
	}
	return cols
}

// ChsqlResult returns columns for using in SELECT query.
func (c Columns) ChsqlResult() []chsql.ResultColumn {
	cols := make([]chsql.ResultColumn, 0, len(c))
	for _, col := range c {
		cols = append(cols, chsql.ResultColumn{
			Name: col.Name,
			Expr: chsql.Ident(col.Name),
			Data: col.Data,
		})
	}
	return cols
}

// Reset columns.
func (c Columns) Reset() {
	for _, col := range c {
		col.Data.Reset()
	}
}
