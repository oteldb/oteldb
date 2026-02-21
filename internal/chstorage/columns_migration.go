package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"

	"github.com/go-faster/oteldb/internal/ddl"
)

type migrationColumns struct {
	table   proto.ColStr // table name
	ddl     proto.ColStr // DDL
	ddlHash proto.ColStr // SHA256(DDL)
}

func (c *migrationColumns) columns() Columns {
	return []Column{
		{Name: "table", Data: &c.table},
		{Name: "ddl", Data: &c.ddl},
		{Name: "ddl_hash", Data: &c.ddlHash},
	}
}

func (c *migrationColumns) Input() proto.Input    { return c.columns().Input() }
func (c *migrationColumns) Result() proto.Results { return c.columns().Result() }

func newMigrationColumns() *migrationColumns {
	return &migrationColumns{}
}

func (c *migrationColumns) Mapping() map[string]migration {
	data := make(map[string]migration, c.table.Rows())
	for i := 0; i < c.table.Rows(); i++ {
		data[c.table.Row(i)] = migration{
			DDL:     c.ddl.Row(i),
			DDLHash: c.ddlHash.Row(i),
		}
	}
	return data
}

type migration struct {
	DDL     string
	DDLHash string
}

func (c *migrationColumns) Save(m map[string]migration) {
	for k, v := range m {
		c.table.Append(k)
		c.ddl.Append(v.DDL)
		c.ddlHash.Append(v.DDLHash)
	}
}

func (c *migrationColumns) DDL() ddl.Table {
	return ddl.Table{
		Name: "migration",
		Engine: ddl.Engine{
			Type: "ReplacingMergeTree",
			Args: []string{"ts"},
		},
		OrderBy: []string{"table"},
		Columns: []ddl.Column{
			{Name: "table", Type: "String"},
			{Name: "ddl", Type: "String"},
			{Name: "ddl_hash", Type: "String"},
			{Name: "ts", Type: "DateTime", Default: "now()"},
		},
	}
}
