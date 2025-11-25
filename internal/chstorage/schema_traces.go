package chstorage

import (
	"github.com/ClickHouse/ch-go/proto"

	"github.com/go-faster/oteldb/internal/ddl"
)

const (
	kindDDL      = `'KIND_UNSPECIFIED' = 0,'KIND_INTERNAL' = 1,'KIND_SERVER' = 2,'KIND_CLIENT' = 3,'KIND_PRODUCER' = 4,'KIND_CONSUMER' = 5`
	valueTypeDDL = `'EMPTY' = 0,'STR' = 1,'INT' = 2,'DOUBLE' = 3,'BOOL' = 4,'MAP' = 5,'SLICE' = 6,'BYTES' = 7`
	scopeTypeDDL = `'NONE' = 0, 'RESOURCE' = 1, 'SPAN' = 2, 'INSTRUMENTATION' = 3`
)

func newTracesTagsDDL() ddl.Table {
	return ddl.Table{
		Engine: ddl.Engine{Type: "ReplacingMergeTree"},
		Columns: []ddl.Column{
			{
				Name: "name",
				Type: proto.ColumnTypeLowCardinality.Sub(proto.ColumnTypeString),
			},
			{
				Name: "value",
				Type: proto.ColumnTypeString,
			},
			{
				Name: "value_type",
				Type: proto.ColumnTypeEnum8.Sub(valueTypeDDL),
			},
			{
				Name: "scope",
				Type: proto.ColumnTypeEnum8.Sub(scopeTypeDDL),
			},
		},
		OrderBy: []string{"value_type", "name", "value"},
	}
}
