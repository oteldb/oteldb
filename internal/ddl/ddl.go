// Package ddl provides facilities for generating DDL statements and auxiliary code.
package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Column of [Table].
type Column struct {
	Name         string
	Comment      string
	Default      string
	Type         proto.ColumnType
	Codec        proto.ColumnType
	Materialized string
}

// Table description.
type Table struct {
	Name        string
	Cluster     string
	Columns     []Column
	Indexes     []Index
	OrderBy     []string
	PrimaryKey  []string
	PartitionBy string
	Engine      Engine
	TTL         TTL
}

// Engine describes engine type and its parameters.
type Engine struct {
	Type string
	Args []string
}

// Replicated returns replicated version of the engine.
//
// If replication is not supported by the engine, returns false.
func (e Engine) Replicated(prefix, tableName string) (Engine, bool) {
	if !strings.Contains(e.Type, "MergeTree") {
		return e, false
	}
	return Engine{
		Type: "Replicated" + e.Type,
		Args: append([]string{
			fmt.Sprintf("'%s/%s/{uuid}'", prefix, tableName),
			"'{replica}'",
		}, e.Args...),
	}, true
}

type TTL struct {
	Field string
	Delta time.Duration
}

type Index struct {
	Name        string
	Target      string
	Type        string
	Params      []string
	Granularity int
}

func (i Index) string(nameFormat string) string {
	var b strings.Builder
	b.WriteString("INDEX ")
	fmt.Fprintf(&b, nameFormat, Backtick(i.Name))
	b.WriteString(" ")
	b.WriteString(i.Target)
	b.WriteString(" TYPE ")
	b.WriteString(i.Type)
	if len(i.Params) > 0 {
		b.WriteString("(")
		b.WriteString(strings.Join(i.Params, ", "))
		b.WriteString(")")
	}
	if i.Granularity > 0 {
		b.WriteString(" GRANULARITY ")
		b.WriteString(strconv.Itoa(i.Granularity))
	}
	return b.String()
}

// Backtick adds backticks to the string.
func Backtick(s string) string {
	return "`" + s + "`"
}

func backticks(ss []string, columnNames map[string]struct{}) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		// Backtick only column names.
		if _, ok := columnNames[s]; ok {
			out[i] = Backtick(s)
		} else {
			// Ignore in case if key expression contains functions.
			out[i] = s
		}
	}
	return out
}

// Create generates DDL query for table creation.
func Create(t Table) (string, error) {
	return create(t, false)
}

// CreateIfNotExists generates DDL query for table creation with IF NOT EXISTS clause.
func CreateIfNotExists(t Table) (string, error) {
	return create(t, true)
}

func create(t Table, ifNotExist bool) (string, error) {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if ifNotExist {
		b.WriteString("IF NOT EXISTS ")
	}
	if t.Name == "" {
		b.WriteString(Backtick("table"))
	} else {
		b.WriteString(Backtick(t.Name))
	}
	if t.Cluster != "" {
		b.WriteString(" ON CLUSTER ")
		b.WriteByte('\'')
		b.WriteString(t.Cluster)
		b.WriteByte('\'')
	}
	b.WriteString("\n(")
	var (
		maxColumnLen     int
		maxColumnTypeLen int
	)
	for _, c := range t.Columns {
		if len(c.Name) > maxColumnLen {
			maxColumnLen = len(c.Name)
		}
		if len(c.Type.String()) > maxColumnTypeLen && !strings.HasPrefix(c.Type.String(), "Enum") {
			maxColumnTypeLen = len(c.Type.String())
		}
	}
	hasIndexes := len(t.Indexes) > 0
	columnNames := map[string]struct{}{}
	for i, c := range t.Columns {
		columnNames[c.Name] = struct{}{}
		b.WriteString("\n")
		var col strings.Builder
		col.WriteString("\t")
		if c.Name == "" {
			// Comment row.
			col.WriteString("-- ")
			col.WriteString(c.Comment)
			b.WriteString(col.String())
			continue
		}
		nameFormat := "%-" + strconv.Itoa(maxColumnLen+2) + "s"
		fmt.Fprintf(&col, nameFormat, Backtick(c.Name))
		col.WriteString(" ")
		typeFormat := "%-" + strconv.Itoa(maxColumnTypeLen) + "s"
		if c.Codec == "" && c.Comment == "" {
			typeFormat = "%s"
		}
		fmt.Fprintf(&col, typeFormat, c.Type.String())
		if c.Materialized != "" {
			col.WriteString(" MATERIALIZED ")
			col.WriteString(c.Materialized)
		}
		if c.Default != "" {
			col.WriteString(" DEFAULT ")
			col.WriteString(c.Default)
		}
		if c.Comment != "" {
			col.WriteString(" COMMENT ")
			col.WriteString("'")
			col.WriteString(c.Comment)
			col.WriteString("'")
		}
		if c.Codec != "" {
			col.WriteString(" CODEC(")
			col.WriteString(c.Codec.String())
			col.WriteRune(')')
		}
		b.WriteString(col.String())
		if hasIndexes || i < len(t.Columns)-1 {
			b.WriteString(",")
		}
	}
	var maxIndexLen int
	for _, c := range t.Indexes {
		if len(c.Name) > maxIndexLen {
			maxIndexLen = len(c.Name)
		}
	}
	for i, c := range t.Indexes {
		if i == 0 {
			b.WriteString("\n")
		}
		b.WriteString("\n\t")
		nameFormat := "%-" + strconv.Itoa(maxIndexLen+2) + "s"
		b.WriteString(c.string(nameFormat))
		if i < len(t.Indexes)-1 {
			b.WriteString(",")
		}
	}
	b.WriteString("\n)\n")
	b.WriteString("ENGINE = ")

	if typ := t.Engine.Type; typ == "" {
		typ = "Null"
		b.WriteString(typ)
	} else {
		b.WriteString(typ)
		if args := t.Engine.Args; len(args) > 0 {
			b.WriteByte('(')
			for i, arg := range args {
				if i != 0 {
					b.WriteByte(',')
				}
				b.WriteString(arg)
			}
			b.WriteByte(')')
		}
	}
	b.WriteString("\n")
	if t.PartitionBy != "" {
		b.WriteString("PARTITION BY ")
		b.WriteString(t.PartitionBy)
		b.WriteString("\n")
	}
	if len(t.OrderBy) > 0 {
		b.WriteString("ORDER BY (")
		b.WriteString(strings.Join(backticks(t.OrderBy, columnNames), ", "))
		b.WriteString(")\n")
	}
	if len(t.PrimaryKey) > 0 {
		b.WriteString("PRIMARY KEY (")
		b.WriteString(strings.Join(backticks(t.PrimaryKey, columnNames), ", "))
		b.WriteString(")\n")
	}
	if t.TTL.Delta > 0 && t.TTL.Field != "" {
		fmt.Fprintf(&b, "TTL toDateTime(%s) + toIntervalSecond(%d)",
			Backtick(t.TTL.Field), t.TTL.Delta/time.Second)
	}
	return b.String(), nil
}
