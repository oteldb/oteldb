package ddl

import (
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	for _, table := range []Table{
		{
			Name: "logs",
			Engine: Engine{
				Type: "MergeTree",
			},
			Cluster: "foo",
			TTL: TTL{
				Delta: time.Hour * 10,
				Field: "timestamp",
			},
			OrderBy: []string{"a", "toStartOfHour(b)"},
			Columns: []Column{
				{
					Name: "a",
					Type: "Int32",
				},
				{
					Name:  "bar",
					Type:  proto.ColumnType("LowCardinality").Sub(proto.ColumnTypeString),
					Codec: "ZSTD(1)",
				},
				{
					Name:    "c",
					Type:    "String",
					Codec:   "ZSTD(1)",
					Comment: "foo.bar",
				},
			},
			Indexes: []Index{
				{
					Name:        "idx_trace_id",
					Target:      "trace_id",
					Type:        "bloom_filter",
					Params:      []string{"0.001"},
					Granularity: 1,
				},
				{
					Name:        "idx_body",
					Target:      "body",
					Type:        "tokenbf_v1",
					Params:      []string{"32768", "3", "0"},
					Granularity: 1,
				},
				{
					Name:        "idx_ts",
					Target:      "timestamp",
					Type:        "minmax",
					Granularity: 8192,
				},
				{
					Name:   "attribute_keys",
					Target: "arrayConcat(JSONExtractKeys(attribute), JSONExtractKeys(scope), JSONExtractKeys(resource))",
					Type:   "set",
					Params: []string{"100"},
				},
			},
		},
	} {
		t.Run(table.Name, func(t *testing.T) {
			s, err := CreateIfNotExists(table)
			require.NoError(t, err)
			gold.Str(t, s, "ddl."+table.Name+".sql")
		})
	}
	s, err := CreateIfNotExists(Table{
		Engine: Engine{
			Type: "ReplacingMergeTree",
			Args: []string{
				"a",
			},
		},
		OrderBy: []string{"a", "b"},
		Columns: []Column{
			{
				Name:    "a",
				Type:    "Int32",
				Default: "now()",
			},
			{
				Name:         "b",
				Type:         "Int32",
				Materialized: "1",
			},
		},
	})
	require.NoError(t, err)
	gold.Str(t, s, "ddl.sql")
}
