package chsql

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

func TestSelect(t *testing.T) {
	start := time.Unix(0, 17175110000000000)
	end := time.Unix(0, 17176110000000000)

	tests := []struct {
		build   func() *SelectQuery
		wantErr bool
	}{
		{
			func() *SelectQuery {
				return Select(
					"logs",
					Column("timestamp", nil),
					Column("body", nil),
					Column("attributes", nil),
					Column("resource", nil),
				).
					Distinct(true).
					Where(
						InTimeRange(
							"timestamp",
							start, end,
							proto.PrecisionNano,
						),
						Or(
							Eq(
								JSONExtractField(Ident("attributes"), "label", "String"),
								String("value"),
							),
							Eq(
								JSONExtractField(Ident("resource"), "label", "String"),
								String("value"),
							),
						),
						Not(Contains("body", "line")),
						In(
							Hex(Ident("span_id")),
							TupleValues("deaddead", "aaaabbbb"),
						),
						Eq(
							Hex(Ident("trace_id")),
							Unhex(Value("deaddead")),
						),
					).
					Limit(1000)
			},
			false,
		},
		// Test subqueries.
		{
			func() *SelectQuery {
				return SelectFrom(
					Select("spans",
						Column("span_id", nil),
						Column("timestamp", nil),
					).Where(
						Value(true),
						Gt(Ident("duration"), Value(3.14)),
						Lt(Ident("duration"), Value[float32](3.14)),
					),
					Column("span_id", nil),
				).
					Limit(1)
			},
			false,
		},
		// Test ORDER BY.
		{
			func() *SelectQuery {
				return Select("spans",
					Column("timestamp", nil),
				).Order(
					Ident("timestamp"),
					Desc,
				)
			},
			false,
		},
		{
			func() *SelectQuery {
				return Select("spans",
					Column("timestamp", nil),
				).Order(
					Ident("timestamp"),
					Asc,
				).Order(
					Ident("duration"),
					Desc,
				)
			},
			false,
		},
		// Ensure aliasing is properly handled.
		//
		// Alias expression.
		{
			func() *SelectQuery {
				return Select("spans", ResultColumn{
					Name: "name",
					Expr: ToString(Ident("name")),
				})
			},
			false,
		},
		// User-defined alias.
		{
			func() *SelectQuery {
				return Select("spans", ResultColumn{
					Name: "name",
					Expr: binaryOp(
						Ident("column"),
						"AS",
						Ident("spanName"),
					),
				})
			},
			false,
		},
		// User-defined alias with the same name as column.
		{
			func() *SelectQuery {
				return Select("spans", ResultColumn{
					Name: "column",
					Expr: binaryOp(
						Ident("column"),
						"AS",
						Ident("spanName"),
					),
				})
			},
			false,
		},
		// Test PREWHERE.
		{
			func() *SelectQuery {
				return Select(
					"logs",
					Column("body", nil),
				).
					Prewhere(
						HasToken(Ident("body"), "Error"),
					)
			},
			false,
		},
		// Test GROUP BY/HAVING.
		{
			func() *SelectQuery {
				return Select(
					"logs",
					Column("body", nil),
				).
					GroupBy(Ident("body"), Ident("timestamp")).
					Having(
						HasToken(Ident("body"), "Error"),
						HasToken(Ident("level"), "Error"),
					)
			},
			false,
		},

		// No columns.
		{
			func() *SelectQuery { return Select("logs") },
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		name := fmt.Sprintf("Test%d", i+1)
		t.Run(name, func(t *testing.T) {
			q := tt.build()
			p := GetPrinter()

			err := q.WriteSQL(p)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			gold.Str(t, p.String(), name+".sql")
		})
	}
}
