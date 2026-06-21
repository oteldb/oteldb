package main

import (
	"testing"
	"time"

	"github.com/oteldb/oteldb/cmd/otelbench/chtracker"
	"github.com/oteldb/oteldb/cmd/otelbench/logqlbench"
)

func TestSummarizeLogQLQueriesAveragesClickHouseReadsPerRun(t *testing.T) {
	t.Parallel()

	rows := summarizeLogQLQueries([]logqlbench.LogQLReportQuery{
		{
			Type:          "instant",
			Title:         "query",
			Description:   "best-case query.",
			DurationNanos: int64(10 * time.Millisecond),
			Queries: []chtracker.QueryReport{
				{ReadBytes: 100, ReadRows: 10},
				{ReadBytes: 50, ReadRows: 5},
			},
		},
		{
			Type:          "instant",
			Title:         "query",
			Description:   "best-case query.",
			DurationNanos: int64(20 * time.Millisecond),
			Queries: []chtracker.QueryReport{
				{ReadBytes: 150, ReadRows: 15},
			},
		},
	})
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.Runs != 2 {
		t.Fatalf("runs = %d, want 2", row.Runs)
	}
	if row.ReadBytes != 150 {
		t.Fatalf("read bytes = %d, want 150", row.ReadBytes)
	}
	if row.ReadRows != 15 {
		t.Fatalf("read rows = %d, want 15", row.ReadRows)
	}
}
