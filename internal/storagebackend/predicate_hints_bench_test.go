package storagebackend

import (
	"testing"

	"github.com/oteldb/storage/query/fetch"

	"github.com/oteldb/oteldb/internal/traceql"
)

func BenchmarkFiniteHintPlanning(b *testing.B) {
	for _, tt := range []struct {
		name, pattern string
		values        int
	}{
		{name: "literal", pattern: `^checkout$`, values: 1},
		{name: "alternation", pattern: `^(foo|bar)$`, values: 2},
		{name: "factored", pattern: `^(foo|foobar)$`, values: 2},
		{name: "empty", pattern: `^$`, values: 1},
		{name: "empty_alternative", pattern: `^(|foo)$`, values: 2},
		{name: "set_33", pattern: `^[a-zA-G]$`, values: 33},
		{name: "set_64", pattern: `^[a-zA-Z0-9_-]$`, values: 64},
	} {
		b.Run(tt.name, func(b *testing.B) {
			var value traceql.Static
			value.SetString(tt.pattern)
			b.ReportAllocs()
			for b.Loop() {
				c := fetch.Condition{Column: "name"}
				rawStringConditionHints(&c, traceql.OpRe, value)
				if len(c.AnyEqual) != tt.values {
					b.Fatal("unexpected set size", len(c.AnyEqual))
				}
			}
		})
	}
}
