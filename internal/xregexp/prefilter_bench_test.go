package xregexp

import "testing"

func BenchmarkRegexpPrefilters(b *testing.B) {
	for _, tt := range []struct {
		name, pattern string
		values        int
	}{
		{name: "literal", pattern: `^checkout$`, values: 1},
		{name: "escaped_literal", pattern: `^checkout\.process$`, values: 1},
		{name: "alternation", pattern: `^(foo|bar)$`, values: 2},
		{name: "factored", pattern: `^(foo|foobar)$`, values: 2},
		{name: "empty", pattern: `^$`, values: 1},
		{name: "empty_alternative", pattern: `^(|foo)$`, values: 2},
		{name: "leading_empty", pattern: `^(|foo)bar$`, values: 2},
		{name: "classes", pattern: `^[ab][12]$`, values: 4},
		{name: "set_33", pattern: `^[a-zA-G]$`, values: 33},
		{name: "set_64", pattern: `^[a-zA-Z0-9_-]$`, values: 64},
		{name: "rejected", pattern: `^[a-z][a-z]$`},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				values := FiniteSet(tt.pattern)
				count := 0
				if values != nil {
					for range values {
						count++
					}
				}
				if count != tt.values {
					b.Fatal("unexpected finite set size", count)
				}
			}
		})
	}
	b.Run("required_literals", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			count := 0
			for range Literals(`foo.*bar.*baz`) {
				count++
			}
			if count != 3 {
				b.Fatal("unexpected literal count", count)
			}
		}
	})
}
