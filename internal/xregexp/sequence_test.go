package xregexp

import (
	"iter"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrefilterSequences(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, pattern string
		sequence      func(string) iter.Seq[string]
		want          []string
	}{
		{name: "finite", pattern: `^(foo|bar)$`, sequence: FiniteSet, want: []string{"bar", "foo"}},
		{name: "finite_empty", pattern: `^$`, sequence: FiniteSet, want: []string{""}},
		{name: "required", pattern: `foo.*bar.*baz`, sequence: Literals, want: []string{"foo", "bar", "baz"}},
		{name: "invalid_finite", pattern: `(`, sequence: FiniteSet},
		{name: "invalid_required", pattern: `(`, sequence: Literals},
		{name: "too_many", pattern: `^(foo|[a-z][a-z])$`, sequence: FiniteSet},
		{name: "too_large", pattern: "^(foo|" + strings.Repeat("x", maxFiniteBytes+1) + ")$", sequence: FiniteSet},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sequence := tt.sequence(tt.pattern)
			require.NotNil(t, sequence)
			if len(tt.want) > 0 {
				count := 0
				for range sequence {
					count++
					break
				}
				require.Equal(t, 1, count)
			}
			require.Equal(t, tt.want, slices.Collect(sequence))
			require.Equal(t, tt.want, slices.Collect(sequence), "enumeration can be repeated after early termination")
		})
	}
}
