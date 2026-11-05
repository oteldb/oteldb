package storagebackend

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func TestNativeKeyTupleBoundaries(t *testing.T) {
	t.Parallel()
	key := func(name, value string, kind labels.MatchType) nativeQueryKey {
		return nativeKey("count", scarecrow.WindowGrid{}, "cpu", []*labels.Matcher{labels.MustNewMatcher(kind, name, value)})
	}
	require.NotEqual(t, key("ab", "c", labels.MatchEqual), key("a", "bc", labels.MatchEqual))
	require.NotEqual(t, key("a;", "b", labels.MatchEqual), key("a", ";b", labels.MatchEqual))
	require.NotEqual(t, key("a", "b", labels.MatchEqual), key("a", "b", labels.MatchNotEqual))
	ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "z", "last"), labels.MustNewMatcher(labels.MatchEqual, "a", "first")}
	before := slices.Clone(ms)
	_ = nativeKey("count", scarecrow.WindowGrid{}, "cpu", ms)
	require.Equal(t, before, ms, "canonical sorting must not rewrite parser-owned matchers")
}

func FuzzNativeKeyTuples(f *testing.F) {
	f.Add("a", "bc", "d")
	f.Add("a;", "b", "\x00")
	f.Fuzz(func(t *testing.T, a, b, c string) {
		first := nativeKey("count", scarecrow.WindowGrid{}, "cpu", []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, a+b, c)})
		second := nativeKey("count", scarecrow.WindowGrid{}, "cpu", []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, a, b+c)})
		if b != "" {
			require.NotEqual(t, first, second)
		}
	})
}

var (
	nativeKeyBenchmarkSink nativeQueryKey
	legacyKeyBenchmarkSink string
)

func BenchmarkNativeQueryKey(b *testing.B) {
	for _, count := range []int{1, 6, 20} {
		ms := make([]*labels.Matcher, count)
		for i := range ms {
			ms[i] = labels.MustNewMatcher(labels.MatchEqual, "label"+strconv.Itoa(count-i), "host:9100/"+strconv.Itoa(i))
		}
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			b.Run("quoted", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					parts := make([]string, len(ms))
					for i, m := range ms {
						parts[i] = strconv.Itoa(int(m.Type)) + ":" + strconv.Quote(m.Name) + ":" + strconv.Quote(m.Value)
					}
					slices.Sort(parts)
					legacyKeyBenchmarkSink = strings.Join(parts, ";")
				}
			})
			b.Run("hash128", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					nativeKeyBenchmarkSink = nativeKey("count", scarecrow.WindowGrid{}, "cpu", ms)
				}
			})
		})
	}
}
