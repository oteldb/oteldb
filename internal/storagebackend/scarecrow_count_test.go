package storagebackend_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestScarecrowCountExcludesLeftBoundary(t *testing.T) {
	t.Parallel()
	base := time.Unix(2_000_000, 0)
	store := ingestGauge(t.Context(), t, base, []timeSample{{offset: -30 * time.Second, value: 1}})
	t.Cleanup(func() { require.NoError(t, store.Close(t.Context())) })
	scanner := storagebackend.New(store).ScarecrowScanner()
	counter := scanner.(scarecrow.SeriesCounter)
	grouped := scanner.(scarecrow.GroupedSeriesCounter)
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "ot_gauge")}
	for _, tc := range []struct {
		name string
		mint int64
		want uint64
	}{
		{"at boundary", base.Add(-30 * time.Second).UnixMilli(), 0},
		{"inside window", base.Add(-30*time.Second - time.Millisecond).UnixMilli(), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := counter.CountSeries(t.Context(), tc.mint, base.UnixMilli(), matchers)
			require.NoError(t, err)
			assert.Equal(t, tc.want, n)
			groups, err := grouped.CountSeriesBy(t.Context(), tc.mint, base.UnixMilli(), "foo", matchers)
			require.NoError(t, err)
			if tc.want == 0 {
				assert.Empty(t, groups)
			} else {
				assert.Equal(t, map[string]uint64{"bar": 1}, groups)
			}
		})
	}
}

func TestScarecrowCountGridNativeGaps(t *testing.T) {
	t.Parallel()
	base := time.Unix(2_000_000, 0)
	store := ingestGauge(t.Context(), t, base, []timeSample{{-10 * time.Minute, 1}, {-9 * time.Minute, 2}, {-8 * time.Minute, 3}, {0, 4}, {time.Minute, 5}})
	t.Cleanup(func() { require.NoError(t, store.Close(t.Context())) })
	backend := storagebackend.New(store)
	raw := scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true})
	native := scarecrow.NewEngine(scarecrow.Opts{EnableAtModifier: true, NewScanner: func(storage.Queryable) scarecrow.Scanner { return backend.ScarecrowScanner() }})
	for _, qs := range []string{
		`count(ot_gauge)`, `count by(foo)(ot_gauge)`, `count(count by(foo)(ot_gauge))`,
		`sum(ot_gauge) / scalar(count(count by(foo)(ot_gauge)))`,
		`scalar(count(count by(foo)(ot_gauge))) + sum(ot_gauge)`,
		fmt.Sprintf(`scalar(count(ot_gauge @ %d))`, base.Add(6*time.Minute).Unix()),
		fmt.Sprintf(`scalar(count(count by(foo)(ot_gauge @ %d)))`, base.Add(6*time.Minute).Unix()),
	} {
		t.Run(qs, func(t *testing.T) {
			reference, err := raw.NewRangeQuery(t.Context(), backend, nil, qs, base.Add(-12*time.Minute), base.Add(7*time.Minute), time.Minute)
			require.NoError(t, err)
			defer reference.Close()
			actual, err := native.NewRangeQuery(t.Context(), backend, nil, qs, base.Add(-12*time.Minute), base.Add(7*time.Minute), time.Minute)
			require.NoError(t, err)
			defer actual.Close()
			want, got := reference.Exec(t.Context()), actual.Exec(t.Context())
			require.NoError(t, want.Err)
			require.NoError(t, got.Err)
			assert.Equal(t, want.Value.Type(), got.Value.Type())
			assert.Equal(t, want.Value.String(), got.Value.String())
		})
	}
}
