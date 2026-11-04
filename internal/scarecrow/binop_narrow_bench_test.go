package scarecrow_test

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

func BenchmarkBinopSelectorNarrowing(b *testing.B) {
	st := promqltest.LoadedStorage(b, benchCorpus(1_000))
	b.Cleanup(func() { require.NoError(b, st.Close()) })
	engine := scarecrow.NewEngine(scarecrow.Opts{})
	for _, tt := range []struct{ name, query string }{
		{"sparse", `counter{instance="i1"} * on(instance) gauge`},
		{"dense", `counter * on(instance) gauge`},
		{"unchanged", `counter + gauge`},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q, err := engine.NewRangeQuery(b.Context(), st, nil, tt.query, time.Unix(0, 0), time.Unix(600, 0), 15*time.Second)
				require.NoError(b, err)
				require.NoError(b, q.Exec(b.Context()).Err)
				q.Close()
			}
		})
	}
}
