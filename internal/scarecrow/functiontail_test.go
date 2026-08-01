package scarecrow_test

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// functionTailCorpus exercises M12's function tail: date/time functions, the range functions
// that need a regression or exact order statistics, the ts_of_* family, and the query-context
// accessors (already free via promql.PreprocessExpr, but worth pinning here too).
const functionTailCorpus = `
load 10s
  counter{job="a"} 0 10 20 30 45 45 10 20 30 40
  counter{job="b"} 5 5 5 5 5 5 5 5 5 5
  gauge{job="a"}   1 2 3 -4 5 NaN 7 8 9 10
  sparse{job="a"}  1 _ _ _ 5 _ _ _ 9 _
`

var functionTailQueries = []string{
	// Date/time, with and without an argument.
	`time()`,
	`year()`,
	`year(vector(1136239445))`,
	`month(vector(1136239445))`,
	`day_of_month(vector(1136239445))`,
	`day_of_week(vector(1136239445))`,
	`day_of_year(vector(1136239445))`,
	`days_in_month(vector(1136239445))`,
	`hour(vector(1136239445))`,
	`minute(vector(1136239445))`,

	// Range functions needing a regression or exact order statistics.
	`deriv(counter[1m])`,
	`deriv(gauge[1m])`,
	`predict_linear(counter[1m], 3600)`,
	`predict_linear(counter[1m] offset 10s, 3600)`,
	`predict_linear(gauge[2m], 100)`,
	`quantile_over_time(0, counter[2m])`,
	`quantile_over_time(0.5, counter[2m])`,
	`quantile_over_time(0.9, gauge[2m])`,
	`quantile_over_time(1, counter[2m])`,
	`mad_over_time(counter[2m])`,
	`mad_over_time(gauge[2m])`,

	// The ts_of_* family.
	`ts_of_first_over_time(sparse[2m])`,
	`ts_of_last_over_time(sparse[2m])`,
	`ts_of_max_over_time(counter[2m])`,
	`ts_of_min_over_time(counter[2m])`,

	// Query-context accessors, free via promql.PreprocessExpr but worth pinning.
	`start()`,
	`end()`,
	`step()`,
	`range()`,
	`counter + start()`,
}

func functionTailEngines() (ours *scarecrow.Engine, theirs *promql.Engine) {
	opts := parser.Options{EnableExperimentalFunctions: true}

	ours = scarecrow.NewEngine(scarecrow.Opts{
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		Parser:               opts,
	})
	theirs = promql.NewEngine(promql.EngineOpts{
		MaxSamples:               1e6,
		Timeout:                  time.Minute,
		LookbackDelta:            5 * time.Minute,
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
		Parser:                   parser.NewParser(opts),
	})

	return ours, theirs
}

func TestFunctionTailInstant(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, functionTailCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	ours, theirs := functionTailEngines()

	for _, at := range []time.Duration{0, 35 * time.Second, 90 * time.Second} {
		for _, qs := range functionTailQueries {
			t.Run(qs+" at="+at.String(), func(t *testing.T) {
				t.Parallel()

				ts := time.Unix(0, 0).Add(at)

				want := execInstant(t, theirs, st, qs, ts)
				got := execInstant(t, ours, st, qs, ts)

				requireSameValue(t, want, got)
			})
		}
	}
}

func TestFunctionTailRange(t *testing.T) {
	t.Parallel()

	st := promqltest.LoadedStorage(t, functionTailCorpus)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	ours, theirs := functionTailEngines()

	start, end := time.Unix(0, 0), time.Unix(90, 0)

	for _, step := range []time.Duration{10 * time.Second, 30 * time.Second} {
		for _, qs := range functionTailQueries {
			t.Run(qs+" step="+step.String(), func(t *testing.T) {
				t.Parallel()

				want := execRange(t, theirs, st, qs, start, end, step)
				got := execRange(t, ours, st, qs, start, end, step)

				requireSameValue(t, want, got)
			})
		}
	}
}
