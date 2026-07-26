package scarecrow_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// unsupportedFiles lists upstream promqltest corpus files the engine cannot yet pass. They are
// skipped rather than run, so an unfinished milestone does not drown the signal from the files
// that are supposed to work.
//
// This is a ratchet: as milestones land, delete entries. Never add one to make a build green —
// an entry appearing here means a capability regressed, which is exactly what the corresponding
// milestone is supposed to prevent.
var unsupportedFiles = map[string]string{
	"testdata/range_queries.test":       "M2/M6: binops, sort(), native histogram series",
	"testdata/start_timestamps.test":    "created timestamps (iterator AtST) — not scoped by any milestone yet",
	"testdata/aggregators.test":         "topk/bottomk/quantile/count_values, plus native histograms",
	"testdata/at_modifier.test":         "M3: @ modifier",
	"testdata/name_label_dropping.test": "delayed __name__ removal (Prometheus 3 DropName) — not scoped by any milestone",
	"testdata/operators.test":           "M6: native histogram operands; plus group_x with comparison operators",
	"testdata/duration_expression.test": "M3: duration expressions",
	"testdata/extended_vectors.test":    "M1: selectors",
	"testdata/fill-modifier.test":       "M3: fill modifier",
	"testdata/functions.test":           "M2: instant functions",
	"testdata/histograms.test":          "M6: native histograms",
	"testdata/info.test":                "M2: info()",
	"testdata/limit.test":               "M2: limitk/limit_ratio",
	"testdata/native_histograms.test":   "M6: native histograms",
	"testdata/subquery.test":            "M3: subqueries",
	"testdata/type_and_unit.test":       "M2: type and unit metadata",
}

// scoreboard adapts *testing.T to promqltest.TBRun so the corpus can be run file by file with
// unsupported files skipped and the supported ones tallied.
//
// promqltest.TBRun is testing.TB plus Run, and testing.TB cannot be implemented outside the
// testing package. Embedding *testing.T satisfies it while letting Run be overridden, which is
// the only hook the corpus runner offers.
type scoreboard struct {
	*testing.T

	passed  []string
	failed  []string
	skipped []string
}

func (s *scoreboard) Run(name string, f func(*testing.T)) bool {
	if reason, ok := unsupportedFiles[name]; ok {
		s.skipped = append(s.skipped, fmt.Sprintf("%s (%s)", name, reason))
		return true
	}

	ok := s.T.Run(name, f)
	if ok {
		s.passed = append(s.passed, name)
	} else {
		s.failed = append(s.failed, name)
	}

	return ok
}

// TestPromQLCompliance runs Prometheus' own PromQL corpus against the engine. It is the
// project's headline correctness metric: the pass count it reports is what each milestone is
// judged by.
func TestPromQLCompliance(t *testing.T) {
	engine := scarecrow.NewEngine(scarecrow.Opts{
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		Parser: parser.Options{
			EnableExperimentalFunctions:  true,
			ExperimentalDurationExpr:     true,
			EnableExtendedRangeSelectors: true,
			EnableBinopFillModifiers:     true,
		},
	})

	sb := &scoreboard{T: t}
	promqltest.RunBuiltinTests(sb, engine)

	total := len(sb.passed) + len(sb.failed) + len(sb.skipped)
	t.Logf("promqltest corpus: %d/%d files passing (%d skipped as unsupported)",
		len(sb.passed), total, len(sb.skipped))

	slices.Sort(sb.skipped)
	for _, s := range sb.skipped {
		t.Logf("  skipped: %s", s)
	}

	// Guards the ratchet from the other side: a file removed from the skip list but still
	// failing, or a corpus file that vanished upstream, both surface here.
	for name := range unsupportedFiles {
		if slices.Contains(sb.passed, name) {
			t.Errorf("%s is listed as unsupported but passed; remove it from unsupportedFiles", name)
		}
	}
}
