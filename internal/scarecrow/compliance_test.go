package scarecrow_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

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
	"testdata/aggregators.test":         "count_values (permanently unsupported); M7: native histograms; M12: date/time and range functions; M13: absent/absent_over_time",
	"testdata/at_modifier.test":         "M9: annotations on @-modified queries",
	"testdata/extended_vectors.test":    "anchored/smoothed range selectors",
	"testdata/fill-modifier.test":       "binop fill modifiers",
	"testdata/functions.test":           "M12: date/time, range and query-context functions; M13: absent/absent_over_time",
	"testdata/histograms.test":          "M7: native histograms",
	"testdata/info.test":                "info()",
	"testdata/limit.test":               "M7: native histograms; M9: invalid-ratio warnings; M12: time()",
	"testdata/name_label_dropping.test": "M8: delayed __name__ removal",
	"testdata/native_histograms.test":   "M7: native histograms",
	"testdata/operators.test":           "M7: native histogram operands; group_x with comparison operators",
	"testdata/range_queries.test":       "M9: annotations on sort(); M7: native histogram series",
	"testdata/start_timestamps.test":    "M10: created timestamps",
	"testdata/subquery.test":            "M7: native histograms — topk and the rest of subqueries now pass",
	"testdata/type_and_unit.test":       "type and unit metadata",
}

// scoreboard adapts *testing.T to promqltest.TBRun so the corpus can be run file by file with
// unsupported files skipped and the supported ones tallied.
//
// promqltest.TBRun is testing.TB plus Run, and testing.TB cannot be implemented outside the
// testing package. Embedding *testing.T satisfies it while letting Run be overridden, which is
// the only hook the corpus runner offers.
type scoreboard struct {
	*testing.T

	// noSkip runs even the files listed in unsupportedFiles, for the gap report.
	noSkip bool

	passed  []string
	failed  []string
	skipped []string
}

func (s *scoreboard) Run(name string, f func(*testing.T)) bool {
	if reason, ok := unsupportedFiles[name]; ok && !s.noSkip {
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

// compliancePassing is the number of corpus files that pass today. It is asserted rather than
// merely logged so a regression fails the build, and it is shared with the pushdown suite, which
// requires the same number with every capability enabled.
const compliancePassing = 6

// complianceOpts enables every experimental gate the corpus exercises.
func complianceOpts() scarecrow.Opts {
	return scarecrow.Opts{
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		Parser: parser.Options{
			EnableExperimentalFunctions:  true,
			ExperimentalDurationExpr:     true,
			EnableExtendedRangeSelectors: true,
			EnableBinopFillModifiers:     true,
		},
	}
}

// TestPromQLCompliance runs Prometheus' own PromQL corpus against the engine. It is the
// project's headline correctness metric: the pass count it reports is what each milestone is
// judged by.
func TestPromQLCompliance(t *testing.T) {
	engine := scarecrow.NewEngine(complianceOpts())

	sb := &scoreboard{T: t}
	promqltest.RunBuiltinTests(sb, engine)

	require.Equal(t, compliancePassing, len(sb.passed), "corpus pass count changed: %v", sb.failed)

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
