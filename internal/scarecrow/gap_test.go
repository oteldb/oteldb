package scarecrow_test

import (
	"os"
	"testing"

	"github.com/prometheus/prometheus/promql/promqltest"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// TestPromQLGap runs the whole upstream corpus with nothing skipped, to measure how far the
// engine is from it.
//
// [TestPromQLCompliance] answers "which files pass", which is the number a milestone is judged
// by, but it is coarse: one unimplemented function fails a 413-case file, and the skip list then
// hides the other 412 cases. promqltest runs each `eval` as its own subtest and keeps going after
// one fails, so running unskipped gives per-case granularity for free.
//
// This is a report, not a gate: it is expected to fail until every milestone lands, which is why
// it runs only under SCARECROW_GAP=1. Each eval is a subtest, so the pass rate is whatever counts
// them:
//
//	SCARECROW_GAP=1 go test -run TestPromQLGap -v ./internal/scarecrow/ |
//	  awk '/--- (PASS|FAIL): .*\/line_/ {n++; if ($2 == "PASS:") p++} END {printf "%d/%d\n", p, n}'
func TestPromQLGap(t *testing.T) {
	if os.Getenv("SCARECROW_GAP") == "" {
		t.Skip("set SCARECROW_GAP=1 to run the unskipped corpus gap report")
	}

	promqltest.RunBuiltinTests(&scoreboard{T: t, noSkip: true}, scarecrow.NewEngine(complianceOpts()))
}
