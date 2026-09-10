package storagebackend

import (
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

// maxSeverityNumber is the largest severity number OTLP names ([plog.SeverityNumberFatal4]).
const maxSeverityNumber = int64(plog.SeverityNumberFatal4)

// levelCondition resolves the selector's level matchers to a per-record condition on the severity
// column, so the fetch drops non-matching records before they are materialized into entries.
//
// It is a **superset**, never an exact filter: a level is the severity number's name when the number
// is set, else the record's severity text ([logqlabels.Level]), and a condition sees one column. So
// every value the number cannot decide — unspecified (0), or a number OTLP does not name — passes
// through to the in-memory [matchSelector] re-check, which is what keeps a text-only record
// (`SeverityText: "NOTICE"`, no number) from being dropped here.
//
// For the same reason the level never counts as applied: `selectorPushed` stays false whenever a
// level matcher is present.
//
// The condition carries no [fetch.Condition.AnyEqual] hint, for two reasons. It would be unsound:
// a set hint must contain every value Match accepts, and Match accepts any number OTLP does not
// name — an unbounded set. And it would buy nothing: severity is an int64 column, the record engine
// builds blooms over byte columns only, and it compiles an int column to a per-value memo that
// consults Match alone. So no part is skipped by this; what is saved is the materialization of the
// records it drops.
func levelCondition(matchers []logql.LabelMatcher) (fetch.Condition, bool) {
	var level preparedSelector
	for _, m := range prepareSelector(matchers) {
		if m.level {
			level = append(level, m)
		}
	}
	if len(level) == 0 {
		return fetch.Condition{}, false
	}

	keep := make([]bool, maxSeverityNumber+1)
	kept := 0
	for n := int64(1); n <= maxSeverityNumber; n++ {
		if !level.matchesLevel(logqlabels.Level(plog.SeverityNumber(n), "")) {
			continue
		}
		keep[n] = true
		kept++
	}
	if kept == int(maxSeverityNumber) {
		return fetch.Condition{}, false
	}

	return fetch.Condition{
		Column: siglog.ColSeverity,
		Match: func(v signal.Value) bool {
			n := v.Int()
			if n < 1 || n > maxSeverityNumber {
				return true
			}
			return keep[n]
		},
	}, true
}

// matchesLevel reports whether value satisfies every matcher, which must all be level matchers —
// so [preparedMatcher.matches] resolves them case-insensitively, exactly as the per-record check
// does.
func (p preparedSelector) matchesLevel(value string) bool {
	for _, m := range p {
		if !m.matches(value) {
			return false
		}
	}
	return true
}
