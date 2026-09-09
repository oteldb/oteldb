package storagebackend

import (
	"context"

	siglog "github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/internal/logstorage"
)

// isLevelLabelName reports whether name is one of the severity-derived labels. They are neither
// stream identity nor record attributes — every record carries severity in its own columns — so
// they need an enumeration path of their own.
func isLevelLabelName(name string) bool {
	return name == logstorage.LabelSeverity || name == logstorage.LabelDetectedLevel
}

// levelValues returns the distinct levels the records matching opts were written with, spelled as
// [logqlabels.LabelSet.SetFromRecord] writes them.
//
// The answer comes from the data rather than from the enum: a tenant that only ever logs info and
// error should offer two levels, not the fourteen plog can name.
//
// It reads only the level: the fetch projects the two severity columns, so the attributes column —
// whose decode dominates a record materialization — is never touched. That is as narrow as this can
// be today. [storage.Storage.ColumnValues] would answer a bytes column straight from the part
// dictionaries, but a level comes from the severity *number* first, and numeric columns are not
// enumerable.
//
// The result is a superset when the selector is not fully pushed down: re-checking it in memory
// needs the label set this projection deliberately does not read. That matches ColumnValues, which
// is documented as answering with a window superset, and it is the right way to be wrong for
// autocomplete — offering a level that matches nothing beats hiding one that does.
func (q *LogQuerier) levelValues(ctx context.Context, opts logstorage.LabelsOptions) ([]string, error) {
	lo, hi := seriesWindow(opts.Start, opts.End)
	node := &logStreamNode{q: q, selector: opts.Query.Matchers}
	batches, _, err := node.fetchBatches(ctx, lo, hi, fetchOptions{
		projection: []string{siglog.ColSeverity, siglog.ColSeverityText},
	})
	if err != nil {
		return nil, err
	}

	values := map[string]struct{}{}
	for _, batch := range batches {
		cols := newLogColumns(batch)
		for i := range batch.Timestamps {
			if v := levelValue(cols.severity, cols.severityText, i); v != "" {
				values[v] = struct{}{}
			}
		}
	}

	// A level matcher is never pushed into the fetch (severity is not an attribute key), so apply it
	// here: `label_values(detected_level, {detected_level=~"e.*"})` should narrow the answer.
	for _, m := range opts.Query.Matchers {
		if !isLevelLabelName(string(m.Label)) {
			continue
		}
		for v := range values {
			if !matchLevel(m, v) {
				delete(values, v)
			}
		}
	}

	out := sortedKeys(values)
	if opts.Limit > 0 && len(out) > opts.Limit {
		out = out[:opts.Limit]
	}

	return out, nil
}
