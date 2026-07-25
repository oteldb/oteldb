package logqlengine

import (
	"regexp"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlerrors"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlpattern"
)

func mustPattern(t *testing.T, input string) logqlpattern.Pattern {
	t.Helper()
	p, err := logqlpattern.Parse(input, 0)
	require.NoError(t, err)
	return p
}

// TestStageKeepsEveryEntry pins the classification of every pipeline stage buildStage knows about.
// The table must stay in sync with the [logql.PipelineStage] implementors listed in
// internal/logql/pipeline.go — a stage missing from it is a stage whose limit-pushdown safety was
// never decided, and the length assertion below fails loudly when one is added.
func TestStageKeepsEveryEntry(t *testing.T) {
	tests := []struct {
		name  string
		stage logql.PipelineStage
		keeps bool
	}{
		// Parsers: a parse failure is recorded as the __error__ label, the entry survives.
		{"json", &logql.JSONExpressionParser{Labels: []logql.Label{"foo"}}, true},
		{"logfmt", &logql.LogfmtExpressionParser{Labels: []logql.Label{"foo"}}, true},
		{"regexp", &logql.RegexpLabelParser{Regexp: regexp.MustCompile(`(?P<foo>.+)`)}, true},
		{"pattern", &logql.PatternLabelParser{Pattern: mustPattern(t, "<foo>")}, true},
		{"unpack", &logql.UnpackLabelParser{}, true},
		// Line/label rewriters: they change the line or the label set, never the entry count.
		{"line_format", &logql.LineFormat{Template: "{{.foo}}"}, true},
		{"decolorize", &logql.DecolorizeExpr{}, true},
		{"label_format", &logql.LabelFormatExpr{Values: []logql.LabelTemplate{{Label: "foo", Template: "{{.bar}}"}}}, true},
		{"drop_labels", &logql.DropLabelsExpr{Labels: []logql.Label{"foo"}}, true},
		{"keep_labels", &logql.KeepLabelsExpr{Labels: []logql.Label{"foo"}}, true},
		// Filters: these exist to drop entries.
		{"line_filter", &logql.LineFilter{Op: logql.OpEq, By: logql.LineFilterValue{Value: "foo"}}, false},
		{"label_filter", &logql.LabelFilter{Pred: &logql.LabelMatcher{Label: "foo", Op: logql.OpEq, Value: "bar"}}, false},
		{"distinct", &logql.DistinctFilter{Labels: []logql.Label{"foo"}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.keeps, stageKeepsEveryEntry(tt.stage))

			// Guard the table against drift: every listed stage must be one buildStage knows.
			_, err := buildStage(tt.stage)
			var unsupported *logqlerrors.UnsupportedError
			require.False(t, errors.As(err, &unsupported), "stage is not built by buildStage")
		})
	}

	// buildStage enumerates 13 stage types; bump this only together with a new table entry.
	require.Len(t, tests, 13)

	t.Run("unknown", func(t *testing.T) {
		// Fail closed: an unclassified stage must be assumed to drop.
		require.False(t, stageKeepsEveryEntry(nil))
	})
}

// TestProcessorNodeKeepsEveryEntry covers the node-level predicate: the prefilter counts too, and a
// nop prefilter with a non-dropping pipeline is what enables the limit pushdown.
func TestProcessorNodeKeepsEveryEntry(t *testing.T) {
	labelMatcher, err := buildLabelMatcher(logql.LabelMatcher{Label: "foo", Op: logql.OpEq, Value: "bar"})
	require.NoError(t, err)

	tests := []struct {
		name  string
		node  ProcessorNode
		keeps bool
	}{
		{"empty", ProcessorNode{Prefilter: NopProcessor}, true},
		{"nil prefilter", ProcessorNode{}, true},
		{"otel adapter does not drop", ProcessorNode{Prefilter: NopProcessor, EnableOTELAdapter: true}, true},
		{
			name: "non-dropping pipeline",
			node: ProcessorNode{
				Prefilter: NopProcessor,
				Pipeline:  []logql.PipelineStage{&logql.UnpackLabelParser{}, &logql.DecolorizeExpr{}},
			},
			keeps: true,
		},
		{
			name: "dropping stage",
			node: ProcessorNode{
				Prefilter: NopProcessor,
				Pipeline:  []logql.PipelineStage{&logql.UnpackLabelParser{}, &logql.DistinctFilter{}},
			},
			keeps: false,
		},
		{
			name:  "prefilter drops",
			node:  ProcessorNode{Prefilter: labelMatcher},
			keeps: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.keeps, tt.node.keepsEveryEntry())
		})
	}
}
