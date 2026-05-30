package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

func TestUnpackExtractor(t *testing.T) {
	tests := []struct {
		input         string
		expectLine    string
		expectLabels  map[logql.Label]pcommon.Value
		wantFilterErr bool
	}{
		{`{}`, `{}`, nil, false},
		{`{"_entry":"packed line"}`, "packed line", nil, false},
		{
			`{"label":"foo", "_entry":"packed line"}`,
			`packed line`,
			map[logql.Label]pcommon.Value{
				"label": pcommon.NewValueStr("foo"),
			},
			false,
		},
		// Ensure parser allows dots in labels for now.
		{
			`{"label.name":"foo"}`,
			`{"label.name":"foo"}`,
			map[logql.Label]pcommon.Value{
				"label.name": pcommon.NewValueStr("foo"),
			},
			false,
		},

		// Ignore non-string fields.
		{`{"_entry":10}`, `{"_entry":10}`, nil, false},
		{`{"label":10}`, `{"label":10}`, nil, false},

		// Invalid label name
		{`{"0":"foo"}`, `{"0":"foo"}`, nil, true},
		{`{"\u0000":"foo"}`, `{"\u0000":"foo"}`, nil, true},

		// Invalid JSON.
		{`{`, `{`, nil, true},
		{`{"_entry": "\u"}`, `{"_entry": "\u"}`, nil, true},
		{`{"label": "\u"}`, `{"label": "\u"}`, nil, true},
		// Wrong JSON type.
		{`[]`, `[]`, nil, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			e, err := buildUnpackExtractor(&logql.UnpackLabelParser{})
			require.NoError(t, err)

			set := logqlabels.NewLabelSet()
			newLine, ok := e.Process(0, tt.input, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, newLine, tt.expectLine)
			require.True(t, ok)

			if tt.wantFilterErr {
				_, ok = set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			details, _ := set.GetString(logql.ErrorDetailsLabel)
			require.False(t, ok, "got error: %s, details: %s", errMsg, details)

			for k, expect := range tt.expectLabels {
				got, ok := set.Get(k)
				require.Truef(t, ok, "key %q", k)
				require.Equal(t, expect, got)
			}
		})
	}
}
