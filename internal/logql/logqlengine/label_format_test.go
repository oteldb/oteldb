package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
)

func TestLabelFormat(t *testing.T) {
	tests := []struct {
		input         map[logql.Label]pcommon.Value
		rename        []logql.RenameLabel
		templates     []logql.LabelTemplate
		expectLabels  map[logql.Label]pcommon.Value
		wantFilterErr bool
	}{
		// No label.
		{
			map[logql.Label]pcommon.Value{},
			[]logql.RenameLabel{{From: "foo", To: "bar"}},
			nil,
			map[logql.Label]pcommon.Value{},
			false,
		},

		// Rename.
		{
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("foo"),
			},
			[]logql.RenameLabel{{From: "foo", To: "bar"}},
			nil,
			map[logql.Label]pcommon.Value{
				"bar": pcommon.NewValueStr("foo"),
			},
			false,
		},

		// Rename and template
		{
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("foo"),
			},
			[]logql.RenameLabel{
				{From: "foo", To: "bar"},
			},
			[]logql.LabelTemplate{
				{Label: "tmpl", Template: `{{ __timestamp__.Unix }}`},
				{Label: "tmpl2", Template: `{{ __line__ }}`},
			},
			map[logql.Label]pcommon.Value{
				"bar":   pcommon.NewValueStr("foo"),
				"tmpl":  pcommon.NewValueStr("1700000001"),
				"tmpl2": pcommon.NewValueStr("original line"),
			},
			false,
		},

		// Template error.
		{
			map[logql.Label]pcommon.Value{},
			nil,
			[]logql.LabelTemplate{
				{Label: "tmpl", Template: `{{ unixToTime "" }}`},
			},
			map[logql.Label]pcommon.Value{},
			true,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			e, err := buildLabelFormat(&logql.LabelFormatExpr{
				Labels: tt.rename,
				Values: tt.templates,
			})
			require.NoError(t, err)

			set := newLabelSet(tt.input)
			newLine, ok := e.Process(1700000001_000000000, "original line", set)
			// Ensure that processor does not change the line.
			require.Equal(t, "original line", newLine)
			require.True(t, ok)

			if tt.wantFilterErr {
				_, ok = set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			require.False(t, ok, "got error: %s", errMsg)

			for k, expect := range tt.expectLabels {
				got, ok := set.Get(k)
				require.Truef(t, ok, "key %q", k)
				require.Equal(t, expect, got)
			}
		})
	}
}
