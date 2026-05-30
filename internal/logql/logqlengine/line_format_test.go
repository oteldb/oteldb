package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
)

func TestLineFormat(t *testing.T) {
	tests := []struct {
		labels        map[logql.Label]pcommon.Value
		tmpl          string
		wantLine      string
		wantFilterErr bool
	}{
		{
			map[logql.Label]pcommon.Value{},
			`{{ __timestamp__.Unix }}`,
			`1700000001`,
			false,
		},
		{
			map[logql.Label]pcommon.Value{},
			`{{ printf "%q" __line__  }}`,
			`"original line"`,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("bar"),
			},
			`{{ .foo }}`,
			`bar`,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("bar"),
			},
			`{{ alignLeft 2 .foo }}`,
			`ba`,
			false,
		},

		{
			map[logql.Label]pcommon.Value{},
			`{{ unixToTime "" }}`,
			`original line`,
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := newLabelSet(tt.labels)

			f, err := buildLineFormat(&logql.LineFormat{
				Template: tt.tmpl,
			})
			require.NoError(t, err)

			newLine, gotOk := f.Process(1700000001_000000000, "original line", set)
			require.True(t, gotOk)
			require.Equal(t, tt.wantLine, newLine)

			if tt.wantFilterErr {
				_, ok := set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			require.False(t, ok, "got error: %s", errMsg)
		})
	}
}
