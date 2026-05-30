package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

func TestDecolorize(t *testing.T) {
	tests := []struct {
		inputLine     string
		wantLine      string
		wantFilterErr bool
	}{
		{``, ``, false},
		{`foo`, `foo`, false},
		{"\x1b", "\x1b", false},
		{"\x1b[1;31m bold red \x1b[0m", " bold red ", false},
		{"\x1b[1;31m Hello", " Hello", false},
		{"\x1b[2;37;41m World", " World", false},
		{"\x1b[4m\x1b[44m Blue Background Underline \x1b[0m", " Blue Background Underline ", false},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := logqlabels.NewLabelSet()

			f, err := buildDecolorize(&logql.DecolorizeExpr{})
			require.NoError(t, err)

			newLine, gotOk := f.Process(1700000001_000000000, tt.inputLine, set)
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
