package logql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabelMatcher_String(t *testing.T) {
	tests := []struct {
		m    LabelMatcher
		want string
	}{
		{LabelMatcher{"label", OpEq, "value", nil}, `label="value"`},
		{LabelMatcher{"label", OpNotEq, "value", nil}, `label!="value"`},
		{LabelMatcher{"label", OpRe, "^value$", nil}, `label=~"^value$"`},
		{LabelMatcher{"label", OpNotRe, "^value$", nil}, `label!~"^value$"`},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, tt.m.String())

			// Ensure stringer produces valid LogQL.
			_, err := Parse("{"+s+"}", ParseOptions{})
			require.NoError(t, err)
		})
	}
}
