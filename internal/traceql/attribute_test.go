package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAttribute(t *testing.T) {
	tests := []struct {
		attr    string
		wantA   Attribute
		wantErr bool
	}{
		{`.service.name`, Attribute{Name: "service.name"}, false},
		{`span.service.name`, Attribute{Name: "service.name", Scope: ScopeSpan}, false},
		{`resource.service.name`, Attribute{Name: "service.name", Scope: ScopeResource}, false},
		{`parent.span.service.name`, Attribute{Name: "service.name", Scope: ScopeSpan, Parent: true}, false},
		{`status`, Attribute{Prop: SpanStatus}, false},

		{`{`, Attribute{}, true},
		{``, Attribute{}, true},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotA, err := ParseAttribute(tt.attr)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantA, gotA)
		})
	}
}
