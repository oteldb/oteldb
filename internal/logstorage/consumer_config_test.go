package logstorage_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/logstorage"
)

func TestParseAttributeRef(t *testing.T) {
	tests := []struct {
		ref     string
		want    logstorage.AttributeRef
		wantErr bool
	}{
		{"", logstorage.AttributeRef{}, true},
		{"\n", logstorage.AttributeRef{}, true},

		{"attributes.log.iostream", logstorage.AttributeRef{Name: "log.iostream", Location: logstorage.AttributesLocation}, false},
		{"scope.name", logstorage.AttributeRef{Name: "name", Location: logstorage.ScopeLocation}, false},
		{"resource.service.name", logstorage.AttributeRef{Name: "service.name", Location: logstorage.ResourceLocation}, false},
		{"body.level", logstorage.AttributeRef{Name: "level", Location: logstorage.BodyLocation}, false},

		{"service.name", logstorage.AttributeRef{Name: "service.name", Location: logstorage.AnyLocation}, false},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, gotErr := logstorage.ParseAttributeRef(tt.ref)
			if tt.wantErr {
				require.Error(t, gotErr)
				return
			}
			require.NoError(t, gotErr)
			require.Equal(t, tt.want, got)
		})
	}
}
