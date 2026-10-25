package storagebackend

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
)

func TestTraceProjectionFallback(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		fields []traceql.Attribute
	}{
		{name: "unspecified"},
		{name: "unknown_property", fields: []traceql.Attribute{{Prop: 255}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Nil(t, traceProjection(tt.fields))
		})
	}
}
