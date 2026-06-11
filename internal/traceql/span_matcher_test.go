package traceql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestSpanMatcher_String(t *testing.T) {
	tests := []struct {
		m    SpanMatcher
		want string
	}{
		{
			SpanMatcher{
				Attribute: Attribute{
					Prop: SpanName,
				},
				Op: OpEq,
				Static: Static{
					Type: TypeString,
					Str:  "span",
				},
			},
			`name = "span"`,
		},
		{
			SpanMatcher{
				Attribute: Attribute{
					Name: "http.status_code",
				},
				Op: OpGte,
				Static: Static{
					Type: TypeInt,
					Data: 400,
				},
			},
			`.http.status_code >= 400`,
		},
		{
			SpanMatcher{
				Attribute: Attribute{
					Prop: TraceDuration,
				},
				Op: OpLte,
				Static: Static{
					Type: TypeDuration,
					Data: uint64(time.Second),
				},
			},
			`traceDuration <= 1s`,
		},
		{
			SpanMatcher{
				Attribute: Attribute{
					Prop: SpanStatus,
				},
				Op: OpEq,
				Static: Static{
					Type: TypeSpanStatus,
					Data: uint64(ptrace.StatusCodeOk),
				},
			},
			`status = ok`,
		},
		{
			SpanMatcher{
				Attribute: Attribute{
					Prop: SpanKind,
				},
				Op: OpEq,
				Static: Static{
					Type: TypeSpanKind,
					Data: uint64(ptrace.SpanKindClient),
				},
			},
			`kind = client`,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, s)

			// Ensure stringer produces valid
			_, err := Parse("{" + s + "}")
			require.NoError(t, err)
		})
	}
}
