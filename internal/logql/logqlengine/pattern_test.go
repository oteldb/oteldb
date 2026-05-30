package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlpattern"
)

func TestPatternExtractor(t *testing.T) {
	tests := []struct {
		input         string
		pattern       string
		expectLabels  map[logql.Label]pcommon.Value
		wantFilterErr bool
	}{
		{
			`127.0.0.1 userid user [01/Jan/2000:00:00:00 0000] "GET /path HTTP/1.0" 200 10`,
			`<ip> <userid> <user> [<_>] "<method> <path> <_>" <status> <size>`,
			map[logql.Label]pcommon.Value{
				"ip":     pcommon.NewValueStr("127.0.0.1"),
				"userid": pcommon.NewValueStr("userid"),
				"user":   pcommon.NewValueStr("user"),
				"method": pcommon.NewValueStr("GET"),
				"path":   pcommon.NewValueStr("/path"),
				"status": pcommon.NewValueStr("200"),
				"size":   pcommon.NewValueStr("10"),
			},
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			e, err := buildPatternExtractor(&logql.PatternLabelParser{
				Pattern: logqlpattern.MustParse(tt.pattern, logqlpattern.ExtractorFlags),
			})
			require.NoError(t, err)

			set := logqlabels.NewLabelSet()
			newLine, ok := e.Process(0, tt.input, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, tt.input, newLine)
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
