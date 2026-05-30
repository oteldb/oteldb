package logqlengine

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
)

func TestRegexpExtractor(t *testing.T) {
	tests := []struct {
		input        string
		expectLabels map[logql.Label]pcommon.Value
	}{
		{``, nil},
		{
			`GET /foo HTTP/1.1`,
			map[logql.Label]pcommon.Value{
				"method":  pcommon.NewValueStr("GET"),
				"path":    pcommon.NewValueStr("/foo"),
				"version": pcommon.NewValueStr("HTTP/1.1"),
			},
		},
		{
			`HEAD /foo HTTP/1.1`,
			map[logql.Label]pcommon.Value{
				"method":  pcommon.NewValueStr("HEAD"),
				"path":    pcommon.NewValueStr("/foo"),
				"version": pcommon.NewValueStr("HTTP/1.1"),
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			re := regexp.MustCompile(`(?P<method>(GET|HEAD|POST|PUT|PATCH))\s+(?P<path>[\/\w]+)\s+(?P<version>HTTP/[\d\.]+)`)
			mapping := map[int]logql.Label{}
			for i, name := range re.SubexpNames() {
				if name == "" {
					continue
				}
				mapping[i] = logql.Label(name)
			}

			e, err := buildRegexpExtractor(&logql.RegexpLabelParser{
				Regexp:  re,
				Mapping: mapping,
			})
			require.NoError(t, err)

			set := logqlabels.NewLabelSet()
			newLine, ok := e.Process(0, tt.input, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, tt.input, newLine)
			require.True(t, ok)

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
