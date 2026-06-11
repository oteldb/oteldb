package lokicompliance

import (
	"fmt"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpandQuery(t *testing.T) {
	arg := "quantile"
	cfg := &Config{
		TestCases: []*TestCasePattern{
			{
				Query: "{{ ." + arg + " }}",
			},
		},
	}

	ts := time.Time{}
	tcs, err := ExpandQuery(cfg, ts, ts, 0)
	require.NoError(t, err)

	var queries []string
	for _, tc := range tcs {
		queries = append(queries, tc.Query)
	}
	require.NotEmpty(t, queries)
	require.ElementsMatch(t, queries, testVariantArgs[arg])
}

func TestCollectVariantArgs(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{
			``,
			nil,
		},
		{
			`{{ .quantile }}`,
			[]string{"quantile"},
		},
		{
			`{{ .quantile }}+{{ .quantile }}`,
			[]string{"quantile"},
		},
		{
			`{{ .unwrapRangeAggOp }}( {} [{{ .range }}] )`,
			[]string{"range", "unwrapRangeAggOp"},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			templ, err := template.New("templ").Parse(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, collectVariantArgs(templ))
		})
	}
}
