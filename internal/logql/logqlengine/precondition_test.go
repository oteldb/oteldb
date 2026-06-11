package logqlengine

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/logql"
)

func TestExtractLabelQueryConditions(t *testing.T) {
	tests := []struct {
		sel           logql.Selector
		labelCaps     []logql.BinOp
		wantPrefilter bool
		conds         queryConditions
		wantErr       bool
	}{
		{
			logql.Selector{
				Matchers: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpEq, Value: "bar"},
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			[]logql.BinOp{logql.OpEq, logql.OpNotEq},
			false,
			queryConditions{
				Labels: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpEq, Value: "bar"},
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			false,
		},
		{
			logql.Selector{
				Matchers: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpRe, Value: "bar.+", Re: regexp.MustCompile("bar.+")},
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			[]logql.BinOp{logql.OpEq, logql.OpNotEq},
			true,
			queryConditions{
				Labels: []logql.LabelMatcher{
					{Label: "bar", Op: logql.OpNotEq, Value: "foo"},
				},
			},
			false,
		},
		{
			logql.Selector{
				Matchers: []logql.LabelMatcher{
					{Label: "foo", Op: logql.OpRe, Value: "bar.+", Re: regexp.MustCompile("bar.+")},
					{Label: "bar", Op: logql.OpNotRe, Value: "foo.+", Re: regexp.MustCompile("foo.+")},
				},
			},
			[]logql.BinOp{logql.OpEq, logql.OpNotEq},
			true,
			queryConditions{},
			false,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var caps QuerierCapabilities
			caps.Label.Add(tt.labelCaps...)

			conds, err := extractQueryConditions(caps, tt.sel)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.wantPrefilter {
				require.NotEqual(t, NopProcessor, conds.prefilter)
			} else {
				require.Equal(t, NopProcessor, conds.prefilter)
			}
			require.Equal(t, tt.conds.Labels, conds.Labels)
		})
	}
}
