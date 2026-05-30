package logqlabels

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
)

func TestAggregatedLabels(t *testing.T) {
	defaultSet := map[string]string{
		"foo": "1",
		"bar": "2",
		"baz": "3",
	}
	tests := []struct {
		set     map[string]string
		by      []logql.Label
		without []logql.Label
		want    []string
	}{
		// By.
		{
			defaultSet,
			[]logql.Label{"foo"},
			nil,
			[]string{"foo"},
		},
		{
			defaultSet,
			[]logql.Label{"foo", "bar"},
			nil,
			[]string{"foo", "bar"},
		},
		// Without.
		{
			defaultSet,
			nil,
			[]logql.Label{"bar", "baz"},
			[]string{"foo"},
		},
		// Both.
		{
			defaultSet,
			[]logql.Label{"foo", "bar"},
			[]logql.Label{"bar"},
			[]string{"foo"},
		},
		{
			defaultSet,
			[]logql.Label{"foo", "bar", "baz"},
			[]logql.Label{"bar"},
			[]string{"foo", "baz"},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := NewLabelSet()
			for k, v := range tt.set {
				set.Set(logql.Label(k), pcommon.NewValueStr(v))
			}

			buildWays := []struct {
				name  string
				build func(set LabelSet, by, without []logql.Label) AggregatedLabels
			}{
				{
					"ByThenWithout",
					func(set LabelSet, by, without []logql.Label) AggregatedLabels {
						labels := AggregatedLabelsFromSet(
							set,
							nil,
							nil,
						)
						if len(tt.by) > 0 {
							labels = labels.By(tt.by...)
						}
						if len(tt.without) > 0 {
							labels = labels.Without(tt.without...)
						}
						return labels
					},
				},
				{
					"WithoutThenBy",
					func(set LabelSet, by, without []logql.Label) AggregatedLabels {
						labels := AggregatedLabelsFromSet(
							set,
							nil,
							nil,
						)
						if len(tt.without) > 0 {
							labels = labels.Without(tt.without...)
						}
						if len(tt.by) > 0 {
							labels = labels.By(tt.by...)
						}
						return labels
					},
				},
				{
					"Constructor",
					func(set LabelSet, by, without []logql.Label) AggregatedLabels {
						return AggregatedLabelsFromSet(
							set,
							buildSet(nil, by...),
							buildSet(nil, without...),
						)
					},
				},
			}
			for _, bw := range buildWays {
				bw := bw
				t.Run(bw.name, func(t *testing.T) {
					labels := bw.build(set, tt.by, tt.without)
					got := labels.AsLokiAPI()
					for _, k := range tt.want {
						require.Contains(t, got, k)
					}
				})
			}
		})
	}
}
