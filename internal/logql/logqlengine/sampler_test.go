package logqlengine

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
)

func TestSampleExtractor(t *testing.T) {
	type inputEntry struct {
		line   string
		labels map[logql.Label]pcommon.Value
	}
	tests := []struct {
		op     logql.RangeOp
		unwrap *logql.UnwrapExpr
		input  inputEntry
		want   float64
		wantOk bool
	}{
		// Just count lines, i.e. return 1 for every line.
		{
			logql.RangeOpCount,
			nil,
			inputEntry{
				line: "foo",
			},
			1,
			true,
		},
		// Count bytes.
		{
			logql.RangeOpBytes,
			nil,
			inputEntry{
				line: "foo",
			},
			3,
			true,
		},
		{
			logql.RangeOpBytesRate,
			nil,
			inputEntry{
				line: "foobarbaz",
			},
			9,
			true,
		},
		// Label unwrapping.
		{
			logql.RangeOpSum,
			&logql.UnwrapExpr{
				Label: "extract",
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"extract": pcommon.NewValueDouble(3.14),
				},
			},
			3.14,
			true,
		},
		{
			logql.RangeOpAvg,
			&logql.UnwrapExpr{
				Label: "extract",
				Op:    "bytes",
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"extract": pcommon.NewValueStr("2kib"),
				},
			},
			float64(2 * 1024),
			true,
		},
		{
			logql.RangeOpFirst,
			&logql.UnwrapExpr{
				Label: "extract",
				Op:    "duration",
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"extract": pcommon.NewValueStr("1m"),
				},
			},
			60,
			true,
		},
		{
			logql.RangeOpLast,
			&logql.UnwrapExpr{
				Label: "extract",
				Op:    "duration_seconds",
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"extract": pcommon.NewValueStr("30s"),
				},
			},
			30,
			true,
		},
		// Post filter.
		{
			logql.RangeOpAvg,
			&logql.UnwrapExpr{
				Label: "extract",
				Filters: []logql.LabelMatcher{
					{
						Label: "extract",
						Op:    logql.OpEq,
						Value: "30",
					},
				},
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"extract": pcommon.NewValueStr("30"),
				},
			},
			30,
			true,
		},
		// Label does not pass filter.
		{
			logql.RangeOpAvg,
			&logql.UnwrapExpr{
				Label: "extract",
				Filters: []logql.LabelMatcher{
					{
						Label: "extract",
						Op:    logql.OpNotEq,
						Value: "30",
					},
				},
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"extract": pcommon.NewValueStr("30"),
				},
			},
			0,
			false,
		},
		{
			logql.RangeOpAvg,
			&logql.UnwrapExpr{
				Label: "extract",
				Filters: []logql.LabelMatcher{
					{
						Label: "extract",
						Op:    logql.OpRe,
						Value: `^\d+$`,
						Re:    regexp.MustCompile(`^\d+$`),
					},
					{
						Label: "extract",
						Op:    logql.OpNotEq,
						Value: "30",
					},
				},
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"extract": pcommon.NewValueStr("30"),
				},
			},
			0,
			false,
		},
		// Invalid number.
		{
			logql.RangeOpMin,
			&logql.UnwrapExpr{
				Label: "invalid",
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"invalid": pcommon.NewValueStr("foobar"),
				},
			},
			0,
			true,
		},
		// Invalid bytes.
		{
			logql.RangeOpMin,
			&logql.UnwrapExpr{
				Label: "invalid",
				Op:    "bytes",
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"invalid": pcommon.NewValueStr("foobar"),
				},
			},
			0,
			true,
		},
		// Invalid duration.
		{
			logql.RangeOpMin,
			&logql.UnwrapExpr{
				Label: "invalid",
				Op:    "duration",
			},
			inputEntry{
				line: "foo",
				labels: map[logql.Label]pcommon.Value{
					"invalid": pcommon.NewValueStr("foobar"),
				},
			},
			0,
			true,
		},
		// Label not exist
		{
			logql.RangeOpMax,
			&logql.UnwrapExpr{
				Label: "not_exist",
			},
			inputEntry{
				line: "foo",
			},
			0,
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			e, err := buildSampleExtractor(&logql.RangeAggregationExpr{
				Op: tt.op,
				Range: logql.LogRangeExpr{
					Unwrap: tt.unwrap,
				},
			})
			require.NoError(t, err)

			set := newLabelSet(tt.input.labels)
			got, gotOk := e.Extract(Entry{
				Timestamp: 1,
				Line:      tt.input.line,
				Set:       set,
			})
			if !tt.wantOk {
				require.False(t, gotOk)
				return
			}
			require.True(t, gotOk)
			require.Equal(t, tt.want, got)
		})
	}
}
