package logqlengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql"
)

func TestStringLabelFilter(t *testing.T) {
	tests := []struct {
		input  map[logql.Label]pcommon.Value
		label  string
		op     logql.BinOp
		value  string
		wantOk bool
	}{
		// No label.
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			logql.OpEq,
			"",
			true,
		},
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			logql.OpNotEq,
			"",
			false,
		},

		// Has match.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("foo"),
			},
			"exist",
			logql.OpEq,
			"foo",
			true,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("foo"),
			},
			"exist",
			logql.OpNotEq,
			"bar",
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := newLabelSet(tt.input)

			f, err := buildLabelMatcher(logql.LabelMatcher{
				Label: logql.Label(tt.label),
				Op:    tt.op,
				Value: tt.value,
			})
			require.NoError(t, err)

			newLine, gotOk := f.Process(0, ``, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, ``, newLine)
			require.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestDurationLabelFilter(t *testing.T) {
	tests := []struct {
		input         map[logql.Label]pcommon.Value
		label         string
		op            logql.BinOp
		value         time.Duration
		wantOk        bool
		wantFilterErr bool
	}{
		// No label.
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			logql.OpEq,
			10 * time.Second,
			false,
			false,
		},

		// Has match.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("10s"),
			},
			"exist",
			logql.OpEq,
			10 * time.Second,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("9s"),
			},
			"exist",
			logql.OpNotEq,
			10 * time.Second,
			true,
			false,
		},
		// Greater than.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("11s"),
			},
			"exist",
			logql.OpGt,
			10 * time.Second,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("11s"),
			},
			"exist",
			logql.OpGte,
			10 * time.Second,
			true,
			false,
		},
		// Less than.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("9s"),
			},
			"exist",
			logql.OpLt,
			10 * time.Second,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("9s"),
			},
			"exist",
			logql.OpLte,
			10 * time.Second,
			true,
			false,
		},

		// Parsing error.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`foo`),
			},
			"exist",
			logql.OpEq,
			10 * time.Second,
			false,
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := newLabelSet(tt.input)

			f, err := buildDurationLabelFilter(&logql.DurationFilter{
				Label: logql.Label(tt.label),
				Op:    tt.op,
				Value: tt.value,
			})
			require.NoError(t, err)

			newLine, gotOk := f.Process(0, ``, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, ``, newLine)

			if tt.wantFilterErr {
				_, ok := set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			require.False(t, ok, "got error: %s", errMsg)

			require.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestBytesLabelFilter(t *testing.T) {
	tests := []struct {
		input         map[logql.Label]pcommon.Value
		label         string
		op            logql.BinOp
		value         uint64
		wantOk        bool
		wantFilterErr bool
	}{
		// No label.
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			logql.OpEq,
			10,
			false,
			false,
		},

		// Has match.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("10b"),
			},
			"exist",
			logql.OpEq,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("9b"),
			},
			"exist",
			logql.OpNotEq,
			10,
			true,
			false,
		},
		// Greater than.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("11b"),
			},
			"exist",
			logql.OpGt,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("11b"),
			},
			"exist",
			logql.OpGte,
			10,
			true,
			false,
		},
		// Less than.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("9b"),
			},
			"exist",
			logql.OpLt,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("9b"),
			},
			"exist",
			logql.OpLte,
			10,
			true,
			false,
		},

		// Parsing error.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`foo`),
			},
			"exist",
			logql.OpEq,
			10,
			false,
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := newLabelSet(tt.input)

			f, err := buildBytesLabelFilter(&logql.BytesFilter{
				Label: logql.Label(tt.label),
				Op:    tt.op,
				Value: tt.value,
			})
			require.NoError(t, err)

			newLine, gotOk := f.Process(0, ``, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, ``, newLine)

			if tt.wantFilterErr {
				_, ok := set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			require.False(t, ok, "got error: %s", errMsg)

			require.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestNumberLabelFilter(t *testing.T) {
	tests := []struct {
		input         map[logql.Label]pcommon.Value
		label         string
		op            logql.BinOp
		value         float64
		wantOk        bool
		wantFilterErr bool
	}{
		// No label.
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			logql.OpEq,
			10,
			false,
			false,
		},

		// Has match.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueDouble(10),
			},
			"exist",
			logql.OpEq,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueInt(10),
			},
			"exist",
			logql.OpEq,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr("10"),
			},
			"exist",
			logql.OpEq,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueDouble(9),
			},
			"exist",
			logql.OpNotEq,
			10,
			true,
			false,
		},
		// Greater than.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueDouble(11),
			},
			"exist",
			logql.OpGt,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueDouble(11),
			},
			"exist",
			logql.OpGte,
			10,
			true,
			false,
		},
		// Less than.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueDouble(9),
			},
			"exist",
			logql.OpLt,
			10,
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueDouble(9),
			},
			"exist",
			logql.OpLte,
			10,
			true,
			false,
		},

		// Parsing error.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`foo`),
			},
			"exist",
			logql.OpEq,
			10,
			false,
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := newLabelSet(tt.input)

			f, err := buildNumberLabelFilter(&logql.NumberFilter{
				Label: logql.Label(tt.label),
				Op:    tt.op,
				Value: tt.value,
			})
			require.NoError(t, err)

			newLine, gotOk := f.Process(0, ``, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, ``, newLine)

			if tt.wantFilterErr {
				_, ok := set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			require.False(t, ok, "got error: %s", errMsg)

			require.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestIPLabelFilter(t *testing.T) {
	tests := []struct {
		input         map[logql.Label]pcommon.Value
		label         logql.Label
		pattern       string
		wantOk        bool
		wantFilterErr bool
	}{
		// No label.
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			"192.168.1.1",
			false,
			false,
		},
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			"192.168.1.0-192.168.1.255",
			false,
			false,
		},
		{
			map[logql.Label]pcommon.Value{},
			`not_exist`,
			"192.168.1.0/24",
			false,
			false,
		},

		// Has match.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`192.168.1.1`),
			},
			"exist",
			"192.168.1.1",
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`192.168.1.1`),
			},
			"exist",
			"192.168.1.0-192.168.1.255",
			true,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`192.168.1.1`),
			},
			"exist",
			"192.168.1.0/24",
			true,
			false,
		},

		// No match.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`127.0.0.1`),
			},
			"exist",
			"192.168.1.1",
			false,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`127.0.0.1`),
			},
			"exist",
			"192.168.1.0-192.168.1.255",
			false,
			false,
		},
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`127.0.0.1`),
			},
			"exist",
			"192.168.1.0/24",
			false,
			false,
		},

		// Parsing error.
		{
			map[logql.Label]pcommon.Value{
				"exist": pcommon.NewValueStr(`127.0.0.`),
			},
			"exist",
			"192.168.1.0",
			false,
			true,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			_, hasLabel := tt.input[tt.label]
			set := newLabelSet(tt.input)

			for _, cse := range []struct {
				op     logql.BinOp
				wantOk bool
			}{
				{logql.OpEq, hasLabel && tt.wantOk},
				{logql.OpNotEq, hasLabel && !tt.wantOk},
			} {
				f, err := buildIPLabelFilter(&logql.IPFilter{
					Label: tt.label,
					Op:    cse.op,
					Value: tt.pattern,
				})
				require.NoError(t, err)

				newLine, gotOk := f.Process(0, ``, set)
				// Ensure that extractor does not change the line.
				require.Equal(t, ``, newLine)

				if tt.wantFilterErr {
					_, ok := set.GetError()
					require.True(t, ok)
					return
				}
				errMsg, ok := set.GetError()
				require.False(t, ok, "got error: %s", errMsg)

				require.Equal(t, cse.wantOk, gotOk)
			}
		})
	}
}
