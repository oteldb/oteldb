package logql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLineFilter_String(t *testing.T) {
	tests := []struct {
		f    LineFilter
		want string
	}{
		{
			LineFilter{
				Op: OpEq,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`|= "line"`,
		},
		{
			LineFilter{
				Op: OpNotEq,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`!= "line"`,
		},
		{
			LineFilter{
				Op: OpRe,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`|~ "line"`,
		},
		{
			LineFilter{
				Op: OpNotRe,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`!~ "line"`,
		},
		{
			LineFilter{
				Op: OpPattern,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`|> "line"`,
		},
		{
			LineFilter{
				Op: OpNotPattern,
				By: LineFilterValue{
					Value: "line",
				},
			},
			`!> "line"`,
		},
		{
			LineFilter{
				Op: OpEq,
				By: LineFilterValue{
					Value: "127.0.0.1",
					IP:    true,
				},
			},
			`|= ip("127.0.0.1")`,
		},
		{
			LineFilter{
				Op: OpEq,
				By: LineFilterValue{
					Value: "foo",
				},
				Or: []LineFilterValue{
					{Value: "bar"},
				},
			},
			`|= "foo" or "bar"`,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.f.String()
			require.Equal(t, tt.want, s)

			// Ensure stringer produces valid LogQL.
			_, err := Parse(`{foo="bar"} `+s, ParseOptions{})
			require.NoError(t, err)
		})
	}
}

func TestLabelPredicateBinOp_String(t *testing.T) {
	tests := []struct {
		m    LabelPredicateBinOp
		want string
	}{
		{
			LabelPredicateBinOp{
				Left: &LabelPredicateParen{
					X: &LabelMatcher{"service", OpEq, "sus4", nil},
				},
				Op:    OpAnd,
				Right: &LabelMatcher{"request", OpNotEq, "PUT", nil},
			},
			`(service="sus4") and request!="PUT"`,
		},
		{
			LabelPredicateBinOp{
				Left: &LabelPredicateParen{
					X: &LabelMatcher{"service", OpEq, "sus4", nil},
				},
				Op:    OpOr,
				Right: &LabelMatcher{"request", OpNotEq, "PUT", nil},
			},
			`(service="sus4") or request!="PUT"`,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, tt.m.String())

			// Ensure stringer produces valid LogQL.
			_, err := Parse(`{label="value"} |`+s, ParseOptions{})
			require.NoError(t, err)
		})
	}
}

func TestIPFilter_String(t *testing.T) {
	tests := []struct {
		m    IPFilter
		want string
	}{
		{IPFilter{"label", OpEq, "127.0.0.1"}, `label = ip("127.0.0.1")`},
		{IPFilter{"label", OpNotEq, "127.0.0.1/24"}, `label != ip("127.0.0.1/24")`},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, tt.m.String())

			// Ensure stringer produces valid LogQL.
			_, err := Parse(`{label="value"} |`+s, ParseOptions{})
			require.NoError(t, err)
		})
	}
}

func TestDurationFilter_String(t *testing.T) {
	tests := []struct {
		m    DurationFilter
		want string
	}{
		{DurationFilter{"label", OpEq, time.Millisecond}, `label == 1ms`},
		{DurationFilter{"label", OpNotEq, time.Second}, `label != 1s`},
		{DurationFilter{"label", OpGt, time.Minute + time.Second}, `label > 1m1s`},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, tt.m.String())

			// Ensure stringer produces valid LogQL.
			_, err := Parse(`{label="value"} |`+s, ParseOptions{})
			require.NoError(t, err)
		})
	}
}

func TestBytesFilter_String(t *testing.T) {
	tests := []struct {
		m    BytesFilter
		want string
	}{
		{BytesFilter{"label", OpEq, 1024}, `label == 1.0KiB`},
		{BytesFilter{"label", OpNotEq, 1024 * 1024}, `label != 1.0MiB`},
		{BytesFilter{"label", OpGt, 1024 * 1024 * 1024}, `label > 1.0GiB`},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, tt.m.String())

			// Ensure stringer produces valid LogQL.
			_, err := Parse(`{label="value"} |`+s, ParseOptions{})
			require.NoError(t, err)
		})
	}
}

func TestNumberFilter_String(t *testing.T) {
	tests := []struct {
		m    NumberFilter
		want string
	}{
		{NumberFilter{"label", OpEq, 3.14}, `label == 3.14`},
		{NumberFilter{"label", OpNotEq, 3.14}, `label != 3.14`},
		{NumberFilter{"label", OpGt, 3.14}, `label > 3.14`},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, tt.m.String())

			// Ensure stringer produces valid LogQL.
			_, err := Parse(`{label="value"} |`+s, ParseOptions{})
			require.NoError(t, err)
		})
	}
}
