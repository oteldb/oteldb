package chsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestInTimeRange(t *testing.T) {
	tests := []struct {
		start time.Time
		end   time.Time
		prec  proto.Precision
		want  string
	}{
		{
			time.Time{},
			time.Time{},
			proto.PrecisionNano,
			"true",
		},
		{
			time.Unix(5, 1),
			time.Time{},
			proto.PrecisionNano,
			"(timestamp >= toDateTime64('1970-01-01 00:00:05.000000001', 9))",
		},
		{
			time.Time{},
			time.Unix(6, 10),
			proto.PrecisionNano,
			"(timestamp <= toDateTime64('1970-01-01 00:00:06.00000001', 9))",
		},
		{
			time.Unix(5, 1),
			time.Unix(6, 10),
			proto.PrecisionNano,
			"((timestamp >= toDateTime64('1970-01-01 00:00:05.000000001', 9)) AND (timestamp <= toDateTime64('1970-01-01 00:00:06.00000001', 9)))",
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := InTimeRange("timestamp", tt.start, tt.end, tt.prec)

			p := GetPrinter()
			require.NoError(t, p.WriteExpr(got))
			require.Equal(t, tt.want, p.String())
		})
	}
}

func TestInterval(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "toIntervalNanosecond(0)"},
		{time.Microsecond, "toIntervalMicrosecond(1)"},
		{time.Millisecond, "toIntervalMillisecond(1)"},
		{time.Second, "toIntervalSecond(1)"},
		{2 * time.Second, "toIntervalSecond(2)"},
		{62 * time.Second, "toIntervalSecond(62)"},
		{time.Minute, "toIntervalMinute(1)"},
		{time.Hour, "toIntervalHour(1)"},
		{24 * time.Hour, "toIntervalDay(1)"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := Interval(tt.d)

			p := GetPrinter()
			require.NoError(t, p.WriteExpr(got))
			require.Equal(t, tt.want, p.String())
		})
	}
}

func TestJoinAnd(t *testing.T) {
	tests := []struct {
		args []Expr
		want string
	}{
		{nil, "true"},
		{[]Expr{Ident("foo")}, "foo"},
		{
			[]Expr{
				Ident("foo"),
				Ident("bar"),
			},
			"(foo AND bar)",
		},
		{
			[]Expr{
				Ident("foo"),
				Ident("bar"),
				Ident("baz"),
			},
			"(foo AND bar AND baz)",
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := JoinAnd(tt.args...)

			p := GetPrinter()
			err := p.WriteExpr(got)
			require.NoError(t, err)
			require.Equal(t, tt.want, p.String())
		})
	}
}
