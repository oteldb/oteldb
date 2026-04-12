package chsql

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// UnixNano returns time.Time as unix nano timestamp.
func UnixNano(t time.Time) Expr {
	return Integer(t.UnixNano())
}

// DateTime returns an expression representing [time.Time] as DateTime.
func DateTime(t time.Time) Expr {
	return ToDateTime(Integer(t.Unix()))
}

// DateTime64 returns an expression representing [time.Time] with given precision as DateTime64.
func DateTime64(t time.Time, prec proto.Precision) Expr {
	s := t.UTC().Format("2006-01-02 15:04:05.999999999")
	return ToDateTime64(String(s), prec)
}

// Interval converts duration d to ClickHouse Interval expression.
func Interval(d time.Duration) Expr {
	return MakeIntervalLitFromDuration(d).Expr()
}

// IntervalLit defines an interval.
type IntervalLit struct {
	value int64
	prec  IntervalPrecision
}

// MakeIntervalLit returns an [IntervalLit] with given precision.
func MakeIntervalLit[I int | int8 | int16 | int32 | int64](val I, prec IntervalPrecision) IntervalLit {
	return IntervalLit{value: int64(val), prec: prec}
}

// MakeIntervalLitFromDuration creates an [IntervalLit] from given duration.
func MakeIntervalLitFromDuration(d time.Duration) IntervalLit {
	prec := GetIntervalPrecision(d)
	return IntervalLit{
		value: prec.Duration(d),
		prec:  prec,
	}
}

// Expr returns SQL literal for this duration.
func (l IntervalLit) Expr() Expr {
	return l.prec.Expr(l.value)
}

// IntervalPrecision sets interval precision.
type IntervalPrecision uint8

const (
	IntervalYear = IntervalPrecision(iota + 1)
	IntervalQuarter
	IntervalMonth
	IntervalWeek
	IntervalDay
	IntervalHour
	IntervalMinute
	IntervalSecond
	IntervalMillisecond
	IntervalMicrosecond
	IntervalNanosecond
)

// Expr returns SQL literal for given duration.
func (p IntervalPrecision) Expr(val int64) Expr {
	return Function(p.convertFn(), Integer(val))
}

// Duration returns duration truncated to precision p.
func (p IntervalPrecision) Duration(d time.Duration) int64 {
	return int64(d / p.durationMul())
}

func (p IntervalPrecision) durationMul() time.Duration {
	switch p {
	case IntervalYear:
		return 365 * 24 * time.Hour
	case IntervalQuarter:
		return 90 * 24 * time.Hour
	case IntervalMonth:
		return 30 * 24 * time.Hour
	case IntervalWeek:
		return 7 * 24 * time.Hour
	case IntervalDay:
		return 24 * time.Hour
	case IntervalHour:
		return time.Hour
	case IntervalMinute:
		return time.Minute
	case IntervalSecond:
		return time.Second
	case IntervalMillisecond:
		return time.Millisecond
	case IntervalMicrosecond:
		return time.Microsecond
	default:
		return time.Nanosecond
	}
}

func (p IntervalPrecision) convertFn() string {
	switch p {
	case IntervalYear:
		return "toIntervalYear"
	case IntervalQuarter:
		return "toIntervalQuarter"
	case IntervalMonth:
		return "toIntervalMonth"
	case IntervalWeek:
		return "toIntervalWeek"
	case IntervalDay:
		return "toIntervalDay"
	case IntervalHour:
		return "toIntervalHour"
	case IntervalMinute:
		return "toIntervalMinute"
	case IntervalSecond:
		return "toIntervalSecond"
	case IntervalMillisecond:
		return "toIntervalMillisecond"
	case IntervalMicrosecond:
		return "toIntervalMicrosecond"
	default:
		return "toIntervalNanosecond"
	}
}

// GetIntervalPrecision returns coarest [IntervalPrecision] to represent given duration.
func GetIntervalPrecision(d time.Duration) IntervalPrecision {
	for p := IntervalYear; p <= IntervalNanosecond; p++ {
		mul := p.durationMul()
		if d%mul == 0 && d/mul != 0 {
			return p
		}
	}
	return IntervalNanosecond
}

// InTimeRange returns boolean expression to filter by [start:end].
func InTimeRange(column string, start, end time.Time, prec proto.Precision) Expr {
	var (
		columnExpr = Ident(column)
		expr       Expr
	)
	if !start.IsZero() {
		expr = Gte(columnExpr, DateTime64(start, prec))
	}
	if !end.IsZero() {
		endExpr := Lte(columnExpr, DateTime64(end, prec))
		if expr.IsZero() {
			expr = endExpr
		} else {
			expr = And(expr, endExpr)
		}
	}
	if expr.IsZero() {
		expr = Bool(true)
	}
	return expr
}

// ColumnEq returns new `=` operation on column and literal.
func ColumnEq[V litValue](column string, right V) Expr {
	return binaryOp(Ident(column), "=", Value(right))
}

// Contains returns boolean expression to filter strings containing needle.
func Contains(column, needle string) Expr {
	return Gt(
		PositionUTF8(Ident(column), String(needle)),
		Integer(0),
	)
}

// JoinAnd joins given expressions using AND op.
//
//   - If len(args) == 0, returns `true` literal.
//   - If len(args) == 1, returns first argument.
//   - Otherwise, joins arguments with AND.
func JoinAnd(args ...Expr) Expr {
	switch len(args) {
	case 0:
		return Bool(true)
	case 1:
		return args[0]
	default:
		return joinBinaryOp("AND", args)
	}
}

// JoinOr joins given expressions using OR op.
//
//   - If len(args) == 0, returns `true` literal.
//   - If len(args) == 1, returns first argument.
//   - Otherwise, joins arguments with OR.
func JoinOr(args ...Expr) Expr {
	switch len(args) {
	case 0:
		return Bool(true)
	case 1:
		return args[0]
	default:
		return joinBinaryOp("OR", args)
	}
}
