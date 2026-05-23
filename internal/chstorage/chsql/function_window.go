package chsql

import (
	"strconv"
	"time"
)

// WindowOrderSpec is an expression with its sort direction, used in the ORDER BY
// clause of an OVER window specification.
type WindowOrderSpec struct {
	Expr  Expr
	Order Order
}

// WindowAsc returns a [WindowOrderSpec] with ascending order.
func WindowAsc(e Expr) WindowOrderSpec {
	return WindowOrderSpec{Expr: e, Order: Asc}
}

// WindowDesc returns a [WindowOrderSpec] with descending order.
func WindowDesc(e Expr) WindowOrderSpec {
	return WindowOrderSpec{Expr: e, Order: Desc}
}

// Over builds `<fn> OVER (PARTITION BY <partitionBy...> ORDER BY <orderBy...>)`.
func Over(fn Expr, partitionBy []Expr, orderBy []WindowOrderSpec) Expr {
	args := make([]Expr, 0, 1+len(partitionBy)+len(orderBy))
	args = append(args, fn)
	args = append(args, partitionBy...)
	for _, o := range orderBy {
		dir := "ASC"
		if o.Order == Desc {
			dir = "DESC"
		}
		args = append(args, Expr{typ: exprSortDir, tok: dir, args: []Expr{o.Expr}})
	}
	return Expr{
		typ:  exprWindowFunc,
		tok:  strconv.Itoa(len(partitionBy)),
		args: args,
	}
}

// LagInFrame returns `lagInFrame(<arg>, <offset>, <default>)`.
func LagInFrame(arg, offset, defaultVal Expr) Expr {
	return Function("lagInFrame", arg, offset, defaultVal)
}

// FirstValue returns `first_value(<arg>)` function call expression.
func FirstValue(arg Expr) Expr {
	return Function("first_value", arg)
}

// LastValue returns `last_value(<arg>)` function call expression.
func LastValue(arg Expr) Expr {
	return Function("last_value", arg)
}

// HopStart returns `hopStart(<time_attr>, <hop_interval>, <window_interval>)` function call expression.
func HopStart(arg Expr, hopInterval, windowInterval IntervalLit) Expr {
	return Function("hopStart", arg, hopInterval.Expr(), windowInterval.Expr())
}

// HopEnd returns `hopEnd(<time_attr>, <hop_interval>, <window_interval>)` function call expression.
func HopEnd(arg Expr, hopInterval, windowInterval IntervalLit) Expr {
	return Function("hopEnd", arg, hopInterval.Expr(), windowInterval.Expr())
}

// Hop returns `hop(<time_attr>, <hop_interval>, <window_interval>)` function call expression.
func Hop(arg Expr, hopInterval, windowInterval IntervalLit) Expr {
	return Function("hop", arg, hopInterval.Expr(), windowInterval.Expr())
}

// TumbleStart returns `tumbleStart(<time_attr>, <hop_interval>)` function call expression.
func TumbleStart(arg Expr, hopInterval time.Duration) Expr {
	return Function("tumbleStart", arg, Interval(hopInterval))
}

// TumbleEnd returns `tumbleEnd(<time_attr>, <hop_interval>)` function call expression.
func TumbleEnd(arg Expr, hopInterval time.Duration) Expr {
	return Function("tumbleEnd", arg, Interval(hopInterval))
}

// Tumble returns `tumble(<time_attr>, <hop_interval>)` function call expression.
func Tumble(arg Expr, hopInterval time.Duration) Expr {
	return Function("tumble", arg, Interval(hopInterval))
}
