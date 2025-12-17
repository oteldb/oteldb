package chsql

import "time"

// FirstValue returns `first_value(<arg>)` function call expression.
func FirstValue(arg Expr) Expr {
	return Function("first_value", arg)
}

// LastValue returns `last_value(<arg>)` function call expression.
func LastValue(arg Expr) Expr {
	return Function("last_value", arg)
}

// HopStart returns `hopStart(<time_attr>, <hop_interval>, <window_interval>)` function call expression.
func HopStart(arg Expr, hopInterval, windowInterval time.Duration) Expr {
	return Function("hopStart", arg, Interval(hopInterval), Interval(windowInterval))
}

// HopEnd returns `hopEnd(<time_attr>, <hop_interval>, <window_interval>)` function call expression.
func HopEnd(arg Expr, hopInterval, windowInterval time.Duration) Expr {
	return Function("hopEnd", arg, Interval(hopInterval), Interval(windowInterval))
}

// Hop returns `hop(<time_attr>, <hop_interval>, <window_interval>)` function call expression.
func Hop(arg Expr, hopInterval, windowInterval time.Duration) Expr {
	return Function("hop", arg, Interval(hopInterval), Interval(windowInterval))
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
