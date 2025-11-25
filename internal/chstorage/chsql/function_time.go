package chsql

import (
	"github.com/ClickHouse/ch-go/proto"
)

// ToUnixTimestamp64Nano returns `toUnixTimestamp64Nano(<arg>)` function call expression.
func ToUnixTimestamp64Nano(arg Expr) Expr {
	return Function("toUnixTimestamp64Nano", arg)
}

// ToDateTime64 returns `toDateTime64(<arg>, <prec>)` function call expression.
func ToDateTime64(arg Expr, prec proto.Precision) Expr {
	return Function("toDateTime64", arg, Integer(int(prec)))
}

// ToStartOfDay returns `toStartOfHour(<arg>)` function call expression.
func ToStartOfHour(arg Expr) Expr {
	return Function("toStartOfHour", arg)
}
