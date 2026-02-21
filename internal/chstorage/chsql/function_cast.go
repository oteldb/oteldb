package chsql

// Cast returns `CAST(<x>, <typ>)` function call expression.
func Cast(x Expr, typ string) Expr {
	return Function("CAST", x, String(typ))
}

// ToLowCardinality returns `ToLowCardinality(<arg>)` function call expression.
func ToLowCardinality(arg Expr) Expr {
	return Function("toLowCardinality", arg)
}

// ToString returns `toString(<arg>)` function call expression.
func ToString(arg Expr) Expr {
	return Function("toString", arg)
}

// ToInt64 returns `toInt64(<arg>)` function call expression.
func ToInt64(arg Expr) Expr {
	return Function("toInt64", arg)
}

// ToUInt64 returns `toUInt64(<arg>)` function call expression.
func ToUInt64(arg Expr) Expr {
	return Function("toUInt64", arg)
}

// ToFloat64 returns `toFloat64(<arg>)` function call expression.
func ToFloat64(arg Expr) Expr {
	return Function("toFloat64", arg)
}
