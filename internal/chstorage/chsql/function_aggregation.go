package chsql

// ArgMax returns `argMax(<arg>, <val>)` function call expression.
func ArgMax(arg, val Expr) Expr {
	return Function("argMax", arg, val)
}

// ArgMin returns `argMin(<arg>, <val>)` function call expression.
func ArgMin(arg, val Expr) Expr {
	return Function("argMin", arg, val)
}

// AnyLast returns `anyLast(<expr>)` function call expression.
func AnyLast(expr Expr) Expr {
	return Function("anyLast", expr)
}

// Min returns `min(<expr>)` function call expression.
func Min(expr Expr) Expr {
	return Function("min", expr)
}

// Max returns `max(<expr>)` function call expression.
func Max(expr Expr) Expr {
	return Function("max", expr)
}

// Sum returns `sum(<expr>)` function call expression.
func Sum(expr Expr) Expr {
	return Function("sum", expr)
}

// Avg returns `avg(<expr>)` function call expression.
func Avg(expr Expr) Expr {
	return Function("avg", expr)
}

// Count returns `count()` function call expression.
func Count() Expr {
	return Function("count")
}

// GroupArray returns `groupArray(<arg>)` function call expression.
func GroupArray(arg Expr) Expr {
	return Function("groupArray", arg)
}
