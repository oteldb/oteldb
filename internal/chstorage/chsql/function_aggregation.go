package chsql

// ArgMax returns `argMax(<arg>, <val>)` function call expression.
func ArgMax(arg, val Expr) Expr {
	return Function("argMax", arg, val)
}

// ArgMin returns `argMin(<arg>, <val>)` function call expression.
func ArgMin(arg, val Expr) Expr {
	return Function("argMin", arg, val)
}
