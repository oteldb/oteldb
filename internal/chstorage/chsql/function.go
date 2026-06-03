package chsql

// Function reutrns function call expression.
func Function(name string, args ...Expr) Expr {
	return Expr{typ: exprFunction, tok: name, args: args}
}

// Coalesce returns `coalesce(<args>...)` function call expression.
func Coalesce(args ...Expr) Expr {
	return Function("coalesce", args...)
}

// Has returns `has(<arr>, <elem>)` function call expression.
func Has(arr, elem Expr) Expr {
	return Function("has", arr, elem)
}

// Map returns `map(<args>...)` function call expression.
func Map(args ...Expr) Expr {
	return Function("map", args...)
}

// MapConcat returns `mapConcat(<args>...)` function call expression.
func MapConcat(args ...Expr) Expr {
	return Function("mapConcat", args...)
}

// Array returns `array(<args>...)` function call expression.
func Array(args ...Expr) Expr {
	return Function("array", args...)
}

// ArrayElement returns `arrayElement(<arr>, <index>)` function call expression.
func ArrayElement(arr, index Expr) Expr {
	return Function("arrayElement", arr, index)
}

// TupleElement returns `tupleElement(<tup>, <index>)` function call expression.
func TupleElement(tup Expr, index int) Expr {
	return Function("tupleElement", tup, Integer(index))
}

// If returns `if(<cond>, <then>, <else>)` function call expression.
func If(cond, then, elseExpr Expr) Expr {
	return Function("if", cond, then, elseExpr)
}

// Greatest returns `greatest(<args>...)` function call expression.
func Greatest(args ...Expr) Expr {
	return Function("greatest", args...)
}

// Least returns `least(<args>...)` function call expression.
func Least(args ...Expr) Expr {
	return Function("least", args...)
}

// IntDiv returns `intDiv(<a>, <b>)` function call expression.
func IntDiv(a, b Expr) Expr {
	return Function("intDiv", a, b)
}

// ArrayConcat returns `arrayConcat(<args>...)` function call expression.
func ArrayConcat(args ...Expr) Expr {
	return Function("arrayConcat", args...)
}

// ArrayJoin returns `arrayJoin(<args>...)` function call expression.
func ArrayJoin(args ...Expr) Expr {
	return Function("arrayJoin", args...)
}

// ArrayFilter returns `arrayFilter(<lambda>, <arr>)` function call expression.
func ArrayFilter(lambda, arr Expr) Expr {
	return Function("arrayFilter", lambda, arr)
}

// ArrayMap returns `arrayMap(<lambda>, <arr>)` function call expression.
func ArrayMap(lambda, arr Expr) Expr {
	return Function("arrayMap", lambda, arr)
}

// ArraySort returns `arraySort(<lambda>, <arr>)` function call expression.
func ArraySort(lambda, arr Expr) Expr {
	return Function("arraySort", lambda, arr)
}

// ArraySum returns `arraySum(<lambda>, <args>...)` function call expression.
func ArraySum(lambda Expr, args ...Expr) Expr {
	return Function("arraySum", append([]Expr{lambda}, args...)...)
}

// ArrayPopFront returns `arrayPopFront(<arr>)` function call expression.
func ArrayPopFront(arr Expr) Expr {
	return Function("arrayPopFront", arr)
}

// ArrayPopBack returns `arrayPopBack(<arr>)` function call expression.
func ArrayPopBack(arr Expr) Expr {
	return Function("arrayPopBack", arr)
}

// Range returns `range(<n>)` function call expression.
func Range(n Expr) Expr {
	return Function("range", n)
}

// Hex returns `hex(<arg>)` function call expression.
func Hex(arg Expr) Expr {
	return Function("hex", arg)
}

// Unhex returns `unhex(<arg>)` function call expression.
func Unhex(arg Expr) Expr {
	return Function("unhex", arg)
}
