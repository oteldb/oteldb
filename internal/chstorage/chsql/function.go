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

// ArrayConcat returns `arrayConcat(<args>...)` function call expression.
func ArrayConcat(args ...Expr) Expr {
	return Function("arrayConcat", args...)
}

// ArrayJoin returns `arrayJoin(<args>...)` function call expression.
func ArrayJoin(args ...Expr) Expr {
	return Function("arrayJoin", args...)
}

// Hex returns `hex(<arg>)` function call expression.
func Hex(arg Expr) Expr {
	return Function("hex", arg)
}

// Unhex returns `unhex(<arg>)` function call expression.
func Unhex(arg Expr) Expr {
	return Function("unhex", arg)
}
