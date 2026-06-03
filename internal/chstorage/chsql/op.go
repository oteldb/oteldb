package chsql

func unaryOp(op string, sub Expr) Expr {
	return Expr{typ: exprUnaryOp, tok: op, args: []Expr{sub}}
}

// Not returns new `NOT` operation.
func Not(sub Expr) Expr {
	return unaryOp("NOT", sub)
}

func binaryOp(left Expr, op string, right Expr) Expr {
	return Expr{typ: exprBinaryOp, tok: op, args: []Expr{left, right}}
}

func joinBinaryOp(op string, args []Expr) Expr {
	return Expr{typ: exprBinaryOp, tok: op, args: args}
}

// Eq returns new `=` operation.
func Eq(left, right Expr) Expr {
	return binaryOp(left, "=", right)
}

// NotEq returns new `!=` operation.
func NotEq(left, right Expr) Expr {
	return binaryOp(left, "!=", right)
}

// Gt returns new `>` operation.
func Gt(left, right Expr) Expr {
	return binaryOp(left, ">", right)
}

// Gte returns new `>=` operation.
func Gte(left, right Expr) Expr {
	return binaryOp(left, ">=", right)
}

// Lt returns new `<` operation.
func Lt(left, right Expr) Expr {
	return binaryOp(left, "<", right)
}

// Lte returns new `<=` operation.
func Lte(left, right Expr) Expr {
	return binaryOp(left, "<=", right)
}

// And returns new `AND` operation.
func And(left, right Expr) Expr {
	return binaryOp(left, "AND", right)
}

// Or returns new `OR` operation.
func Or(left, right Expr) Expr {
	return binaryOp(left, "OR", right)
}

// Minus returns new `-` operation.
func Minus(left, right Expr) Expr {
	return binaryOp(left, "-", right)
}

// Sub returns new `-` operation.
func Sub(left, right Expr) Expr {
	return Minus(left, right)
}

// Add returns new `+` operation.
func Add(left, right Expr) Expr {
	return binaryOp(left, "+", right)
}

// Mul returns new `*` operation.
func Mul(left, right Expr) Expr {
	return binaryOp(left, "*", right)
}

// Div returns new `/` operation.
func Div(left, right Expr) Expr {
	return binaryOp(left, "/", right)
}

// Mod returns new `%` operation.
func Mod(left, right Expr) Expr {
	return binaryOp(left, "%", right)
}

// In returns new `IN` operation.
func In(left, right Expr) Expr {
	return binaryOp(left, "IN", right)
}
