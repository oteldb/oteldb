package traceql

// HintedExpr attaches query hints to a complete search or metrics expression.
// Parsing preserves hints; it does not implement their execution semantics.
type HintedExpr struct {
	Expr  Expr
	Hints []Hint
}

func (*HintedExpr) expr() {}

// Hint is a named static query option. Names and value types are engine-specific.
type Hint struct {
	Name  string
	Value Static
}
