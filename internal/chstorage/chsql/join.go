package chsql

type joinType uint8

const (
	// only matching rows are returned.
	innerJoin = joinType(iota + 1)
	// non-matching rows from left table are returned in addition to matching rows.
	leftOuterJoin
	// non-matching rows from right table are returned in addition to matching rows.
	rightOuterJoin
	// non-matching rows from both tables are returned in addition to matching rows.
	fullOuterJoin
	// produces cartesian product of whole tables, "join keys" are not specified.
	crossJoin
)

type joinExpr struct {
	typ   joinType
	table string
	alias string
	on    Expr
}
