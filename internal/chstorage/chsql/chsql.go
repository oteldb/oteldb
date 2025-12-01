// Package chsql provides fluent Clickhouse SQL query builder.
package chsql

import (
	"fmt"
	"strings"

	"github.com/go-faster/errors"
)

// Query is an query interface.
type Query interface {
	WriteSQL(p *Printer) error
}

// Order defines sorting order.
type Order uint8

// String implements [fmt.Stringer].
func (o Order) String() string {
	switch o {
	case Desc:
		return "DESC"
	case Asc:
		return "ASC"
	default:
		return fmt.Sprintf("unknown order %d", o)
	}
}

const (
	Desc Order = iota + 1
	Asc
)

// Printer prints SQL query.
type Printer struct {
	sb        strings.Builder
	needSpace bool
}

// GetPrinter creates a new [Printer].
func GetPrinter() *Printer {
	return new(Printer)
}

// PutPrinter returns [Printer] to pool.
func PutPrinter(*Printer) {}

// String returns query.
func (p *Printer) String() string {
	return p.sb.String()
}

func (p *Printer) maybeSpace() {
	if p.needSpace {
		p.sb.WriteByte(' ')
		p.needSpace = false
	}
}

// Comma writes a comma.
func (p *Printer) Comma() {
	p.needSpace = false
	p.sb.WriteByte(',')
}

// OpenParen writes a paren.
func (p *Printer) OpenParen() {
	p.maybeSpace()
	p.sb.WriteByte('(')
}

// CloseParen writes a paren.
func (p *Printer) CloseParen() {
	p.sb.WriteByte(')')
	p.needSpace = true
}

// Ident writes an identifier.
func (p *Printer) Ident(tok string) {
	p.maybeSpace()
	p.sb.WriteString(tok)
	p.needSpace = true
}

// Literal writes an literal.
func (p *Printer) Literal(lit string) {
	p.maybeSpace()
	p.sb.WriteString(lit)
	p.needSpace = true
}

// WriteExpr writes given expression.
func (p *Printer) WriteExpr(e Expr) error {
	switch e.typ {
	case exprIdent:
		p.Ident(e.tok)

		return nil
	case exprLiteral:
		p.Literal(e.tok)

		return nil
	case exprUnaryOp:
		if l := len(e.args); l != 1 {
			return errors.Errorf("unary expression must have exacty one arg, got %d", l)
		}

		p.Ident(e.tok)
		p.OpenParen()
		if err := p.WriteExpr(e.args[0]); err != nil {
			return err
		}
		p.CloseParen()

		return nil
	case exprBinaryOp:
		if l := len(e.args); l < 2 {
			return errors.Errorf("binary expression must have at least two args, got %d", l)
		}

		p.OpenParen()
		for i, arg := range e.args {
			if i != 0 {
				p.Ident(e.tok)
			}
			if err := p.WriteExpr(arg); err != nil {
				return err
			}
		}
		p.CloseParen()

		return nil
	case exprFunction:
		p.Ident(e.tok)
		p.needSpace = false
		p.OpenParen()
		for i, arg := range e.args {
			if i != 0 {
				p.sb.WriteByte(',')
			}
			if err := p.WriteExpr(arg); err != nil {
				return err
			}
		}
		p.CloseParen()

		return nil
	case exprTuple:
		p.OpenParen()
		for i, arg := range e.args {
			if i != 0 {
				p.sb.WriteByte(',')
			}
			if err := p.WriteExpr(arg); err != nil {
				return err
			}
		}
		p.CloseParen()

		return nil
	case exprSubQuery:
		if e.subQuery == nil {
			return errors.New("subquery is nil")
		}

		p.OpenParen()
		if err := e.subQuery.WriteSQL(p); err != nil {
			return err
		}
		p.CloseParen()

		return nil
	default:
		return errors.Errorf("unexpected expression type %v", e.typ)
	}
}

// And writes `AND` ident.
func (p *Printer) And() {
	p.Ident("AND")
}

// With writes `WITH` ident.
func (p *Printer) With() {
	p.Ident("WITH")
}

// Select writes `SELECT` ident.
func (p *Printer) Select() {
	p.Ident("SELECT")
}

// Distinct writes `DISTINCT` ident.
func (p *Printer) Distinct() {
	p.Ident("DISTINCT")
}

// Final writes `FINAL` ident.
func (p *Printer) Final() {
	p.Ident("FINAL")
}

// From writes `FROM` ident.
func (p *Printer) From() {
	p.Ident("FROM")
}

// Prewhere writes `PREWHERE` ident.
func (p *Printer) Prewhere() {
	p.Ident("PREWHERE")
}

// Where writes `WHERE` ident.
func (p *Printer) Where() {
	p.Ident("WHERE")
}

// Group writes `GROUP` ident.
func (p *Printer) Group() {
	p.Ident("GROUP")
}

// Having writes `HAVING` ident.
func (p *Printer) Having() {
	p.Ident("HAVING")
}

// Order writes `ORDER` ident.
func (p *Printer) Order() {
	p.Ident("ORDER")
}

// By writes `BY` ident.
func (p *Printer) By() {
	p.Ident("BY")
}

// Asc writes `ASC` ident.
func (p *Printer) Asc() {
	p.Ident("ASC")
}

// Desc writes `DESC` ident.
func (p *Printer) Desc() {
	p.Ident("DESC")
}

// Limit writes `LIMIT` ident.
func (p *Printer) Limit() {
	p.Ident("LIMIT")
}
