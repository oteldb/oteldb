package chsql

import (
	"context"
	"strconv"
	"strings"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// SelectQuery is a SELECT query builder.
type SelectQuery struct {
	table    string
	sub      *SelectQuery
	distinct bool
	final    bool
	with     []WithColumn
	columns  []ResultColumn

	// prewhere is a set of expression joined by AND
	prewhere []Expr
	// where is a set of expression joined by AND
	where   []Expr
	groupBy []Expr
	having  []Expr
	order   []orderExpr

	limit int
}

var _ Query = (*SelectQuery)(nil)

type orderExpr struct {
	expr  Expr
	order Order
}

// Select creates a new [SelectQuery].
func Select(table string, columns ...ResultColumn) *SelectQuery {
	return &SelectQuery{
		table:   table,
		columns: columns,
	}
}

// SelectFrom creates a new [SelectQuery] from subquery.
func SelectFrom(sub *SelectQuery, columns ...ResultColumn) *SelectQuery {
	return &SelectQuery{
		sub:     sub,
		columns: columns,
	}
}

// With adds common table expressions to a query.
func (q *SelectQuery) With(name string, expr Expr) *SelectQuery {
	q.with = append(q.with, WithColumn{Name: name, Expr: expr})
	return q
}

// Distinct sets if query is `DISTINCT`.
func (q *SelectQuery) Distinct(b bool) *SelectQuery {
	q.distinct = b
	return q
}

// Final sets if query is `FINAL`.
func (q *SelectQuery) Final(b bool) *SelectQuery {
	q.final = b
	return q
}

// Prewhere adds filters to query.
func (q *SelectQuery) Prewhere(filters ...Expr) *SelectQuery {
	q.prewhere = append(q.prewhere, filters...)
	return q
}

// Where adds filters to query.
func (q *SelectQuery) Where(filters ...Expr) *SelectQuery {
	q.where = append(q.where, filters...)
	return q
}

// GroupBy adds grouping to query.
func (q *SelectQuery) GroupBy(groups ...Expr) *SelectQuery {
	q.groupBy = append(q.groupBy, groups...)
	return q
}

// Having adds grouped filters to query.
func (q *SelectQuery) Having(filters ...Expr) *SelectQuery {
	q.having = append(q.having, filters...)
	return q
}

// Order adds order to query.
func (q *SelectQuery) Order(e Expr, order Order) *SelectQuery {
	q.order = append(q.order, orderExpr{expr: e, order: order})
	return q
}

// Limit sets query limit.
//
// If n is equal to or less than zero, limit is ignored.
func (q *SelectQuery) Limit(n int) *SelectQuery {
	q.limit = n
	return q
}

// OnResult defines [ch.Query.OnResult] callback type.
type OnResult = func(ctx context.Context, block proto.Block) error

// Prepare builds SQL query and passes columns to [ch.Query].
func (q *SelectQuery) Prepare(onResult OnResult) (ch.Query, error) {
	p := GetPrinter()
	defer PutPrinter(p)

	if err := q.WriteSQL(p); err != nil {
		return ch.Query{}, err
	}

	return ch.Query{
		Body:     p.String(),
		Result:   q.Results(),
		OnResult: onResult,
	}, nil
}

// Results returns list of result columns.
func (q *SelectQuery) Results() (r proto.Results) {
	r = make(proto.Results, len(q.columns))
	for i, c := range q.columns {
		r[i] = proto.ResultColumn{
			Name: c.Name,
			Data: c.Data,
		}
	}
	return r
}

// WriteSQL writes SQL query.
func (q *SelectQuery) WriteSQL(p *Printer) error {
	if len(q.with) > 0 {
		p.With()
		for i, c := range q.with {
			if i != 0 {
				p.Comma()
			}

			cexpr := c.Expr
			if cexpr.IsZero() {
				// If expression is not defined, assume that column
				// name is expected.
				cexpr = Ident(c.Name)
			}
			cexpr = aliasColumn(c.Name, cexpr)

			if err := p.WriteExpr(cexpr); err != nil {
				return errors.Wrapf(err, "with %q", c.Name)
			}
		}
	}
	p.Select()
	if q.distinct {
		p.Distinct()
	}

	if len(q.columns) == 0 {
		return errors.New("no columns")
	}
	for i, c := range q.columns {
		if i != 0 {
			p.Comma()
		}

		cexpr := c.Expr
		if cexpr.IsZero() {
			// If expression is not defined, assume that column
			// name is expected.
			cexpr = Ident(c.Name)
		}
		cexpr = aliasColumn(c.Name, cexpr)

		if err := p.WriteExpr(cexpr); err != nil {
			return errors.Wrapf(err, "column %q", c.Name)
		}
	}
	p.From()
	switch {
	case q.table != "":
		p.Ident(q.table)
		if q.final {
			p.Final()
		}
	case q.sub != nil:
		p.OpenParen()
		if err := q.sub.WriteSQL(p); err != nil {
			return err
		}
		p.CloseParen()
	default:
		return errors.New("either table or sub-query must be present")
	}
	if len(q.prewhere) > 0 {
		p.Prewhere()
		for i, filter := range q.prewhere {
			if i != 0 {
				p.And()
			}
			if err := p.WriteExpr(filter); err != nil {
				return errors.Wrapf(err, "prewhere %d", i)
			}
		}
	}
	if len(q.where) > 0 {
		p.Where()
		for i, filter := range q.where {
			if i != 0 {
				p.And()
			}
			if err := p.WriteExpr(filter); err != nil {
				return errors.Wrapf(err, "where %d", i)
			}
		}
	}
	if len(q.groupBy) > 0 {
		p.Group()
		p.By()

		for i, e := range q.groupBy {
			if i != 0 {
				p.Comma()
			}
			if err := p.WriteExpr(e); err != nil {
				return errors.Wrapf(err, "group by %d", i)
			}
		}
	}
	if len(q.having) > 0 {
		p.Having()

		for i, e := range q.having {
			if i != 0 {
				p.And()
			}
			if err := p.WriteExpr(e); err != nil {
				return errors.Wrapf(err, "having %d", i)
			}
		}
	}
	if len(q.order) > 0 {
		p.Order()
		p.By()

		for i, e := range q.order {
			if i != 0 {
				p.Comma()
			}
			if err := p.WriteExpr(e.expr); err != nil {
				return errors.Wrapf(err, "order by %d", i)
			}
			switch e.order {
			case Asc:
				p.Asc()
			case Desc:
				p.Desc()
			default:
				return errors.Errorf("unexpected order %v", e.order)
			}
		}
	}

	if q.limit > 0 {
		p.Limit()
		p.Literal(strconv.Itoa(q.limit))
	}
	return nil
}

func aliasColumn(name string, cexpr Expr) Expr {
	if cexpr.typ == exprBinaryOp && strings.EqualFold(cexpr.tok, "AS") {
		// If expression already aliased, rename the alias.
		if len(cexpr.args) < 2 {
			// Return invalid expression as-is.
			return cexpr
		}
		cexpr = cexpr.args[0]
	}
	if cexpr.typ == exprIdent && cexpr.tok == name {
		// Do not alias expression if it is an identifier (column name)
		// with same name.
		return cexpr
	}
	return binaryOp(cexpr, "AS", Ident(name))
}

// ResultColumn defines a column result.
type ResultColumn struct {
	Name string
	Expr Expr
	Data proto.ColResult
}

// Column returns new [ResultColumn].
func Column(name string, data proto.ColResult) ResultColumn {
	return ResultColumn{
		Name: name,
		Expr: Ident(name),
		Data: data,
	}
}

// WithColumn defines a Common table expression.
type WithColumn struct {
	Name string
	Expr Expr
}
