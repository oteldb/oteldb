package traceql

import (
	"regexp/syntax"
	"strings"

	"github.com/oteldb/oteldb/internal/traceql/lexer"
)

// Autocomplete is a AND set of spanset matchers.
type Autocomplete struct {
	Matchers []SpanMatcher
}

// String implements [fmt.Stringer] for [Autocomplete].
func (c Autocomplete) String() string {
	if len(c.Matchers) == 0 {
		return "{}"
	}

	var sb strings.Builder
	sb.WriteString("{ ")
	for i, m := range c.Matchers {
		if i != 0 {
			sb.WriteString(" && ")
		}
		// FIXME(tdakkota): suboptimal
		sb.WriteString(m.String())
	}
	sb.WriteString(" }")

	return sb.String()
}

// ParseAutocomplete parses matchers from potentially uncomplete TraceQL spanset filter from string.
func ParseAutocomplete(input string) Autocomplete {
	var e predExtractor
	e.WalkAutocomplete(parseAutocomplete(input))
	if e.Op != SpansetOpAnd {
		return Autocomplete{}
	}

	var (
		matchers = e.Matchers
		n        = 0
	)
	for _, m := range matchers {
		if m.Op.IsRegex() && !isValidRegex(m.Static) {
			// Skip invalid regex matchers.
			continue
		}
		matchers[n] = m
		n++
	}
	matchers = matchers[:n]
	if len(matchers) == 0 {
		matchers = nil
	}

	return Autocomplete{
		Matchers: matchers,
	}
}

func isValidRegex(re Static) bool {
	if re.Type != TypeString {
		return false
	}
	_, err := syntax.Parse(re.AsString(), syntax.Perl)
	return err == nil
}

type autocompleteExpr struct {
	Matchers []BinaryFieldExpr
}

// parseAutocomplete parses matchers from potentially uncomplete TraceQL spanset filter from string.
func parseAutocomplete(input string) (c autocompleteExpr) {
	p, err := newParser(input)
	if err != nil {
		return c
	}

	if err := p.consume(lexer.OpenBrace); err != nil {
		return c
	}

	for {
		left, ok, err := parseSimpleFieldExpr(&p)
		if err != nil || !ok {
			return c
		}

		op, ok := p.peekBinaryOp()
		if !ok || !(op.IsOrdering() || op.IsRegex()) {
			return c
		}
		// Consume op.
		p.next()

		right, ok, err := parseSimpleFieldExpr(&p)
		switch {
		case err != nil:
			return c
		case !ok:
			// Handle cases like `{ .foo = <missing field> && .bar = 10 }`.
			op, ok = p.peekBinaryOp()
			if !ok {
				return c
			}
			if op != OpAnd {
				return autocompleteExpr{}
			}
		default:
			c.Matchers = append(c.Matchers, BinaryFieldExpr{
				Left:  left,
				Op:    op,
				Right: right,
			})
		}

		switch t := p.peek(); t.Type {
		case lexer.EOF:
			return c
		case lexer.CloseBrace:
			p.next()
			return c
		default:
			op, ok := p.peekBinaryOp()
			if !ok {
				return c
			}
			if op != OpAnd {
				return autocompleteExpr{}
			}
			// Consume op.
			p.next()
		}
	}
}

func parseSimpleFieldExpr(p *parser) (FieldExpr, bool, error) {
	switch s, ok, err := p.tryStatic(); {
	case err != nil:
		return nil, false, err
	case ok:
		return s, true, nil
	}

	if a, ok := p.tryAttribute(); ok {
		return &a, true, nil
	}
	return nil, false, nil
}
