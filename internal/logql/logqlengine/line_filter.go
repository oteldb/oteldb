package logqlengine

import (
	"fmt"
	"net/netip"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlerrors"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func buildLineFilter(stage *logql.LineFilter) (Processor, error) {
	if len(stage.Or) > 0 {
		var (
			op     = stage.Op
			negate bool
		)
		switch op {
		case logql.OpNotEq:
			op = logql.OpEq
			negate = true
		case logql.OpNotRe:
			op = logql.OpRe
			negate = true
		case logql.OpNotPattern:
			op = logql.OpPattern
			negate = true
		}

		matchers := make([]StringMatcher, 0, len(stage.Or)+1)

		matcher, err := buildLineMatcher(op, stage.By)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, matcher)

		for _, by := range stage.Or {
			m, err := buildLineMatcher(op, by)
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, m)
		}
		return &OrLineFilter{
			matchers: matchers,
			negate:   negate,
		}, nil
	}

	matcher, err := buildLineMatcher(stage.Op, stage.By)
	if err != nil {
		return nil, err
	}
	return &LineFilter{matcher: matcher}, nil
}

func buildLineMatcher(op logql.BinOp, by logql.LineFilterValue) (StringMatcher, error) {
	switch op {
	case logql.OpPattern, logql.OpNotPattern:
		return nil, &logqlerrors.UnsupportedError{Msg: fmt.Sprintf("%s line filter is unsupported", op)}
	}

	if by.IP {
		matcher, err := buildIPMatcher(by.Value)
		if err != nil {
			return nil, err
		}
		switch op {
		case logql.OpEq:
			return IPLineMatcher{matcher: matcher}, nil
		case logql.OpNotEq:
			return NotMatcher[string, IPLineMatcher]{
				Next: IPLineMatcher{matcher: matcher},
			}, nil
		default:
			return nil, errors.Errorf("unexpected operation %q", op)
		}
	}
	return buildStringMatcher(op, by.Value, by.Re, false)
}

// OrLineFilter is a line matching Processor.
type OrLineFilter struct {
	matchers []StringMatcher
	negate   bool
}

func (lf *OrLineFilter) match(line string) bool {
	// TODO(tdakkota): cache IP captures
	for _, m := range lf.matchers {
		if m.Match(line) {
			return true
		}
	}
	return false
}

// Process implements Processor.
func (lf *OrLineFilter) Process(_ otelstorage.Timestamp, line string, _ logqlabels.LabelSet) (_ string, keep bool) {
	// TODO(tdakkota): cache IP captures
	keep = lf.match(line)
	if lf.negate {
		keep = !keep
	}
	return line, keep
}

// LineFilter is a line matching Processor.
type LineFilter struct {
	matcher StringMatcher
}

// Process implements Processor.
func (lf *LineFilter) Process(_ otelstorage.Timestamp, line string, _ logqlabels.LabelSet) (_ string, keep bool) {
	keep = lf.matcher.Match(line)
	return line, keep
}

// IPLineMatcher looks for IP address in a line and applies matcher to it.
type IPLineMatcher struct {
	matcher IPMatcher
}

var _ StringMatcher = (*IPLineMatcher)(nil)

// Match implements StringMatcher.
func (lf IPLineMatcher) Match(line string) bool {
	for i := 0; i < len(line); {
		c := line[i]
		if !isHexDigit(c) && c != ':' {
			i++
			continue
		}

		if capture, ok := tryCaptureIPv4(line[i:]); ok {
			i += len(capture)

			ip, err := netip.ParseAddr(capture)
			if err == nil && lf.matcher.Match(ip) {
				return true
			}
			continue
		}
		if capture, ok := tryCaptureIPv6(line[i:]); ok {
			i += len(capture)

			ip, err := netip.ParseAddr(capture)
			if err == nil && lf.matcher.Match(ip) {
				return true
			}
			continue
		}
		i++
	}

	return false
}

func tryCaptureIPv4(s string) (string, bool) {
	if len(s) < 4 || !isDigit(s[0]) {
		return "", false
	}

	switch byte('.') {
	case s[1], s[2], s[3]:
	default:
		return "", false
	}

	for i, c := range []byte(s) {
		if !isDigit(c) && c != '.' {
			s = s[:i]
			break
		}
	}

	return s, true
}

func tryCaptureIPv6(s string) (string, bool) {
	if len(s) < 2 {
		return "", false
	}

	switch {
	case s[0] == ':' && s[1] == ':':
		// ::1
	case isHexDigit(s[0]):
		for _, c := range []byte(s[1:]) {
			switch {
			case isHexDigit(c):
				continue
			case c == ':':
				goto match
			}
		}
		return "", false
	default:
		return "", false
	}

match:
	for i, c := range []byte(s) {
		if !isHexDigit(c) && c != ':' && c != '.' {
			s = s[:i]
			break
		}
	}

	return s, true
}

func isHexDigit(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}
