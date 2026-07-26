package scarecrow

import (
	"math"
	"regexp"
	"strings"

	"github.com/go-faster/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// buildInstantCall plans the instant (non-range-vector) functions. ok reports whether the call
// was one of them, so the caller can fall through to the range-vector path.
func (p *planner) buildInstantCall(e *parser.Call) (Operator, bool, error) {
	name := e.Func.Name

	if fn, isUnary := unaryFuncs[name]; isUnary {
		input, err := p.buildArg(e, 0)
		if err != nil {
			return nil, true, err
		}

		return newUnaryFn(input, name, fn), true, nil
	}

	switch name {
	case "pi":
		return newNumberLiteral(math.Pi, p.ec), true, nil

	case "clamp", "clamp_min", "clamp_max":
		return p.buildClamp(e)

	case "round":
		return p.buildRound(e)

	case "timestamp":
		// timestamp() over a selector reports each chosen sample's own timestamp, which only the
		// selector knows. Over anything else the value is produced at step time, so the step
		// timestamp is the answer.
		if vs, ok := unwrapVectorSelector(e.Args[0]); ok {
			return newTimestampSelect(
				p.scanner, vs.LabelMatchers, vs.OriginalOffset, vs.Timestamp, p.ec,
			), true, nil
		}

		input, err := p.buildArg(e, 0)
		if err != nil {
			return nil, true, err
		}

		return newTimestampFn(input, p.ec), true, nil

	case "vector":
		input, err := p.buildArg(e, 0)
		if err != nil {
			return nil, true, err
		}

		return newVectorFn(input), true, nil

	case "scalar":
		input, err := p.buildArg(e, 0)
		if err != nil {
			return nil, true, err
		}

		return newScalarFn(input, p.ec), true, nil

	case "label_replace":
		return p.buildLabelReplace(e)

	case "label_join":
		return p.buildLabelJoin(e)
	}

	return nil, false, nil
}

// buildArg plans argument i of a call.
func (p *planner) buildArg(e *parser.Call, i int) (Operator, error) {
	if i >= len(e.Args) {
		return nil, errors.Errorf("%s: missing argument %d", e.Func.Name, i)
	}

	return p.build(e.Args[i])
}

// stringArg reads a string-literal argument, which the parser has already type-checked.
func stringArg(e *parser.Call, i int) (string, error) {
	if i >= len(e.Args) {
		return "", errors.Errorf("%s: missing argument %d", e.Func.Name, i)
	}

	lit, ok := e.Args[i].(*parser.StringLiteral)
	if !ok {
		return "", unsupportedf("%s: non-literal string argument", e.Func.Name)
	}

	return lit.Val, nil
}

func (p *planner) buildClamp(e *parser.Call) (Operator, bool, error) {
	input, err := p.buildArg(e, 0)
	if err != nil {
		return nil, true, err
	}

	var minOp, maxOp Operator

	switch e.Func.Name {
	case "clamp":
		if minOp, err = p.buildArg(e, 1); err != nil {
			return nil, true, err
		}

		if maxOp, err = p.buildArg(e, 2); err != nil {
			return nil, true, err
		}

	case "clamp_min":
		if minOp, err = p.buildArg(e, 1); err != nil {
			return nil, true, err
		}

	case "clamp_max":
		if maxOp, err = p.buildArg(e, 1); err != nil {
			return nil, true, err
		}
	}

	return newClampFn(input, e.Func.Name, minOp, maxOp, p.ec), true, nil
}

func (p *planner) buildRound(e *parser.Call) (Operator, bool, error) {
	input, err := p.buildArg(e, 0)
	if err != nil {
		return nil, true, err
	}

	var toArg Operator
	if len(e.Args) > 1 {
		if toArg, err = p.buildArg(e, 1); err != nil {
			return nil, true, err
		}
	}

	return newRoundFn(input, toArg, p.ec), true, nil
}

func (p *planner) buildLabelReplace(e *parser.Call) (Operator, bool, error) {
	input, err := p.buildArg(e, 0)
	if err != nil {
		return nil, true, err
	}

	dst, err := stringArg(e, 1)
	if err != nil {
		return nil, true, err
	}

	repl, err := stringArg(e, 2)
	if err != nil {
		return nil, true, err
	}

	src, err := stringArg(e, 3)
	if err != nil {
		return nil, true, err
	}

	pattern, err := stringArg(e, 4)
	if err != nil {
		return nil, true, err
	}

	re, err := regexp.Compile("^(?:" + pattern + ")$")
	if err != nil {
		return nil, true, errors.Errorf("invalid regular expression in label_replace(): %s", pattern)
	}

	if !model.UTF8Validation.IsValidLabelName(dst) {
		return nil, true, errors.Errorf("invalid destination label name in label_replace(): %s", dst)
	}

	rewrite := func(ls labels.Labels) (labels.Labels, error) {
		src := ls.Get(src)

		indexes := re.FindStringSubmatchIndex(src)
		if indexes == nil {
			// A non-matching series is returned unchanged.
			return ls, nil
		}

		res := re.ExpandString([]byte{}, repl, src, indexes)

		lb := labels.NewBuilder(ls)
		if len(res) == 0 {
			lb.Del(dst)
		} else {
			lb.Set(dst, string(res))
		}

		return lb.Labels(), nil
	}

	return newLabelFn(input, "label_replace", rewrite), true, nil
}

func (p *planner) buildLabelJoin(e *parser.Call) (Operator, bool, error) {
	input, err := p.buildArg(e, 0)
	if err != nil {
		return nil, true, err
	}

	dst, err := stringArg(e, 1)
	if err != nil {
		return nil, true, err
	}

	sep, err := stringArg(e, 2)
	if err != nil {
		return nil, true, err
	}

	srcs := make([]string, 0, max(0, len(e.Args)-3))

	for i := 3; i < len(e.Args); i++ {
		s, err := stringArg(e, i)
		if err != nil {
			return nil, true, err
		}

		if !model.UTF8Validation.IsValidLabelName(s) {
			return nil, true, errors.Errorf("invalid source label name in label_join(): %s", s)
		}

		srcs = append(srcs, s)
	}

	if !model.UTF8Validation.IsValidLabelName(dst) {
		return nil, true, errors.Errorf("invalid destination label name in label_join(): %s", dst)
	}

	rewrite := func(ls labels.Labels) (labels.Labels, error) {
		parts := make([]string, len(srcs))
		for i, s := range srcs {
			parts[i] = ls.Get(s)
		}

		joined := strings.Join(parts, sep)

		lb := labels.NewBuilder(ls)
		if joined == "" {
			lb.Del(dst)
		} else {
			lb.Set(dst, joined)
		}

		return lb.Labels(), nil
	}

	return newLabelFn(input, "label_join", rewrite), true, nil
}

// unwrapVectorSelector reports whether expr is a plain vector selector, possibly wrapped by
// parens or step-invariance.
func unwrapVectorSelector(expr parser.Expr) (*parser.VectorSelector, bool) {
	for {
		switch e := expr.(type) {
		case *parser.ParenExpr:
			expr = e.Expr
		case *parser.StepInvariantExpr:
			expr = e.Expr
		case *parser.VectorSelector:
			return e, true
		default:
			return nil, false
		}
	}
}
