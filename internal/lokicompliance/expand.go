package lokicompliance

import (
	"slices"
	"strconv"
	"strings"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/go-faster/errors"
	"golang.org/x/exp/maps"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

var (
	testVariantArgs = map[string][]string{
		"range":  {"1s", "15s", "1m", "5m", "15m", "1h"},
		"offset": {"1s", "5s", "15s"},
		"topN":   {"1", "2", "5", "10"},
		"quantile": {
			"0.1",
			"0.5",
			"0.75",
			"0.95",
			"0.90",
			"0.99",
			"1",
			"1.5",
		},

		"simpleRangeAggOp": {
			"count_over_time",
			"rate",
			"bytes_over_time",
			"bytes_rate",
		},
		"unwrapRangeAggOp": {
			"rate_counter",
			"avg_over_time",
			"sum_over_time",
			"min_over_time",
			"max_over_time",
			"stdvar_over_time",
			"stddev_over_time",
			"first_over_time",
			"last_over_time",
		},
		"groupRangeAggOp": {
			"avg_over_time",
			"stddev_over_time",
			"stdvar_over_time",
			"max_over_time",
			"min_over_time",
			"first_over_time",
			"last_over_time",
		},
		"unwrapExpr": {
			"unwrap status",
			"unwrap duration(took)",
			"unwrap bytes(size)",
		},
		"unwrapLabels": {
			strings.Join([]string{"status", "took", "size"}, ","),
		},
		"simpleVecAggOp":    {"sum", "avg", "max", "min", "count", "stddev", "stdvar"},
		"sortVecAggOp":      {"sort", "sort_desc"},
		"topBottomVecAggOp": {"topk", "bottomk"},

		"lineFilterOp": {"|=", "!=", "~=", "!~"},
		"arithBinOp":   {"+", "-", "*", "/", "%", "^"},
		"cmpBinOp":     {"==", "!=", ">", ">=", "<", "<="},
		"setBinOp":     {"and", "or", "unless"},
	}

	templateFuncMap = template.FuncMap{
		"quote": strconv.Quote,
	}
)

// ExpandQuery expands given test case.
func ExpandQuery(cfg *Config, start, end time.Time, step time.Duration) (r []*TestCase, _ error) {
	var (
		params    = cfg.QueryParameters
		limit     = 10000
		direction = lokiapi.DirectionForward
	)
	if l := params.Limit; l != nil {
		limit = *l
	}
	if d := params.Direction; d != "" {
		direction = d
	}

	for _, tc := range cfg.TestCases {
		templ, err := template.New("query").
			Funcs(templateFuncMap).
			Parse(tc.Query)
		if err != nil {
			return nil, errors.Wrapf(err, "parse query template %q", tc.Query)
		}

		// Collect variant args from template.
		args := collectVariantArgs(templ)
		if err := getQueries(
			templ,
			args,
			make(map[string]string, len(args)),
			func(query string) {
				r = append(r, &TestCase{
					Query:          query,
					SkipComparison: tc.SkipComparison,
					ShouldFail:     tc.ShouldFail,
					ShouldBeEmpty:  tc.ShouldBeEmpty,
					Start:          start,
					End:            end,
					Step:           step,
					Limit:          limit,
					Direction:      direction,
				})
			},
		); err != nil {
			return nil, errors.Wrapf(err, "expand query %q", tc.Query)
		}
	}

	return r, nil
}

func getQueries(
	t *template.Template,
	variantArgs []string,
	args map[string]string,
	add func(string),
) error {
	if len(variantArgs) == 0 {
		q, err := execTemplateToString(t, args)
		if err != nil {
			return err
		}
		add(q)
		return nil
	}

	arg := variantArgs[0]
	values, ok := testVariantArgs[arg]
	if !ok {
		return errors.Errorf("unknown arg %q", arg)
	}
	variantArgs = variantArgs[1:]

	for _, val := range values {
		args[arg] = val
		if err := getQueries(t, variantArgs, args, add); err != nil {
			return err
		}
	}
	return nil
}

func execTemplateToString(t *template.Template, data any) (string, error) {
	var sb strings.Builder
	if err := t.Execute(&sb, data); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func collectVariantArgs(t *template.Template) []string {
	n := t.Root
	if n == nil {
		return nil
	}

	unique := map[string]struct{}{}
	walkTemplate(t.Root, func(n parse.Node) {
		field, ok := n.(*parse.FieldNode)
		if !ok || len(field.Ident) != 1 {
			return
		}
		arg := field.Ident[0]

		if _, ok := testVariantArgs[arg]; !ok {
			return
		}
		unique[arg] = struct{}{}
	})

	if len(unique) == 0 {
		return nil
	}

	args := maps.Keys(unique)
	slices.Sort(args)
	return args
}

func walkTemplate(n parse.Node, cb func(parse.Node)) {
	cb(n)
	switch n := n.(type) {
	case *parse.ListNode:
		for _, sub := range n.Nodes {
			walkTemplate(sub, cb)
		}
	case *parse.PipeNode:
		for _, sub := range n.Decl {
			walkTemplate(sub, cb)
		}
		for _, sub := range n.Cmds {
			walkTemplate(sub, cb)
		}
	case *parse.ActionNode:
		if sub := n.Pipe; sub != nil {
			walkTemplate(sub, cb)
		}
	case *parse.CommandNode:
		for _, sub := range n.Args {
			walkTemplate(sub, cb)
		}
	case *parse.ChainNode:
		if sub := n.Node; sub != nil {
			walkTemplate(sub, cb)
		}
	case *parse.BranchNode:
		if sub := n.Pipe; sub != nil {
			walkTemplate(sub, cb)
		}
		if sub := n.List; sub != nil {
			walkTemplate(sub, cb)
		}
		if sub := n.ElseList; sub != nil {
			walkTemplate(sub, cb)
		}
	case *parse.IfNode:
		walkTemplate(&n.BranchNode, cb)
	case *parse.RangeNode:
		walkTemplate(&n.BranchNode, cb)
	case *parse.WithNode:
		walkTemplate(&n.BranchNode, cb)
	case *parse.TemplateNode:
		if sub := n.Pipe; sub != nil {
			walkTemplate(sub, cb)
		}
	}
}
