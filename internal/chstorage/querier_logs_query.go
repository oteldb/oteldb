package chstorage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/xattribute"
)

// LogsQuery defines a logs query.
type LogsQuery[E any] struct {
	Start, End time.Time
	Sel        LogsSelector
	Direction  logqlengine.Direction
	Limit      int

	Mapper func(logstorage.Record) (E, error)
}

// Execute executes the query using given querier.
func (v *LogsQuery[E]) Execute(ctx context.Context, q *Querier) (_ iterators.Iterator[E], rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.LogsQuery.Eval",
		trace.WithAttributes(
			xattribute.UnixNano("logql.range.start", v.Start),
			xattribute.UnixNano("logql.range.end", v.End),
			attribute.Stringer("logql.direction", v.Direction),
			attribute.Int("logql.limit", v.Limit),
			xattribute.StringerSlice("logql.label_matchers", v.Sel.Labels),
			xattribute.StringerSlice("logql.line_matchers", v.Sel.Line),
			xattribute.StringerSlice("logql.label_predicates", v.Sel.PipelineLabels),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	mapping, err := q.getLabelMapping(ctx, v.Sel.mappingLabels())
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	var (
		out   = newLogColumns()
		query = chsql.Select(table, out.ChsqlResult()...).
			Where(
				chsql.InTimeRange("timestamp", v.Start, v.End, out.timestamp.Precision),
			)
	)
	v.Sel.addPredicates(ctx, query, mapping, q)

	switch d := v.Direction; d {
	case logqlengine.DirectionBackward:
		query.Order(chsql.Ident("timestamp"), chsql.Desc)
	case logqlengine.DirectionForward:
		query.Order(chsql.Ident("timestamp"), chsql.Asc)
	default:
		return nil, errors.Errorf("unexpected direction %q", d)
	}
	query.Limit(v.Limit)

	var data []E
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			return out.ForEach(func(r logstorage.Record) error {
				e, err := v.Mapper(r)
				if err != nil {
					return err
				}
				data = append(data, e)
				return nil
			})
		},

		Type:   "QueryLogs",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(data), nil
}

// SampleQuery defines a sample query.
type SampleQuery struct {
	Start, End     time.Time
	Sel            LogsSelector
	Sampling       SamplingOp
	GroupingLabels []logql.Label
}

// sampleQueryColumns defines result columns of [SampleQuery].
type sampleQueryColumns struct {
	Timestamp proto.ColDateTime64
	Sample    proto.ColFloat64
	Labels    *proto.ColMap[string, string]
}

func (c *sampleQueryColumns) Result() proto.Results {
	return proto.Results{
		{Name: "timestamp", Data: &c.Timestamp},
		{Name: "sample", Data: &c.Sample},
		{Name: "labels", Data: c.Labels},
	}
}

// Execute executes the query using given querier.
func (v *SampleQuery) Execute(ctx context.Context, q *Querier) (_ logqlengine.SampleIterator, rerr error) {
	table := q.tables.Logs

	ctx, span := q.tracer.Start(ctx, "chstorage.logs.SampleQuery.Eval",
		trace.WithAttributes(
			xattribute.UnixNano("logql.range.start", v.Start),
			xattribute.UnixNano("logql.range.end", v.End),
			attribute.String("logql.sampling", v.Sampling.String()),
			xattribute.StringerSlice("logql.grouping_labels", v.GroupingLabels),
			xattribute.StringerSlice("logql.label_matchers", v.Sel.Labels),
			xattribute.StringerSlice("logql.line_matchers", v.Sel.Line),
			xattribute.StringerSlice("logql.label_predicates", v.Sel.PipelineLabels),

			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	// Gather all labels for mapping fetch.
	labels := v.Sel.mappingLabels()
	for _, l := range v.GroupingLabels {
		labels = append(labels, string(l))
	}
	mapping, err := q.getLabelMapping(ctx, labels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	sampleExpr, err := getSampleExpr(v.Sampling)
	if err != nil {
		return nil, err
	}

	entries := make([]chsql.Expr, 0, len(v.GroupingLabels)*2)
	for _, key := range v.GroupingLabels {
		label := string(key)
		if key, ok := mapping[label]; ok {
			label = key
		}

		labelExpr, ok := q.getMaterializedLabelColumn(label)
		if !ok {
			labelExpr = firstAttrSelector(label)
		}

		entries = append(entries,
			chsql.String(string(key)),
			// Ensure `LowCardinality` column type.
			chsql.Cast(labelExpr, "LowCardinality(String)"),
		)
	}

	var (
		columns = sampleQueryColumns{
			Timestamp: proto.ColDateTime64{},
			Sample:    proto.ColFloat64{},
			Labels: proto.NewMap(
				new(proto.ColStr),
				new(proto.ColStr).LowCardinality(),
			),
		}

		query = chsql.Select(table,
			chsql.Column("timestamp", &columns.Timestamp),
			chsql.ResultColumn{
				Name: "sample",
				Expr: chsql.ToFloat64(sampleExpr),
				Data: &columns.Sample,
			},
			chsql.ResultColumn{
				Name: "labels",
				Expr: chsql.Map(entries...),
				Data: columns.Labels,
			},
		).Where(
			chsql.InTimeRange("timestamp", v.Start, v.End, columns.Timestamp.Precision),
		)
	)
	v.Sel.addPredicates(ctx, query, mapping, q)
	query.Order(chsql.Ident("timestamp"), chsql.Asc)

	var result []logqlmetric.SampledEntry
	if err := q.do(ctx, selectQuery{
		Query: query,
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < columns.Timestamp.Rows(); i++ {
				timestamp := columns.Timestamp.Row(i)
				sample := columns.Sample.Row(i)
				// FIXME(tdakkota): allocates unnecessary map.
				labels := columns.Labels.Row(i)

				result = append(result, logqlmetric.SampledEntry{
					Timestamp: pcommon.NewTimestampFromTime(timestamp),
					Sample:    sample,
					Set:       logqlabels.AggregatedLabelsFromMap(labels),
				})
			}
			return nil
		},

		Type:   "QuerySamples",
		Signal: "logs",
		Table:  table,
	}); err != nil {
		return nil, err
	}

	return iterators.Slice(result), nil
}

// SamplingOp defines a sampler operation.
type SamplingOp int

const (
	// CountSampling counts lines.
	CountSampling SamplingOp = iota + 1
	// BytesSampling counts line lengths in bytes.
	BytesSampling
)

// String implments [fmt.Stringer].
func (s SamplingOp) String() string {
	switch s {
	case CountSampling:
		return "count"
	case BytesSampling:
		return "bytes"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

func getSampleExpr(op SamplingOp) (chsql.Expr, error) {
	switch op {
	case CountSampling:
		return chsql.Integer(1), nil
	case BytesSampling:
		return chsql.Length(chsql.Ident("body")), nil
	default:
		return chsql.Expr{}, errors.Errorf("unexpected sampling op: %v", op)
	}
}

// LogsSelector defines common parameters for logs selection.
type LogsSelector struct {
	Labels         []logql.LabelMatcher
	Line           []logql.LineFilter
	PipelineLabels []logql.LabelPredicate
}

func (s LogsSelector) mappingLabels() []string {
	labels := make([]string, 0, len(s.Labels)+len(s.PipelineLabels))
	for _, m := range s.Labels {
		labels = append(labels, string(m.Label))
	}
	for _, p := range s.PipelineLabels {
		labels = collectPredicateLabels(labels, p)
	}
	return labels
}

func (s LogsSelector) addPredicates(
	ctx context.Context,
	query *chsql.SelectQuery,
	mapping map[string]string,
	q *Querier,
) {
	lg := nopLogger
	if logqlengine.IsExplainQuery(ctx) {
		lg = zctx.From(ctx)
	}

	for _, m := range s.Labels {
		query.Where(q.logQLLabelMatcher(m, mapping))
	}

	var c tokenCollector
	c.tokenLimit = 20
	for _, m := range s.Line {
		query.Where(q.lineFilter(m, &c))
	}
	{
		column := chsql.Ident("body")
		for tok := range c.tokens {
			if ce := lg.Check(zap.DebugLevel, "Adding hasToken"); ce != nil {
				ce.Write(zap.String("token", tok))
			}
			query.Where(chsql.HasToken(column, tok))
		}
	}

	for _, m := range s.PipelineLabels {
		query.Where(q.logQLLabelPredicate(m, mapping))
	}
}

// tokenCollector collects and deduplicates tokens in line filters.
type tokenCollector struct {
	tokens     map[string]struct{}
	tokenLimit int
}

func (c *tokenCollector) Add(value string) {
	haveLimit := c.tokenLimit > 0

	switch {
	case c.tokens == nil:
		hint := max(c.tokenLimit, 0)
		c.tokens = make(map[string]struct{}, hint)
	case haveLimit && len(c.tokens) >= c.tokenLimit:
		return
	}

	chsql.CollectTokens(value, func(tok string) bool {
		c.tokens[tok] = struct{}{}
		if haveLimit && len(c.tokens) >= c.tokenLimit {
			return false
		}
		return true
	})
}

func (q *Querier) lineFilter(m logql.LineFilter, c *tokenCollector) (e chsql.Expr) {
	defer func() {
		switch m.Op {
		case logql.OpNotEq, logql.OpNotRe:
			e = chsql.Not(e)
		}
	}()

	matcher := func(op logql.BinOp, by logql.LineFilterValue) chsql.Expr {
		switch op {
		case logql.OpEq, logql.OpNotEq:
			if val := by.Value; len(m.Or) == 0 && op != logql.OpNotEq {
				c.Add(val)
			}
			return chsql.Contains("body", by.Value)
		case logql.OpRe, logql.OpNotRe:
			return chsql.Match(chsql.Ident("body"), chsql.String(by.Value))
		default:
			panic(fmt.Sprintf("unexpected line matcher op %v", m.Op))
		}
	}

	if len(m.Or) == 0 {
		return matcher(m.Op, m.By)
	}
	matchers := make([]chsql.Expr, 0, len(m.Or)+1)
	matchers = append(matchers, matcher(m.Op, m.By))
	for _, by := range m.Or {
		matchers = append(matchers, matcher(m.Op, by))
	}
	return chsql.JoinOr(matchers...)
}

func (q *Querier) logQLLabelPredicate(
	p logql.LabelPredicate,
	mapping map[string]string,
) (e chsql.Expr) {
	p = logql.UnparenLabelPredicate(p)

	switch p := p.(type) {
	case *logql.LabelPredicateBinOp:
		left := q.logQLLabelPredicate(p.Left, mapping)
		right := q.logQLLabelPredicate(p.Right, mapping)

		switch p.Op {
		case logql.OpAnd:
			return chsql.And(left, right)
		case logql.OpOr:
			return chsql.Or(left, right)
		default:
			panic(fmt.Sprintf("unexpected label predicate binary op: %v", p.Op))
		}
	case *logql.LabelMatcher:
		return q.logQLLabelMatcher(*p, mapping)
	default:
		panic(fmt.Sprintf("unexpected label predicate %T", p))
	}
}

func (q *Querier) logQLLabelMatcher(
	m logql.LabelMatcher,
	mapping map[string]string,
) (e chsql.Expr) {
	defer func() {
		switch m.Op {
		case logql.OpNotEq, logql.OpNotRe:
			e = chsql.Not(e)
		}
	}()

	matchHex := func(column chsql.Expr, m logql.LabelMatcher) (e chsql.Expr) {
		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			return chsql.Eq(
				column,
				chsql.Unhex(chsql.String(m.Value)),
			)
		case logql.OpRe, logql.OpNotRe:
			// FIXME(tdakkota): match is case-sensitive
			return chsql.Match(
				chsql.Hex(column),
				chsql.String(m.Value),
			)
		default:
			panic(fmt.Sprintf("unexpected label matcher op %v", m.Op))
		}
	}

	labelName := string(m.Label)
	unmappedLabel := labelName
	if key, ok := mapping[labelName]; ok {
		labelName = key
	}

	switch labelName {
	case logstorage.LabelSeverity, logstorage.LabelDetectedLevel:
		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			// Direct comparison with severity number.
			var severityNumber uint8
			for i := plog.SeverityNumberUnspecified; i <= plog.SeverityNumberFatal4; i++ {
				if strings.EqualFold(i.String(), m.Value) {
					severityNumber = uint8(i)
					break
				}
			}
			return chsql.ColumnEq("severity_number", severityNumber)
		case logql.OpRe, logql.OpNotRe:
			matches := make([]int, 0, 6)
			for i := plog.SeverityNumberUnspecified; i <= plog.SeverityNumberFatal4; i++ {
				for _, s := range []string{
					i.String(),
					strings.ToLower(i.String()),
					strings.ToUpper(i.String()),
				} {
					if m.Re.MatchString(s) {
						matches = append(matches, int(i))
						break
					}
				}
			}
			return chsql.In(chsql.Ident("severity_number"), chsql.TupleValues(matches...))
		default:
			panic(fmt.Sprintf("unexpected label matcher op %v", m.Op))
		}
	case logstorage.LabelBody:
		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			return chsql.Contains("body", m.Value)
		case logql.OpRe, logql.OpNotRe:
			return chsql.Match(chsql.Ident("body"), chsql.String(m.Value))
		default:
			panic(fmt.Sprintf("unexpected label matcher op %v", m.Op))
		}
	case logstorage.LabelSpanID:
		return matchHex(chsql.Ident("span_id"), m)
	case logstorage.LabelTraceID:
		return matchHex(chsql.Ident("trace_id"), m)
	default:
		if expr, ok := q.getMaterializedLabelColumn(unmappedLabel); ok {
			switch m.Op {
			case logql.OpEq, logql.OpNotEq:
				return chsql.Eq(expr, chsql.String(m.Value))
			case logql.OpRe, logql.OpNotRe:
				return chsql.Match(expr, chsql.String(m.Value))
			default:
				panic(fmt.Sprintf("unexpected label matcher op %v", m.Op))
			}
		}

		// TODO: how to match integers, booleans, floats, arrays?
		var (
			selector = firstAttrSelector(labelName)
			sub      chsql.Expr
		)
		switch m.Op {
		case logql.OpEq, logql.OpNotEq:
			sub = chsql.Eq(selector, chsql.String(m.Value))
		case logql.OpRe, logql.OpNotRe:
			sub = chsql.Match(selector, chsql.String(m.Value))
		default:
			panic(fmt.Sprintf("unexpected label matcher op %v", m.Op))
		}

		keysExprs := make([]chsql.Expr, 0, 3)
		// Search in all attributes.
		for _, column := range []string{
			colAttrs,
			colScope,
			colResource,
		} {
			keysExprs = append(keysExprs, chsql.JSONExtractKeys(chsql.Ident(column)))
		}

		// Force Clickhouse to use index.
		return chsql.And(
			chsql.Has(
				chsql.ArrayConcat(keysExprs...),
				chsql.String(labelName),
			),
			sub,
		)
	}
}

func collectPredicateLabels(to []string, p logql.LabelPredicate) []string {
	p = logql.UnparenLabelPredicate(p)

	var label logql.Label
	switch p := p.(type) {
	case *logql.LabelPredicateBinOp:
		to = collectPredicateLabels(to, p.Left)
		to = collectPredicateLabels(to, p.Right)
		return to
	case *logql.LabelMatcher:
		label = p.Label
	case *logql.DurationFilter:
		label = p.Label
	case *logql.BytesFilter:
		label = p.Label
	case *logql.NumberFilter:
		label = p.Label
	case *logql.IPFilter:
		label = p.Label
	default:
		panic(fmt.Sprintf("unexpected label predicate %T", p))
	}
	to = append(to, string(label))
	return to
}
