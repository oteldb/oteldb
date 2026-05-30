package logqlbench

import (
	"context"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"

	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/lokihandler"
)

// Query is a benchmarked query.
type Query interface {
	Type() QueryType
	Header() QueryHeader
	Query() string
	Matchers() []string
	Execute(ctx context.Context, client *lokiapi.Client, p *LogQLBenchmark) error
}

// QueryType is a type of query.
type QueryType string

const (
	InstantQueryType     QueryType = "instant"
	RangeQueryType       QueryType = "range"
	SeriesQueryType      QueryType = "series"
	LabelsQueryType      QueryType = "labels"
	LabelValuesQueryType QueryType = "label_values"
)

var _ = []Query{
	&InstantQuery{},
	&RangeQuery{},
	&SeriesQuery{},
	&LabelsQuery{},
	&LabelValuesQuery{},
}

// QueryHeader is common for all queries.
type QueryHeader struct {
	ID int `yaml:"-"`

	Title       string `yaml:"title,omitempty"`
	Description string `yaml:"description,omitempty"`
}

// InstantQuery is an instant (`/loki/api/v1/query`) query.
type InstantQuery struct {
	QueryHeader `yaml:"header,inline"`

	Start string `yaml:"start,omitempty"`
	LogQL string `yaml:"query,omitempty"`
}

// Type returns the query type.
func (q *InstantQuery) Type() QueryType {
	return InstantQueryType
}

// Header returns the query header.
func (q *InstantQuery) Header() QueryHeader {
	return q.QueryHeader
}

// Query returns the query string.
func (q *InstantQuery) Query() string {
	return q.LogQL
}

// Matchers returns selectors for the query.
func (q *InstantQuery) Matchers() []string {
	return nil
}

// Execute executes the instant query.
func (q *InstantQuery) Execute(ctx context.Context, client *lokiapi.Client, p *LogQLBenchmark) error {
	start, err := lokihandler.ParseTimestamp(q.Start, p.start)
	if err != nil {
		return errors.Wrap(err, "parse start")
	}

	resp, err := client.Query(ctx, lokiapi.QueryParams{
		Time:  toLokiTimestamp(start),
		Query: q.LogQL,
		Limit: lokiapi.NewOptInt(p.limit),
	})
	if err != nil {
		return errors.Wrap(err, "query")
	}

	if isEmptyQueryResponse(resp.Data) && !p.AllowEmpty {
		return errors.New("unexpected empty data")
	}
	return nil
}

// RangeQuery is a range (`/loki/api/v1/query_range`) query.
type RangeQuery struct {
	QueryHeader `yaml:"header,inline"`

	Start string        `yaml:"start,omitempty"`
	End   string        `yaml:"end,omitempty"`
	Step  time.Duration `yaml:"step,omitempty"`
	LogQL string        `yaml:"query,omitempty"`
}

// Type returns the query type.
func (q *RangeQuery) Type() QueryType {
	return RangeQueryType
}

// Header returns the query header.
func (q *RangeQuery) Header() QueryHeader {
	return q.QueryHeader
}

// Query returns the query string.
func (q *RangeQuery) Query() string {
	return q.LogQL
}

// Matchers returns selectors for the query.
func (q *RangeQuery) Matchers() []string {
	return nil
}

// Execute executes the range query.
func (q *RangeQuery) Execute(ctx context.Context, client *lokiapi.Client, p *LogQLBenchmark) error {
	start, err := lokihandler.ParseTimestamp(q.Start, p.start)
	if err != nil {
		return errors.Wrap(err, "parse start")
	}
	end, err := lokihandler.ParseTimestamp(q.End, p.end)
	if err != nil {
		return errors.Wrap(err, "parse end")
	}

	resp, err := client.QueryRange(ctx, lokiapi.QueryRangeParams{
		Start: toLokiTimestamp(start),
		End:   toLokiTimestamp(end),
		Query: q.LogQL,
		Step:  toLokiDuration(q.Step),
		Limit: lokiapi.NewOptInt(p.limit),
	})
	if err != nil {
		return errors.Wrap(err, "query range")
	}

	if isEmptyQueryResponse(resp.Data) && !p.AllowEmpty {
		return errors.New("unexpected empty data")
	}
	return nil
}

func isEmptyQueryResponse(data lokiapi.QueryResponseData) bool {
	switch typ := data.Type; typ {
	case lokiapi.StreamsResultQueryResponseData:
		streams := data.StreamsResult
		return len(streams.Result) == 0
	case lokiapi.ScalarResultQueryResponseData:
		return false
	case lokiapi.VectorResultQueryResponseData:
		vector := data.VectorResult
		return len(vector.Result) == 0
	case lokiapi.MatrixResultQueryResponseData:
		matrix := data.MatrixResult
		return len(matrix.Result) == 0
	default:
		return true
	}
}

// SeriesQuery is a series (`/loki/api/v1/series`) query.
type SeriesQuery struct {
	QueryHeader `yaml:"header,inline"`

	Start string   `yaml:"start,omitempty"`
	End   string   `yaml:"end,omitempty"`
	Match []string `yaml:"match,omitempty"`
}

// Type returns the query type.
func (q *SeriesQuery) Type() QueryType {
	return SeriesQueryType
}

// Header returns the query header.
func (q *SeriesQuery) Header() QueryHeader {
	return q.QueryHeader
}

// Query returns the query string.
func (q *SeriesQuery) Query() string {
	return ""
}

// Matchers returns selectors for the query.
func (q *SeriesQuery) Matchers() []string {
	return q.Match
}

// Execute executes the series query.
func (q *SeriesQuery) Execute(ctx context.Context, client *lokiapi.Client, p *LogQLBenchmark) error {
	start, err := lokihandler.ParseTimestamp(q.Start, p.start)
	if err != nil {
		return errors.Wrap(err, "parse start")
	}
	end, err := lokihandler.ParseTimestamp(q.End, p.end)
	if err != nil {
		return errors.Wrap(err, "parse end")
	}

	resp, err := client.Series(ctx, lokiapi.SeriesParams{
		Start: toLokiTimestamp(start),
		End:   toLokiTimestamp(end),
		Match: q.Match,
	})
	if err != nil {
		return errors.Wrap(err, "query series")
	}

	if len(resp.Data) == 0 && !p.AllowEmpty {
		return errors.New("unexpected empty data")
	}
	return nil
}

// LabelsQuery is a labels (`/loki/api/v1/labels`) query.
type LabelsQuery struct {
	QueryHeader `yaml:"header,inline"`

	Start string `yaml:"start,omitempty"`
	End   string `yaml:"end,omitempty"`
}

// Type returns the query type.
func (q *LabelsQuery) Type() QueryType {
	return LabelsQueryType
}

// Header returns the query header.
func (q *LabelsQuery) Header() QueryHeader {
	return q.QueryHeader
}

// Query returns the query string.
func (q *LabelsQuery) Query() string {
	return ""
}

// Matchers returns selectors for the query.
func (q *LabelsQuery) Matchers() []string {
	return nil
}

// Execute executes the labels query.
func (q *LabelsQuery) Execute(ctx context.Context, client *lokiapi.Client, p *LogQLBenchmark) error {
	start, err := lokihandler.ParseTimestamp(q.Start, p.start)
	if err != nil {
		return errors.Wrap(err, "parse start")
	}
	end, err := lokihandler.ParseTimestamp(q.End, p.end)
	if err != nil {
		return errors.Wrap(err, "parse end")
	}

	resp, err := client.Labels(ctx, lokiapi.LabelsParams{
		Start: toLokiTimestamp(start),
		End:   toLokiTimestamp(end),
	})
	if err != nil {
		return errors.Wrap(err, "query labels")
	}

	if len(resp.Data) == 0 && !p.AllowEmpty {
		return errors.New("unexpected empty data")
	}
	return nil
}

// LabelValuesQuery is a label values (`/loki/api/v1/label_values`) query.
type LabelValuesQuery struct {
	QueryHeader `yaml:"header,inline"`

	Name  string `yaml:"name"`
	Start string `yaml:"start,omitempty"`
	End   string `yaml:"end,omitempty"`
	Match string `yaml:"match,omitempty"`
}

// Type returns the query type.
func (q *LabelValuesQuery) Type() QueryType {
	return LabelValuesQueryType
}

// Header returns the query header.
func (q *LabelValuesQuery) Header() QueryHeader {
	return q.QueryHeader
}

// Query returns the query string.
func (q *LabelValuesQuery) Query() string {
	return q.Name
}

// Matchers returns selectors for the query.
func (q *LabelValuesQuery) Matchers() []string {
	return []string{q.Match}
}

// Execute executes the label values query.
func (q *LabelValuesQuery) Execute(ctx context.Context, client *lokiapi.Client, p *LogQLBenchmark) error {
	start, err := lokihandler.ParseTimestamp(q.Start, p.start)
	if err != nil {
		return errors.Wrap(err, "parse start")
	}
	end, err := lokihandler.ParseTimestamp(q.End, p.end)
	if err != nil {
		return errors.Wrap(err, "parse end")
	}
	var matcher lokiapi.OptString
	if q.Match != "" {
		matcher.SetTo(q.Match)
	}

	resp, err := client.LabelValues(ctx, lokiapi.LabelValuesParams{
		Name:  q.Name,
		Start: toLokiTimestamp(start),
		End:   toLokiTimestamp(end),
		Query: matcher,
	})
	if err != nil {
		return errors.Wrap(err, "query label values")
	}

	if len(resp.Data) == 0 && !p.AllowEmpty {
		return errors.New("unexpected empty data")
	}
	return nil
}

// Input defines queries config.
type Input struct {
	Instant     []InstantQuery     `yaml:"instant"`
	Range       []RangeQuery       `yaml:"range"`
	Series      []SeriesQuery      `yaml:"series"`
	Labels      []LabelsQuery      `yaml:"labels"`
	LabelValues []LabelValuesQuery `yaml:"label_values"`
}

func (p *LogQLBenchmark) each(ctx context.Context, fn func(ctx context.Context, q Query) error) error {
	data, err := os.ReadFile(p.Input)
	if err != nil {
		return errors.Wrap(err, "read input")
	}

	var input Input
	if err := yaml.Unmarshal(data, &input); err != nil {
		return errors.Wrap(err, "unmarshal input")
	}

	var (
		id     int
		nextID = func() (r int) {
			r = id
			id++
			return r
		}
	)

	for i := range input.Instant {
		q := &input.Instant[i]
		q.ID = nextID()

		if err := fn(ctx, q); err != nil {
			return errors.Wrapf(err, "instant query %d: %q", i, q.Query())
		}
	}

	for i := range input.Range {
		q := &input.Range[i]
		q.ID = nextID()

		if err := fn(ctx, q); err != nil {
			return errors.Wrapf(err, "range query %d: %q", i, q.Query())
		}
	}

	for i := range input.Series {
		q := &input.Series[i]
		q.ID = nextID()

		if err := fn(ctx, q); err != nil {
			return errors.Wrapf(err, "series query %d: %#v", i, q.Matchers())
		}
	}

	for i := range input.Labels {
		q := &input.Labels[i]
		q.ID = nextID()

		if err := fn(ctx, q); err != nil {
			return errors.Wrapf(err, "labels query %d", i)
		}
	}

	for i := range input.LabelValues {
		q := &input.LabelValues[i]
		q.ID = nextID()

		if err := fn(ctx, q); err != nil {
			return errors.Wrapf(err, "label values query %d: %q", i, q.Query())
		}
	}

	return nil
}
