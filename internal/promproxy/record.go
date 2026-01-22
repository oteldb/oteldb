package promproxy

import (
	"encoding/json"
	"io"
	"strconv"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/promapi"
)

// Recorder of prometheus queries.
type Recorder struct {
	encoder *json.Encoder
}

// NewRecorder returns new Recorder.
func NewRecorder(writer io.Writer) *Recorder {
	e := json.NewEncoder(writer)
	return &Recorder{encoder: e}
}

func (r *Recorder) encode(v any) error {
	if r == nil {
		return nil
	}
	var q Query
	switch t := v.(type) {
	case RangeQuery:
		q.SetRangeQuery(t)
	case InstantQuery:
		q.SetInstantQuery(t)
	case SeriesQuery:
		q.SetSeriesQuery(t)
	default:
		return errors.Errorf("unknown type %T", t)
	}
	if err := r.encoder.Encode(q); err != nil {
		return errors.Wrap(err, "encode")
	}
	return nil
}

func ts(v promapi.PrometheusTimestamp) time.Time {
	seconds, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(seconds, 0)
}

func step(o promapi.OptString) OptInt {
	v, ok := o.Get()
	if !ok {
		return OptInt{}
	}
	seconds, err := strconv.Atoi(v)
	if err != nil {
		return OptInt{}
	}
	return NewOptInt(seconds)
}

func optTS(v promapi.OptPrometheusTimestamp) OptDateTime {
	var dt OptDateTime
	if t, ok := v.Get(); ok {
		dt.SetTo(ts(t))
	}
	return dt
}

func (r *Recorder) RecordSeriesQuery(v promapi.GetSeriesParams) error {
	return r.encode(SeriesQuery{
		Start:    optTS(v.Start),
		End:      optTS(v.End),
		Matchers: v.Match,
	})
}

func (r *Recorder) RecordGetQuery(v promapi.GetQueryParams) error {
	return r.encode(InstantQuery{
		Query: v.Query,
		Time:  optTS(v.Time),
	})
}

// RecordGetQueryRange records range query.
func (r *Recorder) RecordGetQueryRange(v promapi.GetQueryRangeParams) error {
	return r.encode(RangeQuery{
		Query: v.Query,
		Start: NewOptDateTime(ts(v.Start)),
		End:   NewOptDateTime(ts(v.End)),
		Step:  step(v.Step),
	})
}
