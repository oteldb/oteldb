package profilestorage

import (
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/profileql"
)

// SeriesQuerier returns totals without constructing or resolving flamegraphs.
type SeriesQuerier interface {
	SelectSeries(ctx context.Context, params SelectSeriesParams) ([]int64, error)
}

// SelectSeriesParams describes half-open buckets over [Start, End).
type SelectSeriesParams struct {
	Type       profileql.ProfileType
	Matchers   []profileql.LabelMatcher
	Start, End time.Time
	Step       time.Duration
}

// NumBuckets returns the number of bucket intervals.
func (p SelectSeriesParams) NumBuckets() (int, error) {
	if p.Start.IsZero() || p.End.IsZero() || !p.End.After(p.Start) || p.Step <= 0 {
		return 0, errors.New("series requires an increasing time range and positive step")
	}
	if !time.Unix(0, p.Start.UnixNano()).Equal(p.Start) || !time.Unix(0, p.End.UnixNano()).Equal(p.End) {
		return 0, errors.New("profile series time range exceeds timestamp bounds")
	}
	span := p.End.Sub(p.Start)
	if !p.Start.Add(span).Equal(p.End) {
		return 0, errors.New("profile series time range is too large")
	}
	count := (span-1)/p.Step + 1
	if uint64(count) > uint64(^uint(0)>>1) {
		return 0, errors.New("too many series buckets")
	}
	return int(count), nil
}
