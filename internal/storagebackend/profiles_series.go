package storagebackend

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
	sigprofile "github.com/oteldb/storage/signal/profile"

	"github.com/oteldb/oteldb/internal/profilestorage"
)

var _ profilestorage.SeriesQuerier = (*ProfileQuerier)(nil)

// SelectSeries sums profile values without reading stack IDs or symbols.
func (q *ProfileQuerier) SelectSeries(ctx context.Context, params profilestorage.SelectSeriesParams) ([]int64, error) {
	ctx = q.b.queryContext(ctx)
	n, err := params.NumBuckets()
	if err != nil {
		return nil, err
	}
	if uint64(n) > math.MaxInt64/8 {
		return nil, readbudget.ErrExceeded
	}
	held := int64(n) * 8
	budget := readbudget.From(ctx)
	if err := budget.Reserve(held); err != nil {
		return nil, err
	}
	defer budget.Release(held)
	values := make([]int64, n)
	matchers, err := profileFetchMatchers(&params.Type, params.Matchers)
	if err != nil {
		return nil, err
	}
	lo, hi := params.Start.UnixNano(), params.End.UnixNano()
	if hi <= lo {
		return nil, errors.New("profile series time range exceeds timestamp bounds")
	}
	req := fetch.Request{
		Tenant: q.b.tenant, Signal: signal.Profile, Start: lo, End: hi - 1,
		Matchers: matchers, Projection: []string{sigprofile.ColValue}, Recycle: true,
	}
	it, err := q.b.src.ProfileFetcher(q.b.tenant).Fetch(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "fetch profile series")
	}
	defer func() { _ = it.Close() }()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "read profile series")
		}
		if batch == nil {
			break
		}
		column, hasValue := batch.Column(sigprofile.ColValue)
		for i, ts := range batch.Timestamps {
			if i&4095 == 0 {
				if err := ctx.Err(); err != nil {
					batch.Release()
					return nil, err
				}
			}
			if ts < lo || ts >= hi {
				continue
			}
			bucket := int(time.Duration(ts-lo) / params.Step)
			if bucket < 0 || bucket >= n {
				continue
			}
			value := int64(1)
			if hasValue && i < len(column.Int64) {
				value = column.Int64[i]
			}
			values[bucket] += value
		}
		batch.Release()
	}
	return values, nil
}
