package profilehandler

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/go-faster/errors"
	querierv1 "github.com/grafana/pyroscope/api/gen/proto/go/querier/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
	sigprofile "github.com/oteldb/storage/signal/profile"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type (
	timelineSample struct{ ts, value int64 }
	timelineSource struct {
		storagebackend.Source
		samples      []timelineSample
		requests     []fetch.Request
		resolvers    int
		missingValue bool
		failure      error
		budget       *readbudget.Budget
		cancel       context.CancelFunc
	}
)

func (s *timelineSource) WithQueryBudget(ctx context.Context) context.Context {
	return readbudget.With(ctx, s.budget)
}

func (s *timelineSource) ProfileResolver(context.Context, signal.TenantID, int64, int64) (*sigprofile.Resolver, error) {
	s.resolvers++
	return sigprofile.NewResolverFrom(), nil
}
func (s *timelineSource) ProfileFetcher(...signal.TenantID) fetch.Fetcher { return s }
func (s *timelineSource) Fetch(_ context.Context, r fetch.Request) (fetch.Iterator, error) {
	s.requests = append(s.requests, r)
	if s.cancel != nil {
		s.cancel()
	}
	if s.failure != nil {
		return nil, s.failure
	}
	batch := &fetch.Batch{}
	var values []int64
	var stacks [][]byte
	for _, sample := range s.samples {
		if sample.ts >= r.Start && sample.ts <= r.End {
			batch.Timestamps = append(batch.Timestamps, sample.ts)
			values = append(values, sample.value)
			stacks = append(stacks, make([]byte, 16))
		}
	}
	if !s.missingValue {
		batch.Columns = append(batch.Columns, fetch.Int64Column(sigprofile.ColValue, values))
	}
	batch.Columns = append(batch.Columns, fetch.BytesColumn(sigprofile.ColStackID, stacks))
	return fetch.NewSliceIterator([]*fetch.Batch{batch}), nil
}

func TestSelectSeriesNativeUsesOneValueFetch(t *testing.T) {
	t.Parallel()
	source := &timelineSource{}
	start := time.UnixMilli(1000)
	for i := range 120 {
		source.samples = append(source.samples, timelineSample{start.Add(time.Duration(i) * time.Second).UnixNano(), int64(i + 1)})
	}
	source.samples = append(source.samples, timelineSample{start.Add(-time.Nanosecond).UnixNano(), 999}, timelineSample{start.Add(120 * time.Second).UnixNano(), 999})
	q := storagebackend.NewQuery(source).Profiles()
	handler := NewQuerierService(q, profileqlengine.NewEngine(q, profileqlengine.Options{}))
	result, err := handler.SelectSeries(t.Context(), connect.NewRequest(&querierv1.SelectSeriesRequest{ProfileTypeID: "process_cpu:cpu:nanoseconds:cpu:nanoseconds", Start: start.UnixMilli(), End: start.Add(120 * time.Second).UnixMilli(), Step: 1}))
	require.NoError(t, err)
	require.Len(t, result.Msg.Series, 1)
	require.Len(t, result.Msg.Series[0].Points, 120)
	for i, p := range result.Msg.Series[0].Points {
		assert.Equal(t, start.Add(time.Duration(i)*time.Second).UnixMilli(), p.Timestamp)
		assert.Equal(t, float64(i+1), p.Value)
	}
	require.Len(t, source.requests, 1)
	assert.Equal(t, []string{sigprofile.ColValue}, source.requests[0].Projection)
	assert.Equal(t, start.UnixNano(), source.requests[0].Start)
	assert.Equal(t, start.Add(120*time.Second).Add(-time.Nanosecond).UnixNano(), source.requests[0].End)
	assert.Zero(t, source.resolvers)
}

func BenchmarkProfileTimeline(b *testing.B) {
	start := time.UnixMilli(1000)
	source := &timelineSource{}
	for i := range 120 {
		source.samples = append(source.samples, timelineSample{start.Add(time.Duration(i) * time.Second).UnixNano(), 7})
	}
	q := storagebackend.NewQuery(source).Profiles()
	handler := NewQuerierService(q, profileqlengine.NewEngine(q, profileqlengine.Options{}))
	req := connect.NewRequest(&querierv1.SelectSeriesRequest{ProfileTypeID: "process_cpu:cpu:nanoseconds:cpu:nanoseconds", Start: start.UnixMilli(), End: start.Add(120 * time.Second).UnixMilli(), Step: 1})
	fetches, resolvers := 0, 0
	b.ReportAllocs()
	for b.Loop() {
		source.requests = nil
		source.resolvers = 0
		result, err := handler.SelectSeries(b.Context(), req)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Msg.Series[0].Points) != 120 {
			b.Fatal("incomplete timeline")
		}
		fetches, resolvers = len(source.requests), source.resolvers
	}
	b.ReportMetric(float64(fetches), "fetches/op")
	b.ReportMetric(float64(resolvers), "resolvers/op")
}

func TestNativeTimelineBoundariesAndDefaults(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_000_000, 0)
	typ, err := profileql.ParseProfileType("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
	require.NoError(t, err)
	params := profilestorage.SelectSeriesParams{Type: typ, Start: base, End: base.Add(2500 * time.Millisecond), Step: time.Second}
	samples := []timelineSample{{base.Add(-time.Nanosecond).UnixNano(), 999}, {base.UnixNano(), 3}, {base.Add(500 * time.Millisecond).UnixNano(), 5}, {base.Add(time.Second).UnixNano(), -2}, {base.Add(2 * time.Second).UnixNano(), 7}, {params.End.Add(-time.Nanosecond).UnixNano(), 11}, {params.End.UnixNano(), 999}}
	for _, missing := range []bool{false, true} {
		source := &timelineSource{samples: samples, missingValue: missing}
		got, err := storagebackend.NewQuery(source).Profiles().SelectSeries(t.Context(), params)
		require.NoError(t, err)
		want := []int64{8, -2, 18}
		if missing {
			want = []int64{2, 1, 2}
		}
		assert.Equal(t, want, got)
		assert.Zero(t, source.resolvers)
		require.Len(t, source.requests, 1)
	}
}

func TestNativeTimelineBudgetAndCancellation(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_000_000, 0)
	typ, err := profileql.ParseProfileType("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
	require.NoError(t, err)
	params := profilestorage.SelectSeriesParams{Type: typ, Start: base, End: base.Add(3 * time.Second), Step: time.Second}
	source := &timelineSource{budget: readbudget.New(23)}
	_, err = storagebackend.NewQuery(source).Profiles().SelectSeries(t.Context(), params)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	assert.Empty(t, source.requests)
	assert.EqualValues(t, 23, source.budget.Remaining())
	sentinel := errors.New("read failed")
	source = &timelineSource{budget: readbudget.New(32), failure: sentinel}
	_, err = storagebackend.NewQuery(source).Profiles().SelectSeries(t.Context(), params)
	require.ErrorIs(t, err, sentinel)
	assert.EqualValues(t, 32, source.budget.Remaining())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source = &timelineSource{samples: []timelineSample{{base.UnixNano(), 7}}, cancel: cancel, budget: readbudget.New(32)}
	_, err = storagebackend.NewQuery(source).Profiles().SelectSeries(ctx, params)
	require.ErrorIs(t, err, context.Canceled)
	assert.EqualValues(t, 32, source.budget.Remaining())
	source = &timelineSource{budget: readbudget.New(1024)}
	params.Step = time.Nanosecond
	params.End = base.Add(3 * time.Second)
	_, err = storagebackend.NewQuery(source).Profiles().SelectSeries(t.Context(), params)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	assert.Empty(t, source.requests)
}

func TestSelectSeriesStrictBucketLimit(t *testing.T) {
	t.Parallel()
	source := &timelineSource{}
	q := storagebackend.NewQuery(source).Profiles()
	handler := NewQuerierService(q, profileqlengine.NewEngine(q, profileqlengine.Options{}))
	result, err := handler.SelectSeries(t.Context(), connect.NewRequest(&querierv1.SelectSeriesRequest{ProfileTypeID: "process_cpu:cpu:nanoseconds:cpu:nanoseconds", Start: 1000, End: 1121, Step: .001}))
	require.NoError(t, err)
	require.Len(t, result.Msg.Series[0].Points, 120)
}
