package clusterquery

import (
	"bytes"
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigprofile "github.com/oteldb/storage/signal/profile"
	sigtrace "github.com/oteldb/storage/signal/trace"
)

// MetricSeries implements [storagebackend.Source].
func (s *Source) MetricSeries(
	ctx context.Context, t signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	return s.series(ctx, signal.Metric, t, matchers, start, end)
}

// LogSeries implements [storagebackend.Source].
func (s *Source) LogSeries(
	ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	return s.series(ctx, signal.Log, tenant, matchers, start, end)
}

// TraceSeries implements [storagebackend.Source].
func (s *Source) TraceSeries(
	ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	return s.series(ctx, signal.Trace, tenant, matchers, start, end)
}

// ProfileSeries implements [storagebackend.Source].
func (s *Source) ProfileSeries(
	ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	return s.series(ctx, signal.Profile, tenant, matchers, start, end)
}

// series enumerates a tenant's stream identities across every shard, concatenating the results: a
// stream lives in exactly one shard, so the per-shard sets are disjoint and no dedup is needed.
func (s *Source) series(
	ctx context.Context, sig signal.Signal, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	var all []signal.Series

	for _, sk := range s.shardKeys(tenant) {
		got, err := s.rt.Series(ctx, sig, sk, matchers, start, end)
		if err != nil {
			return nil, errors.Wrapf(err, "list %s series of shard %q", sig, sk)
		}

		all = append(all, got...)
	}

	return all, nil
}

// LogKeys implements [storagebackend.Source].
func (s *Source) LogKeys(
	ctx context.Context, tenant signal.TenantID, start, end int64,
) ([]storage.KeyInfo, error) {
	return s.keys(ctx, signal.Log, tenant, start, end)
}

// TraceKeys implements [storagebackend.Source].
func (s *Source) TraceKeys(
	ctx context.Context, tenant signal.TenantID, start, end int64,
) ([]storage.KeyInfo, error) {
	return s.keys(ctx, signal.Trace, tenant, start, end)
}

// keys enumerates a tenant's distinct attribute keys across every shard. A key can appear on streams
// in more than one shard, so the shards' answers are unioned with their scope bits OR-ed per key.
func (s *Source) keys(
	ctx context.Context, sig signal.Signal, tenant signal.TenantID, start, end int64,
) ([]storage.KeyInfo, error) {
	scopes := map[string]uint8{}

	for _, sk := range s.shardKeys(tenant) {
		got, err := s.rt.Keys(ctx, sig, sk, start, end)
		if err != nil {
			return nil, errors.Wrapf(err, "list %s keys of shard %q", sig, sk)
		}

		for _, k := range got {
			scopes[string(k.Key)] |= k.Scope
		}
	}

	keys := make([]string, 0, len(scopes))
	for k := range scopes {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	out := make([]storage.KeyInfo, len(keys))
	for i, k := range keys {
		out[i] = storage.KeyInfo{Key: []byte(k), Scope: storage.KeyScope(scopes[k])}
	}

	return out, nil
}

// ColumnValues implements [storagebackend.Source]. It enumerates one column's (or one attribute
// key's) distinct values across every shard and unions them: a value can occur in more than one
// shard, so unlike [Source.series] the shards' answers overlap and are deduplicated.
//
// The per-shard Limit is deliberately the caller's, not a share of it. A shard cannot know which of
// its values survive the union, so splitting the budget would return fewer than Limit values while
// more existed. The union is truncated once, after sorting, which is where the limit means what the
// caller asked for.
func (s *Source) ColumnValues(
	ctx context.Context, tenant signal.TenantID, req storage.ValuesRequest,
) ([][]byte, error) {
	seen := map[string]struct{}{}

	for _, sk := range s.shardKeys(tenant) {
		got, err := s.rt.Values(ctx, cluster.ValuesRequest{
			Signal:  req.Signal,
			Column:  req.Column,
			AttrKey: req.AttrKey,
			Start:   req.Start,
			End:     req.End,
			Limit:   req.Limit,
		}, sk)
		if err != nil {
			return nil, errors.Wrapf(err, "list %s values of shard %q", req.Signal, sk)
		}

		for _, v := range got {
			seen[string(v)] = struct{}{}
		}
	}

	values := make([]string, 0, len(seen))
	for v := range seen {
		values = append(values, v)
	}

	slices.Sort(values)

	if req.Limit > 0 && len(values) > req.Limit {
		values = values[:req.Limit]
	}

	out := make([][]byte, len(values))
	for i, v := range values {
		out[i] = []byte(v)
	}

	return out, nil
}

// Trace implements [storagebackend.Source]: an equality condition on the trace id column, which the
// owners prune by its per-part bloom, over the whole time range (a trace id carries no window).
func (s *Source) Trace(
	ctx context.Context, tenant signal.TenantID, traceID []byte,
) ([]*fetch.Batch, error) {
	want := bytes.Clone(traceID)
	cond := fetch.Condition{
		Column: sigtrace.ColTraceID,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: sigtrace.ColTraceID, Value: string(want)},
	}

	it, err := s.TraceFetcher(tenant).Fetch(ctx, fetch.Request{
		Signal: signal.Trace, Start: 0, End: 1<<63 - 1,
		Conditions: []fetch.Condition{cond}, AllConditions: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch trace")
	}

	return fetch.Drain(ctx, it)
}

// ProfileResolver implements [storagebackend.Source]. A stack's symbols live in whichever shard
// ingested it, so every shard's symbol tables are collected and unioned — content addressing makes
// that a plain dedup, so a flamegraph over samples from several shards resolves every stack id.
func (s *Source) ProfileResolver(ctx context.Context, tenant signal.TenantID) (*sigprofile.Resolver, error) {
	keys := s.shardKeys(tenant)
	parts := make([]map[string][]byte, 0, len(keys))

	for _, sk := range keys {
		tables, err := s.rt.Side(ctx, signal.Profile, sk)
		if err != nil {
			return nil, errors.Wrapf(err, "load profile symbols of shard %q", sk)
		}

		if len(tables) > 0 {
			parts = append(parts, tables)
		}
	}

	tables, err := sigprofile.NewSymbolStore().Union(parts)
	if err != nil {
		return nil, errors.Wrap(err, "union profile symbols")
	}

	return sigprofile.NewResolver(tables)
}
