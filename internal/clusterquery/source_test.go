package clusterquery_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/clusterquery"
	"github.com/oteldb/oteldb/internal/etcdtest"
)

// clusterRoot is the etcd key prefix these tests coordinate under.
const clusterRoot = "/test"

// series builds a one-label stream identity.
func series(name string) signal.Series {
	return signal.Series{
		Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("__name__"), Value: signal.StringValue([]byte(name)),
		}),
	}
}

// nameOf reads back the label [series] set.
func nameOf(s signal.Series) string {
	v, _ := s.Attributes.Get([]byte("__name__"))

	return string(v.Str())
}

// fakeNode is a storage node as far as the ring is concerned: it registers as a member and serves
// the read and enumeration RPCs from a canned per-shard-key set of streams. A shard key it was not
// given data for is disclaimed with [cluster.ErrShardAbsent], exactly as a node that the ring
// points at but which holds nothing does.
type fakeNode struct {
	addr string
	// held maps a shard key to the streams that shard holds on this node.
	held map[string][]string
	// keys maps a shard key to the record-attribute keys the node reports for it.
	keys map[string][]cluster.KeyInfo
	// traceIDs maps a shard key to the trace id of each span row the node holds for it. When set,
	// a read of that shard answers with those rows instead of the metric-shaped streams.
	traceIDs map[string][]string
}

func startNode(t *testing.T, endpoint, id string, held map[string][]string) *fakeNode {
	t.Helper()

	n := &fakeNode{addr: etcdtest.FreeAddr(t), held: held, keys: map[string][]cluster.KeyInfo{}}

	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, cluster.ReadHandler(n.fetch, n.fetch, n.fetch, n.fetch))
	mux.Handle(cluster.SeriesPath, cluster.SeriesHandler(n.series))
	mux.Handle(cluster.KeysPath, cluster.KeysHandler(n.keyList))

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", n.addr)
	require.NoError(t, err)

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	m, err := etcd.Join(t.Context(), client, clusterRoot, etcd.Member{ID: id, Addr: n.addr}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close(context.WithoutCancel(t.Context())) })

	return n
}

// streams returns the shard's streams, or reports the shard absent.
func (n *fakeNode) streams(shardKey string) ([]signal.Series, error) {
	names, ok := n.held[shardKey]
	if !ok {
		return nil, cluster.ErrShardAbsent
	}

	// Ascending by id, which is what every fetch producer promises its consumers.
	out := make([]signal.Series, 0, len(names))
	for _, name := range names {
		out = append(out, series(name))
	}

	return fetch.SortSeries(out), nil
}

// fetch answers a read RPC with one sample per held stream. Matchers are ignored on purpose: a real
// peer only applies the equality subset, so its answer is a superset that has to be narrowed above.
func (n *fakeNode) fetch(
	_ context.Context, shardKey string, start, _ int64, _ []fetch.Matcher,
) ([]*fetch.Batch, error) {
	streams, err := n.streams(shardKey)
	if err != nil {
		return nil, err
	}

	if ids, ok := n.traceIDs[shardKey]; ok {
		return []*fetch.Batch{n.spanBatch(start, ids)}, nil
	}

	out := make([]*fetch.Batch, 0, len(streams))
	for _, s := range streams {
		out = append(out, &fetch.Batch{
			ID: s.Hash(), Series: s, Timestamps: []int64{start}, Values: []float64{1},
		})
	}

	return out, nil
}

func (n *fakeNode) series(
	_ context.Context, _ signal.Signal, shardKey string, _, _ int64, _ []fetch.Matcher,
) ([]signal.Series, error) {
	return n.streams(shardKey)
}

func (n *fakeNode) keyList(
	_ context.Context, _ signal.Signal, shardKey string, _, _ int64,
) ([]cluster.KeyInfo, error) {
	if _, ok := n.held[shardKey]; !ok {
		return nil, cluster.ErrShardAbsent
	}

	return n.keys[shardKey], nil
}

// openRouter opens a router over endpoint and waits until it sees the ring.
func openRouter(t *testing.T, endpoint string, rf, shards int) *router.Router {
	t.Helper()

	rt, err := router.Open(t.Context(), router.Config{
		Etcd: []string{endpoint}, Root: clusterRoot, RF: rf, ShardsPerTenant: shards,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool { return len(rt.Members()) > 0 },
		10*time.Second, 10*time.Millisecond, "router sees the cluster")

	return rt
}

// shardKeys is every shard key the default tenant is split into.
func shardKeys(shards int) []string {
	var out []string
	for _, sk := range cluster.ShardKeys(cluster.DefaultTenant, shards) {
		out = append(out, string(sk))
	}

	return out
}

// drainNames fetches through f and returns the streams it yielded.
func drainNames(t *testing.T, f fetch.Fetcher, matchers []fetch.Matcher) []string {
	t.Helper()

	it, err := f.Fetch(t.Context(), fetch.Request{Start: 1, End: 100, Matchers: matchers})
	require.NoError(t, err)

	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)

	var out []string
	for _, b := range batches {
		out = append(out, nameOf(b.Series))
	}

	sort.Strings(out)

	return out
}

// TestFetcherGathersEveryShard is the point of the shard fan-out: a tenant split into N shards must
// be read from all N, or a query silently answers from a fraction of the data.
func TestFetcherGathersEveryShard(t *testing.T) {
	t.Parallel()

	const shards = 4

	endpoint := etcdtest.Start(t)

	keys := shardKeys(shards)
	require.Len(t, keys, shards)

	held := map[string][]string{}
	for i, sk := range keys {
		held[sk] = []string{string(rune('a' + i))}
	}

	startNode(t, endpoint, "node-a", held)

	src := clusterquery.New(openRouter(t, endpoint, 1, shards))

	assert.Equal(t, []string{"a", "b", "c", "d"}, drainNames(t, src.Fetcher(""), nil))

	got, err := src.MetricSeries(t.Context(), "", nil, 1, 100)
	require.NoError(t, err)

	var names []string
	for _, s := range got {
		names = append(names, nameOf(s))
	}

	sort.Strings(names)
	assert.Equal(t, []string{"a", "b", "c", "d"}, names)
}

// TestFetcherReappliesMatchers pins, from the consumer's side, that what a read yields is what the
// matchers select: only the equality subset of a matcher set is serializable, so a peer legitimately
// returns a superset and a non-equality matcher would otherwise not filter anything at all. The
// router narrows it, so this is a contract test on the router rather than on code in this package.
func TestFetcherReappliesMatchers(t *testing.T) {
	t.Parallel()

	endpoint := etcdtest.Start(t)
	startNode(t, endpoint, "node-a", map[string][]string{
		string(cluster.DefaultTenant): {"kept", "dropped"},
	})

	src := clusterquery.New(openRouter(t, endpoint, 1, 1))

	// No Spec, so nothing about this matcher reaches the peer.
	matchers := []fetch.Matcher{{
		Name:  []byte("__name__"),
		Match: func(v signal.Value) bool { return string(v.Str()) == "kept" },
	}}

	assert.Equal(t, []string{"kept"}, drainNames(t, src.Fetcher(""), matchers))

	got, err := src.MetricSeries(t.Context(), "", matchers, 1, 100)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "kept", nameOf(got[0]))
}

// TestSeriesFailsOverAbsentOwner pins that "I do not hold this shard" is a failover, not a result:
// accepting the first owner's empty answer would drop every stream the shard holds elsewhere.
func TestSeriesFailsOverAbsentOwner(t *testing.T) {
	t.Parallel()

	endpoint := etcdtest.Start(t)

	key := string(cluster.DefaultTenant)

	// Two owners of the one shard, only one of which has backfilled it.
	startNode(t, endpoint, "node-a", map[string][]string{})
	startNode(t, endpoint, "node-b", map[string][]string{key: {"only-here"}})

	rt := openRouter(t, endpoint, 2, 1)
	require.Eventually(t, func() bool { return len(rt.Members()) == 2 },
		10*time.Second, 10*time.Millisecond, "router sees both nodes")

	src := clusterquery.New(rt)

	got, err := src.MetricSeries(t.Context(), "", nil, 1, 100)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "only-here", nameOf(got[0]))

	assert.Equal(t, []string{"only-here"}, drainNames(t, src.Fetcher(""), nil))
}

// TestSeriesEmptyWhenEveryOwnerDisclaims pins the other half of the absence rule: once every owner
// says it does not hold the shard, empty is the real answer rather than an error.
func TestSeriesEmptyWhenEveryOwnerDisclaims(t *testing.T) {
	t.Parallel()

	endpoint := etcdtest.Start(t)
	startNode(t, endpoint, "node-a", map[string][]string{})

	src := clusterquery.New(openRouter(t, endpoint, 1, 1))

	got, err := src.MetricSeries(t.Context(), "", nil, 1, 100)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestLogKeysUnionsShards pins that key enumeration unions across shards with the scope bits OR-ed:
// a key can appear as a resource attribute on one shard's streams and a record attribute on
// another's, and either half alone misdescribes it.
func TestLogKeysUnionsShards(t *testing.T) {
	t.Parallel()

	const shards = 2

	endpoint := etcdtest.Start(t)

	keys := shardKeys(shards)

	node := startNode(t, endpoint, "node-a", map[string][]string{keys[0]: nil, keys[1]: nil})
	node.keys[keys[0]] = []cluster.KeyInfo{
		{Key: []byte("host"), Scope: uint8(1)},
		{Key: []byte("level"), Scope: uint8(4)},
	}
	node.keys[keys[1]] = []cluster.KeyInfo{{Key: []byte("host"), Scope: uint8(4)}}

	src := clusterquery.New(openRouter(t, endpoint, 1, shards))

	got, err := src.LogKeys(t.Context(), "", 1, 100)
	require.NoError(t, err)

	names := make([]string, 0, len(got))
	scopes := map[string]uint8{}

	for _, k := range got {
		names = append(names, string(k.Key))
		scopes[string(k.Key)] = uint8(k.Scope)
	}

	assert.True(t, slices.IsSorted(names), "keys are returned in a deterministic order")
	assert.Equal(t, []string{"host", "level"}, names)
	assert.Equal(t, uint8(5), scopes["host"], "the scope bits of both shards")
	assert.Equal(t, uint8(4), scopes["level"])
}

// spanBatch answers a trace read with one span row per trace id. The conditions a trace-by-id read
// carries never reach a peer, so this is deliberately every row of the shard.
func (n *fakeNode) spanBatch(start int64, ids []string) *fetch.Batch {
	s := series("spans")

	b := &fetch.Batch{
		ID:     s.Hash(),
		Series: s,
		Columns: []fetch.NamedColumn{
			{Name: sigtrace.ColTraceID},
			{Name: sigtrace.ColName},
		},
	}
	for i, id := range ids {
		b.Timestamps = append(b.Timestamps, start+int64(i))
		b.Columns[0].Bytes = append(b.Columns[0].Bytes, []byte(id))
		b.Columns[1].Bytes = append(b.Columns[1].Bytes, fmt.Appendf(nil, "span-%d", i))
	}

	return b
}

// TestTraceByIDNarrowsToOneTrace pins the read that has no matchers at all: trace-by-id is a single
// columnar condition, and conditions do not cross the wire. Narrowing a peer's answer by matchers
// alone left the condition unapplied, so every span the shard held came back as one trace — and a
// trace id that exists nowhere returned the whole window instead of nothing.
func TestTraceByIDNarrowsToOneTrace(t *testing.T) {
	t.Parallel()

	endpoint := etcdtest.Start(t)
	node := startNode(t, endpoint, "node-a", map[string][]string{
		string(cluster.DefaultTenant): {"spans"},
	})
	node.traceIDs = map[string][]string{
		string(cluster.DefaultTenant): {"aaa", "bbb", "aaa", "ccc"},
	}

	src := clusterquery.New(openRouter(t, endpoint, 1, 1))

	rows := func(id string) []string {
		t.Helper()

		batches, err := src.Trace(t.Context(), "", []byte(id))
		require.NoError(t, err)

		var out []string
		for _, b := range batches {
			col, ok := b.Column(sigtrace.ColTraceID)
			require.True(t, ok)

			for _, v := range col.Bytes {
				out = append(out, string(v))
			}
		}

		return out
	}

	assert.Equal(t, []string{"aaa", "aaa"}, rows("aaa"))
	assert.Equal(t, []string{"bbb"}, rows("bbb"))
	assert.Empty(t, rows("nope"), "a trace id nothing holds must return nothing, not the window")
}
