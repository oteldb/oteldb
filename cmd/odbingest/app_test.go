package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/prometheus/prometheus/prompb"

	"github.com/oteldb/oteldb/internal/otlpdirect"
	"github.com/oteldb/oteldb/internal/promrw"
)

func freeAddr(tb testing.TB) string {
	tb.Helper()

	var lc net.ListenConfig

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	defer func() { _ = l.Close() }()

	return l.Addr().String()
}

// startEtcd runs an embedded etcd and returns its client endpoint.
func startEtcd(t *testing.T) string {
	t.Helper()

	lc := url.URL{Scheme: "http", Host: freeAddr(t)}
	lp := url.URL{Scheme: "http", Host: freeAddr(t)}

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{lc}
	cfg.AdvertiseClientUrls = []url.URL{lc}
	cfg.ListenPeerUrls = []url.URL{lp}
	cfg.AdvertisePeerUrls = []url.URL{lp}
	cfg.InitialCluster = cfg.Name + "=" + lp.String()

	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}

	return lc.String()
}

// fakeNode is a storage node as far as routing is concerned: it registers in the ring and serves
// the primary-write endpoint, recording what it was asked to apply.
type fakeNode struct {
	addr string

	mu      sync.Mutex
	shards  map[string]int // shard key → samples applied
	series  map[signal.SeriesID]signal.Series
	records int
	batches int
}

func startNode(t *testing.T, endpoint, root, id string) *fakeNode {
	t.Helper()

	n := &fakeNode{
		addr:   freeAddr(t),
		shards: map[string]int{},
		series: map[signal.SeriesID]signal.Series{},
	}

	mux := http.NewServeMux()
	mux.Handle(cluster.PrimaryWritePath, cluster.PrimaryWriteHandler(n.apply))

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", n.addr)
	require.NoError(t, err)

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	m, err := etcd.Join(t.Context(), client, root, etcd.Member{ID: id, Addr: n.addr}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close(context.WithoutCancel(t.Context())) })

	return n
}

// apply decodes the WAL frame the router sent, exactly as a real primary would before handing it
// to its engine.
func (n *fakeNode) apply(_ context.Context, _ signal.Signal, shardKey string, walBytes []byte) (cluster.Reject, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.batches++

	return cluster.Reject{}, wal.Replay(walBytes, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			n.series[id] = s

			return nil
		},
		OnSamples: func(_ signal.SeriesID, ts []int64, _ []float64) error {
			n.shards[shardKey] += len(ts)

			return nil
		},
		OnRecords: func(signal.SeriesID, []byte) error {
			n.records++

			return nil
		},
	})
}

// applied is how many records and samples the node was asked to apply, across every signal.
func (n *fakeNode) applied() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	total := n.records
	for _, c := range n.shards {
		total += c
	}

	return total
}

func (n *fakeNode) samples() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	total := 0
	for _, c := range n.shards {
		total += c
	}

	return total
}

// names returns the __name__ of every series this node was sent.
func (n *fakeNode) names() []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	var out []string
	for _, s := range n.series {
		if v, ok := s.Attributes.Get([]byte("__name__")); ok {
			out = append(out, string(v.Str()))
		}
	}

	return out
}

// writeRequest encodes a remote write request with the Prometheus types, so the test drives the
// handler with the bytes a real sender produces.
func writeRequest(tb testing.TB, tss ...prompb.TimeSeries) []byte {
	tb.Helper()

	raw, err := (&prompb.WriteRequest{Timeseries: tss}).Marshal()
	require.NoError(tb, err)

	return snappy.Encode(nil, raw)
}

func series(name string, at time.Time, v float64) prompb.TimeSeries {
	return prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: name},
			{Name: "job", Value: "api"},
		},
		Samples: []prompb.Sample{{Timestamp: at.UnixMilli(), Value: v}},
	}
}

// newTestSink builds the real sink over a router pointed at endpoint.
func newTestSink(t *testing.T, endpoint, root string, shards int) *clusterSink {
	t.Helper()

	rt, err := router.Open(t.Context(), router.Config{
		Etcd: []string{endpoint}, Root: root, RF: 1, ShardsPerTenant: shards,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close(context.WithoutCancel(t.Context())) })

	require.Eventually(t, func() bool { return len(rt.Members()) > 0 },
		10*time.Second, 10*time.Millisecond, "router sees the cluster")

	sink, err := newClusterSink(rt, nil, noop.NewMeterProvider())
	require.NoError(t, err)

	return sink
}

// TestIngestRoutesToPrimary is the end-to-end check that odbingest is a routing tier: a remote
// write request must arrive at the ring primary as a WAL frame the node can replay, with no
// storage engine anywhere in odbingest.
func TestIngestRoutesToPrimary(t *testing.T) {
	t.Parallel()

	const root = "/test"

	endpoint := startEtcd(t)
	node := startNode(t, endpoint, root, "node-a")

	h := promrw.NewHandler(newTestSink(t, endpoint, root, 1), promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: time.Hour},
	})

	at := time.Now().Truncate(time.Second)
	body := writeRequest(t,
		series("http_requests_total", at, 42),
		series("go_goroutines", at, 7),
	)

	// Twice: the second request reuses (and overwrites) every pooled buffer the first one aliased,
	// so anything the framing failed to copy would arrive corrupt.
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body)
	}

	assert.Equal(t, 4, node.samples(), "both points, both times")
	assert.ElementsMatch(t, []string{"http_requests_total", "go_goroutines"}, node.names())
}

// TestIngestSpreadsAcrossShards pins that sharding actually routes: with more shards than one, a
// tenant's series arrive under distinct shard keys rather than all pinned to the bare tenant.
func TestIngestSpreadsAcrossShards(t *testing.T) {
	t.Parallel()

	const (
		root   = "/test"
		shards = 4
	)

	endpoint := startEtcd(t)
	node := startNode(t, endpoint, root, "node-a")

	h := promrw.NewHandler(newTestSink(t, endpoint, root, shards), promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: time.Hour},
	})

	at := time.Now().Truncate(time.Second)

	var tss []prompb.TimeSeries
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"} {
		tss = append(tss, series(name, at, 1))
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(writeRequest(t, tss...)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body)

	assert.Equal(t, 12, node.samples())

	node.mu.Lock()
	defer node.mu.Unlock()

	assert.Greater(t, len(node.shards), 1, "series spread over more than one shard")

	// Every shard key must be one this tenant actually owns, and carry the shard marker.
	for key := range node.shards {
		assert.Equal(t, cluster.DefaultTenant, cluster.TenantOfShard(signal.TenantID(key)))
		assert.Contains(t, key, cluster.ShardSep)
	}
}

// TestIngestFailsWhenClusterIsUnreachable pins that a write with nowhere to go is refused rather
// than silently accepted: odbingest holds nothing, so a 202 it cannot back is data loss.
func TestIngestFailsWhenClusterIsUnreachable(t *testing.T) {
	t.Parallel()

	endpoint := startEtcd(t) // no nodes join

	rt, err := router.Open(t.Context(), router.Config{Etcd: []string{endpoint}, Root: "/test", RF: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close(context.WithoutCancel(t.Context())) })

	sink, err := newClusterSink(rt, nil, noop.NewMeterProvider())
	require.NoError(t, err)

	h := promrw.NewHandler(sink, promrw.HandlerConfig{
		Options: promrw.Options{TimeThreshold: time.Hour},
	})

	body := writeRequest(t, series("up", time.Now(), 1))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code, "the sender must retry, not move on")
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "odbingest.yml")
	require.NoError(t, os.WriteFile(path, []byte("cluster:\n  etcd: [\"127.0.0.1:2379\"]\n"), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfg.validate())

	assert.Equal(t, []string{"127.0.0.1:2379"}, cfg.Cluster.Etcd)
	assert.Equal(t, ":19291", cfg.RemoteWrite.Bind)
	assert.Equal(t, "/", cfg.RemoteWrite.Path)
	assert.Equal(t, 15*time.Second, cfg.RemoteWrite.ShutdownTimeout)
}

// TestValidateRequiresCluster pins that odbingest refuses to start without somewhere to write,
// rather than accepting traffic it can only drop.
func TestValidateRequiresCluster(t *testing.T) {
	t.Parallel()

	var cfg Config
	cfg.setDefaults()

	require.Error(t, cfg.validate())
}

// otlpPost sends a serialized OTLP export request at path.
func otlpPost(t *testing.T, h http.Handler, path string, raw []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-protobuf")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec
}

// TestOTLPRoutesEverySignalToPrimary is the end-to-end check for the OTLP pack: each signal's
// export must arrive at the ring primary as a WAL frame the node can replay, with odbingest
// holding nothing.
func TestOTLPRoutesEverySignalToPrimary(t *testing.T) {
	t.Parallel()

	const root = "/test"

	endpoint := startEtcd(t)
	node := startNode(t, endpoint, root, "node-a")

	mux := http.NewServeMux()
	otlpdirect.NewHandler(newTestSink(t, endpoint, root, 1), otlpdirect.HandlerConfig{}).Register(mux)

	at := time.Now().Truncate(time.Second)

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(at))
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.Body().SetStr("request completed")

	logsRaw, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("GET /things")
	sp.SetTraceID([16]byte{1})
	sp.SetSpanID([8]byte{2})
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(at))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Millisecond)))

	tracesRaw, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)

	md := pmetric.NewMetrics()
	mt := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	mt.SetName("http_requests_total")

	gp := mt.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetTimestamp(pcommon.NewTimestampFromTime(at))
	gp.SetDoubleValue(42)

	metricsRaw, err := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)

	for _, tt := range []struct {
		path string
		raw  []byte
	}{
		{otlpdirect.LogsPath, logsRaw},
		{otlpdirect.TracesPath, tracesRaw},
		{otlpdirect.MetricsPath, metricsRaw},
	} {
		// Twice, so the pooled buffers the batch aliased are overwritten before the assertion.
		for range 2 {
			rec := otlpPost(t, mux, tt.path, tt.raw)
			require.Equal(t, http.StatusOK, rec.Code, "%s: %s", tt.path, rec.Body)
		}
	}

	// Two of each signal: the logs and traces frames carry records, the metric frame carries a
	// sample, and all three had to reach a primary to be counted.
	assert.Equal(t, 6, node.applied(), "every signal's export reached the primary")
}
