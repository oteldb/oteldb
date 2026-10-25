package clusterquery_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/clusterquery"
	"github.com/oteldb/oteldb/internal/etcdtest"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestLokiRoutedAbsentAndTypedMatchers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	local := storagebackend.New(s)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ld := plog.NewLogs()
	for i, value := range []any{nil, "", "prod", "dev", []byte{1, 2}, []any{"prod"}, map[string]any{"k": "v"}} {
		rl := ld.ResourceLogs().AppendEmpty()
		if value != nil {
			require.NoError(t, rl.Resource().Attributes().PutEmpty("env").FromRaw(value))
		}
		r := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Second).UnixNano()))
		r.Body().SetStr(string(rune('a' + i)))
	}
	require.NoError(t, local.ConsumeLogs(ctx, ld))
	mux := http.NewServeMux()
	mux.Handle(cluster.ReadPath, cluster.NewReadHandler(func(ctx context.Context, r fetch.Request) ([]*fetch.Batch, error) {
		it, err := s.LogFetcher(r.Tenant).Fetch(ctx, r)
		if err != nil {
			return nil, err
		}
		return fetch.Drain(ctx, it)
	}))
	mux.Handle(cluster.KeysPath, cluster.KeysHandler(func(ctx context.Context, _ signal.Signal, tenant string, lo, hi int64) ([]cluster.KeyInfo, error) {
		keys, err := s.LogKeys(ctx, signal.TenantID(tenant), lo, hi)
		if err != nil {
			return nil, err
		}
		out := make([]cluster.KeyInfo, len(keys))
		for i, key := range keys {
			out[i] = cluster.KeyInfo{Key: key.Key, Scope: uint8(key.Scope)}
		}
		return out, nil
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	endpoint := etcdtest.Start(t)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	member, err := etcd.Join(ctx, client, clusterRoot, etcd.Member{ID: "logs", Addr: strings.TrimPrefix(srv.URL, "http://")}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = member.Close(context.Background()) })
	routed := storagebackend.NewQuery(clusterquery.New(openRouter(t, endpoint, 1, 1), 0))
	for _, tt := range []struct {
		name, value string
		op          logql.BinOp
		want        []string
	}{
		{"empty", "", logql.OpEq, []string{"a", "b"}},
		{"negative", "dev", logql.OpNotEq, []string{"a", "b", "c", "e", "f", "g"}},
		{"negative regexp", "prod|dev", logql.OpNotRe, []string{"a", "b", "e", "f", "g"}},
		{"bytes", "AQI=", logql.OpEq, []string{"e"}},
		{"slice", `["prod"]`, logql.OpEq, []string{"f"}},
		{"map", `{"k":"v"}`, logql.OpEq, []string{"g"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := logql.LabelMatcher{Label: "env", Op: tt.op, Value: tt.value}
			if m.Op == logql.OpNotRe {
				m.Re = regexp.MustCompile("^(?:" + m.Value + ")$")
			}
			for _, b := range []*storagebackend.Backend{local, routed} {
				n, err := b.Logs().Query(ctx, []logql.LabelMatcher{m})
				require.NoError(t, err)
				params := logqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(time.Hour), Limit: -1}
				it, err := n.EvalPipeline(ctx, params)
				require.NoError(t, err)
				var entry logqlengine.Entry
				var lines []string
				for it.Next(&entry) {
					lines = append(lines, entry.Line)
				}
				require.NoError(t, it.Err())
				assert.ElementsMatch(t, tt.want, lines)
				params.Limit, params.Direction = 1, logqlengine.DirectionBackward
				it, err = n.EvalPipeline(ctx, params)
				require.NoError(t, err)
				require.True(t, it.Next(&entry))
				assert.Equal(t, tt.want[len(tt.want)-1], entry.Line)
				assert.False(t, it.Next(&entry))
				require.NoError(t, it.Err())
			}
		})
	}
}
