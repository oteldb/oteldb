package storagebackup_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/storagebackup"
)

// baseTenant is the logical tenant the shard keys below split.
const baseTenant signal.TenantID = "default"

// shardRouter emulates a cluster's placement inside a single-node engine: it keys each stream by
// the shard its identity hashes to, exactly as [cluster.ShardKeyOf] does on the write path. A real
// cluster needs etcd, which a hermetic test cannot have; what it would do to the tenant key is
// this.
func shardRouter(n int) func(signal.Resource, signal.Scope) signal.TenantID {
	return func(r signal.Resource, sc signal.Scope) signal.TenantID {
		id := signal.Series{Resource: r, Scope: sc}.Hash()
		return cluster.ShardKeyOf(baseTenant, cluster.ShardOf(id, n), n)
	}
}

// writeSpread writes one log record per service, so the streams hash across several shards instead
// of piling onto one.
func writeSpread(tb testing.TB, store *storage.Storage) {
	tb.Helper()

	services := []string{"api", "worker", "cron", "gateway", "billing", "search", "auth", "cache"}
	for i, svc := range services {
		var logs siglog.Logs
		rl := logs.AddResource()
		rl.Resource = signal.Resource{Attributes: attrs(str("service.name", svc))}
		sl := rl.AddScope()
		sl.Scope = signal.Scope{Name: []byte("scope")}

		r := sl.AddRecord()
		r.Timestamp = at(4, i)
		r.SeverityNumber = 9
		r.Body = []byte("body-" + svc)
		r.Attributes = attrs(str("service", svc))

		acc, err := store.WriteLogs(tb.Context(), logs)
		require.NoError(tb, err)
		require.Zero(tb, acc.Rejected)
	}
}

// occupiedShards reports which of a tenant's n shard keys hold log data.
func occupiedShards(tb testing.TB, store *storage.Storage, n int) []signal.TenantID {
	tb.Helper()

	var out []signal.TenantID
	for _, key := range cluster.ShardKeys(baseTenant, n) {
		if len(collectLogs(tb, store, key)) > 0 {
			out = append(out, key)
		}
	}
	return out
}

// TestReshard is the property that justifies the design: a backup taken from a two-shard cluster
// restores into a three-shard one and stays readable, because restore goes through the write path
// and the destination re-derives every shard key.
func TestReshard(t *testing.T) {
	t.Parallel()

	const (
		srcShards = 2
		dstShards = 3
	)

	lg := zaptest.NewLogger(t)
	src := newStore(t, shardRouter(srcShards))
	writeSpread(t, src)

	srcKeys := occupiedShards(t, src, srcShards)
	require.Len(t, srcKeys, srcShards, "the sample must actually span the source's shards")

	dir := t.TempDir()
	stats, err := storagebackup.NewBackup(src, lg, backupOptions()).Create(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files, "both shards fold into one logical tenant's day file")

	// The backup records the logical tenant, not the source's shard keys. Recording the keys would
	// pin the data to a cluster with exactly srcShards shards, which is the whole problem.
	tenants, err := os.ReadDir(filepath.Join(dir, "log"))
	require.NoError(t, err)
	require.Len(t, tenants, 1)
	require.Equal(t, string(baseTenant), tenants[0].Name())

	dst := newStore(t, shardRouter(dstShards))
	_, err = storagebackup.NewRestore(storagebackend.New(dst), lg, storagebackup.RestoreOptions{}).
		Restore(t.Context(), dir)
	require.NoError(t, err)

	dstKeys := occupiedShards(t, dst, dstShards)
	require.Len(t, dstKeys, dstShards, "the restored data must spread over the destination's shards")
	require.Contains(t, dstKeys, cluster.ShardKeyOf(baseTenant, 2, dstShards),
		"a shard key that does not exist in the source's shape must now hold data")

	require.Equal(t,
		collectLogs(t, src, cluster.ShardKeys(baseTenant, srcShards)...),
		collectLogs(t, dst, cluster.ShardKeys(baseTenant, dstShards)...),
		"every record survives the re-keying",
	)
}

// TestReshardToSingleShard covers the other direction: collapsing a sharded tenant back onto one
// key, which is what an operator does when shards_per_tenant is lowered.
func TestReshardToSingleShard(t *testing.T) {
	t.Parallel()

	lg := zaptest.NewLogger(t)
	src := newStore(t, shardRouter(4))
	writeSpread(t, src)

	dir := t.TempDir()
	_, err := storagebackup.NewBackup(src, lg, backupOptions()).Create(t.Context(), dir)
	require.NoError(t, err)

	dst := newStore(t, nil)
	_, err = storagebackup.NewRestore(storagebackend.New(dst), lg, storagebackup.RestoreOptions{}).
		Restore(t.Context(), dir)
	require.NoError(t, err)

	require.Equal(t,
		collectLogs(t, src, cluster.ShardKeys(baseTenant, 4)...),
		collectLogs(t, dst, baseTenant),
	)
}
