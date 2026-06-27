package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
)

// applyOption applies a storage.Option to a fresh Options and returns it, so tests can inspect what
// clusterOption configured.
func applyOption(t *testing.T, opt storage.Option) storage.Options {
	t.Helper()
	var o storage.Options
	require.NotNil(t, opt)
	opt(&o)
	return o
}

func TestClusterOption(t *testing.T) {
	lg := zap.NewNop()

	t.Run("DisabledWhenNoEtcd", func(t *testing.T) {
		opt, err := clusterOption(nil, lg)
		require.NoError(t, err)
		require.Nil(t, opt)

		opt, err = clusterOption(&StorageClusterConfig{ID: "n1"}, lg) // no etcd endpoints
		require.NoError(t, err)
		require.Nil(t, opt)
	})

	t.Run("Explicit", func(t *testing.T) {
		opt, err := clusterOption(&StorageClusterConfig{
			Etcd: []string{"http://etcd:2379"}, ID: "node-1", Zone: "z1",
			Addr: "node-1:9000", RF: 2, ShardsPerTenant: 4, Root: "/x",
		}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.NotNil(t, o.Cluster)
		require.Equal(t, []string{"http://etcd:2379"}, o.Cluster.Etcd)
		require.Equal(t, "node-1", o.Cluster.Self.ID)
		require.Equal(t, "z1", o.Cluster.Self.Zone)
		require.Equal(t, "node-1:9000", o.Cluster.Self.Addr)
		require.Equal(t, 2, o.Cluster.RF)
		require.Equal(t, 4, o.Cluster.ShardsPerTenant)
		require.Equal(t, "/x", o.Cluster.Root)
	})

	t.Run("AddrDerivedFromID", func(t *testing.T) {
		opt, err := clusterOption(&StorageClusterConfig{Etcd: []string{"e"}, ID: "node-7"}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.Equal(t, "node-7:7946", o.Cluster.Self.Addr, "default replication port")

		opt, err = clusterOption(&StorageClusterConfig{Etcd: []string{"e"}, ID: "node-7", Port: 1234}, lg)
		require.NoError(t, err)
		o = applyOption(t, opt)
		require.Equal(t, "node-7:1234", o.Cluster.Self.Addr)
	})

	t.Run("IDAndAddrDefaultToHostname", func(t *testing.T) {
		host, err := os.Hostname()
		require.NoError(t, err)
		opt, err := clusterOption(&StorageClusterConfig{Etcd: []string{"e"}}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.Equal(t, host, o.Cluster.Self.ID)
		require.Equal(t, host+":7946", o.Cluster.Self.Addr)
	})
}

// TestStorageClusterConfigYAML checks the cluster block parses from the documented YAML shape via
// the real config loader.
func TestStorageClusterConfigYAML(t *testing.T) {
	const data = `
storage:
  backend: file
  dir: /data
  cluster:
    etcd: ["http://etcd-1:2379", "http://etcd-2:2379"]
    id: oteldb-1
    zone: a
    addr: oteldb-1:7946
    rf: 3
    shards_per_tenant: 8
    root: /oteldb
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	require.Equal(t, "file", cfg.Storage.Backend)
	require.NotNil(t, cfg.Storage.Cluster)
	require.Equal(t, []string{"http://etcd-1:2379", "http://etcd-2:2379"}, cfg.Storage.Cluster.Etcd)
	require.Equal(t, "oteldb-1", cfg.Storage.Cluster.ID)
	require.Equal(t, "a", cfg.Storage.Cluster.Zone)
	require.Equal(t, "oteldb-1:7946", cfg.Storage.Cluster.Addr)
	require.Equal(t, 3, cfg.Storage.Cluster.RF)
	require.Equal(t, 8, cfg.Storage.Cluster.ShardsPerTenant)
	require.Equal(t, "/oteldb", cfg.Storage.Cluster.Root)
}
