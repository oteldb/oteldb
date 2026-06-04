package chstorage

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/ddl"
)

func TestMigratorOptions_AddFlags(t *testing.T) {
	opts := MigratorOptions{}
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts.AddFlags(fs)

	require.NoError(t, fs.Parse([]string{
		"--cluster", "my-cluster",
		"--keeper-path-prefix", "/my/prefix",
		"--ttl", "15d",
		"--replicated",
	}))

	require.Equal(t, "my-cluster", opts.Cluster)
	require.Equal(t, "/my/prefix", opts.KeeperPathPrefix)
	require.Equal(t, 15*24*time.Hour, opts.TTL)
	require.True(t, opts.Replicated)

	// Test invalid TTL
	require.Error(t, fs.Parse([]string{"--ttl", "invalid"}))

	// Test empty TTL
	opts = MigratorOptions{}
	fs = pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--ttl", ""}))
	require.Equal(t, time.Duration(0), opts.TTL)
}

func TestMigrator_migrationTableName(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{})
		require.Equal(t, "migration", m.migrationTableName())
	})
	t.Run("Replicated", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{Replicated: true})
		require.Equal(t, "migration_replicated", m.migrationTableName())
	})
}

func TestMigrator_ddlQueryHash(t *testing.T) {
	m := NewMigrator(nil, MigratorOptions{})
	query := "CREATE TABLE foo (a Int32) ENGINE = Memory"
	want := fmt.Sprintf("%x", sha256.Sum256([]byte(query)))
	require.Equal(t, want, m.ddlQueryHash(query))
}

func TestMigrator_tableSchema(t *testing.T) {
	schema := ddl.Table{
		Engine: ddl.Engine{Type: "MergeTree"},
	}

	t.Run("Default", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{})
		name, s, err := m.tableSchema("foo", schema)
		require.NoError(t, err)
		require.Equal(t, "foo", name)
		require.Equal(t, "MergeTree", s.Engine.Type)
	})

	t.Run("Replicated", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{
			Replicated:       true,
			KeeperPathPrefix: "/clickhouse/tables",
		})
		name, s, err := m.tableSchema("foo", schema)
		require.NoError(t, err)
		require.Equal(t, "foo_replicated", name)
		require.Equal(t, "ReplicatedMergeTree", s.Engine.Type)
		require.Equal(t, []string{"'/clickhouse/tables/foo_replicated/{uuid}'", "'{replica}'"}, s.Engine.Args)
	})

	t.Run("ReplicatedError", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{Replicated: true})
		s := ddl.Table{Engine: ddl.Engine{Type: "Memory"}} // Memory cannot be replicated
		_, _, err := m.tableSchema("foo", s)
		require.ErrorContains(t, err, "does not support replication")
	})
}

func TestMigrator_distributedTableSchema(t *testing.T) {
	schema := ddl.Table{
		Engine:      ddl.Engine{Type: "MergeTree"},
		Indexes:     []ddl.Index{{Name: "idx"}},
		OrderBy:     []string{"a"},
		PrimaryKey:  []string{"a"},
		PartitionBy: "toYYYYMMDD(timestamp)",
	}

	t.Run("Default", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{})
		_, _, err := m.distributedTableSchema("default", "foo", schema)
		require.ErrorContains(t, err, "only supported when Replicated Mode is enabled")
	})

	t.Run("Replicated", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{Replicated: true, Cluster: "my_cluster"})
		name, s, err := m.distributedTableSchema("default", "foo", schema)
		require.NoError(t, err)
		require.Equal(t, "foo_distributed", name)

		// Distributed tables should strip Indexes, OrderBy, PrimaryKey, PartitionBy
		require.Empty(t, s.Indexes)
		require.Empty(t, s.OrderBy)
		require.Empty(t, s.PrimaryKey)
		require.Empty(t, s.PartitionBy)

		require.Equal(t, "Distributed", s.Engine.Type)
		require.Equal(t, []string{"'my_cluster'", "default", "foo_distributed"}, s.Engine.Args)
	})
}

func TestMigrator_generateQuery(t *testing.T) {
	m := NewMigrator(nil, MigratorOptions{
		Replicated: true,
		Cluster:    "my_cluster",
		TTL:        24 * time.Hour,
	})

	schema := ddl.Table{
		Columns: []ddl.Column{{Name: "timestamp", Type: "DateTime64(9)"}},
		TTL:     ddl.TTL{Field: "timestamp"},
		Engine:  ddl.Engine{Type: "MergeTree"},
	}

	q, err := m.generateQuery("foo", schema, false)
	require.NoError(t, err)
	require.Contains(t, q, "CREATE TABLE `foo` ON CLUSTER 'my_cluster'")
	require.Contains(t, q, "TTL toDateTime(`timestamp`) + toIntervalSecond(86400)")

	qNotExists, err := m.generateQuery("foo", schema, true)
	require.NoError(t, err)
	require.Contains(t, qNotExists, "CREATE TABLE IF NOT EXISTS `foo` ON CLUSTER 'my_cluster'")
}

func TestMigrator_backupCandidates(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{})
		c := m.backupCandidates()
		require.NotEmpty(t, c)
		require.Contains(t, c, "logs_backup")
		require.Contains(t, c, "metrics_points_backup")
		require.NotContains(t, c, "logs_replicated_backup")
	})

	t.Run("Replicated", func(t *testing.T) {
		m := NewMigrator(nil, MigratorOptions{Replicated: true})
		c := m.backupCandidates()
		require.NotEmpty(t, c)
		require.Contains(t, c, "logs_backup")
		require.Contains(t, c, "logs_replicated_backup")
		require.Contains(t, c, "logs_distributed_backup")
	})
}
