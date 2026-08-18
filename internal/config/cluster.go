package config

import (
	"time"

	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster/router"
)

// Cluster points a stateless node at the ring. Such a node joins nothing and stores nothing: it
// follows membership read-only and talks to the owners of each shard.
//
// Every field here must match what the storage nodes are configured with. A mismatched
// ShardsPerTenant or RF does not fail — it resolves a different owner set than the nodes do, so
// writes land where no read will look for them and reads look where the data is not.
type Cluster struct {
	// Etcd is the endpoint list the cluster coordinates membership through. Required.
	Etcd []string `json:"etcd" yaml:"etcd"`
	// Root is the etcd key prefix for the cluster's state. Empty ⇒ "/oteldb".
	Root string `json:"root" yaml:"root"`
	// RF is the replication factor. Zero ⇒ 3.
	RF int `json:"rf" yaml:"rf"`
	// ShardsPerTenant is how many shards each tenant's data is split into. Zero or one ⇒ the
	// tenant is the shard.
	ShardsPerTenant int `json:"shards_per_tenant" yaml:"shards_per_tenant"`
	// DialTimeout bounds the initial etcd connection. Zero ⇒ 5s.
	DialTimeout time.Duration `json:"dial_timeout" yaml:"dial_timeout"`
}

// RouterConfig builds the routing view of the cluster described by cfg.
func (cfg Cluster) RouterConfig(lg *zap.Logger) router.Config {
	return router.Config{
		Etcd:            cfg.Etcd,
		Root:            cfg.Root,
		RF:              cfg.RF,
		ShardsPerTenant: cfg.ShardsPerTenant,
		DialTimeout:     cfg.DialTimeout,
		Logger:          lg,
	}
}
