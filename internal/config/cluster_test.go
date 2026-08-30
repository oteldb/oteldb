package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap/zaptest"

	"github.com/oteldb/oteldb/internal/config"
)

// TestRouterConfigCarriesTracerProvider pins the wiring a nil provider breaks silently: the shard
// owners keep reporting their own spans, so a cluster query's trace looks instrumented while every
// client-side and hedge span is dropped.
func TestRouterConfigCarriesTracerProvider(t *testing.T) {
	t.Parallel()

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		_ = tp.Shutdown(t.Context())
	})

	cfg := config.Cluster{
		Etcd:            []string{"127.0.0.1:2379"},
		Root:            "/oteldb",
		RF:              2,
		ShardsPerTenant: 4,
		DialTimeout:     time.Second,
	}

	got := cfg.RouterConfig(zaptest.NewLogger(t), tp)

	require.NotNil(t, got.TracerProvider, "routed RPCs report no spans without a provider")
	assert.Same(t, tp, got.TracerProvider)

	assert.Equal(t, cfg.Etcd, got.Etcd)
	assert.Equal(t, cfg.Root, got.Root)
	assert.Equal(t, cfg.RF, got.RF)
	assert.Equal(t, cfg.ShardsPerTenant, got.ShardsPerTenant)
	assert.Equal(t, cfg.DialTimeout, got.DialTimeout)
}
