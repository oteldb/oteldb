package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/config"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "odbselect.yml")
	require.NoError(t, os.WriteFile(path, []byte(
		"cluster:\n  etcd: [\"127.0.0.1:2379\"]\n  shards_per_tenant: 8\nloki:\n  bind: \"-\"\n",
	), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfg.validate())

	assert.Equal(t, []string{"127.0.0.1:2379"}, cfg.Cluster.Etcd)
	assert.Equal(t, 8, cfg.Cluster.ShardsPerTenant)
	assert.Equal(t, ":9090", cfg.Prometheus.Bind)
	assert.Equal(t, ":3200", cfg.Tempo.Bind)
	assert.False(t, enabled(cfg.Loki.Bind))
	assert.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
}

// TestValidateRequiresCluster pins that odbselect refuses to start without a cluster to read: it
// holds nothing, so an empty answer it could serve instead is worse than not starting.
func TestValidateRequiresCluster(t *testing.T) {
	t.Parallel()

	var cfg Config
	cfg.setDefaults()

	require.Error(t, cfg.validate())
}

// TestValidateRequiresAnAPI pins that a node serving no API is refused rather than started idle.
func TestValidateRequiresAnAPI(t *testing.T) {
	t.Parallel()

	cfg := Config{Cluster: config.Cluster{Etcd: []string{"127.0.0.1:2379"}}}
	for _, bind := range []*string{
		&cfg.Prometheus.Bind, &cfg.Loki.Bind, &cfg.Tempo.Bind, &cfg.Pyroscope.Bind,
	} {
		*bind = "-"
	}

	require.Error(t, cfg.validate())
}

// fakeMembers reports a fixed member count, standing in for the router's live ring view.
type fakeMembers int

func (n fakeMembers) memberCount() int { return int(n) }

// TestReadinessGatesOnTheRing pins that a query node stays out of the load balancer until it has
// somewhere to read from: an empty ring answers every query with an empty result, which looks like
// data rather than an outage.
func TestReadinessGatesOnTheRing(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		members int
		want    int
	}{
		{"empty ring", 0, http.StatusServiceUnavailable},
		{"ring has a member", 1, http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := readinessHandler(fakeMembers(tt.members).memberCount)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))

			assert.Equal(t, tt.want, rec.Code)
		})
	}
}
