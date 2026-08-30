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

	path := filepath.Join(t.TempDir(), "odbadmin.yml")
	require.NoError(t, os.WriteFile(path, []byte(
		"cluster:\n  etcd: [\"127.0.0.1:2379\"]\n  rf: 2\nnodes:\n  timeout: 3s\n",
	), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfg.validate())

	assert.Equal(t, []string{"127.0.0.1:2379"}, cfg.Cluster.Etcd)
	assert.Equal(t, 2, cfg.Cluster.RF)
	assert.Equal(t, ":8090", cfg.Admin.Bind)
	assert.Equal(t, ":13133", cfg.Health.Bind)
	assert.Equal(t, "http", cfg.Nodes.Scheme)
	assert.Equal(t, defaultNodeAdminPort, cfg.Nodes.Port)
	assert.Equal(t, 3*time.Second, cfg.Nodes.Timeout)
	assert.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
}

// TestValidate pins that odbadmin refuses to start without something to aggregate or somewhere to
// serve it: it holds no data, so either omission leaves it running with nothing to say.
func TestValidate(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		cfg  func(*Config)
	}{
		{"no cluster", func(*Config) {}},
		{"no admin bind", func(c *Config) {
			c.Cluster.Etcd = []string{"127.0.0.1:2379"}
			c.Admin.Bind = "-"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var cfg Config
			cfg.setDefaults()
			tt.cfg(&cfg)

			require.Error(t, cfg.validate())
		})
	}
}

// TestValidateAccepts pins that a minimal config is enough: the cluster to read and the defaults.
func TestValidateAccepts(t *testing.T) {
	t.Parallel()

	cfg := Config{Cluster: config.Cluster{Etcd: []string{"127.0.0.1:2379"}}}
	cfg.setDefaults()

	require.NoError(t, cfg.validate())
}

// fakeMembers reports a fixed member count, standing in for the router's live ring view.
type fakeMembers int

func (n fakeMembers) memberCount() int { return int(n) }

// TestReadinessGatesOnTheRing pins that the aggregator stays out of the load balancer until it has
// somewhere to read from: with no member to ask, every report is empty and reads as a cluster that
// holds nothing rather than as an outage.
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
