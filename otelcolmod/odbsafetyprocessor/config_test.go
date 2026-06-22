package odbsafetyprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name: "default",
		},
		{
			name: "negative soft rate",
			mutate: func(c *Config) {
				c.SoftMaxRatePerSecond = -1
			},
			wantErr: "soft_max_rate_per_second must be non-negative, got -1",
		},
		{
			name: "unknown mode",
			mutate: func(c *Config) {
				c.OnExcess = "explode"
			},
			wantErr: `on_excess must be one of consume, drop, sample, compact, truncate, got "explode"`,
		},
		{
			name: "negative sample first",
			mutate: func(c *Config) {
				c.SampleFirst = -1
			},
			wantErr: "sample_first must be non-negative, got -1",
		},
		{
			name: "truncate requires window",
			mutate: func(c *Config) {
				c.OnExcess = "truncate"
				c.CompactWindow = 0
			},
			wantErr: "compact_window must be positive, got 0s",
		},
		{
			name: "compact requires threshold",
			mutate: func(c *Config) {
				c.OnExcess = "compact"
				c.CompactThreshold = 0
			},
			wantErr: "compact_threshold must be positive, got 0",
		},
		{
			name: "compact requires buckets",
			mutate: func(c *Config) {
				c.OnExcess = "compact"
				c.CompactMaxBuckets = 0
			},
			wantErr: "compact_max_buckets must be positive, got 0",
		},
		{
			name: "compact rejects negative truncate threshold",
			mutate: func(c *Config) {
				c.OnExcess = "compact"
				c.TruncateThreshold = -1
			},
			wantErr: "truncate_threshold must be non-negative, got -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.CompactWindow = time.Second
			if tt.mutate != nil {
				tt.mutate(cfg)
			}

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}
