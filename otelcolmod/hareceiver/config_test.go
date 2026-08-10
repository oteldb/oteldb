package hareceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

func testConfig(modify func(*Config)) *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "https://homeassistant.example:8123"
	cfg.Token = "token"
	cfg.Sources = []Source{{Kind: SourceKindHost}}
	if modify != nil {
		modify(cfg)
	}
	return cfg
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{name: "Valid"},
		{
			name: "AllSources",
			modify: func(c *Config) {
				c.Sources = []Source{
					{Kind: SourceKindHost},
					{Kind: SourceKindCore},
					{Kind: SourceKindSupervisor},
					{Kind: SourceKindAddon, Addon: "core_ssh"},
				}
			},
		},
		{
			name:    "NoEndpoint",
			modify:  func(c *Config) { c.Endpoint = "" },
			wantErr: "endpoint is required",
		},
		{
			name:    "NoToken",
			modify:  func(c *Config) { c.Token = "" },
			wantErr: "token is required",
		},
		{
			name:    "NoSources",
			modify:  func(c *Config) { c.Sources = nil },
			wantErr: "at least one source is required",
		},
		{
			name:    "EmptyKind",
			modify:  func(c *Config) { c.Sources = []Source{{}} },
			wantErr: "kind is required",
		},
		{
			name:    "UnknownKind",
			modify:  func(c *Config) { c.Sources = []Source{{Kind: "hass"}} },
			wantErr: `unknown kind "hass"`,
		},
		{
			name:    "AddonWithoutSlug",
			modify:  func(c *Config) { c.Sources = []Source{{Kind: SourceKindAddon}} },
			wantErr: "addon is required",
		},
		{
			name: "SlugOnNonAddon",
			modify: func(c *Config) {
				c.Sources = []Source{{Kind: SourceKindHost, Addon: "core_ssh"}}
			},
			wantErr: "addon is only allowed",
		},
		{
			name: "DuplicateSource",
			modify: func(c *Config) {
				c.Sources = []Source{{Kind: SourceKindHost}, {Kind: SourceKindHost}}
			},
			wantErr: `duplicate source "host"`,
		},
		{
			name: "DistinctAddons",
			modify: func(c *Config) {
				c.Sources = []Source{
					{Kind: SourceKindAddon, Addon: "a"},
					{Kind: SourceKindAddon, Addon: "b"},
				}
			},
		},
		{
			name: "DuplicateAddon",
			modify: func(c *Config) {
				c.Sources = []Source{
					{Kind: SourceKindAddon, Addon: "a"},
					{Kind: SourceKindAddon, Addon: "a"},
				}
			},
			wantErr: `duplicate source "addon/a"`,
		},
		{
			name:    "ZeroPollInterval",
			modify:  func(c *Config) { c.PollInterval = 0 },
			wantErr: "poll_interval must be positive",
		},
		{
			name:    "NegativePollInterval",
			modify:  func(c *Config) { c.PollInterval = -time.Second },
			wantErr: "poll_interval must be positive",
		},
		{
			name:    "ZeroBatchSize",
			modify:  func(c *Config) { c.BatchSize = 0 },
			wantErr: "batch_size must be positive",
		},
		{
			name: "WithStorage",
			modify: func(c *Config) {
				id := component.MustNewID("file_storage")
				c.StorageID = &id
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := testConfig(tt.modify).Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestSourcePath(t *testing.T) {
	tests := []struct {
		src      Source
		wantPath string
		wantName string
		wantKey  string
	}{
		{
			src:      Source{Kind: SourceKindHost},
			wantPath: "api/hassio/host/logs",
			wantName: "host",
			wantKey:  "cursor/host",
		},
		{
			src:      Source{Kind: SourceKindCore},
			wantPath: "api/hassio/core/logs",
			wantName: "core",
			wantKey:  "cursor/core",
		},
		{
			src:      Source{Kind: SourceKindSupervisor},
			wantPath: "api/hassio/supervisor/logs",
			wantName: "supervisor",
			wantKey:  "cursor/supervisor",
		},
		{
			src:      Source{Kind: SourceKindAddon, Addon: "core_ssh"},
			wantPath: "api/hassio/addons/core_ssh/logs",
			wantName: "addon/core_ssh",
			wantKey:  "cursor/addon/core_ssh",
		},
	}

	for _, tt := range tests {
		t.Run(tt.wantName, func(t *testing.T) {
			require.Equal(t, tt.wantPath, tt.src.Path())
			require.Equal(t, tt.wantName, tt.src.Name())
			require.Equal(t, tt.wantKey, tt.src.StorageKey())
		})
	}
}

func TestCursorStateRangeHeader(t *testing.T) {
	tests := []struct {
		state cursorState
		batch int
		want  string
	}{
		{cursorState{Anchor: "s=abc", Skip: 1}, 100, "entries=s=abc:1:100"},
		{cursorState{Anchor: "s=abc", Skip: 0}, 1, "entries=s=abc:0:1"},
		{cursorState{Anchor: "s=abc", Skip: 500}, 500, "entries=s=abc:500:500"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.state.rangeHeader(tt.batch))
	}
}

func TestCursorStateAdvance(t *testing.T) {
	t.Run("ReAnchors", func(t *testing.T) {
		s := cursorState{Anchor: "s=a", Skip: 1}.advance("s=b", 10)
		require.Equal(t, cursorState{Anchor: "s=b", Skip: 10}, s)
	})
	t.Run("KeepsAnchorWithoutCursor", func(t *testing.T) {
		s := cursorState{Anchor: "s=a", Skip: 1}.advance("", 10)
		require.Equal(t, cursorState{Anchor: "s=a", Skip: 11}, s)
	})
}
