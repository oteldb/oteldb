package tetragonreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantError bool
	}{
		{
			name:      "empty endpoint",
			cfg:       Config{},
			wantError: true,
		},
		{
			name: "valid endpoint",
			cfg: Config{
				ClientConfig: createTestClientConfig("tetragon:54321"),
			},
			wantError: false,
		},
		{
			name: "with cluster id",
			cfg: Config{
				ClientConfig: createTestClientConfig("localhost:54321"),
				ClusterID:    12345,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
