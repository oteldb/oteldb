package xbytes

import (
	"testing"

	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytes_YAML(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"100MB", 100 * 1000 * 1000},
		{"100MiB", 100 * 1024 * 1024},
		{"104857600", 104857600},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var b Bytes
			err := yaml.Unmarshal([]byte(tt.input), &b)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, int64(b))
		})
	}
}

func TestBytes_UnmarshalText(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		wantErr  bool
	}{
		{"100MB", 100 * 1000 * 1000, false},
		{"100MiB", 100 * 1024 * 1024, false},
		{"100", 100, false},
		{"invalid", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var b Bytes
			err := b.UnmarshalText([]byte(tt.input))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, int64(b))
		})
	}
}

func TestBytes_MarshalText(t *testing.T) {
	var b Bytes = 100 * 1000 * 1000
	text, err := b.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "100 MB", string(text))
}
