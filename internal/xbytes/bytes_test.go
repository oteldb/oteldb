package xbytes

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/go-faster/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// maxFloatBytes is the largest size that survives the round trip. humanize parses through a
// float64, so the mantissa quantizes anything near the limit to a multiple of 1024: the next
// representable value above it is 1<<63, which no longer fits.
const maxFloatBytes = math.MaxInt64 - 1023

// bytesCases covers the accepted forms and the sizes that used to wrap negative. It is shared by
// the direct, YAML and JSON tests: all three route through [Bytes.UnmarshalText].
var bytesCases = []struct {
	input    string
	expected int64
	wantErr  bool
}{
	{input: "100MB", expected: 100 * 1000 * 1000},
	{input: "100MiB", expected: 100 * 1024 * 1024},
	{input: "1GB", expected: 1000 * 1000 * 1000},
	{input: "1GiB", expected: 1024 * 1024 * 1024},
	{input: "104857600", expected: 104857600},
	{input: "100", expected: 100},
	{input: "0", expected: 0},
	{input: "9223372036854774784", expected: maxFloatBytes},
	{input: "9223372036854775807", wantErr: true},
	{input: "9223372036854775808", wantErr: true},
	{input: "8EiB", wantErr: true},
	{input: "10EB", wantErr: true},
	{input: "16EB", wantErr: true},
	{input: "invalid", wantErr: true},
	{input: "-5", wantErr: true},
}

func TestBytes_UnmarshalText(t *testing.T) {
	for _, tt := range bytesCases {
		t.Run(tt.input, func(t *testing.T) {
			var b Bytes
			err := b.UnmarshalText([]byte(tt.input))
			if tt.wantErr {
				require.Error(t, err)
				assert.Zero(t, int64(b), "a rejected size must not be stored")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, int64(b))
		})
	}
}

func TestBytes_YAML(t *testing.T) {
	for _, tt := range bytesCases {
		t.Run(tt.input, func(t *testing.T) {
			var b Bytes
			err := yaml.Unmarshal([]byte(tt.input), &b)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, int64(b))
		})
	}

	t.Run("Unset", func(t *testing.T) {
		var cfg struct {
			Size Bytes `yaml:"size"`
		}
		require.NoError(t, yaml.Unmarshal([]byte("other: 1\n"), &cfg))
		assert.Zero(t, int64(cfg.Size))
	})
}

func TestBytes_JSON(t *testing.T) {
	for _, tt := range bytesCases {
		t.Run(tt.input, func(t *testing.T) {
			raw, err := json.Marshal(tt.input)
			require.NoError(t, err)

			var b Bytes
			err = json.Unmarshal(raw, &b)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, int64(b))
		})
	}

	t.Run("Unset", func(t *testing.T) {
		var cfg struct {
			Size Bytes `json:"size"`
		}
		require.NoError(t, json.Unmarshal([]byte(`{"other":1}`), &cfg))
		assert.Zero(t, int64(cfg.Size))
	})
}

func TestBytes_MarshalText(t *testing.T) {
	var b Bytes = 100 * 1000 * 1000
	text, err := b.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "100 MB", string(text))

	t.Run("Negative", func(t *testing.T) {
		_, err := Bytes(-5).MarshalText()
		require.Error(t, err, "a negative must not render as a huge size")
	})
}
