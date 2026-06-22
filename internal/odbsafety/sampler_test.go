package odbsafety

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSamplerFirstThenEvery(t *testing.T) {
	sampler := NewSampler(Config{SampleFirst: 2, SampleThereafter: 3})

	var got []bool
	for range 9 {
		got = append(got, sampler())
	}
	require.Equal(t, []bool{true, true, true, false, false, true, false, false, true}, got)
}

func TestNewSamplerFallsBackToRate(t *testing.T) {
	t.Run("rate 1 always samples", func(t *testing.T) {
		sampler := NewSampler(Config{SampleRate: 1})
		for range 10 {
			require.True(t, sampler())
		}
	})

	t.Run("rate 0 never samples", func(t *testing.T) {
		sampler := NewSampler(Config{SampleRate: 0})
		for range 10 {
			require.False(t, sampler())
		}
	})

	t.Run("explicit opt-out of first/thereafter still uses rate", func(t *testing.T) {
		sampler := NewSampler(Config{SampleFirst: 0, SampleThereafter: 0, SampleRate: 1})
		require.True(t, sampler())
	})
}
