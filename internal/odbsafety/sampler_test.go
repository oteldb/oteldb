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

func TestNewSamplerZeroValueNeverSamples(t *testing.T) {
	sampler := NewSampler(Config{})
	for range 10 {
		require.False(t, sampler())
	}
}
