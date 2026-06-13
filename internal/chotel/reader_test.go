package chotel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewReaderOptions(t *testing.T) {
	reader := NewReader(nil,
		WithLag(10*time.Second),
		WithLookback(2*time.Minute),
	)

	assert.Equal(t, 10*time.Second, reader.lag)
	assert.Equal(t, 2*time.Minute, reader.lookback)
}

func TestNewReaderOptionDefaults(t *testing.T) {
	reader := NewReader(nil,
		WithLag(0),
		WithLookback(-time.Second),
	)

	assert.Equal(t, DefaultLag, reader.lag)
	assert.Equal(t, DefaultLookback, reader.lookback)
}

func TestReaderAdvance(t *testing.T) {
	reader := NewReader(nil)
	first := time.Unix(10, 0)
	second := time.Unix(20, 0)

	reader.Advance(first)
	reader.Advance(time.Time{})
	reader.Advance(first.Add(-time.Second))
	reader.Advance(second)

	assert.Equal(t, second, reader.latest)
}

func TestMaxFinishTime(t *testing.T) {
	first := time.Unix(10, 0)
	second := time.Unix(20, 0)
	spans := testNamedSpans{
		{name: "first", traceSeed: 1, endDelta: first.Sub(first)},
		{name: "second", traceSeed: 1, endDelta: second.Sub(first)},
	}.traces(first)

	assert.Equal(t, second, MaxFinishTime(spans))
	assert.True(t, MaxFinishTime(nil).IsZero())
}
