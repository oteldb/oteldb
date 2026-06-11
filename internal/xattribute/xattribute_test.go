package xattribute

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

type justStringer struct{}

func (justStringer) String() string {
	return "justStringer"
}

type panicStringer struct{}

func (panicStringer) String() string {
	panic("bad stringer")
}

func TestStringerSlice(t *testing.T) {
	tests := []struct {
		k    string
		v    []fmt.Stringer
		want attribute.KeyValue
	}{
		{
			"key",
			nil,
			attribute.StringSlice("key", nil),
		},
		{
			"key",
			[]fmt.Stringer{justStringer{}},
			attribute.StringSlice("key", []string{"justStringer"}),
		},
		{
			"key",
			[]fmt.Stringer{panicStringer{}},
			attribute.StringSlice("key", []string{"<stringer panic>:bad stringer"}),
		},
		{
			"key",
			[]fmt.Stringer{nil},
			attribute.StringSlice("key", []string{"<stringer panic>:runtime error: invalid memory address or nil pointer dereference"}),
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, StringerSlice(tt.k, tt.v))
		})
	}
}

func BenchmarkStringerSlice(b *testing.B) {
	var (
		s    [4]justStringer
		sink attribute.KeyValue
	)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sink = StringerSlice("key", s[:])
	}

	runtime.KeepAlive(sink)
}

func TestStringMap(t *testing.T) {
	tests := []struct {
		k    string
		m    map[string]string
		want attribute.KeyValue
	}{
		{
			"key",
			nil,
			attribute.StringSlice("key", nil),
		},
		{
			"key",
			map[string]string{"a": "1", "b": "2"},
			attribute.StringSlice("key", []string{"a=1", "b=2"}),
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, StringMap(tt.k, tt.m))
		})
	}
}
