// Package attributeprototype contains isolated research, not an ingest implementation.
package attributeprototype

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/oteldb/storage/signal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// These prototypes deliberately accept only string labels, as in remote write.
func copied(m pcommon.Map) signal.Attributes {
	out := make(signal.Attributes, 0, m.Len())
	for k, v := range m.All() {
		if v.Type() != pcommon.ValueTypeStr {
			panic("string-label prototype")
		}
		out = append(out, signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v.Str()))})
	}
	return signal.NewAttributes(out...)
}

type arena struct {
	data  []byte
	attrs signal.Attributes
}

func (a *arena) reset() {
	clear(a.attrs)
	a.attrs = a.attrs[:0]
	a.data = a.data[:0]
}

func (a *arena) copyString(s string) []byte {
	start := len(a.data)
	a.data = append(a.data, s...)
	return a.data[start:len(a.data):len(a.data)]
}

func (a *arena) convert(m pcommon.Map) signal.Attributes {
	start := len(a.attrs)
	for k, v := range m.All() {
		if v.Type() != pcommon.ValueTypeStr {
			panic("string-label prototype")
		}
		a.attrs = append(a.attrs, signal.KeyValue{Key: a.copyString(k), Value: signal.StringValue(a.copyString(v.Str()))})
	}
	return signal.NewAttributes(a.attrs[start:len(a.attrs):len(a.attrs)]...)
}

type (
	label struct{ key, value string }
	entry struct {
		encoded []byte
		attrs   signal.Attributes
	}
	dictionary struct {
		scratch []label
		encoded []byte
		entries map[signal.SeriesID]entry
		limit   int
		hash    func([]byte) signal.SeriesID
	}
)

func newDictionary(limit int) *dictionary {
	return &dictionary{entries: make(map[signal.SeriesID]entry), limit: limit, hash: signal.HashBytes}
}

func (d *dictionary) convert(m pcommon.Map) signal.Attributes {
	d.scratch = d.scratch[:0]
	for k, v := range m.All() {
		if v.Type() != pcommon.ValueTypeStr {
			panic("string-label prototype")
		}
		d.scratch = append(d.scratch, label{k, v.Str()})
	}
	slices.SortStableFunc(d.scratch, func(a, b label) int { return strings.Compare(a.key, b.key) })
	d.encoded = binary.AppendUvarint(d.encoded[:0], uint64(len(d.scratch)))
	for _, l := range d.scratch {
		d.encoded = binary.AppendUvarint(d.encoded, uint64(len(l.key)))
		d.encoded = append(d.encoded, l.key...)
		d.encoded = append(d.encoded, byte(signal.KindStr))
		d.encoded = binary.AppendUvarint(d.encoded, uint64(len(l.value)))
		d.encoded = append(d.encoded, l.value...)
	}
	clear(d.scratch)
	id := d.hash(d.encoded)
	if e, ok := d.entries[id]; ok && bytes.Equal(e.encoded, d.encoded) {
		return e.attrs
	}
	// Bounded entry count only: byte-budget eviction is required for production.
	if len(d.entries) >= d.limit {
		clear(d.entries)
	}
	e := entry{encoded: bytes.Clone(d.encoded), attrs: copied(m)}
	d.entries[id] = e
	return e.attrs
}

func (a *arena) descriptors(m pcommon.Map) signal.Attributes {
	start := len(a.attrs)
	for k, v := range m.All() {
		if v.Type() != pcommon.ValueTypeStr {
			panic("string-label prototype")
		}
		a.attrs = append(a.attrs, signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v.Str()))})
	}
	return signal.NewAttributes(a.attrs[start:len(a.attrs):len(a.attrs)]...)
}

func fixture(i int) pcommon.Map {
	m := pcommon.NewMap()
	m.PutStr("mode", "idle")
	m.PutStr("instance", fmt.Sprintf("host-%08d:9100", i))
	m.PutStr("job", "node-exporter")
	m.PutStr("cpu", "0")
	return m
}

func TestOwnershipAndCanonicalization(t *testing.T) {
	for _, test := range []struct {
		name        string
		makeConvert func() func(pcommon.Map) signal.Attributes
	}{
		{"copied", func() func(pcommon.Map) signal.Attributes { return copied }},
		{"arena", func() func(pcommon.Map) signal.Attributes { return new(arena).convert }},
		{"dictionary", func() func(pcommon.Map) signal.Attributes { return newDictionary(2).convert }},
	} {
		t.Run(test.name, func(t *testing.T) {
			convert := test.makeConvert()
			m := fixture(0)
			expected := copied(m)
			got := convert(m)
			reverse := pcommon.NewMap()
			for i := len(expected) - 1; i >= 0; i-- {
				reverse.PutStr(string(expected[i].Key), string(expected[i].Value.Str()))
			}
			require.True(t, expected.Equal(convert(reverse)))
			m.Clear()
			for i := 1; i < 10; i++ {
				convert(fixture(i))
			}
			require.True(t, expected.Equal(got), "source mutation, growth, and eviction must preserve outstanding results")
		})
	}
	d := newDictionary(2)
	d.hash = func([]byte) signal.SeriesID { return signal.SeriesID{} }
	first := d.convert(fixture(0))
	second := d.convert(fixture(1))
	require.False(t, first.Equal(second), "forced collisions must not return another identity")
	require.True(t, copied(fixture(0)).Equal(d.convert(fixture(0))))
	require.LessOrEqual(t, len(d.entries), 2)
}

func FuzzCanonicalStringLabels(f *testing.F) {
	f.Add("instance", "host-00000000:9100", "mode", "idle")
	f.Add("a", "b\x00c", "a\x00b", "c")
	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 string) {
		m := pcommon.NewMap()
		m.PutStr(k1, v1)
		m.PutStr(k2, v2)
		want := copied(m)
		d := newDictionary(2)
		got := d.convert(m)
		require.Equal(t, want.AppendHashInput(nil), d.encoded)
		require.True(t, want.Equal(got))
		a := new(arena)
		require.True(t, want.Equal(a.convert(m)))
	})
}

func TestLayout(t *testing.T) {
	t.Logf("KeyValue=%d Value=%d label=%d entry=%d SeriesID=%d", unsafe.Sizeof(signal.KeyValue{}), unsafe.Sizeof(signal.Value{}), unsafe.Sizeof(label{}), unsafe.Sizeof(entry{}), unsafe.Sizeof(signal.SeriesID{}))
}

var result signal.Attributes

func BenchmarkAttributeConversion(b *testing.B) {
	for _, workload := range []struct {
		name  string
		count int
	}{{"repeat", 1024}, {"churn", 4096}} {
		input := make([]pcommon.Map, workload.count)
		for i := range input {
			input[i] = fixture(i)
		}
		for _, mode := range []string{"copied", "descriptors", "arena", "dictionary"} {
			b.Run(workload.name+"/"+mode, func(b *testing.B) {
				a := new(arena)
				d := newDictionary(2048)
				convert := copied
				if mode == "descriptors" {
					convert = a.descriptors
				}
				if mode == "arena" {
					convert = a.convert
				}
				if mode == "dictionary" {
					convert = d.convert
				}
				// Warm descriptors/dictionary separately from measurement.
				for i := range 1024 {
					result = convert(input[i])
				}
				a.reset()
				b.ReportAllocs()
				batch := 0
				for b.Loop() {
					a.reset()
					for i := range 1024 {
						result = convert(input[(batch*1024+i)%len(input)])
					}
					batch++
				}
			})
		}
	}
}

func TestRetainedBackingStorage(t *testing.T) {
	a := new(arena)
	d := newDictionary(2048)
	for i := range 1024 {
		m := fixture(i)
		a.convert(m)
		d.convert(m)
	}
	var dictionaryPayload int
	for _, e := range d.entries {
		dictionaryPayload += cap(e.encoded) + cap(e.attrs)*int(unsafe.Sizeof(signal.KeyValue{}))
		for _, kv := range e.attrs {
			dictionaryPayload += cap(kv.Key) + cap(kv.Value.Str())
		}
	}
	t.Logf("arena latest buffers: bytes len=%d cap=%d descriptors len=%d cap=%d backing=%d", len(a.data), cap(a.data), len(a.attrs), cap(a.attrs), cap(a.data)+cap(a.attrs)*int(unsafe.Sizeof(signal.KeyValue{})))
	t.Logf("dictionary entries=%d payload capacities=%d; excludes map, entry slots, scratch, allocator rounding", len(d.entries), dictionaryPayload)
	a.reset()
	require.Zero(t, len(a.attrs))
	require.Zero(t, len(a.data))
}
