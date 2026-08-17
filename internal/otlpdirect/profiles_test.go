package otlpdirect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/profile"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

func marshalProfiles(tb testing.TB, pd pprofile.Profiles) []byte {
	tb.Helper()

	raw, err := (&pprofile.ProtoMarshaler{}).MarshalProfiles(pd)
	require.NoError(tb, err)

	return raw
}

// convertBothProfiles decodes pd directly and via the pdata path, canonicalized for comparison.
//
// Unlike the other signals, the reference is taken from a round trip rather than from pd itself:
// pdata's profiles marshaler rewrites resource and scope attributes into string-table references,
// mutating pd and growing its dictionary as it goes. The bytes on the wire are therefore the only
// thing the two paths can be compared on, which is also exactly what a receiver sees.
func convertBothProfiles(tb testing.TB, pd pprofile.Profiles) (direct, viaPdata *profile.Profiles) {
	tb.Helper()

	raw := marshalProfiles(tb, pd)

	var c otlpdirect.ProfilesConverter

	direct, err := c.Convert(raw)
	require.NoError(tb, err)

	decoded, err := (&pprofile.ProtoUnmarshaler{}).UnmarshalProfiles(raw)
	require.NoError(tb, err)

	viaPdata = &profile.Profiles{}
	require.Zero(tb, pdataconv.AppendProfiles(viaPdata, decoded))

	return canonicalProfiles(direct), canonicalProfiles(viaPdata)
}

// fullProfiles builds a batch touching every field the decoder reads: the whole shared dictionary,
// resource and scope identity, and a profile with samples carrying values, timestamps, attribute
// indices and a link.
func fullProfiles(tb testing.TB) pprofile.Profiles {
	tb.Helper()

	pd := pprofile.NewProfiles()

	dict := pd.Dictionary()
	for _, s := range []string{
		"", "samples", "count", "cpu", "nanoseconds", "main", "main.main",
		"/src/main.go", "/usr/lib/libc.so", "thread.name", "ms",
	} {
		dict.StringTable().Append(s)
	}

	fn := dict.FunctionTable().AppendEmpty()
	fn.SetNameStrindex(5)
	fn.SetSystemNameStrindex(6)
	fn.SetFilenameStrindex(7)
	fn.SetStartLine(42)

	fn2 := dict.FunctionTable().AppendEmpty()
	fn2.SetNameStrindex(6)

	mp := dict.MappingTable().AppendEmpty()
	mp.SetMemoryStart(0x400000)
	mp.SetMemoryLimit(0x500000)
	mp.SetFileOffset(0x1000)
	mp.SetFilenameStrindex(8)
	mp.AttributeIndices().Append(0, 1)

	dict.MappingTable().AppendEmpty()

	loc := dict.LocationTable().AppendEmpty()
	loc.SetMappingIndex(0)
	loc.SetAddress(0x4010a0)
	loc.AttributeIndices().Append(1)

	ln := loc.Lines().AppendEmpty()
	ln.SetFunctionIndex(0)
	ln.SetLine(17)
	ln.SetColumn(3)

	ln2 := loc.Lines().AppendEmpty()
	ln2.SetFunctionIndex(1)
	ln2.SetLine(90)

	loc2 := dict.LocationTable().AppendEmpty()
	loc2.SetMappingIndex(1)
	loc2.SetAddress(0x401200)

	dict.StackTable().AppendEmpty().LocationIndices().Append(0, 1)
	dict.StackTable().AppendEmpty().LocationIndices().Append(1)
	dict.StackTable().AppendEmpty()

	at := dict.AttributeTable().AppendEmpty()
	at.SetKeyStrindex(9)
	at.SetUnitStrindex(10)
	at.Value().SetStr("worker-1")

	at2 := dict.AttributeTable().AppendEmpty()
	at2.SetKeyStrindex(1)
	at2.Value().SetInt(7)

	at3 := dict.AttributeTable().AppendEmpty()
	at3.Value().SetDouble(1.5)

	lk := dict.LinkTable().AppendEmpty()
	lk.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	lk.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})

	dict.LinkTable().AppendEmpty()

	rp := pd.ResourceProfiles().AppendEmpty()
	rp.SetSchemaUrl("https://schema.example/resource")
	rp.Resource().Attributes().PutStr("service.name", "api")
	rp.Resource().Attributes().PutInt("process.pid", 1234)

	nested := rp.Resource().Attributes().PutEmptyMap("host")
	nested.PutStr("name", "node-a")
	nested.PutEmptySlice("tags").AppendEmpty().SetStr("prod")

	sp := rp.ScopeProfiles().AppendEmpty()
	sp.SetSchemaUrl("https://schema.example/scope")
	sp.Scope().SetName("go.opentelemetry.io/example")
	sp.Scope().SetVersion("1.2.3")
	sp.Scope().Attributes().PutBool("experimental", true)
	sp.Scope().Attributes().PutStr("lang", "go")

	pr := sp.Profiles().AppendEmpty()
	pr.SampleType().SetTypeStrindex(1)
	pr.SampleType().SetUnitStrindex(2)
	pr.PeriodType().SetTypeStrindex(3)
	pr.PeriodType().SetUnitStrindex(4)
	pr.SetTime(1_700_000_000_000_000_000)
	pr.SetDurationNano(30_000_000_000)
	pr.SetPeriod(10_000_000)
	pr.SetProfileID(pprofile.ProfileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	pr.SetDroppedAttributesCount(3)
	pr.AttributeIndices().Append(0, 2)

	s := pr.Samples().AppendEmpty()
	s.SetStackIndex(0)
	s.SetLinkIndex(0)
	s.AttributeIndices().Append(1)
	s.Values().Append(10, 20, 30)
	s.TimestampsUnixNano().Append(1_700_000_000_000_000_001, 1_700_000_000_000_000_002)

	s2 := pr.Samples().AppendEmpty()
	s2.SetStackIndex(1)
	s2.Values().Append(5)

	pr2 := sp.Profiles().AppendEmpty()
	pr2.SampleType().SetTypeStrindex(3)
	pr2.SetTime(1_700_000_001_000_000_000)
	pr2.Samples().AppendEmpty().SetStackIndex(2)

	return pd
}

func TestConvertProfilesMatchesPdata(t *testing.T) {
	t.Parallel()

	direct, viaPdata := convertBothProfiles(t, fullProfiles(t))
	assert.Equal(t, viaPdata, direct)
}

// TestConvertProfilesMinimal covers the batch a profiler emits with nothing optional set.
func TestConvertProfilesMinimal(t *testing.T) {
	t.Parallel()

	pd := pprofile.NewProfiles()
	pd.Dictionary().StringTable().Append("")

	pr := pd.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty().Profiles().AppendEmpty()
	pr.SetTime(1)
	pr.Samples().AppendEmpty()

	direct, viaPdata := convertBothProfiles(t, pd)
	require.Equal(t, viaPdata, direct)

	got := direct.Resources[0].Scopes[0].Profiles[0]
	assert.Empty(t, got.AttributeIndices)
	assert.Empty(t, got.ProfileID)
	assert.Len(t, got.Samples, 1)
	assert.Empty(t, got.Samples[0].Values)
}

// TestConvertProfilesDictionary pins that every table lands index-preserving, so the indices the
// samples, stacks and locations carry still resolve to the entry the producer meant.
func TestConvertProfilesDictionary(t *testing.T) {
	t.Parallel()

	var c otlpdirect.ProfilesConverter

	got, err := c.Convert(marshalProfiles(t, fullProfiles(t)))
	require.NoError(t, err)

	dict := got.Dictionary
	require.GreaterOrEqual(t, len(dict.Strings), 11)
	assert.Empty(t, string(dict.Strings[0]))
	assert.Equal(t, "samples", string(dict.Strings[1]))
	assert.Equal(t, "/usr/lib/libc.so", string(dict.Strings[8]))

	require.Len(t, dict.Functions, 2)
	assert.Equal(t, "main", string(dict.Strings[dict.Functions[0].NameStrindex]))
	assert.Equal(t, "main.main", string(dict.Strings[dict.Functions[0].SystemNameStrindex]))
	assert.Equal(t, "/src/main.go", string(dict.Strings[dict.Functions[0].FilenameStrindex]))
	assert.Equal(t, int64(42), dict.Functions[0].StartLine)

	require.Len(t, dict.Mappings, 2)
	assert.Equal(t, uint64(0x400000), dict.Mappings[0].MemoryStart)
	assert.Equal(t, uint64(0x500000), dict.Mappings[0].MemoryLimit)
	assert.Equal(t, uint64(0x1000), dict.Mappings[0].FileOffset)
	assert.Equal(t, []int32{0, 1}, dict.Mappings[0].AttributeIndices)
	assert.Empty(t, dict.Mappings[1].AttributeIndices)

	require.Len(t, dict.Locations, 2)
	require.Len(t, dict.Locations[0].Lines, 2)
	assert.Equal(t, int64(17), dict.Locations[0].Lines[0].Line)
	assert.Equal(t, int64(3), dict.Locations[0].Lines[0].Column)
	assert.Equal(t, int32(1), dict.Locations[0].Lines[1].FunctionIndex)
	assert.Equal(t, uint64(0x4010a0), dict.Locations[0].Address)
	assert.Empty(t, dict.Locations[1].Lines)

	require.Len(t, dict.Stacks, 3)
	assert.Equal(t, []int32{0, 1}, dict.Stacks[0].LocationIndices)
	assert.Equal(t, []int32{1}, dict.Stacks[1].LocationIndices)
	assert.Empty(t, dict.Stacks[2].LocationIndices)

	require.Len(t, dict.Attributes, 3)
	assert.Equal(t, "thread.name", string(dict.Strings[dict.Attributes[0].KeyStrindex]))
	assert.Equal(t, "ms", string(dict.Strings[dict.Attributes[0].UnitStrindex]))
	assert.Equal(t, "worker-1", string(dict.Attributes[0].Value.Str()))
	assert.Equal(t, int64(7), dict.Attributes[1].Value.Int())
	assert.InDelta(t, 1.5, dict.Attributes[2].Value.Double(), 0)

	require.Len(t, dict.Links, 2)
	assert.Len(t, dict.Links[0].TraceID, 16)
	assert.Len(t, dict.Links[0].SpanID, 8)
	assert.Empty(t, dict.Links[1].TraceID)

	pr := got.Resources[0].Scopes[0].Profiles[0]
	assert.Equal(t, "samples", string(dict.Strings[pr.SampleType.TypeStrindex]))
	assert.Equal(t, "count", string(dict.Strings[pr.SampleType.UnitStrindex]))
	assert.Equal(t, "cpu", string(dict.Strings[pr.PeriodType.TypeStrindex]))
	assert.Equal(t, "nanoseconds", string(dict.Strings[pr.PeriodType.UnitStrindex]))
	assert.Equal(t, []int64{10, 20, 30}, pr.Samples[0].Values)
	assert.Len(t, pr.Samples[0].TimestampsUnixNano, 2)
	assert.Equal(t, []int32{1}, pr.Samples[0].AttributeIndices)
}

// TestConvertProfilesResolvesAttributeReferences pins the profiles-only indirection: pdata rewrites
// resource and scope attribute keys and string values into string-table indices before marshaling,
// so a decoder that only reads the inline key sees empty attributes.
func TestConvertProfilesResolvesAttributeReferences(t *testing.T) {
	t.Parallel()

	var c otlpdirect.ProfilesConverter

	got, err := c.Convert(marshalProfiles(t, fullProfiles(t)))
	require.NoError(t, err)

	attrs := got.Resources[0].Resource.Attributes
	require.NotEmpty(t, attrs)

	found := map[string]string{}
	for _, kv := range attrs {
		found[string(kv.Key)] = string(kv.Value.Str())
	}

	assert.Equal(t, "api", found["service.name"])
	assert.Contains(t, found, "process.pid")

	scope := got.Resources[0].Scopes[0].Scope
	require.Len(t, scope.Attributes, 2)
	assert.Equal(t, "experimental", string(scope.Attributes[0].Key))
	assert.Equal(t, "lang", string(scope.Attributes[1].Key))
	assert.Equal(t, "go", string(scope.Attributes[1].Value.Str()))
}

// TestConvertProfilesManySamples pins that the scratch reused across samples does not bleed one
// sample's values, timestamps or attribute indices into the next.
func TestConvertProfilesManySamples(t *testing.T) {
	t.Parallel()

	pd := pprofile.NewProfiles()
	pd.Dictionary().StringTable().Append("")

	sp := pd.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty()

	for p := range 3 {
		pr := sp.Profiles().AppendEmpty()
		pr.SetTime(1)
		pr.SetPeriod(int64(p))

		if p%2 == 0 {
			pr.AttributeIndices().Append(int32(p))
		}

		for i := range 10 {
			s := pr.Samples().AppendEmpty()
			s.SetStackIndex(int32(i))
			s.Values().Append(int64(i))

			if i%3 == 0 {
				s.TimestampsUnixNano().Append(uint64(i))
			}

			if i%4 == 0 {
				s.AttributeIndices().Append(int32(i))
			}
		}
	}

	direct, viaPdata := convertBothProfiles(t, pd)
	require.Equal(t, viaPdata, direct)

	samples := direct.Resources[0].Scopes[0].Profiles[0].Samples
	require.Len(t, samples, 10)
	assert.Empty(t, samples[1].TimestampsUnixNano, "a sample without timestamps keeps none")
	assert.Empty(t, samples[1].AttributeIndices, "a sample without attributes keeps none")
	assert.Empty(t, direct.Resources[0].Scopes[0].Profiles[1].AttributeIndices)
}

func TestConvertProfilesIsFieldOrderIndependent(t *testing.T) {
	t.Parallel()

	var asc, desc otlpdirect.ProfilesConverter

	up, err := asc.Convert(encodeProfiles(true))
	require.NoError(t, err)

	down, err := desc.Convert(encodeProfiles(false))
	require.NoError(t, err)

	require.Equal(t, canonicalProfiles(down), canonicalProfiles(up))

	rp := up.Resources[0]
	assert.Equal(t, "https://schema.example/resource", string(rp.Resource.SchemaURL))
	assert.Equal(t, "https://schema.example/scope", string(rp.Scopes[0].Scope.SchemaURL))
	assert.Equal(t, "scope-name", string(rp.Scopes[0].Scope.Name))

	// The dictionary is written after the resources in ascending order, so a resolved key here
	// proves it was decoded before the walk that needs it.
	require.Len(t, rp.Resource.Attributes, 1)
	assert.Equal(t, "service.name", string(rp.Resource.Attributes[0].Key))
	assert.Equal(t, "api", string(rp.Resource.Attributes[0].Value.Str()))

	pr := rp.Scopes[0].Profiles[0]
	assert.Equal(t, int64(1_700_000_000_000_000_000), pr.TimeNanos)
	assert.Equal(t, int64(30_000_000_000), pr.DurationNanos)
	assert.Equal(t, int64(10_000_000), pr.Period)
	assert.Equal(t, "profile-id-0000a", string(pr.ProfileID))
	assert.Equal(t, []int32{0}, pr.AttributeIndices)
	require.Len(t, pr.Samples, 1)
	assert.Equal(t, []int64{10, 20}, pr.Samples[0].Values)

	dict := up.Dictionary
	require.Len(t, dict.Locations, 1)
	assert.Len(t, dict.Locations[0].Lines, 1)
	assert.Equal(t, int64(17), dict.Locations[0].Lines[0].Line)
	assert.Equal(t, uint64(0x400000), dict.Mappings[0].MemoryStart)
	assert.Equal(t, []int32{0}, dict.Stacks[0].LocationIndices)
}

func TestConvertProfilesEmpty(t *testing.T) {
	t.Parallel()

	var c otlpdirect.ProfilesConverter

	got, err := c.Convert(nil)
	require.NoError(t, err)
	assert.Empty(t, got.Resources)
	assert.Empty(t, got.Dictionary.Strings)

	direct, viaPdata := convertBothProfiles(t, pprofile.NewProfiles())
	assert.Equal(t, viaPdata, direct)
}

func TestConvertProfilesReuseIsIsolated(t *testing.T) {
	t.Parallel()

	var c otlpdirect.ProfilesConverter

	second := pprofile.NewProfiles()
	second.Dictionary().StringTable().Append("")
	second.Dictionary().StringTable().Append("cpu")
	second.Dictionary().StackTable().AppendEmpty().LocationIndices().Append(0)

	pr := second.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty().Profiles().AppendEmpty()
	pr.SetTime(2)
	pr.SampleType().SetTypeStrindex(1)
	pr.Samples().AppendEmpty().Values().Append(1)

	rawSecond := marshalProfiles(t, second)

	_, err := c.Convert(marshalProfiles(t, fullProfiles(t)))
	require.NoError(t, err)

	got, err := c.Convert(rawSecond)
	require.NoError(t, err)

	var fresh otlpdirect.ProfilesConverter

	want, err := fresh.Convert(rawSecond)
	require.NoError(t, err)

	assert.Equal(t, canonicalProfiles(want), canonicalProfiles(got))
}

func BenchmarkConvertProfiles(b *testing.B) {
	pd := pprofile.NewProfiles()

	dict := pd.Dictionary()
	for _, s := range []string{"", "samples", "count", "cpu", "nanoseconds", "main", "/src/main.go"} {
		dict.StringTable().Append(s)
	}

	fn := dict.FunctionTable().AppendEmpty()
	fn.SetNameStrindex(5)
	fn.SetFilenameStrindex(6)

	for i := range 100 {
		loc := dict.LocationTable().AppendEmpty()
		loc.SetAddress(uint64(0x400000 + i))
		loc.Lines().AppendEmpty().SetLine(int64(i))

		dict.StackTable().AppendEmpty().LocationIndices().Append(int32(i), int32((i+1)%100))
	}

	rp := pd.ResourceProfiles().AppendEmpty()
	rp.Resource().Attributes().PutStr("service.name", "api")

	sp := rp.ScopeProfiles().AppendEmpty()
	sp.Scope().SetName("bench")

	pr := sp.Profiles().AppendEmpty()
	pr.SampleType().SetTypeStrindex(1)
	pr.SampleType().SetUnitStrindex(2)
	pr.SetTime(1_700_000_000_000_000_000)

	for i := range 1_000 {
		s := pr.Samples().AppendEmpty()
		s.SetStackIndex(int32(i % 100))
		s.Values().Append(int64(i))
		s.TimestampsUnixNano().Append(uint64(1_700_000_000_000_000_000 + i))
	}

	raw := marshalProfiles(b, pd)

	b.Run("Direct", func(b *testing.B) {
		var c otlpdirect.ProfilesConverter

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			if _, err := c.Convert(raw); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Pdata", func(b *testing.B) {
		var u pprofile.ProtoUnmarshaler

		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))

		for b.Loop() {
			decoded, err := u.UnmarshalProfiles(raw)
			if err != nil {
				b.Fatal(err)
			}

			dst := &profile.Profiles{}
			pdataconv.AppendProfiles(dst, decoded)
		}
	})
}
