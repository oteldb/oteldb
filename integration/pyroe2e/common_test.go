// Package pyroe2e_test provides end-to-end tests and benchmarks for the ProfileQL/Pyroscope query
// path against the embedded github.com/oteldb/storage engine.
package pyroe2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

const (
	profileSampleType = "cpu"
	profileSampleUnit = "nanoseconds"
	// profileTypeID is the canonical ProfileQL type id for the generated CPU profiles
	// (name:sampleType:sampleUnit:periodType:periodUnit, where name == sampleType).
	profileTypeID = "cpu:cpu:nanoseconds:cpu:nanoseconds"
)

// frames are the function names that make up the generated call stacks (root-most last).
var frames = []string{"main", "serveHTTP", "queryDB", "encodeJSON", "gcAssist"}

// stacks are leaf-first call stacks expressed as indices into frames.
var stacks = [][]int{
	{2, 1, 0}, // queryDB <- serveHTTP <- main
	{3, 1, 0}, // encodeJSON <- serveHTTP <- main
	{4, 0},    // gcAssist <- main
}

// generateProfiles builds a CPU profiles dataset: for each service, profilesPerService profiles
// spaced one second apart starting at start, each carrying a sample on every call stack. It returns
// the batch, the [start, end] window covering it, and the total sample value sum.
func generateProfiles(start time.Time, services []string, profilesPerService int) (pd pprofile.Profiles, end time.Time, total int64) {
	pd = pprofile.NewProfiles()
	dict := pd.Dictionary()

	st := dict.StringTable()
	st.Append("")                // 0: reserved empty
	st.Append(profileSampleType) // 1
	st.Append(profileSampleUnit) // 2
	for _, name := range frames {
		st.Append(name)
	}
	frameStr := func(i int) int32 { return int32(3 + i) }

	dict.MappingTable().AppendEmpty().SetFilenameStrindex(0)
	for i := range frames {
		dict.FunctionTable().AppendEmpty().SetNameStrindex(frameStr(i))
		loc := dict.LocationTable().AppendEmpty()
		loc.SetMappingIndex(0)
		line := loc.Lines().AppendEmpty()
		line.SetFunctionIndex(int32(i))
		line.SetLine(int64(10 * (i + 1)))
	}
	for _, frameRefs := range stacks {
		stk := dict.StackTable().AppendEmpty()
		for _, fr := range frameRefs {
			stk.LocationIndices().Append(int32(fr))
		}
	}

	end = start
	for _, svc := range services {
		rp := pd.ResourceProfiles().AppendEmpty()
		rp.Resource().Attributes().PutStr("service.name", svc)
		scp := rp.ScopeProfiles().AppendEmpty()

		t := start
		for n := range profilesPerService {
			p := scp.Profiles().AppendEmpty()
			p.SampleType().SetTypeStrindex(1)
			p.SampleType().SetUnitStrindex(2)
			p.PeriodType().SetTypeStrindex(1)
			p.PeriodType().SetUnitStrindex(2)
			p.SetTime(pcommon.Timestamp(t.UnixNano()))

			for si := range stacks {
				s := p.Samples().AppendEmpty()
				s.SetStackIndex(int32(si))
				v := int64(100*(si+1) + n)
				s.Values().Append(v)
				s.TimestampsUnixNano().Append(uint64(t.UnixNano()))
				total += v
			}

			t = t.Add(time.Second)
			if t.After(end) {
				end = t
			}
		}
	}
	return pd, end, total
}

// newBackend opens an in-memory storage backend with the generated profiles ingested.
func newBackend(tb testing.TB, services []string, profilesPerService int) (_ *storagebackend.Backend, start, end time.Time, total int64) {
	tb.Helper()
	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = store.Close(ctx) })

	backend := storagebackend.New(store)
	start = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	pd, end, total := generateProfiles(start, services, profilesPerService)
	require.NoError(tb, backend.ConsumeProfiles(ctx, pd))
	return backend, start, end, total
}
