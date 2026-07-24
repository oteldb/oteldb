package storagebackend_test

// Golden benchmarks for the ProfileQL read path over the github.com/oteldb/storage engine.
//
// They mirror the contract of storage's own golden_bench_test.go: one fixed, deterministic corpus
// (no RNG), stable sub-benchmark names (they are the CI baseline — treat them as an API), and
// b.SetBytes on the LOGICAL (uncompressed) row footprint so MB/s is a real scan speed rather than a
// function of the codec's compression ratio. Changing the corpus resets the historical baseline, so
// only do it deliberately.
//
// The corpus is ingested in profileqlRounds rounds, each flushed to its own immutable part, so the
// reads exercise the part-scan path (the head is empty) and the time-windowed select can prune
// parts. Profiles are the only signal with a content-addressed symbol side store, so every
// SelectMergeProfile sub-benchmark also pays symbol-store union + stack resolution — the
// profiles-specific storage path; profile_resolver isolates that cost on its own.

import (
	"context"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profilestorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

const (
	profileqlServices  = 4  // distinct service.name values
	profileqlPods      = 8  // pods per service ⇒ 32 distinct pod values (the high-cardinality label)
	profileqlRounds    = 4  // ingest rounds, each flushed to its own part
	profileqlPerRound  = 64 // samples per stream per round
	profileqlBranches  = 4  // handler frames per service
	profileqlLeaves    = 4  // leaf frames per handler ⇒ 16 distinct stacks per service
	profileqlSampleVal = 1 << 20

	// profileqlSampleInterval is the spacing of consecutive samples of one stream; a round therefore
	// spans profileqlPerRound*profileqlSampleInterval and each part covers a distinct time span.
	profileqlSampleInterval = 10 * time.Second

	// profileqlRowBytes is the logical (uncompressed) footprint of one sample row: timestamp, value,
	// period and duration (4×8 B) plus the 16-byte stack id and the 16-byte profile id. b.SetBytes
	// uses it so MB/s reflects the scanned logical volume, not the encoded part size.
	profileqlRowBytes = 4*8 + 16 + 16

	// profileqlRowsPerStream is the number of rows one stream contributes across all rounds, and
	// profileqlRowsPerType the rows of one profile type across every stream.
	profileqlRowsPerStream = profileqlRounds * profileqlPerRound
	profileqlRowsPerType   = profileqlServices * profileqlPods * profileqlRowsPerStream
)

// profileqlEpoch is the fixed corpus start (2024-01-01T00:00:00Z). Absolute and constant so the
// workload — and therefore the baseline — does not depend on when the benchmark runs.
var profileqlEpoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// profileqlTypes are the two ingested profile types: a CPU-time profile and an allocation profile.
var profileqlTypes = [2]profileql.ProfileType{
	{Name: "cpu", SampleType: "cpu", SampleUnit: "nanoseconds", PeriodType: "cpu", PeriodUnit: "nanoseconds"},
	{Name: "alloc_space", SampleType: "alloc_space", SampleUnit: "bytes", PeriodType: "space", PeriodUnit: "bytes"},
}

// profileqlCorpus is the shared read-only fixture: a flushed store plus the metadata the assertions
// need (the exact stack shapes ingested, so expected flame-tree node counts are derived, not
// hardcoded).
type profileqlCorpus struct {
	querier *storagebackend.ProfileQuerier
	store   *storage.Storage

	// stacks[service] are the root→leaf function-name paths that service emits, in ingest order.
	stacks [][][]string
	// start/end bound the whole corpus; lastRound starts the final round's part.
	start, end, lastRound time.Time
}

// profileqlService returns the service.name of service index s.
func profileqlService(s int) string { return "svc-" + strconv.Itoa(s) }

// profileqlPod returns the globally unique pod name of pod p of service s.
func profileqlPod(s, p int) string { return "pod-" + strconv.Itoa(s*profileqlPods+p) }

// profileqlStacks builds the deterministic call-stack shapes of one service: profileqlBranches ×
// profileqlLeaves root→leaf paths sharing a common prefix, so the symbol store dedups the prefix
// frames and the flame-tree merge folds them into shared nodes.
func profileqlStacks(s int) [][]string {
	prefix := []string{
		"main.main",
		"svc." + profileqlService(s) + ".serve",
		"net/http.serveHTTP",
		"app.middleware",
	}

	out := make([][]string, 0, profileqlBranches*profileqlLeaves)
	for b := range profileqlBranches {
		for l := range profileqlLeaves {
			path := make([]string, 0, len(prefix)+3)
			path = append(path, prefix...)
			path = append(path,
				"app.handler"+strconv.Itoa(b),
				"compute.step"+strconv.Itoa(l),
				"runtime.mallocgc",
			)
			out = append(out, path)
		}
	}

	return out
}

// profileqlBuilder accumulates the OTLP shared dictionary of one round's batch, interning strings
// and materializing one location (with a single line) per function so a resolved frame maps
// one-to-one onto a call-path element.
type profileqlBuilder struct {
	dict    pprofile.ProfilesDictionary
	strings map[string]int32
	funcs   map[string]int32
	stacks  map[string]int32
}

func newProfileqlBuilder(dict pprofile.ProfilesDictionary) *profileqlBuilder {
	b := &profileqlBuilder{
		dict:    dict,
		strings: map[string]int32{},
		funcs:   map[string]int32{},
		stacks:  map[string]int32{},
	}
	dict.StringTable().Append("")
	b.strings[""] = 0

	mp := dict.MappingTable().AppendEmpty()
	mp.SetFilenameStrindex(b.str("/usr/bin/app"))

	return b
}

// str interns s into the string table.
func (b *profileqlBuilder) str(s string) int32 {
	if idx, ok := b.strings[s]; ok {
		return idx
	}
	idx := int32(b.dict.StringTable().Len())
	b.dict.StringTable().Append(s)
	b.strings[s] = idx
	return idx
}

// location returns the location index of the named function, creating the function+location pair on
// first use.
func (b *profileqlBuilder) location(name string) int32 {
	if idx, ok := b.funcs[name]; ok {
		return idx
	}

	fn := b.dict.FunctionTable().AppendEmpty()
	fn.SetNameStrindex(b.str(name))
	fn.SetFilenameStrindex(b.str("app.go"))
	fnIdx := int32(b.dict.FunctionTable().Len() - 1)

	loc := b.dict.LocationTable().AppendEmpty()
	loc.SetMappingIndex(0)
	line := loc.Lines().AppendEmpty()
	line.SetFunctionIndex(fnIdx)
	line.SetLine(int64(fnIdx) * 10)

	idx := int32(b.dict.LocationTable().Len() - 1)
	b.funcs[name] = idx
	return idx
}

// stack returns the stack-table index of a root→leaf path, storing it leaf-first as OTLP requires.
func (b *profileqlBuilder) stack(path []string) int32 {
	key := strings.Join(path, ";")
	if idx, ok := b.stacks[key]; ok {
		return idx
	}

	st := b.dict.StackTable().AppendEmpty()
	for _, name := range slices.Backward(path) {
		st.LocationIndices().Append(b.location(name))
	}

	idx := int32(b.dict.StackTable().Len() - 1)
	b.stacks[key] = idx
	return idx
}

// profileqlRound builds one round's OTLP batch: every (service, pod) resource emits both profile
// types, each with profileqlPerRound samples cycling through that service's stacks. Timestamps of
// round r fall in [epoch+r*span, epoch+(r+1)*span), so each flushed part covers a distinct span.
func profileqlRound(round int, stacks [][][]string) pprofile.Profiles {
	pd := pprofile.NewProfiles()
	b := newProfileqlBuilder(pd.Dictionary())

	// A small per-sample attribute set, so the (bloom-indexed) attrs column is populated.
	threads := make([]int32, 4)
	for i := range threads {
		attr := b.dict.AttributeTable().AppendEmpty()
		attr.SetKeyStrindex(b.str("thread.name"))
		attr.Value().SetStr("worker-" + strconv.Itoa(i))
		threads[i] = int32(b.dict.AttributeTable().Len() - 1)
	}

	roundStart := profileqlEpoch.Add(time.Duration(round) * profileqlPerRound * profileqlSampleInterval)

	for s := range profileqlServices {
		for p := range profileqlPods {
			rp := pd.ResourceProfiles().AppendEmpty()
			res := rp.Resource().Attributes()
			res.PutStr("service.name", profileqlService(s))
			res.PutStr("pod", profileqlPod(s, p))
			res.PutStr("region", "region-"+strconv.Itoa(p%2))
			scp := rp.ScopeProfiles().AppendEmpty()
			scp.Scope().SetName("profiler")

			for ti, typ := range profileqlTypes {
				pr := scp.Profiles().AppendEmpty()
				pr.SampleType().SetTypeStrindex(b.str(typ.SampleType))
				pr.SampleType().SetUnitStrindex(b.str(typ.SampleUnit))
				pr.PeriodType().SetTypeStrindex(b.str(typ.PeriodType))
				pr.PeriodType().SetUnitStrindex(b.str(typ.PeriodUnit))
				pr.SetPeriod(int64(10_000_000))
				pr.SetDurationNano(uint64(profileqlPerRound) * uint64(profileqlSampleInterval))
				pr.SetTime(pcommon.Timestamp(roundStart.UnixNano()))

				var id [16]byte
				id[0], id[1], id[2], id[3] = byte(round), byte(s), byte(p), byte(ti)
				pr.SetProfileID(pprofile.ProfileID(id))

				for i := range profileqlPerRound {
					path := stacks[s][i%len(stacks[s])]
					ts := roundStart.Add(time.Duration(i) * profileqlSampleInterval)

					sm := pr.Samples().AppendEmpty()
					sm.SetStackIndex(b.stack(path))
					sm.Values().Append(profileqlSampleVal)
					sm.TimestampsUnixNano().Append(uint64(ts.UnixNano()))
					sm.AttributeIndices().Append(threads[i%len(threads)])
				}
			}
		}
	}

	return pd
}

// profileqlFixture ingests the canonical corpus into a memory-backed store, flushing each round to
// its own immutable part. The background maintenance loop is disabled so the layout (one part per
// round) is deterministic and no flush races the timed loop.
func profileqlFixture(b *testing.B) *profileqlCorpus {
	b.Helper()

	ctx := context.Background()
	store, err := storage.Open(ctx, storage.Options{},
		storage.WithBackend(backend.Memory()),
		storage.WithFlushInterval(-1),
	)
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })

	be := storagebackend.New(store)

	stacks := make([][][]string, profileqlServices)
	for s := range profileqlServices {
		stacks[s] = profileqlStacks(s)
	}

	for round := range profileqlRounds {
		require.NoError(b, be.ConsumeProfiles(ctx, profileqlRound(round, stacks)))
		require.NoError(b, store.Admin().Flush(ctx, "", signal.Profile))
	}

	span := time.Duration(profileqlPerRound) * profileqlSampleInterval

	return &profileqlCorpus{
		querier:   be.Profiles(),
		store:     store,
		stacks:    stacks,
		start:     profileqlEpoch,
		end:       profileqlEpoch.Add(profileqlRounds*span + time.Second),
		lastRound: profileqlEpoch.Add((profileqlRounds - 1) * span),
	}
}

// profileqlExpectedNodes returns the number of flame-tree nodes the given services' stacks merge
// into: every distinct root→leaf prefix, plus the synthetic root.
func (c *profileqlCorpus) expectedNodes(services ...int) int {
	seen := map[string]struct{}{}
	for _, s := range services {
		for _, path := range c.stacks[s] {
			for i := range path {
				seen[strings.Join(path[:i+1], ";")] = struct{}{}
			}
		}
	}
	return len(seen) + 1
}

// profileqlCountNodes counts the nodes of a flame tree.
func profileqlCountNodes(n *profilestorage.FlameNode) int {
	if n == nil {
		return 0
	}
	total := 1
	for _, c := range n.Children {
		total += profileqlCountNodes(c)
	}
	return total
}

// profileqlSelect is one SelectMergeProfile sub-benchmark: it asserts the tree once (so a silently
// empty result can never look fast), then times repeated evaluations at the logical scan rate.
func profileqlSelect(
	b *testing.B,
	c *profileqlCorpus,
	params profilestorage.SelectProfileParams,
	wantRows, wantNodes int,
) {
	b.Helper()

	ctx := context.Background()

	tree, err := c.querier.SelectMergeProfile(ctx, params)
	require.NoError(b, err)
	require.Equal(b, int64(wantRows)*profileqlSampleVal, tree.Total())
	require.Equal(b, wantNodes, profileqlCountNodes(tree.Root))

	b.SetBytes(int64(wantRows) * profileqlRowBytes)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := c.querier.SelectMergeProfile(ctx, params); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGoldenProfileQL is the definitive ProfileQL read set over the storage engine.
//
//	select_merge_profile/all_services  — the flame-tree merge over every cpu stream (heaviest path)
//	select_merge_profile/single_pod    — the same, narrowed to one stream (postings selectivity)
//	select_merge_profile/regex_service — a regex label matcher over half the services
//	select_merge_profile/alloc_space   — the second profile type (type folded into stream identity)
//	select_merge_profile/recent_window — the last round's window only (part/time pruning)
//	profile_types                      — stream enumeration + reserved-label projection
//	label_names                        — distinct user-label names across all streams
//	label_values/pod                   — values of the high-cardinality label
//	profile_resolver                   — symbol side-store snapshot + decode, isolated
func BenchmarkGoldenProfileQL(b *testing.B) {
	c := profileqlFixture(b)

	cpu, alloc := profileqlTypes[0], profileqlTypes[1]

	b.Run("select_merge_profile/all_services", func(b *testing.B) {
		profileqlSelect(b, c, profilestorage.SelectProfileParams{
			Type:  cpu,
			Start: c.start,
			End:   c.end,
		}, profileqlRowsPerType, c.expectedNodes(0, 1, 2, 3))
	})

	b.Run("select_merge_profile/single_pod", func(b *testing.B) {
		profileqlSelect(b, c, profilestorage.SelectProfileParams{
			Type: cpu,
			Matchers: []profileql.LabelMatcher{
				{Label: "service.name", Op: profileql.OpEq, Value: profileqlService(0)},
				{Label: "pod", Op: profileql.OpEq, Value: profileqlPod(0, 0)},
			},
			Start: c.start,
			End:   c.end,
		}, profileqlRowsPerStream, c.expectedNodes(0))
	})

	b.Run("select_merge_profile/regex_service", func(b *testing.B) {
		profileqlSelect(b, c, profilestorage.SelectProfileParams{
			Type: cpu,
			Matchers: []profileql.LabelMatcher{{
				Label: "service.name",
				Op:    profileql.OpRe,
				Value: "svc-0|svc-1",
				Re:    regexp.MustCompile(`^(?:svc-0|svc-1)$`),
			}},
			Start: c.start,
			End:   c.end,
		}, 2*profileqlPods*profileqlRowsPerStream, c.expectedNodes(0, 1))
	})

	b.Run("select_merge_profile/alloc_space", func(b *testing.B) {
		profileqlSelect(b, c, profilestorage.SelectProfileParams{
			Type:  alloc,
			Start: c.start,
			End:   c.end,
		}, profileqlRowsPerType, c.expectedNodes(0, 1, 2, 3))
	})

	b.Run("select_merge_profile/recent_window", func(b *testing.B) {
		profileqlSelect(b, c, profilestorage.SelectProfileParams{
			Type:  cpu,
			Start: c.lastRound,
			End:   c.end,
		}, profileqlRowsPerType/profileqlRounds, c.expectedNodes(0, 1, 2, 3))
	})

	b.Run("profile_types", func(b *testing.B) {
		ctx := context.Background()
		opts := profilestorage.ProfileTypesOptions{Start: c.start, End: c.end}

		types, err := c.querier.ProfileTypes(ctx, opts)
		require.NoError(b, err)
		require.Len(b, types, len(profileqlTypes))

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			if _, err := c.querier.ProfileTypes(ctx, opts); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("label_names", func(b *testing.B) {
		ctx := context.Background()
		opts := profilestorage.LabelNamesOptions{Start: c.start, End: c.end}

		names, err := c.querier.LabelNames(ctx, opts)
		require.NoError(b, err)
		require.Equal(b, []string{"pod", "region", "service.name"}, names)

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			if _, err := c.querier.LabelNames(ctx, opts); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("label_values/pod", func(b *testing.B) {
		ctx := context.Background()
		opts := profilestorage.LabelValuesOptions{Start: c.start, End: c.end}

		values, err := c.querier.LabelValues(ctx, "pod", opts)
		require.NoError(b, err)
		require.Len(b, values, profileqlServices*profileqlPods)

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			if _, err := c.querier.LabelValues(ctx, "pod", opts); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("profile_resolver", func(b *testing.B) {
		ctx := context.Background()

		r, err := c.store.ProfileResolver(ctx, "")
		require.NoError(b, err)
		require.NotNil(b, r)

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			if _, err := c.store.ProfileResolver(ctx, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
}
