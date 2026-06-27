package pyroe2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/profilestorage"
)

// TestStorageBackend exercises the embedded storage profiles backend end-to-end: generated CPU
// profiles for several services are ingested through the storage ingestion sink (ConsumeProfiles),
// then queried back through the ProfileQL engine and the profiles querier — profile-type and label
// enumeration, a merged flamegraph over all services, and a per-service filtered flamegraph.
//
// Like the other storage_test suites it needs no ClickHouse or Docker — the storage engine runs
// fully in memory — so it runs as a normal unit test.
func TestStorageBackend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	services := []string{"frontend", "backend"}
	backend, start, end, total := newBackend(t, services, 50)

	pq := backend.Profiles()
	opts := profilestorage.ProfileTypesOptions{Start: start.Add(-time.Hour), End: end.Add(time.Hour)}

	t.Run("ProfileTypes", func(t *testing.T) {
		types, err := pq.ProfileTypes(ctx, opts)
		require.NoError(t, err)
		require.Len(t, types, 1)
		require.Equal(t, profileTypeID, types[0].ID())
		require.Equal(t, profileSampleType, types[0].SampleType)
		require.Equal(t, profileSampleUnit, types[0].SampleUnit)
	})

	t.Run("Labels", func(t *testing.T) {
		names, err := pq.LabelNames(ctx, profilestorage.LabelNamesOptions{Start: start.Add(-time.Hour), End: end.Add(time.Hour)})
		require.NoError(t, err)
		require.Contains(t, names, "service.name")

		values, err := pq.LabelValues(ctx, "service.name", profilestorage.LabelValuesOptions{Start: start.Add(-time.Hour), End: end.Add(time.Hour)})
		require.NoError(t, err)
		require.ElementsMatch(t, services, values, "label values are the service names")
	})

	engine := profileqlengine.NewEngine(pq, profileqlengine.Options{})
	params := profileqlengine.EvalParams{Start: start.Add(-time.Hour), End: end.Add(time.Hour)}

	t.Run("MergeAllServices", func(t *testing.T) {
		res, err := engine.Select(ctx, profileTypeID, params)
		require.NoError(t, err)
		require.NotNil(t, res.Tree.Root)
		// The merged flamegraph accumulates every ingested sample value.
		require.Equal(t, total, res.Tree.Total())
		require.NotEmpty(t, res.Tree.Root.Children)
	})

	t.Run("FilterByService", func(t *testing.T) {
		res, err := engine.Select(ctx, profileTypeID+`{service.name="frontend"}`, params)
		require.NoError(t, err)
		require.Positive(t, res.Tree.Total())
		require.Less(t, res.Tree.Total(), total, "single service is a subset of the total")
	})

	t.Run("Flamebearer", func(t *testing.T) {
		fb, err := engine.Eval(ctx, profileTypeID, params)
		require.NoError(t, err)
		require.NotNil(t, fb)
		require.NotEmpty(t, fb.Flamebearer.Names, "flamegraph should resolve function names")
	})

	t.Run("NoMatch", func(t *testing.T) {
		res, err := engine.Select(ctx, profileTypeID+`{service.name="does-not-exist"}`, params)
		require.NoError(t, err)
		require.Zero(t, res.Tree.Total())
	})

	// A non-matching profile type yields an empty tree.
	t.Run("UnknownType", func(t *testing.T) {
		res, err := engine.Select(ctx, `memory:inuse_space:bytes:space:bytes`, params)
		require.NoError(t, err)
		require.Zero(t, res.Tree.Total())
	})
}
