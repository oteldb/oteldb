package pyroe2e_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	querierv1 "github.com/grafana/pyroscope/api/gen/proto/go/querier/v1"
	"github.com/grafana/pyroscope/api/gen/proto/go/querier/v1/querierv1connect"
	typesv1 "github.com/grafana/pyroscope/api/gen/proto/go/types/v1"

	"github.com/oteldb/oteldb/internal/profilehandler"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
)

// TestQuerierServiceConnect exercises the connect QuerierService API (the one Grafana's built-in
// Pyroscope datasource speaks) end-to-end: generated CPU profiles are ingested into the in-memory
// storage backend, served through a real connect HTTP server, and queried back with the generated
// connect client. It mirrors TestStorageBackend but over the wire protocol.
func TestQuerierServiceConnect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	services := []string{"frontend", "backend"}
	backend, start, end, total := newBackend(t, services, 50)

	pq := backend.Profiles()
	engine := profileqlengine.NewEngine(pq, profileqlengine.Options{})
	svc := profilehandler.NewQuerierService(pq, engine)

	path, handler := querierv1connect.NewQuerierServiceHandler(svc)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := querierv1connect.NewQuerierServiceClient(srv.Client(), srv.URL)
	startMs := start.Add(-time.Hour).UnixMilli()
	endMs := end.Add(time.Hour).UnixMilli()

	t.Run("ProfileTypes", func(t *testing.T) {
		resp, err := client.ProfileTypes(ctx, connect.NewRequest(&querierv1.ProfileTypesRequest{
			Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		require.Len(t, resp.Msg.ProfileTypes, 1)
		require.Equal(t, profileTypeID, resp.Msg.ProfileTypes[0].ID)
		require.Equal(t, profileSampleType, resp.Msg.ProfileTypes[0].SampleType)
	})

	t.Run("LabelNames", func(t *testing.T) {
		resp, err := client.LabelNames(ctx, connect.NewRequest(&typesv1.LabelNamesRequest{
			Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		require.Contains(t, resp.Msg.Names, "service.name")
	})

	t.Run("LabelValues", func(t *testing.T) {
		resp, err := client.LabelValues(ctx, connect.NewRequest(&typesv1.LabelValuesRequest{
			Name: "service.name", Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		require.ElementsMatch(t, services, resp.Msg.Names)
	})

	t.Run("LabelValuesProfileType", func(t *testing.T) {
		// The synthetic profile-type label resolves to the profile type IDs.
		resp, err := client.LabelValues(ctx, connect.NewRequest(&typesv1.LabelValuesRequest{
			Name: "__profile_type__", Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		require.Equal(t, []string{profileTypeID}, resp.Msg.Names)
	})

	t.Run("Series", func(t *testing.T) {
		resp, err := client.Series(ctx, connect.NewRequest(&querierv1.SeriesRequest{
			LabelNames: []string{"service.name"}, Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		var got []string
		for _, ls := range resp.Msg.LabelsSet {
			require.Len(t, ls.Labels, 1)
			require.Equal(t, "service.name", ls.Labels[0].Name)
			got = append(got, ls.Labels[0].Value)
		}
		require.ElementsMatch(t, services, got)
	})

	t.Run("SelectMergeStacktraces", func(t *testing.T) {
		resp, err := client.SelectMergeStacktraces(ctx, connect.NewRequest(&querierv1.SelectMergeStacktracesRequest{
			ProfileTypeID: profileTypeID, Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		fg := resp.Msg.Flamegraph
		require.NotNil(t, fg)
		require.Equal(t, total, fg.Total)
		require.Contains(t, fg.Names, "main", "function names should be resolved")
		require.NotEmpty(t, fg.Levels)
	})

	t.Run("SelectMergeStacktracesFiltered", func(t *testing.T) {
		resp, err := client.SelectMergeStacktraces(ctx, connect.NewRequest(&querierv1.SelectMergeStacktracesRequest{
			ProfileTypeID: profileTypeID, LabelSelector: `{service.name="frontend"}`, Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		require.Positive(t, resp.Msg.Flamegraph.Total)
		require.Less(t, resp.Msg.Flamegraph.Total, total, "single service is a subset")
	})

	t.Run("SelectMergeProfile", func(t *testing.T) {
		resp, err := client.SelectMergeProfile(ctx, connect.NewRequest(&querierv1.SelectMergeProfileRequest{
			ProfileTypeID: profileTypeID, Start: startMs, End: endMs,
		}))
		require.NoError(t, err)
		require.NotEmpty(t, resp.Msg.Sample, "pprof profile carries samples")
		require.NotEmpty(t, resp.Msg.Function, "pprof profile carries functions")
	})

	t.Run("SelectSeries", func(t *testing.T) {
		resp, err := client.SelectSeries(ctx, connect.NewRequest(&querierv1.SelectSeriesRequest{
			ProfileTypeID: profileTypeID, Start: startMs, End: endMs, Step: 60,
		}))
		require.NoError(t, err)
		require.Len(t, resp.Msg.Series, 1)
		var sum int64
		for _, p := range resp.Msg.Series[0].Points {
			sum += int64(p.Value)
		}
		require.Equal(t, total, sum, "bucketed totals sum to the overall total")
	})

	t.Run("GetProfileStats", func(t *testing.T) {
		resp, err := client.GetProfileStats(ctx, connect.NewRequest(&typesv1.GetProfileStatsRequest{}))
		require.NoError(t, err)
		require.True(t, resp.Msg.DataIngested)
	})
}
