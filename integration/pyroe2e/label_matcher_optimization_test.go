package pyroe2e_test

import (
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

func TestStorageLabelMatcherEndpoints(t *testing.T) {
	t.Parallel()
	db, start, end, _ := newBackend(t, []string{"frontend", "backend", "xfrontendy"}, 2)
	querier := db.Profiles()
	svc := profilehandler.NewQuerierService(querier, profileqlengine.NewEngine(querier, profileqlengine.Options{}))
	_, handler := querierv1connect.NewQuerierServiceHandler(svc)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := querierv1connect.NewQuerierServiceClient(server.Client(), server.URL)
	for _, tt := range []struct {
		selector string
		services []string
	}{
		{`{service.name=~"frontend|backend"}`, []string{"frontend", "backend"}},
		{`{service.name!~"frontend|backend"}`, []string{"xfrontendy"}},
		{`{service.name=~"frontend|"}`, []string{"frontend"}},
		{`{service.name=~"(?i)FRONTEND"}`, []string{"frontend"}},
	} {
		t.Run(tt.selector, func(t *testing.T) {
			ctx := t.Context()
			lo, hi := start.Add(-time.Second).UnixMilli(), end.Add(time.Second).UnixMilli()
			values, err := client.LabelValues(ctx, connect.NewRequest(&typesv1.LabelValuesRequest{
				Name: "service.name", Matchers: []string{tt.selector}, Start: lo, End: hi,
			}))
			require.NoError(t, err)
			require.ElementsMatch(t, tt.services, values.Msg.Names)
			series, err := client.Series(ctx, connect.NewRequest(&querierv1.SeriesRequest{
				LabelNames: []string{"service.name"}, Matchers: []string{tt.selector}, Start: lo, End: hi,
			}))
			require.NoError(t, err)
			var services []string
			for _, set := range series.Msg.LabelsSet {
				require.Len(t, set.Labels, 1)
				services = append(services, set.Labels[0].Value)
			}
			require.ElementsMatch(t, tt.services, services)
			names, err := client.LabelNames(ctx, connect.NewRequest(&typesv1.LabelNamesRequest{
				Matchers: []string{tt.selector}, Start: lo, End: hi,
			}))
			require.NoError(t, err)
			require.Contains(t, names.Msg.Names, "service.name")
		})
	}
}
