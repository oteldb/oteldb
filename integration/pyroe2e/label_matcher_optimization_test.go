package pyroe2e_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	querierv1 "github.com/grafana/pyroscope/api/gen/proto/go/querier/v1"
	"github.com/grafana/pyroscope/api/gen/proto/go/querier/v1/querierv1connect"
	typesv1 "github.com/grafana/pyroscope/api/gen/proto/go/types/v1"

	"github.com/oteldb/oteldb/internal/profilehandler"
	"github.com/oteldb/oteldb/internal/profileql"
	"github.com/oteldb/oteldb/internal/profileql/profileqlengine"
	"github.com/oteldb/oteldb/internal/pyroscopeapi"
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

func TestStorageSyntheticProfileTypeSelectors(t *testing.T) {
	t.Parallel()
	db, start, end, _ := newBackend(t, []string{"frontend", "backend"}, 2)
	querier := db.Profiles()
	engine := profileqlengine.NewEngine(querier, profileqlengine.Options{})
	_, handler := querierv1connect.NewQuerierServiceHandler(profilehandler.NewQuerierService(querier, engine))
	connectServer := httptest.NewServer(handler)
	t.Cleanup(connectServer.Close)
	client := querierv1connect.NewQuerierServiceClient(connectServer.Client(), connectServer.URL)
	legacy, err := pyroscopeapi.NewServer(profilehandler.NewPyroscopeAPI(querier, engine, profilehandler.PyroscopeAPIOptions{}))
	require.NoError(t, err)
	legacyServer := httptest.NewServer(legacy)
	t.Cleanup(legacyServer.Close)
	lo, hi := start.Add(-time.Second).UnixMilli(), end.Add(time.Second).UnixMilli()
	for _, label := range []string{profileql.LabelName, profileql.LabelProfileType} {
		t.Run(label, func(t *testing.T) {
			for _, tt := range []struct {
				name, query string
				want        []string
			}{
				{"unfiltered", "", []string{profileTypeID}},
				{"matching service", profileTypeID + `{service.name=~"frontend|backend"}`, []string{profileTypeID}},
				{"missing service", profileTypeID + `{service.name=~"absent|missing"}`, nil},
				{"missing type", `memory:inuse_space:bytes:space:bytes{service.name="frontend"}`, nil},
			} {
				t.Run(tt.name, func(t *testing.T) {
					values, err := client.LabelValues(t.Context(), connect.NewRequest(&typesv1.LabelValuesRequest{
						Name: label, Matchers: []string{tt.query}, Start: lo, End: hi,
					}))
					require.NoError(t, err)
					require.ElementsMatch(t, tt.want, values.Msg.Names, "Connect must preserve type and label predicates")
					params := url.Values{
						"label": {label}, "query": {tt.query},
						"from": {strconv.FormatInt(lo, 10)}, "until": {strconv.FormatInt(hi, 10)},
					}
					request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, legacyServer.URL+"/label-values?"+params.Encode(), http.NoBody)
					require.NoError(t, err)
					response, err := legacyServer.Client().Do(request)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, response.Body.Close()) })
					require.Equal(t, http.StatusOK, response.StatusCode)
					var legacyValues []string
					require.NoError(t, json.NewDecoder(response.Body).Decode(&legacyValues))
					require.ElementsMatch(t, tt.want, legacyValues, "legacy API must preserve type and label predicates")
				})
			}
		})
	}
}
