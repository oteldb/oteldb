package tempoe2e_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
)

type tagValueMatcherSource struct {
	storagebackend.Source
	fetches atomic.Int64
	series  atomic.Int64
}

func (s *tagValueMatcherSource) TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	s.fetches.Add(1)
	return s.Source.TraceFetcher(tenants...)
}

func (s *tagValueMatcherSource) TraceSeries(ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64) ([]signal.Series, error) {
	s.series.Add(1)
	return s.Source.TraceSeries(ctx, tenant, matchers, start, end)
}

func TestStorageFilteredTagValues(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	db := storagebackend.New(store)
	base := time.Unix(1_700_000_000, 0).UTC()
	td := ptrace.NewTraces()
	for i, service := range []string{"foo", "bar", "xfooy"} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", service)
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("library-" + service)
		ss.Scope().SetVersion("1.0")
		for n := range 2 {
			sp := ss.Spans().AppendEmpty()
			sp.SetTraceID(pcommon.TraceID{15: byte(i + 1)})
			sp.SetSpanID(pcommon.SpanID{7: byte(n + 1)})
			sp.SetName(service + ".root")
			sp.Status().SetMessage("message-" + service)
			sp.Attributes().PutStr("route", "root")
			if n > 0 {
				sp.SetParentSpanID(pcommon.SpanID{7: 1})
				sp.SetName(service + ".child")
				sp.Attributes().PutStr("route", "child")
			}
			sp.SetStartTimestamp(pcommon.Timestamp(base.Add(time.Duration(n) * time.Millisecond).UnixNano()))
			sp.SetEndTimestamp(pcommon.Timestamp(base.Add(time.Second).UnixNano()))
		}
	}
	require.NoError(t, db.ConsumeTraces(ctx, td))
	source := &tagValueMatcherSource{Source: store}
	querier := storagebackend.NewQuery(source).Traces()
	client := setupDB(t, integration.TraceProvider(t), querier, querier)
	for _, phase := range []string{"head", "parts"} {
		if phase == "parts" {
			require.NoError(t, store.Admin().Flush(ctx, "default", signal.Trace))
		}
		t.Run(phase, func(t *testing.T) {
			for _, tt := range []struct {
				attr, query string
				values      []string
			}{
				{`name`, `{resource.service.name =~ "^(foo|bar)$"}`, []string{"foo.root", "foo.child", "bar.root", "bar.child"}},
				{`name`, `{resource.service.name =~ "foo"}`, []string{"foo.root", "foo.child", "xfooy.root", "xfooy.child"}},
				{`resource.service.name`, `{resource.service.name =~ "^(foo|bar)$"}`, []string{"foo", "bar"}},
				{`resource.service.name`, `{resource.service.name !~ "^(foo|bar)$"}`, []string{"xfooy"}},
				{`span.route`, `{resource.service.name = "foo" && name =~ "child$"}`, []string{"child"}},
				{`name`, `{rootName = "foo.root" && name =~ "child$"}`, []string{"foo.child"}},
				{`rootName`, `{resource.service.name = "foo"}`, []string{"foo.root"}},
				{`rootServiceName`, `{rootName = "foo.root"}`, []string{"foo"}},
				{`statusMessage`, `{resource.service.name = "foo"}`, []string{"message-foo"}},
				{`instrumentation:name`, `{resource.service.name = "foo"}`, []string{"library-foo"}},
				{`instrumentation:version`, `{resource.service.name = "foo"}`, []string{"1.0"}},
				{`resource.service.name`, `{resource.absent = nil}`, []string{"foo", "bar", "xfooy"}},
				{`name`, `{resource.service.name = "foo" && name = "bar.child"}`, nil},
			} {
				t.Run(fmt.Sprintf("%s/%s", tt.attr, tt.query), func(t *testing.T) {
					fetches, series := source.fetches.Load(), source.series.Load()
					response, err := client.SearchTagValuesV2(ctx, tempoapi.SearchTagValuesV2Params{
						AttributeSelector: tt.attr, Q: tempoapi.NewOptString(tt.query),
						Start: tempoTime(base.Add(-time.Second)), End: tempoTime(base.Add(2 * time.Second)),
					})
					require.NoError(t, err)
					var values []string
					for _, tag := range response.TagValues {
						values = append(values, tag.Value.Or(""))
					}
					require.ElementsMatch(t, tt.values, values)
					if tt.attr == "resource.service.name" {
						require.Equal(t, fetches, source.fetches.Load(), "identity-only autocomplete must not fetch span columns")
						require.Greater(t, source.series.Load(), series)
					}
				})
			}
		})
	}
}
