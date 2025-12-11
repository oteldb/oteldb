package tempoe2e_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/integration/requirex"
	"github.com/go-faster/oteldb/integration/tempoe2e"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempohandler"
	"github.com/go-faster/oteldb/internal/traceql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

func readBatchSet(p string) (s tempoe2e.BatchSet, _ error) {
	f, err := os.Open(p)
	if err != nil {
		return s, err
	}
	defer func() {
		_ = f.Close()
	}()
	return tempoe2e.ParseBatchSet(f)
}

func setupDB(
	t *testing.T,
	provider trace.TracerProvider,
	querier tracestorage.Querier,
	engineQuerier traceqlengine.Querier,
) *tempoapi.Client {
	var engine *traceqlengine.Engine
	if engineQuerier != nil {
		engine = traceqlengine.NewEngine(engineQuerier, traceqlengine.Options{
			TracerProvider: provider,
		})
	}
	api := tempohandler.NewTempoAPI(querier, engine, tempohandler.TempoAPIOptions{
		EnableAutocompleteQuery: true,
	})
	tempoh, err := tempoapi.NewServer(api,
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	s := httptest.NewServer(tempoh)
	t.Cleanup(s.Close)

	c, err := tempoapi.NewClient(s.URL,
		tempoapi.WithClient(s.Client()),
		tempoapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)
	return c
}

func loadTestData(ctx context.Context, t *testing.T, inserter tracestorage.Inserter) tempoe2e.BatchSet {
	set, err := readBatchSet("_testdata/traces.json")
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Tags)
	require.NotEmpty(t, set.Traces)

	consumer := tracestorage.NewConsumer(inserter)
	for i, b := range set.Batches {
		if err := consumer.ConsumeTraces(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
	}
	return set
}

func runTest(
	ctx context.Context,
	t *testing.T,
	provider trace.TracerProvider,
	set tempoe2e.BatchSet,
	querier tracestorage.Querier,
	engineQuerier traceqlengine.Querier,
) {
	var (
		resourceTagNames = map[string]struct{}{}
		spanTagNames     = map[string]struct{}{}
	)
	for _, tags := range set.Tags {
		for _, tag := range tags {
			switch tag.Scope {
			case traceql.ScopeResource:
				resourceTagNames[tag.Name] = struct{}{}
			case traceql.ScopeSpan:
				spanTagNames[tag.Name] = struct{}{}
			default:
				t.Fatalf("unexpected scope %v", tag.Scope)
			}
		}
	}

	c := setupDB(t, provider, querier, engineQuerier)
	var (
		start = tempoapi.NewOptUnixSeconds(set.Start.AsTime().Add(-time.Second))
		end   = tempoapi.NewOptUnixSeconds(set.End.AsTime())
	)
	t.Run("SearchTags", func(t *testing.T) {
		for _, tt := range []struct {
			name    string
			params  tempoapi.SearchTagsParams
			want    []string
			wantErr bool
		}{
			{
				"All",
				tempoapi.SearchTagsParams{
					Start: start,
					End:   end,
				},
				maps.Keys(set.Tags),
				false,
			},
			{
				"Intrinsic",
				tempoapi.SearchTagsParams{
					Scope: tempoapi.NewOptTagScope(tempoapi.TagScopeIntrinsic),
					Start: start,
					End:   end,
				},
				traceql.IntrinsicNames(),
				false,
			},
			{
				"Resource",
				tempoapi.SearchTagsParams{
					Scope: tempoapi.NewOptTagScope(tempoapi.TagScopeResource),
					Start: start,
					End:   end,
				},
				maps.Keys(resourceTagNames),
				false,
			},
			{
				"Span",
				tempoapi.SearchTagsParams{
					Scope: tempoapi.NewOptTagScope(tempoapi.TagScopeSpan),
					Start: start,
					End:   end,
				},
				maps.Keys(spanTagNames),
				false,
			},
		} {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				a := require.New(t)

				r, err := c.SearchTags(ctx, tt.params)
				if tt.wantErr {
					var gotErr *tempoapi.ErrorStatusCode
					a.ErrorAs(err, &gotErr)
					return
				}
				a.NoError(err)

				requirex.Unique(t, r.TagNames)
				a.ElementsMatch(tt.want, r.TagNames)
			})
		}
	})
	t.Run("SearchTagsV2", func(t *testing.T) {
		for _, tt := range []struct {
			name    string
			params  tempoapi.SearchTagsV2Params
			want    map[tempoapi.TagScope][]string
			wantErr bool
		}{
			{
				"All",
				tempoapi.SearchTagsV2Params{
					Start: start,
					End:   end,
				},
				map[tempoapi.TagScope][]string{
					tempoapi.TagScopeIntrinsic: traceql.IntrinsicNames(),
					tempoapi.TagScopeResource:  maps.Keys(resourceTagNames),
					tempoapi.TagScopeSpan:      maps.Keys(spanTagNames),
				},
				false,
			},
			{
				"Intrinsic",
				tempoapi.SearchTagsV2Params{
					Scope: tempoapi.NewOptTagScope(tempoapi.TagScopeIntrinsic),
					Start: start,
					End:   end,
				},
				map[tempoapi.TagScope][]string{
					tempoapi.TagScopeIntrinsic: traceql.IntrinsicNames(),
				},
				false,
			},
			{
				"Resource",
				tempoapi.SearchTagsV2Params{
					Scope: tempoapi.NewOptTagScope(tempoapi.TagScopeResource),
					Start: start,
					End:   end,
				},
				map[tempoapi.TagScope][]string{
					tempoapi.TagScopeResource: maps.Keys(resourceTagNames),
				},
				false,
			},
			{
				"Span",
				tempoapi.SearchTagsV2Params{
					Scope: tempoapi.NewOptTagScope(tempoapi.TagScopeSpan),
					Start: start,
					End:   end,
				},
				map[tempoapi.TagScope][]string{
					tempoapi.TagScopeSpan: maps.Keys(spanTagNames),
				},
				false,
			},
		} {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				a := require.New(t)

				r, err := c.SearchTagsV2(ctx, tt.params)
				if tt.wantErr {
					var gotErr *tempoapi.ErrorStatusCode
					a.ErrorAs(err, &gotErr)
					return
				}
				a.NoError(err)

				dedup := map[tempoapi.TagScope]struct{}{}
				for _, s := range r.Scopes {
					a.NotContainsf(dedup, s.Name, "duplicate scope %v", s.Name)
					dedup[s.Name] = struct{}{}

					requirex.Unique(t, s.Tags)
					a.ElementsMatch(tt.want[s.Name], s.Tags)
				}
			})
		}
	})
	t.Run("SearchTagValues", func(t *testing.T) {
		a := require.New(t)

		for tagName, tags := range set.Tags {
			tagValues := map[string]struct{}{}
			for _, t := range tags {
				tagValues[t.Value] = struct{}{}
			}

			r, err := c.SearchTagValues(ctx, tempoapi.SearchTagValuesParams{
				TagName: tagName,
				Start:   start,
				End:     end,
			})
			a.NoError(err)
			a.Len(r.TagValues, len(tagValues))
			for _, val := range r.TagValues {
				a.Containsf(tagValues, val, "check tag %q", tagName)
			}
		}
	})
	t.Run("SearchTagValuesV2", func(t *testing.T) {
		t.Run("Attribute", func(t *testing.T) {
			a := require.New(t)

			for tagName, tags := range set.Tags {
				tagValues := map[string]struct{}{}
				for _, t := range tags {
					tagValues[t.Value] = struct{}{}
				}

				r, err := c.SearchTagValuesV2(ctx, tempoapi.SearchTagValuesV2Params{
					AttributeSelector: "." + tagName,
					Start:             start,
					End:               end,
				})
				a.NoError(err)
				a.Len(r.TagValues, len(tagValues))
				for _, val := range r.TagValues {
					a.Containsf(tagValues, val.Value, "check tag %q", tagName)
				}
			}
		})

		serviceNames := map[string]struct{}{}
		for _, t := range set.Tags["service.name"] {
			serviceNames[t.Value] = struct{}{}
		}

		for _, tt := range []struct {
			name     string
			params   tempoapi.SearchTagValuesV2Params
			wantType string
			want     []string
			wantErr  bool
		}{
			// Resource attribute.
			{
				"ResourceAttribute",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `resource.service.name`,
					Start:             start,
					End:               end,
				},
				"string",
				maps.Keys(serviceNames),
				false,
			},
			// Intrinsics.
			{
				"SpanDuration",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `duration`,
					Start:             start,
					End:               end,
				},
				"duration",
				nil,
				false,
			},
			{
				"SpanChildCount",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `childCount`,
					Start:             start,
					End:               end,
				},
				"integer",
				nil,
				false,
			},
			{
				"SpanName",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `name`,
					Start:             start,
					End:               end,
				},
				"string",
				maps.Keys(set.SpanNames),
				false,
			},
			{
				"SpanNameWithQuery",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `name`,
					Start:             start,
					End:               end,
					Q:                 tempoapi.NewOptString(`{ name = "authenticate" && .service.name = }`),
				},
				"string",
				[]string{"authenticate"},
				false,
			},
			{
				"SpanStatus",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `status`,
					Start:             start,
					End:               end,
				},
				"keyword",
				[]string{
					"unset",
					"ok",
					"error",
				},
				false,
			},
			{
				"SpanKind",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `kind`,
					Start:             start,
					End:               end,
				},
				"keyword",
				[]string{
					"unspecified",
					"internal",
					"server",
					"client",
					"producer",
					"consumer",
				},
				false,
			},
			{
				"SpanParent",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `parent`,
					Start:             start,
					End:               end,
				},
				"string",
				nil,
				false,
			},
			{
				"RootSpanName",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `rootName`,
					Start:             start,
					End:               end,
				},
				"string",
				maps.Keys(set.RootSpanNames),
				false,
			},
			{
				"RootSpanNameWithQuery",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `rootName`,
					Start:             start,
					End:               end,
					Q:                 tempoapi.NewOptString(`{ name = "list-articles" }`),
				},
				"string",
				[]string{"list-articles"},
				false,
			},
			{
				// Ensure that `rootName` would return nothing if we query a
				// non-root span.
				"RootSpanNameNoMatch",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `rootName`,
					Start:             start,
					End:               end,
					Q:                 tempoapi.NewOptString(`{ name = "authenticate" }`),
				},
				"string",
				nil,
				false,
			},
			{
				"RootServiceName",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `rootServiceName`,
					Start:             start,
					End:               end,
				},
				"string",
				maps.Keys(serviceNames),
				false,
			},
			{
				"TraceDuration",
				tempoapi.SearchTagValuesV2Params{
					AttributeSelector: `traceDuration`,
					Start:             start,
					End:               end,
				},
				"duration",
				nil,
				false,
			},
		} {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				a := require.New(t)

				r, err := c.SearchTagValuesV2(ctx, tt.params)
				if tt.wantErr {
					var gotErr *tempoapi.ErrorStatusCode
					a.ErrorAs(err, &gotErr)
					return
				}
				a.NoError(err)

				var got []string
				for _, v := range r.TagValues {
					a.Equal(tt.wantType, v.Type)
					got = append(got, v.Value)
				}
				requirex.Unique(t, got)
				a.ElementsMatch(tt.want, got)
			})
		}
	})
	t.Run("TraceByID", func(t *testing.T) {
		t.Run("Query", func(t *testing.T) {
			a := require.New(t)

			for traceID, trace := range set.Traces {
				uid := uuid.UUID(traceID)

				r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: otelstorage.TraceID(traceID).Hex()})
				a.NoError(err)
				a.IsType(&tempoapi.TraceByID{}, r)

				data, err := io.ReadAll(r.(*tempoapi.TraceByID))
				a.NoError(err)

				var u ptrace.ProtoUnmarshaler
				resp, err := u.UnmarshalTraces(data)
				a.NoError(err)

				a.Equal(resp.SpanCount(), len(trace.Spanset))
				resSpans := resp.ResourceSpans()
				for i := 0; i < resSpans.Len(); i++ {
					scopeSpans := resSpans.At(i).ScopeSpans()
					for i := 0; i < scopeSpans.Len(); i++ {
						spans := scopeSpans.At(i).Spans()
						for i := 0; i < spans.Len(); i++ {
							gotSpan := spans.At(i)
							spanID := gotSpan.SpanID()
							a.Contains(trace.Spanset, spanID, "check trace %q", uid)
							expectSpan := trace.Spanset[spanID]
							// Ensure that spans are equal.
							a.Equal(expectSpan.TraceID(), gotSpan.TraceID())
							a.Equal(expectSpan.SpanID(), gotSpan.SpanID())
							a.Equal(expectSpan.TraceState(), gotSpan.TraceState())
							a.Equal(expectSpan.ParentSpanID(), gotSpan.ParentSpanID())
							a.Equal(expectSpan.Name(), gotSpan.Name())
							a.Equal(expectSpan.Kind(), gotSpan.Kind())
							a.Equal(expectSpan.StartTimestamp(), gotSpan.StartTimestamp())
							a.Equal(expectSpan.EndTimestamp(), gotSpan.EndTimestamp())
							a.Equal(expectSpan.Status(), gotSpan.Status())
							a.Equal(expectSpan.Attributes().AsRaw(), gotSpan.Attributes().AsRaw())
						}
					}
				}
			}
		})

		t.Run("NotFound", func(t *testing.T) {
			a := require.New(t)
			r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: strings.Repeat("0", 16*2)})
			a.NoError(err)
			a.IsType(&tempoapi.TraceByIDNotFound{}, r)
		})
	})
	validateMetadata := func(
		a *require.Assertions,
		metadata tempoapi.TraceSearchMetadata,
		ensure selectedSpans,
	) {
		a.NotEmpty(metadata.RootTraceName)
		a.False(metadata.StartTimeUnixNano.IsZero())

		traceID := pcommon.TraceID(uuid.MustParse(metadata.TraceID))

		trace, ok := set.Traces[traceID]
		a.Truef(ok, "unknown trace %q", traceID)

		ensureSpanIDs, ok := ensure[traceID]
		a.Truef(ok, "trace %q should not be in result", traceID)

		spans := metadata.SpanSet.Value.Spans
		a.Len(spans, len(ensureSpanIDs))

		for _, gotSpan := range spans {
			r, err := hex.DecodeString(gotSpan.SpanID)
			a.NoError(err)
			a.Len(r, 8)
			spanID := *(*pcommon.SpanID)(r)

			// Check that span is expected in result.
			_, ok := ensureSpanIDs[spanID]
			a.Truef(ok, "span %q of trace %q should not be in result", spanID, traceID)

			// Get raw span data.
			a.Contains(trace.Spanset, spanID, "check trace %q", traceID)
			expectSpan := trace.Spanset[spanID]

			a.Equal(expectSpan.Name(), gotSpan.Name.Or(""))
			start := gotSpan.StartTimeUnixNano.UnixNano()
			end := start + gotSpan.DurationNanos
			a.Equal(int64(expectSpan.StartTimestamp()), start)
			a.Equal(int64(expectSpan.EndTimestamp()), end)

			a.Equal(
				getRawMapFromAPI(gotSpan.Attributes),
				expectSpan.Attributes().AsRaw(),
			)
		}
	}

	t.Run("SearchWithLogfmt", func(t *testing.T) {
		t.Run("Search", func(t *testing.T) {
			postOkSet := selectTraces(set, byTags{
				"http.method":      pcommon.NewValueStr("POST"),
				"http.status_code": pcommon.NewValueInt(200),
			})

			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Tags: tempoapi.NewOptString(`http.method=POST http.status_code=200`),
			})
			a.NoError(err)
			a.Len(r.Traces, len(postOkSet))

			for _, metadata := range r.Traces {
				validateMetadata(a, metadata, postOkSet)
			}
		})
		t.Run("Limit", func(t *testing.T) {
			getOkSet := selectTraces(set, byTags{
				"http.method":      pcommon.NewValueStr("GET"),
				"http.status_code": pcommon.NewValueInt(200),
			})

			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Tags:  tempoapi.NewOptString(`http.method=GET http.status_code=200`),
				Limit: tempoapi.NewOptInt(1),
			})
			a.NoError(err)
			a.Len(r.Traces, 1)

			validateMetadata(a, r.Traces[0], getOkSet)
		})
		t.Run("TagsNotExist", func(t *testing.T) {
			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Tags: tempoapi.NewOptString(`clearly.not.exist=amongus http.method=GET`),
			})
			a.NoError(err)
			a.Empty(r.Traces)
		})
	})
	t.Run("SearchWithTraceQL", func(t *testing.T) {
		if engineQuerier == nil {
			t.Skip("TraceQL engine is not available")
			return
		}

		postOkSpans := selectSpans(set, byTags{
			"http.method":      pcommon.NewValueStr("POST"),
			"http.status_code": pcommon.NewValueInt(200),
		})
		t.Run("Search", func(t *testing.T) {
			queries := []struct {
				query      string
				matcherSet selectedSpans
			}{
				// These queries are effectively the same, but written in a different way.
				// Spanset filter test.
				{`{ .http.method = "POST" && .http.status_code = 200 }`, postOkSpans},
				{`{ span.http.method = "POST" && span.http.status_code = 200 }`, postOkSpans},
				{`{ .http.method = "POST" && (.http.status_code >= 200 && .http.status_code <= 200) }`, postOkSpans},
				{`{ .http.method = "POST" && (.http.status_code > 199 && .http.status_code < 201) }`, postOkSpans},
				{`{ .http.method = "POST" && (.http.status_code = 200 || .http.status_code = 1000) }`, postOkSpans},
				{`{ .http.method = "POST" && (.http.status_code - 100) = 100 }`, postOkSpans},
				{`{ .http.method =~ "^POST$" && .http.status_code = 200 }`, postOkSpans},
				{`{ .http.method !~ "(GET|DELETE|PUT|PATCH|TRACE|OPTIONS)" && .http.status_code = 200 }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 && duration > 0ns }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 && traceDuration > 0ns }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 && status != error }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 && kind != unspecified }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 && name != "" }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 && rootName != "" }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 && rootServiceName = "shop-backend" }`, postOkSpans},
				// Scalar filter test.
				{`{ .http.method = "POST" && .http.status_code = 200 } | count() > 0`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | min(.http.status_code) >= 0`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | max(.http.status_code) > 0`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | sum(.http.status_code) > 0`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | avg(.http.status_code) > 0`, postOkSpans},
				// Every span in a set must have status_code = 200, so average, minimum and maximum must be equal to 200 too.
				{`{ .http.method = "POST" && .http.status_code = 200 } | min(.http.status_code) = 200`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | max(.http.status_code) = 200`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | avg(.http.status_code) = 200`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | sum(.http.status_code) >= 200`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | 200 = min(.http.status_code)`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | min(.http.status_code)+min(.http.status_code) = 400`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | (min(.http.status_code)+min(.http.status_code))/2 = 200`, postOkSpans},
				// Expression `sum(.foo) / count()` is same as `avg(.foo)`.
				{`{ .http.method = "POST" && .http.status_code = 200 } | sum(.http.status_code) / count() = 200`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } | sum(.http.status_code) / count() = avg(.http.status_code)`, postOkSpans},
				// Binary spanset expression test.
				{`{ .http.method = "POST" && .http.status_code = 200 } && { .http.method = "POST" && .http.status_code = 200 }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } || { .http.method = "POST" && .http.status_code = 200 }`, postOkSpans},
				{`{ .http.method = "POST" && .http.status_code = 200 } ~ { .http.method = "POST" && .http.status_code = 200 }`, postOkSpans},
				// Binary expression test.
				{
					`( { .http.method = "POST" && .http.status_code = 200 } | count() > 0 )
					 &&
    				 ( { .http.method = "POST" && .http.status_code = 200 } | count() > 0 )`,
					postOkSpans,
				},
				{
					`( { .http.method = "POST" && .http.status_code = 200 } | count() > 0 )
					 ||
    				 ( { .http.method = "POST" && .http.status_code = 200 } | count() > 0 )`,
					postOkSpans,
				},
				{
					`( { .http.method = "POST" && .http.status_code = 200 } | count() > 0 )
					 ~
    				 ( { .http.method = "POST" && .http.status_code = 200 } | count() > 0 )`,
					postOkSpans,
				},

				// Other queries.
				{
					`{ .http.method = "GET" && .http.status_code = 200 && .net.host.name = "shop-backend.local" }`,
					selectSpans(set, byTags{
						"http.method":      pcommon.NewValueStr("GET"),
						"http.status_code": pcommon.NewValueInt(200),
						"net.host.name":    pcommon.NewValueStr("shop-backend.local"),
					}),
				},
				{
					`{ name = "list-articles" }`,
					selectSpans(set, byName("list-articles")),
				},
				{
					`{ name = "list-articles" || name = "clearly-not-exist-name" }`,
					selectSpans(set, byName("list-articles")),
				},
				// Empty set.
				{`{ resource.http.method = "POST" }`, nil},
				{`{ duration > 10h }`, nil},
				{`{ traceDuration > 10h }`, nil},
				{`{ kind = unspecified }`, nil},
				{`{ .http.status_code = 200 } | min(.http.status_code) < 0`, nil},
				{`{ .http.status_code = 200 } | max(.http.status_code) < 0`, nil},
				{`{ .http.status_code = 200 } | sum(.http.status_code) < 0`, nil},
				{`{ .http.status_code = 200 } | avg(.http.status_code) < 0`, nil},
				{`count() > 1000`, nil},
				{`count() < 0`, nil},
				// Ensure that engine properly handles types mismatch.
				{`{ .http.status_code = "200" }`, nil},
				{`{ .http.status_code =~ "^POST$" }`, nil},
				// Search materialized attributes.
				{`{ duration < 0s }`, nil},
				{`{ name = "clearly-does-not-exist" }`, nil},
				{`{ status = ok && status = error }`, nil},
				{`{ kind = client && kind = server }`, nil},
				{`{ .service.namespace = "clearly-does-not-exist" }`, nil},
				{`{ .service.name = "clearly-does-not-exist" }`, nil},
				{`{ .service.instance.id = "clearly-does-not-exist" }`, nil},
			}
			for i, tt := range queries {
				tt := tt
				t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
					t.Parallel()

					defer func() {
						if t.Failed() {
							t.Logf("Query: \n%s", tt.query)
						}
					}()

					a := require.New(t)
					r, err := c.Search(ctx, tempoapi.SearchParams{
						Q:     tempoapi.NewOptString(tt.query),
						Limit: tempoapi.NewOptInt(1_000),
					})
					a.NoError(err)

					for _, metadata := range r.Traces {
						validateMetadata(a, metadata, tt.matcherSet)
					}
					assert.Equal(t, len(tt.matcherSet), len(r.Traces), "matcher set length")
					for traceID, v := range tt.matcherSet {
						for spanID := range v {
							found := false
							for _, metadata := range r.Traces {
								if metadata.TraceID != traceID.String() {
									continue
								}
								for _, gotSpan := range metadata.SpanSet.Value.Spans {
									if gotSpan.SpanID == spanID.String() {
										found = true
										break
									}
								}
							}
							if found {
								t.Logf("[%s-%s] ok", traceID, spanID)
							} else {
								t.Logf("[%s-%s] not found", traceID, spanID)
							}
						}
					}

					// Ensure that local in-memory engine gives the same result.
					r2, err := set.Engine.Eval(ctx, tt.query, traceqlengine.EvalParams{Limit: 200})
					a.NoError(err)

					for _, metadata := range r2.Traces {
						validateMetadata(a, metadata, tt.matcherSet)
					}
					assert.Equal(t, len(r2.Traces), len(r.Traces), "in-memory engine length")

					// Log difference.
					inMemory := map[string]struct{}{}
					for _, metadata := range r2.Traces {
						inMemory[metadata.TraceID] = struct{}{}
					}
					got := map[string]struct{}{}
					for _, metadata := range r.Traces {
						got[metadata.TraceID] = struct{}{}
						if _, ok := inMemory[metadata.TraceID]; ok {
							continue
						}
						t.Logf("[%q]: unexpected", metadata.TraceID)
					}
					for _, metadata := range r2.Traces {
						if _, ok := got[metadata.TraceID]; ok {
							continue
						}
						t.Logf("[%q]: missing", metadata.TraceID)
					}
				})
			}
		})
		t.Run("Limit", func(t *testing.T) {
			getOkSpans := selectSpans(set, byTags{
				"http.method":      pcommon.NewValueStr("GET"),
				"http.status_code": pcommon.NewValueInt(200),
			})

			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Q:     tempoapi.NewOptString(`{ .http.method = "GET" && .http.status_code = 200 }`),
				Limit: tempoapi.NewOptInt(1),
			})
			a.NoError(err)
			a.Len(r.Traces, 1)

			validateMetadata(a, r.Traces[0], getOkSpans)
		})
		t.Run("TagsNotExist", func(t *testing.T) {
			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Q: tempoapi.NewOptString(`{ .clearly.not.exist = "POST" && .http.method = 200 }`),
			})
			a.NoError(err)
			a.Empty(r.Traces)
		})
	})
}

func getRawMapFromAPI(obj []tempoapi.KeyValue) map[string]any {
	r := make(map[string]any, len(obj))
	for _, kv := range obj {
		r[kv.Key] = getRawValueFromAPI(kv.Value)
	}
	return r
}

func getRawValueFromAPI(val tempoapi.AnyValue) any {
	switch val.Type {
	case tempoapi.StringValueAnyValue:
		return val.StringValue.StringValue
	case tempoapi.BoolValueAnyValue:
		return val.BoolValue.BoolValue
	case tempoapi.IntValueAnyValue:
		return val.IntValue.IntValue
	case tempoapi.DoubleValueAnyValue:
		return val.DoubleValue.DoubleValue
	case tempoapi.ArrayValueAnyValue:
		arr := val.ArrayValue.ArrayValue
		r := make([]any, len(arr))
		for i, val := range arr {
			r[i] = getRawValueFromAPI(val)
		}
		return arr
	case tempoapi.KvlistValueAnyValue:
		return getRawMapFromAPI(val.KvlistValue.KvlistValue)
	case tempoapi.BytesValueAnyValue:
		return val.BytesValue.BytesValue
	default:
		panic(fmt.Sprintf("unexpected type %#v", val.Type))
	}
}

type selectedSpans = map[pcommon.TraceID]map[pcommon.SpanID]struct{}

type selector interface {
	Select(span ptrace.Span) bool
}

type byTags map[string]pcommon.Value

func (tags byTags) Select(span ptrace.Span) bool {
	m := span.Attributes()
	for tagName, expect := range tags {
		got, ok := m.Get(tagName)
		if !ok || expect.AsString() != got.AsString() {
			return false
		}
	}
	return true
}

type byName string

func (n byName) Select(span ptrace.Span) bool {
	return span.Name() == string(n)
}

func selectTraces(set tempoe2e.BatchSet, sel selector) (result selectedSpans) {
	return selectSpansets(set, true, sel)
}

func selectSpans(set tempoe2e.BatchSet, sel selector) (result selectedSpans) {
	return selectSpansets(set, false, sel)
}

func selectSpansets(set tempoe2e.BatchSet, matchByTrace bool, sel selector) (result selectedSpans) {
	addSpan := func(traceID pcommon.TraceID, spanID pcommon.SpanID) {
		m, ok := result[traceID]
		if !ok {
			m = map[pcommon.SpanID]struct{}{}
			result[traceID] = m
		}
		m[spanID] = struct{}{}
	}

	result = selectedSpans{}
	for traceID, trace := range set.Traces {
		var anyMatch bool
		for _, span := range trace.Spanset {
			if sel.Select(span) {
				anyMatch = true
				addSpan(traceID, span.SpanID())
			}
		}
		// Add all spans to expected set.
		if matchByTrace && anyMatch {
			for _, span := range trace.Spanset {
				addSpan(traceID, span.SpanID())
			}
		}
	}

	return result
}
