package tempohandler

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

type oneTraceQuerier struct {
	traceID otelstorage.TraceID
}

func (q oneTraceQuerier) SearchTags(context.Context, map[string]string, tracestorage.SearchTagsOptions) (iterators.Iterator[tracestorage.Span], error) {
	return iterators.Empty[tracestorage.Span](), nil
}

func (q oneTraceQuerier) TagNames(context.Context, tracestorage.TagNamesOptions) ([]tracestorage.TagName, error) {
	return nil, nil
}

func (q oneTraceQuerier) TagValues(context.Context, traceql.Attribute, tracestorage.TagValuesOptions) (iterators.Iterator[tracestorage.Tag], error) {
	return iterators.Empty[tracestorage.Tag](), nil
}

func (q oneTraceQuerier) TraceByID(_ context.Context, id otelstorage.TraceID, _ tracestorage.TraceByIDOptions) (iterators.Iterator[tracestorage.Span], error) {
	if id != q.traceID {
		return iterators.Empty[tracestorage.Span](), nil
	}
	span := spanOfService("frontend.request", "frontend")
	span.TraceID = id
	return iterators.Slice([]tracestorage.Span{span}), nil
}

const errorContentType = "application/json; charset=utf-8"

func TestTraceByIDAcceptNegotiation(t *testing.T) {
	t.Parallel()

	var traceID otelstorage.TraceID
	_, err := hex.Decode(traceID[:], []byte("0123456789abcdef0123456789abcdef"))
	require.NoError(t, err)

	api := NewTempoAPI(oneTraceQuerier{traceID: traceID}, nil, TempoAPIOptions{})
	srv, err := tempoapi.NewServer(api)
	require.NoError(t, err)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	for name, tt := range map[string]struct {
		accept      string
		setAccept   bool
		code        int
		contentType string
	}{
		"absent": {
			code: http.StatusOK, contentType: "application/json",
		},
		"empty": {
			accept: "", setAccept: true,
			code: http.StatusOK, contentType: "application/json",
		},
		"wildcard": {
			accept: "*/*", setAccept: true,
			code: http.StatusOK, contentType: "application/json",
		},
		"application wildcard": {
			accept: "application/*", setAccept: true,
			code: http.StatusOK, contentType: "application/json",
		},
		"json": {
			accept: "application/json", setAccept: true,
			code: http.StatusOK, contentType: "application/json",
		},
		"json with charset": {
			accept: "application/json; charset=utf-8", setAccept: true,
			code: http.StatusOK, contentType: "application/json",
		},
		"json with uppercase charset": {
			accept: "application/json; charset=UTF-8", setAccept: true,
			code: http.StatusOK, contentType: "application/json",
		},
		"text json": {
			accept: "text/json", setAccept: true,
			code: http.StatusOK, contentType: "text/json",
		},
		"protobuf": {
			accept: "application/protobuf", setAccept: true,
			code: http.StatusOK, contentType: "application/protobuf",
		},
		"x-protobuf": {
			accept: "application/x-protobuf", setAccept: true,
			code: http.StatusOK, contentType: "application/x-protobuf",
		},
		"browser list": {
			accept: "text/html, application/json;q=0.9, */*;q=0.8", setAccept: true,
			code: http.StatusOK, contentType: "application/json",
		},
		"json with unsupported charset": {
			accept: "application/json; charset=iso-8859-1", setAccept: true,
			code: http.StatusBadRequest, contentType: errorContentType,
		},
		"unsupported type": {
			accept: "application/xml", setAccept: true,
			code: http.StatusBadRequest, contentType: errorContentType,
		},
		// The LLM encoder is not implemented, but the type is recognized, so it must not be
		// rejected as an unknown media type.
		"grafana llm": {
			accept: "application/vnd.grafana.llm+json", setAccept: true,
			code: http.StatusInternalServerError, contentType: errorContentType,
		},
	} {
		for _, path := range []string{"/api/traces/", "/api/v2/traces/"} {
			t.Run(path+name, func(t *testing.T) {
				t.Parallel()

				req, err := http.NewRequest(http.MethodGet, ts.URL+path+traceID.Hex(), http.NoBody)
				require.NoError(t, err)
				if tt.setAccept {
					req.Header.Set("Accept", tt.accept)
				} else {
					req.Header["Accept"] = nil
				}

				resp, err := ts.Client().Do(req)
				require.NoError(t, err)
				defer func() {
					_ = resp.Body.Close()
				}()

				require.Equal(t, tt.code, resp.StatusCode)
				require.Equal(t, tt.contentType, resp.Header.Get("Content-Type"))

				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NotEmpty(t, body)
				if tt.code == http.StatusOK && strings.Contains(tt.contentType, "json") {
					require.True(t, json.Valid(body))
					require.Contains(t, string(body), "frontend.request")
				}
			})
		}
	}
}
