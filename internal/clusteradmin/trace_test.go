package clusteradmin

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// spansNamed returns the recorded spans with the given name.
func spansNamed(spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}

	return out
}

// attrString reads a string attribute off a span, failing if it is absent.
func attrString(t *testing.T, s tracetest.SpanStub, key string) string {
	t.Helper()

	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	t.Fatalf("span %q has no attribute %s", s.Name, key)

	return ""
}

// TestFanoutSpans pins that every node the aggregator asks gets a span of its own, under one
// fan-out span: the response says which node failed, but only the spans say when each call ran and
// how the failing one spent its time.
func TestFanoutSpans(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	a := newTestAggregator(t, 2, map[string]*fakeNode{
		"a": {health: &adminapi.HealthReport{Status: adminapi.HealthStatusHealthy}},
		"b": {err: errors.New("node is on fire")},
	}, func(o *Options) {
		o.TracerProvider = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	})

	_, err := a.GetHealth(t.Context())
	require.NoError(t, err)

	spans := tracetest.SpanStubsFromReadOnlySpans(rec.Ended())

	roots := spansNamed(spans, "clusteradmin.fanout")
	require.Len(t, roots, 1)
	assert.Equal(t, "health", attrString(t, roots[0], "clusteradmin.op"))
	assert.Contains(t, roots[0].Attributes, attribute.Int("clusteradmin.peers", 2))
	assert.Equal(t, codes.Unset, roots[0].Status.Code, "a node failure is a partial answer, not a failed fan-out")

	nodes := spansNamed(spans, "clusteradmin.fanout.node")
	require.Len(t, nodes, 2, "one span per peer, including the one that failed")

	byNode := map[string]tracetest.SpanStub{}
	for _, s := range nodes {
		assert.Equal(t, roots[0].SpanContext.SpanID(), s.Parent.SpanID(), "node spans hang off the fan-out")
		assert.NotEmpty(t, attrString(t, s, "clusteradmin.addr"))
		byNode[attrString(t, s, "clusteradmin.node")] = s
	}
	require.Contains(t, byNode, "a")
	require.Contains(t, byNode, "b")

	assert.Equal(t, codes.Unset, byNode["a"].Status.Code)
	assert.Equal(t, codes.Error, byNode["b"].Status.Code)
	assert.Contains(t, byNode["b"].Status.Description, "node is on fire")
	require.Len(t, byNode["b"].Events, 1, "the failure is recorded on the span, not only in the response")
	assert.Equal(t, "exception", byNode["b"].Events[0].Name)
}

// TestRingPeersPropagatesTraceContext pins that a call to a node is a real client span and carries
// the trace context, so the node's own server span continues the aggregator's trace rather than
// starting a second, unrelated one.
//
// It does not run in parallel: the outbound transport injects with the global propagator, which the
// process configures at startup and this test has to stand in for.
func TestRingPeersPropagatesTraceContext(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })
	otel.SetTextMapPropagator(propagation.TraceContext{})

	api, err := adminapi.NewServer(&fakeNode{info: &adminapi.InstanceInfo{Version: "test"}})
	require.NoError(t, err)

	var got propagation.HeaderCarrier
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = propagation.HeaderCarrier(r.Header.Clone())
		api.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	host, port := splitHostPort(t, ts.URL)

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	peers, err := NewRingPeers(RingPeersOptions{
		Members:        fakeMembership{{ID: host, Addr: host}},
		Scheme:         "http",
		Port:           port,
		TracerProvider: tp,
	})
	require.NoError(t, err)

	resolved, err := peers.Peers()
	require.NoError(t, err)
	require.Len(t, resolved, 1)

	ctx, span := tp.Tracer("test").Start(t.Context(), "caller")
	_, err = resolved[0].Client.GetInfo(ctx)
	span.End()
	require.NoError(t, err)

	require.NotNil(t, got, "the node was not called")

	remote := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(t.Context(), got))
	require.True(t, remote.IsValid(), "no trace context reached the node")
	assert.Equal(t, span.SpanContext().TraceID(), remote.TraceID())

	var client bool
	for _, s := range tracetest.SpanStubsFromReadOnlySpans(rec.Ended()) {
		if s.SpanKind == trace.SpanKindClient {
			client = true
		}
	}
	assert.True(t, client, "the call to a node is a client span")
}

func splitHostPort(t *testing.T, rawURL string) (host string, port int) {
	t.Helper()

	host, portStr, err := net.SplitHostPort(rawURL[len("http://"):])
	require.NoError(t, err)

	port, err = strconv.Atoi(portStr)
	require.NoError(t, err)

	return host, port
}
