package tempohandler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func spanOfService(name, service string) tracestorage.Span {
	attrs := otelstorage.NewAttrs()
	attrs.AsMap().PutStr("service.name", service)

	return tracestorage.Span{
		Name:          name,
		Attrs:         otelstorage.NewAttrs(),
		ResourceAttrs: attrs,
		ScopeName:     "oteldb",
		ScopeAttrs:    otelstorage.NewAttrs(),
	}
}

// TestBatchCollectorGroupsByResource pins that a trace's spans keep their own resource. The
// collector used to group by the ingest batch id, which only the ClickHouse backend fills in, so
// every span of a multi-service trace collapsed onto whichever resource was collected first.
func TestBatchCollectorGroupsByResource(t *testing.T) {
	var c batchCollector
	for _, span := range []tracestorage.Span{
		spanOfService("frontend.request", "frontend"),
		spanOfService("cart.get", "cart"),
		spanOfService("frontend.render", "frontend"),
	} {
		require.NoError(t, c.AddSpan(span))
	}

	got := map[string]string{}
	for _, rs := range c.resourceSpans() {
		var service string
		for _, kv := range rs.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" {
				service = kv.GetValue().GetStringValue()
			}
		}

		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				got[s.GetName()] = service
			}
		}
	}

	require.Equal(t, map[string]string{
		"frontend.request": "frontend",
		"cart.get":         "cart",
		"frontend.render":  "frontend",
	}, got)
	require.Len(t, c.resourceSpans(), 2)
	require.Equal(t, 3, c.SpanCount())
}
