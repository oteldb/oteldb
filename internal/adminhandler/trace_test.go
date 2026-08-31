package adminhandler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// TestEfficiencySpans pins that the backend work behind an efficiency report is traced, and that
// the part listing is a span of its own: it is a second pass over the backend, so the request's
// time splits between the two rather than being one opaque handler span.
func TestEfficiencySpans(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	eng := &fakeEngine{
		efficiency: []storage.TenantEfficiency{{
			Tenant:  signal.TenantID("default"),
			Signals: []storage.SignalEfficiency{{Signal: signal.Metric, Parts: 2}},
		}},
		parts: []storage.PartDetail{
			{PartInfo: storage.PartInfo{ID: "default/metrics/a", Series: 3, Rows: 10}, Bytes: 100},
			{PartInfo: storage.PartInfo{ID: "default/metrics/b", Series: 4, Rows: 20}, Bytes: 200},
		},
	}

	api := NewAdminAPI(Options{
		Engine:         eng,
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)),
	})

	_, err := api.GetEfficiency(t.Context(), adminapi.GetEfficiencyParams{Parts: adminapi.NewOptBool(true)})
	require.NoError(t, err)

	var root, parts tracetest.SpanStub
	for _, s := range tracetest.SpanStubsFromReadOnlySpans(rec.Ended()) {
		switch s.Name {
		case "adminhandler.efficiency":
			root = s
		case "adminhandler.efficiency.attachParts":
			parts = s
		}
	}

	require.Equal(t, "adminhandler.efficiency", root.Name)
	assert.Contains(t, root.Attributes, attribute.Bool("adminhandler.parts", true))
	assert.Contains(t, root.Attributes, attribute.Int("adminhandler.tenants", 1))

	require.Equal(t, "adminhandler.efficiency.attachParts", parts.Name)
	assert.Equal(t, root.SpanContext.SpanID(), parts.Parent.SpanID())
	assert.Contains(t, parts.Attributes, attribute.String("adminhandler.tenant", "default"))
	assert.Contains(t, parts.Attributes, attribute.Int("adminhandler.parts_listed", 2))
}
