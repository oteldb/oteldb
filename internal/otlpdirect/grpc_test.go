package otlpdirect_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// serveGRPC runs the OTLP gRPC services over a real connection, so the tests exercise the wire
// contract an exporter actually speaks rather than the handler in isolation.
func serveGRPC(t *testing.T, sink otlpdirect.Sink) *grpc.ClientConn {
	t.Helper()

	h := otlpdirect.NewHandler(sink, otlpdirect.HandlerConfig{})

	srv := grpc.NewServer(h.GRPCServerOptions()...)
	h.RegisterGRPC(srv)

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient(ln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() { _ = cc.Close() })

	return cc
}

// TestGRPCExportsEverySignal drives the server with the generated OTLP clients — the same stubs a
// real exporter uses — so the hand-registered services and the raw codec are checked against the
// actual protocol, not against our own encoder.
func TestGRPCExportsEverySignal(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	cc := serveGRPC(t, sink)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(1)
	lr.Body().SetStr("hello")

	logsResp, err := plogotlp.NewGRPCClient(cc).Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
	require.NoError(t, err)
	assert.Zero(t, logsResp.PartialSuccess().RejectedLogRecords())

	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName("op")
	sp.SetStartTimestamp(1)

	tracesResp, err := ptraceotlp.NewGRPCClient(cc).Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
	require.NoError(t, err)
	assert.Zero(t, tracesResp.PartialSuccess().RejectedSpans())

	md := pmetric.NewMetrics()
	mt := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	mt.SetName("g")

	gp := mt.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetTimestamp(1)
	gp.SetDoubleValue(1)

	metricsResp, err := pmetricotlp.NewGRPCClient(cc).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
	require.NoError(t, err)
	assert.Zero(t, metricsResp.PartialSuccess().RejectedDataPoints())

	assert.Equal(t, 1, sink.logs)
	assert.Equal(t, 1, sink.spans)
	assert.Equal(t, 1, sink.points)
}

// TestGRPCPartialSuccess pins that the partial-success response survives the round trip and is
// read back by the generated client — the count is what tells an exporter to stop retrying.
func TestGRPCPartialSuccess(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	cc := serveGRPC(t, sink)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	md := pmetric.NewMetrics()

	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("g")

	dps := m.SetEmptyGauge().DataPoints()
	dps.AppendEmpty().SetTimestamp(1) // no value: unrepresentable

	valued := dps.AppendEmpty()
	valued.SetTimestamp(2)
	valued.SetDoubleValue(1)

	resp, err := pmetricotlp.NewGRPCClient(cc).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
	require.NoError(t, err)

	assert.Equal(t, int64(1), resp.PartialSuccess().RejectedDataPoints())
	assert.NotEmpty(t, resp.PartialSuccess().ErrorMessage())
	assert.Equal(t, 1, sink.points, "the representable point is still stored")
}

// TestGRPCWriteFailureIsRetryable pins the status codes an exporter keys off: a write that may not
// have landed is Unavailable so it retries, and a malformed request is InvalidArgument so it does
// not.
func TestGRPCWriteFailureIsRetryable(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{failWith: errors.New("no primary")}
	cc := serveGRPC(t, sink)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetTimestamp(1)

	_, err := plogotlp.NewGRPCClient(cc).Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestGRPCMalformedIsNotRetryable(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	cc := serveGRPC(t, sink)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Invoke the method directly with bytes no decoder can read, bypassing the generated client.
	err := cc.Invoke(ctx, "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
		&rawRequest{data: []byte{0xff, 0xff, 0xff, 0xff}}, &rawRequest{},
		grpc.ForceCodecV2(passthroughCodec{}))

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestGRPCConcurrent drives the server from many goroutines, since the converters and the message
// buffers are pooled across calls.
func TestGRPCConcurrent(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	cc := serveGRPC(t, sink)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(1)
	lr.Body().SetStr("x")

	client := plogotlp.NewGRPCClient(cc)
	req := plogotlp.NewExportRequestFromLogs(ld)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_, err := client.Export(ctx, req)
			assert.NoError(t, err)
		})
	}

	wg.Wait()
	assert.Equal(t, 50, sink.logs)
}

// rawRequest and passthroughCodec let a test send bytes the generated clients cannot construct.
type rawRequest struct{ data []byte }

type passthroughCodec struct{}

func (passthroughCodec) Name() string { return "proto" }

func (passthroughCodec) Marshal(v any) (mem.BufferSlice, error) {
	m, ok := v.(*rawRequest)
	if !ok {
		return nil, errors.Errorf("cannot marshal %T", v)
	}

	return mem.BufferSlice{mem.SliceBuffer(m.data)}, nil
}

func (passthroughCodec) Unmarshal(data mem.BufferSlice, v any) error {
	m, ok := v.(*rawRequest)
	if !ok {
		return errors.Errorf("cannot unmarshal into %T", v)
	}

	m.data = data.Materialize()

	return nil
}
