package otlpdirect

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"

	"github.com/oteldb/storage/signal"
)

// gRPC is what an OTLP exporter speaks by default — the OTel SDKs and the collector's own exporter
// both target 4317 before 4318 — so serving only OTLP/HTTP would leave most senders unable to
// reach an ingester without being reconfigured.
//
// The services are registered by hand rather than from generated stubs: the generated server
// would unmarshal each request into a pdata (or gogo) struct, which is the allocation this package
// exists to avoid. Instead a raw codec hands the request's bytes straight to the same converters
// the HTTP endpoints use, so the two transports share one decode path and one aliasing contract.

// Fully-qualified OTLP service names. They are part of the wire contract, so a client's method
// string must match these exactly.
const (
	logsService     = "opentelemetry.proto.collector.logs.v1.LogsService"
	tracesService   = "opentelemetry.proto.collector.trace.v1.TraceService"
	metricsService  = "opentelemetry.proto.collector.metrics.v1.MetricsService"
	profilesService = "opentelemetry.proto.collector.profiles.v1development.ProfilesService"

	exportMethod = "Export"
)

// rawCodecName is the content subtype the raw codec registers under. gRPC resolves a codec by the
// subtype in the request's content-type, and an OTLP client sends "proto" — so the server is
// given this codec explicitly with [grpc.ForceServerCodecV2] rather than registering it globally,
// which would hijack every other proto service in the process.
const rawCodecName = "proto"

// GRPCServerOptions returns the options an OTLP gRPC server needs: the raw codec that skips
// unmarshaling, and a message-size limit matching the HTTP body limit.
//
// Pass them to [grpc.NewServer], then call [Handler.RegisterGRPC] on the result.
func (h *Handler) GRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ForceServerCodecV2(rawCodec{}),
		grpc.MaxRecvMsgSize(int(h.maxBody)),
	}
}

// RegisterGRPC registers the four OTLP export services on srv. The server must have been built
// with [Handler.GRPCServerOptions].
func (h *Handler) RegisterGRPC(srv grpc.ServiceRegistrar) {
	for _, s := range []struct {
		name   string
		sig    signal.Signal
		ingest ingestFunc
	}{
		{logsService, signal.Log, h.ingestLogs},
		{tracesService, signal.Trace, h.ingestTraces},
		{metricsService, signal.Metric, h.ingestMetrics},
		{profilesService, signal.Profile, h.ingestProfiles},
	} {
		srv.RegisterService(&grpc.ServiceDesc{
			ServiceName: s.name,
			HandlerType: (*any)(nil),
			Methods: []grpc.MethodDesc{{
				MethodName: exportMethod,
				Handler:    h.exportHandler(s.sig, s.ingest),
			}},
			Metadata: s.name,
		}, struct{}{})
	}
}

// exportHandler builds the unary handler for one signal's Export method.
func (h *Handler) exportHandler(sig signal.Signal, ingest ingestFunc) grpc.MethodHandler {
	return func(
		_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor,
	) (any, error) {
		msg, _ := grpcMessages.Get().(*rawMessage)
		defer grpcMessages.Put(msg)

		if err := dec(msg); err != nil {
			return nil, err
		}

		export := func(ctx context.Context, req any) (any, error) {
			m, ok := req.(*rawMessage)
			if !ok {
				return nil, status.Error(codes.Internal, "unexpected request type")
			}

			items, rejected, err := ingest(ctx, m.data)
			if err != nil {
				return nil, h.grpcError(sig, err)
			}

			if h.observe != nil {
				h.observe(Stats{Signal: sig, Bytes: len(m.data), Items: items, Rejected: rejected})
			}

			if rejected == 0 {
				return &rawMessage{}, nil
			}

			return &rawMessage{data: encodePartialSuccess(sig, rejected)}, nil
		}

		if interceptor == nil {
			return export(ctx, msg)
		}

		return interceptor(ctx, msg, &grpc.UnaryServerInfo{
			Server:     struct{}{},
			FullMethod: "/" + serviceOf(sig) + "/" + exportMethod,
		}, export)
	}
}

// grpcError maps a failure to the status code that tells the client what to do with it: a write
// that may not have landed is Unavailable so the client retries, and a request it encoded wrong is
// InvalidArgument so it does not.
func (h *Handler) grpcError(sig signal.Signal, err error) error {
	code := codes.InvalidArgument
	if we := new(writeError); errors.As(err, we) {
		code = codes.Unavailable
	}

	h.lg.Debug("Reject OTLP gRPC export",
		zap.Stringer("signal", sig), zap.Error(err), zap.Stringer("code", code))

	return status.Error(code, err.Error())
}

func serviceOf(sig signal.Signal) string {
	switch sig {
	case signal.Log:
		return logsService
	case signal.Trace:
		return tracesService
	case signal.Profile:
		return profilesService
	default:
		return metricsService
	}
}

// rawMessage carries a request or response body without a generated type behind it.
type rawMessage struct{ data []byte }

var grpcMessages = sync.Pool{New: func() any { return new(rawMessage) }}

// rawCodec hands a request's bytes to the converters unchanged, and writes a response's bytes
// unchanged. It is the whole reason the gRPC path costs the same as the HTTP one.
type rawCodec struct{}

func (rawCodec) Name() string { return rawCodecName }

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	m, ok := v.(*rawMessage)
	if !ok {
		return nil, errors.Errorf("otlpdirect: cannot marshal %T", v)
	}

	if len(m.data) == 0 {
		return nil, nil
	}

	return mem.BufferSlice{mem.SliceBuffer(m.data)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	m, ok := v.(*rawMessage)
	if !ok {
		return errors.Errorf("otlpdirect: cannot unmarshal into %T", v)
	}

	// gRPC frees data as soon as this returns, so the bytes are copied into the message's own
	// buffer — which is pooled, making this the same single copy the HTTP path pays reading a body.
	n := data.Len()
	if cap(m.data) < n {
		m.data = make([]byte, n)
	}

	m.data = m.data[:n]
	data.CopyTo(m.data)

	return nil
}
