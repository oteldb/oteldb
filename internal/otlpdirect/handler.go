package otlpdirect

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/signal/trace"
)

// Sink ingests a converted batch. Only the signals a deployment serves need to be implemented; a
// nil method's endpoint answers 501.
//
// Every batch aliases the request buffer, which the handler recycles as soon as the call returns:
// an implementation retaining any of its bytes must copy them.
type Sink interface {
	WriteLogs(ctx context.Context, batch log.Logs) error
	WriteTraces(ctx context.Context, batch trace.Traces) error
	WriteMetrics(ctx context.Context, batch metric.Metrics) error
	WriteProfiles(ctx context.Context, batch *profile.Profiles) error
}

// Paths are where OTLP/HTTP expects each signal to be served.
const (
	LogsPath     = "/v1/logs"
	TracesPath   = "/v1/traces"
	MetricsPath  = "/v1/metrics"
	ProfilesPath = "/v1/profiles"
)

const (
	// defaultMaxBodyBytes bounds the request body as it arrives.
	defaultMaxBodyBytes = 64 << 20
	// defaultMaxDecodedBytes bounds what a gzip body may expand to. gzip's ratio is unbounded, so
	// the compressed limit alone does not bound the allocation.
	defaultMaxDecodedBytes = 256 << 20

	protobufContentType = "application/x-protobuf"
)

// Stats is what one accepted request ingested.
type Stats struct {
	// Signal names which endpoint served the request.
	Signal signal.Signal
	// Bytes is the decompressed request size.
	Bytes int
	// Items is the number of records, spans, points or samples the request carried.
	Items int
	// Rejected is the number the request carried that were not stored.
	Rejected int
}

// HandlerConfig configures a [Handler].
type HandlerConfig struct {
	// MaxBodyBytes limits the request body. Zero means 64 MiB.
	MaxBodyBytes int64
	// MaxDecodedBytes limits what a compressed body may expand to. Zero means 256 MiB.
	MaxDecodedBytes int64
	// Logger receives rejected-request diagnostics. Nil means no logging.
	Logger *zap.Logger
	// Observer, when set, is called once per accepted request. It runs on the ingest path, so it
	// must be cheap and must not block.
	Observer func(Stats)
}

// Handler serves OTLP/HTTP, decoding each signal straight into the engine's ingest model.
//
// Mount it with [Handler.Register], or mount [Handler.Logs] and its siblings individually.
type Handler struct {
	sink       Sink
	maxBody    int64
	maxDecoded int64
	lg         *zap.Logger
	observe    func(Stats)

	logs     sync.Pool
	traces   sync.Pool
	metrics  sync.Pool
	profiles sync.Pool
	bodies   sync.Pool
}

// NewHandler creates an OTLP/HTTP handler writing to sink.
func NewHandler(sink Sink, cfg HandlerConfig) *Handler {
	h := &Handler{
		sink:       sink,
		maxBody:    cfg.MaxBodyBytes,
		maxDecoded: cfg.MaxDecodedBytes,
		lg:         cfg.Logger,
		observe:    cfg.Observer,
	}

	if h.maxBody == 0 {
		h.maxBody = defaultMaxBodyBytes
	}

	if h.maxDecoded == 0 {
		h.maxDecoded = defaultMaxDecodedBytes
	}

	if h.lg == nil {
		h.lg = zap.NewNop()
	}

	h.logs.New = func() any { return new(LogsConverter) }
	h.traces.New = func() any { return new(TracesConverter) }
	h.metrics.New = func() any { return new(MetricsConverter) }
	h.profiles.New = func() any { return new(ProfilesConverter) }
	h.bodies.New = func() any { return new(body) }

	return h
}

// Register mounts every signal endpoint on mux at its OTLP/HTTP path.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("POST "+LogsPath, h.Logs())
	mux.Handle("POST "+TracesPath, h.Traces())
	mux.Handle("POST "+MetricsPath, h.Metrics())
	mux.Handle("POST "+ProfilesPath, h.Profiles())
}

func (h *Handler) Logs() http.Handler {
	return h.serve(signal.Log, func(ctx context.Context, src []byte) (int, int, error) {
		c, _ := h.logs.Get().(*LogsConverter)
		defer h.logs.Put(c)

		batch, err := c.Convert(src)
		if err != nil {
			return 0, 0, err
		}

		if err := h.sink.WriteLogs(ctx, *batch); err != nil {
			return 0, 0, writeError{err: err}
		}

		return countRecords(batch), 0, nil
	})
}

func (h *Handler) Traces() http.Handler {
	return h.serve(signal.Trace, func(ctx context.Context, src []byte) (int, int, error) {
		c, _ := h.traces.Get().(*TracesConverter)
		defer h.traces.Put(c)

		batch, err := c.Convert(src)
		if err != nil {
			return 0, 0, err
		}

		if err := h.sink.WriteTraces(ctx, *batch); err != nil {
			return 0, 0, writeError{err: err}
		}

		return countSpans(batch), 0, nil
	})
}

func (h *Handler) Metrics() http.Handler {
	return h.serve(signal.Metric, func(ctx context.Context, src []byte) (int, int, error) {
		c, _ := h.metrics.Get().(*MetricsConverter)
		defer h.metrics.Put(c)

		batch, dropped, err := c.Convert(src)
		if err != nil {
			return 0, 0, err
		}

		if err := h.sink.WriteMetrics(ctx, *batch); err != nil {
			return 0, 0, writeError{err: err}
		}

		return countPoints(batch), dropped, nil
	})
}

func (h *Handler) Profiles() http.Handler {
	return h.serve(signal.Profile, func(ctx context.Context, src []byte) (int, int, error) {
		c, _ := h.profiles.Get().(*ProfilesConverter)
		defer h.profiles.Put(c)

		batch, err := c.Convert(src)
		if err != nil {
			return 0, 0, err
		}

		if err := h.sink.WriteProfiles(ctx, batch); err != nil {
			return 0, 0, writeError{err: err}
		}

		return countSamples(batch), 0, nil
	})
}

// ingestFunc decodes one request body and writes it, returning what it ingested and what it could
// not represent.
type ingestFunc func(ctx context.Context, src []byte) (items, rejected int, _ error)

// writeError marks a sink failure, answered with 5xx so the client retries rather than drops the
// batch. Everything else is the request's own fault and answered with 4xx.
type writeError struct{ err error }

func (e writeError) Error() string { return e.err.Error() }
func (e writeError) Unwrap() error { return e.err }

func (h *Handler) serve(sig signal.Signal, ingest ingestFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "" && !isProtobuf(ct) {
			// JSON-encoded OTLP is a valid part of the spec this handler does not implement, so it
			// must be refused rather than parsed as protobuf and rejected as malformed.
			http.Error(w, "only "+protobufContentType+" is supported", http.StatusUnsupportedMediaType)

			return
		}

		b, _ := h.bodies.Get().(*body)
		defer h.bodies.Put(b)

		src, err := b.read(r, h.maxBody, h.maxDecoded)
		if err == nil {
			var items, rejected int

			items, rejected, err = ingest(r.Context(), src)
			if err == nil {
				h.respond(w, sig, len(src), items, rejected)

				return
			}
		}

		code := http.StatusBadRequest
		if we := new(writeError); errors.As(err, we) {
			code = http.StatusInternalServerError
		}

		h.lg.Debug("Reject OTLP request",
			zap.Stringer("signal", sig), zap.Error(err), zap.Int("code", code))
		http.Error(w, err.Error(), code)
	})
}

// respond answers a successful export. OTLP wants a serialized ExportXServiceResponse, and an
// empty message is the full-success form for every signal — the partial-success submessage is
// simply absent, which is what a rejected count of zero means.
func (h *Handler) respond(w http.ResponseWriter, sig signal.Signal, size, items, rejected int) {
	if h.observe != nil {
		h.observe(Stats{Signal: sig, Bytes: size, Items: items, Rejected: rejected})
	}

	w.Header().Set("Content-Type", protobufContentType)
	w.WriteHeader(http.StatusOK)

	if rejected == 0 {
		return
	}

	_, _ = w.Write(encodePartialSuccess(sig, rejected))
}

// body is the per-request read buffer, recycled across requests.
type body struct {
	raw      []byte
	expanded []byte
}

// read returns the request's decoded protobuf bytes, transparently un-gzipping when the sender
// compressed them.
func (b *body) read(r *http.Request, maxBody, maxDecoded int64) ([]byte, error) {
	var err error
	if b.raw, err = readAll(b.raw[:0], http.MaxBytesReader(nil, r.Body, maxBody)); err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	if !strings.EqualFold(strings.TrimSpace(r.Header.Get("Content-Encoding")), "gzip") {
		return b.raw, nil
	}

	zr, err := gzip.NewReader(bytes.NewReader(b.raw))
	if err != nil {
		return nil, errors.Wrap(err, "read gzip header")
	}

	defer func() { _ = zr.Close() }()

	// gzip's expansion ratio is unbounded and the sender chooses it, so the decompressed size is
	// capped independently of the body limit.
	if b.expanded, err = readAll(b.expanded[:0], io.LimitReader(zr, maxDecoded+1)); err != nil {
		return nil, errors.Wrap(err, "decompress body")
	}

	if int64(len(b.expanded)) > maxDecoded {
		return nil, errors.Errorf("body decompresses past the %d byte limit", maxDecoded)
	}

	return b.expanded, nil
}

func countRecords(batch *log.Logs) (n int) {
	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			n += len(batch.Resources[i].Scopes[j].Records)
		}
	}

	return n
}

func countSpans(batch *trace.Traces) (n int) {
	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			n += len(batch.Resources[i].Scopes[j].Spans)
		}
	}

	return n
}

func countPoints(batch *metric.Metrics) (n int) {
	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			for _, mt := range batch.Resources[i].Scopes[j].Metrics {
				n += len(mt.Points)
			}
		}
	}

	return n
}

func countSamples(batch *profile.Profiles) (n int) {
	for i := range batch.Resources {
		for j := range batch.Resources[i].Scopes {
			for _, pr := range batch.Resources[i].Scopes[j].Profiles {
				n += len(pr.Samples)
			}
		}
	}

	return n
}

func isProtobuf(contentType string) bool {
	media, _, _ := strings.Cut(contentType, ";")

	return strings.EqualFold(strings.TrimSpace(media), protobufContentType)
}

// readAll reads r into dst, reusing its capacity.
func readAll(dst []byte, r io.Reader) ([]byte, error) {
	for {
		if len(dst) == cap(dst) {
			dst = append(dst, 0)[:len(dst)]
		}

		n, err := r.Read(dst[len(dst):cap(dst)])
		dst = dst[:len(dst)+n]

		if err != nil {
			if errors.Is(err, io.EOF) {
				return dst, nil
			}

			return dst, err
		}
	}
}
