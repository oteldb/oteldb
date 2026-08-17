package promrw

import (
	"context"
	"io"
	"net/http"
	"sync"

	"github.com/go-faster/errors"
	"github.com/golang/snappy"
	"github.com/oteldb/storage/signal/metric"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/prompb"
)

// Sink ingests a converted metrics batch. It is implemented by the storage backend.
//
// The batch aliases the request's decode buffer, which the handler recycles as soon as the call
// returns: an implementation that retains any of the batch's bytes must copy them.
type Sink interface {
	WriteMetrics(ctx context.Context, batch metric.Metrics) error
}

const (
	// defaultMaxBodyBytes bounds the compressed request body.
	defaultMaxBodyBytes = 64 << 20
	// defaultMaxDecodedBytes bounds what that body is allowed to decompress to. Snappy expands by
	// up to ~68×, and the decoded length is a header field the sender chooses, so the compressed
	// limit alone does not bound the allocation.
	defaultMaxDecodedBytes = 256 << 20
)

// HandlerConfig configures a [Handler].
type HandlerConfig struct {
	// Options is the conversion configuration applied to every request.
	Options Options
	// MaxBodyBytes limits the compressed request body. Zero means 64 MiB.
	MaxBodyBytes int64
	// MaxDecodedBytes limits what the body may decompress to. Zero means 256 MiB.
	MaxDecodedBytes int
	// Logger receives rejected-request diagnostics. Zero means no logging.
	Logger *zap.Logger
	// Observer, when set, is called once per accepted request with what it ingested. It runs on
	// the ingest path, so it must be cheap and must not block.
	Observer func(Stats)
}

// Stats is what one accepted request ingested.
type Stats struct {
	// Message is the schema the request was sent under.
	Message Message
	// Bytes is the decompressed request size.
	Bytes int
	// Series is the number of timeseries the request carried.
	Series int
	// Points is the number of points written, counting the series a native histogram decomposed
	// into.
	Points int
	// Rejected is what the request carried but the conversion did not ingest.
	Rejected Rejected
}

// Handler serves the Prometheus remote write API, writing into sink without going through the
// collector pipeline.
type Handler struct {
	sink       Sink
	opts       Options
	maxBody    int64
	maxDecoded int
	lg         *zap.Logger
	observe    func(Stats)
	pool       sync.Pool
}

// NewHandler creates a remote write handler writing to sink.
func NewHandler(sink Sink, cfg HandlerConfig) *Handler {
	h := &Handler{
		sink:       sink,
		opts:       cfg.Options,
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
	h.pool.New = func() any { return new(requestState) }
	return h
}

// requestState is the per-request scratch: the buffers a request is decoded through and the
// converter holding the batch built from them. All of it is recycled once the write returns.
type requestState struct {
	compressed []byte
	raw        []byte
	req        prompb.WriteRequest
	reqV2      prompb.WriteRequestV2
	conv       Converter
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	msg, err := parseMessage(r.Header.Get("Content-Type"))
	if err != nil {
		h.reject(w, http.StatusUnsupportedMediaType, err)
		return
	}
	if enc := r.Header.Get("Content-Encoding"); !validEncoding(enc) {
		h.reject(w, http.StatusUnsupportedMediaType,
			errors.Errorf("only %s encoding is accepted, got %q", encodingSnappy, enc))
		return
	}
	counts, err := h.handle(r, msg)

	// The written counts are reported before the status, and on the failure path too: a request that
	// stored most of its series and then failed has to say what landed.
	if msg == MessageV2 {
		counts.setWrittenHeaders(w)
	}

	if err != nil {
		h.reject(w, statusOf(err), err)
		return
	}

	// 204, not 202: the spec says a successful write answers with no content, and a sender that
	// treats 202 as "maybe" would keep the batch queued.
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) reject(w http.ResponseWriter, code int, err error) {
	h.lg.Debug("Reject remote write request", zap.Error(err), zap.Int("code", code))
	http.Error(w, err.Error(), code)
}

// statusOf maps a failure to the status that tells the sender what to do with it. Only a 5xx (and
// 429) makes Prometheus retry; every other code is a permanent failure it drops the batch over, so
// a request that might succeed later must never get a 4xx.
func statusOf(err error) int {
	switch {
	case errors.As(err, new(writeError)):
		return http.StatusInternalServerError
	case errors.As(err, new(tooLargeError)):
		return http.StatusRequestEntityTooLarge
	default:
		return http.StatusBadRequest
	}
}

// writeError marks a sink failure, which is answered with 5xx so the client retries rather than
// stops sending. Everything else is the request's own fault and answered with 4xx.
type writeError struct{ err error }

func (e writeError) Error() string { return e.err.Error() }
func (e writeError) Unwrap() error { return e.err }

// tooLargeError marks a body over the size limit, which has its own status so a sender can tell it
// apart from a body it encoded wrong and shrink its batches.
type tooLargeError struct{ err error }

func (e tooLargeError) Error() string { return e.err.Error() }
func (e tooLargeError) Unwrap() error { return e.err }

func (h *Handler) handle(r *http.Request, msg Message) (Counts, error) {
	s, _ := h.pool.Get().(*requestState)
	defer h.pool.Put(s)

	var err error
	if s.compressed, err = readAll(s.compressed[:0], http.MaxBytesReader(nil, r.Body, h.maxBody)); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return Counts{}, tooLargeError{err: errors.Errorf("body is over the %d byte limit", h.maxBody)}
		}

		return Counts{}, errors.Wrap(err, "read body")
	}

	decoded, err := snappy.DecodedLen(s.compressed)
	if err != nil {
		return Counts{}, errors.Wrap(err, "read decompressed length")
	}
	if decoded > h.maxDecoded {
		return Counts{}, errors.Errorf("body decompresses to %d bytes, over the %d limit",
			decoded, h.maxDecoded)
	}

	if s.raw, err = snappy.Decode(s.raw[:cap(s.raw)], s.compressed); err != nil {
		return Counts{}, errors.Wrap(err, "decompress body")
	}

	batch, series, counts, err := s.convert(s.raw, msg, h.opts)
	if err != nil {
		return Counts{}, err
	}

	if err := h.sink.WriteMetrics(r.Context(), *batch); err != nil {
		// Nothing landed, so nothing is reported as written: the sender must resend all of it.
		return Counts{}, writeError{err: errors.Wrap(err, "write metrics")}
	}

	if counts.Rejected.Invalid > 0 {
		h.lg.Warn("Skipped remote write series with unstorable labels",
			zap.Stringer("message", msg), zap.Int("points", counts.Rejected.Invalid))
	}

	if h.observe != nil {
		h.observe(Stats{
			Message:  msg,
			Bytes:    len(s.raw),
			Series:   series,
			Points:   countPoints(batch),
			Rejected: counts.Rejected,
		})
	}

	return counts, nil
}

// convert decodes raw under the given schema and converts it, returning the batch, how many series
// the request carried, and what became of its points.
func (s *requestState) convert(
	raw []byte, msg Message, o Options,
) (_ *metric.Metrics, series int, _ Counts, _ error) {
	if msg == MessageV2 {
		s.reqV2.Reset()
		if err := s.reqV2.Unmarshal(raw); err != nil {
			return nil, 0, Counts{}, errors.Wrap(err, "unmarshal write request 2.0")
		}

		batch, counts := s.conv.ConvertV2(&s.reqV2, o)

		return batch, len(s.reqV2.Timeseries), counts, nil
	}

	s.req.Reset()
	if err := s.req.Unmarshal(raw); err != nil {
		return nil, 0, Counts{}, errors.Wrap(err, "unmarshal write request")
	}

	batch, counts := s.conv.Convert(s.req.Timeseries, o)

	return batch, len(s.req.Timeseries), counts, nil
}

// countPoints sums the points of a converted batch. It walks metric headers, not points, so it
// costs one pass over memory the conversion just wrote.
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
