package lokicompliance

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/golang/snappy"
	"github.com/grafana/loki/pkg/push"
	ht "github.com/ogen-go/ogen/http"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

// GenerateOptions defines options for [LogGenerator.Generate].
type GenerateOptions struct {
	Start, End time.Time
	Step       time.Duration
	Lines      int
	Streams    []string
	Client     ht.Client
}

func (opts *GenerateOptions) setDefaults() {
	switch s, e := opts.Start, opts.End; {
	case s.IsZero() && e.IsZero():
		opts.End = time.Now()
		fallthrough
	case s.IsZero():
		opts.Start = opts.End.Add(-time.Minute)
	case e.IsZero():
		opts.End = opts.Start.Add(time.Minute)
	}
	if opts.Step == 0 {
		opts.Step = time.Second
	}
	if opts.Lines == 0 {
		opts.Lines = 1
	}
	if len(opts.Streams) == 0 {
		opts.Streams = []string{"log.json"}
	}
	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}
}

// GenerateLogs generates logs and sends them to targets
func GenerateLogs(ctx context.Context, targets []string, opts GenerateOptions) error {
	opts.setDefaults()
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec: G404

	streams := map[string]push.Stream{}
	for ts := opts.Start; ts.Before(opts.End); ts = ts.Add(opts.Step) {
		rt := ts
		for i := 0; i < opts.Lines; i++ {
			// NOTE: timestamp sorting is implementation-defined, so
			// 	we add a slight difference to each timestamp within step.
			rt = rt.Add(100 * time.Microsecond)

			stream := randomElement(r, opts.Streams)
			s, ok := streams[stream]
			if !ok {
				s = push.Stream{
					Labels: makeLabels(
						labelPair{Name: "filename", Value: stream},
						labelPair{Name: "job", Value: "varlogs"},
					),
				}
			}
			line := NewLogEntry(r, rt)
			s.Entries = append(s.Entries, push.Entry{
				Timestamp: rt,
				Line:      line.JSON(),
			})
			streams[stream] = s
		}
	}

	req := push.PushRequest{
		Streams: maps.Values(streams),
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	for _, target := range targets {
		target := target
		grp.Go(func() error {
			ctx := grpCtx
			if err := sendLogs(ctx, opts.Client, target, req); err != nil {
				return errors.Wrapf(err, "target %q", target)
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}

	return nil
}

type labelPair struct {
	Name, Value string
}

func makeLabels(labels ...labelPair) string {
	var sb strings.Builder
	sb.WriteByte('{')

	slices.SortFunc(labels, func(a, b labelPair) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i, p := range labels {
		if i != 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s=%q", p.Name, p.Value)
	}

	sb.WriteByte('}')
	return sb.String()
}

func sendLogs(ctx context.Context, client ht.Client, target string, body push.PushRequest) error {
	data, err := body.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshal")
	}
	compressed := snappy.Encode(nil, data)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(compressed))
	if err != nil {
		return errors.Wrap(err, "create request")
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send")
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if code := resp.StatusCode; code >= 400 {
		var errbody strings.Builder
		if _, err := io.Copy(&errbody, resp.Body); err != nil {
			return err
		}
		return errors.Errorf("status code %d: %q", code, &errbody)
	}

	return nil
}

type LogEntry struct {
	Timestamp time.Time
	Level     plog.SeverityNumber

	// HTTP attributes.
	Protocol   string
	Method     string
	ClientIP   netip.Addr
	RemoteIP   netip.Addr
	RemotePort uint16
	Status     int
	Took       time.Duration
	Size       uint64

	SpanID  pcommon.SpanID
	TraceID pcommon.TraceID
}

// NewLogEntry generates new [LogEntry].
func NewLogEntry(r *rand.Rand, ts time.Time) LogEntry {
	return LogEntry{
		Timestamp: ts,
		Level: randomElement(r, []plog.SeverityNumber{
			plog.SeverityNumberTrace,
			plog.SeverityNumberDebug,
			plog.SeverityNumberInfo,
			plog.SeverityNumberWarn,
			plog.SeverityNumberError,
			plog.SeverityNumberFatal,
		}),
		Protocol: randomElement(r, []string{
			semconv.HTTPFlavorHTTP10.Value.String(),
			semconv.HTTPFlavorHTTP11.Value.String(),
			semconv.HTTPFlavorHTTP20.Value.String(),
		}),
		Method: randomElement(r, []string{
			http.MethodGet,
			http.MethodHead,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		}),
		ClientIP:   randomAddr(r),
		RemoteIP:   netip.AddrFrom4([4]byte{127, 0, 0, 1}),
		RemotePort: uint16(r.Intn(math.MaxUint16)),
		Status: randomElement(r, []int{
			http.StatusOK,
			http.StatusCreated,
			http.StatusNoContent,

			http.StatusBadRequest,
			http.StatusUnauthorized,
			http.StatusForbidden,
			http.StatusNotFound,
			http.StatusMethodNotAllowed,
			http.StatusNotAcceptable,
			http.StatusUnsupportedMediaType,

			http.StatusInternalServerError,
			http.StatusNotImplemented,
		}),
		Took:    time.Duration(r.Int63n(int64(30*time.Minute)) + int64(time.Millisecond)),
		Size:    uint64(r.Intn(1024*1024)) + 1025,
		SpanID:  randomSpanID(r),
		TraceID: randomTraceID(r),
	}
}

func randomSpanID(r *rand.Rand) (s pcommon.SpanID) {
	_, _ = r.Read(s[:])
	return s
}

func randomTraceID(r *rand.Rand) (s pcommon.TraceID) {
	_, _ = r.Read(s[:])
	return s
}

func randomAddr(r *rand.Rand) netip.Addr {
	var buf [4]byte
	_, _ = r.Read(buf[:])
	return netip.AddrFrom4(buf)
}

func randomElement[S ~[]T, T any](r *rand.Rand, s S) (zero T) {
	if len(s) == 0 {
		return zero
	}
	return s[r.Intn(len(s))]
}

// JSON returns JSON-encoded line.
func (e LogEntry) JSON() string {
	enc := jx.GetEncoder()
	defer jx.PutEncoder(enc)

	e.EncodeJSON(enc)
	return enc.String()
}

// EncodeJSON encodes entry to given encoder.
func (e LogEntry) EncodeJSON(enc *jx.Encoder) {
	enc.ObjStart()
	enc.Field("ts", func(enc *jx.Encoder) {
		enc.Str(e.Timestamp.String())
	})
	enc.Field("level", func(enc *jx.Encoder) {
		enc.Str(e.Level.String())
	})
	enc.Field("protocol", func(enc *jx.Encoder) {
		enc.Str(e.Protocol)
	})
	enc.Field("method", func(enc *jx.Encoder) {
		enc.Str(e.Method)
	})
	enc.Field("client_ip", func(enc *jx.Encoder) {
		enc.Str(e.ClientIP.String())
	})
	enc.Field("remote_ip", func(enc *jx.Encoder) {
		enc.Str(e.RemoteIP.String())
	})
	enc.Field("remote_port", func(enc *jx.Encoder) {
		enc.UInt16(e.RemotePort)
	})
	enc.Field("status", func(enc *jx.Encoder) {
		enc.Int(e.Status)
	})
	enc.Field("took", func(enc *jx.Encoder) {
		enc.Str(e.Took.String())
	})
	enc.Field("size", func(enc *jx.Encoder) {
		enc.Str(humanize.Bytes(e.Size))
	})
	if id := e.SpanID; !id.IsEmpty() {
		enc.Field("span_id", func(enc *jx.Encoder) {
			enc.Str(id.String())
		})
	}
	if id := e.TraceID; !id.IsEmpty() {
		enc.Field("trace_id", func(enc *jx.Encoder) {
			enc.Str(id.String())
		})
	}
	enc.ObjEnd()
}

// OTEL returns OpenTelemetry log record.
func (e LogEntry) OTEL(r plog.LogRecord) {
	ts := pcommon.NewTimestampFromTime(e.Timestamp)
	r.SetObservedTimestamp(ts)
	r.SetTimestamp(ts)
	r.SetTraceID(e.TraceID)
	r.SetSpanID(e.SpanID)
	r.SetFlags(plog.DefaultLogRecordFlags)
	r.SetSeverityText(e.Level.String())
	r.SetSeverityNumber(e.Level)
	r.Body().SetStr(e.JSON())
	{
		a := r.Attributes()
		a.PutStr(string(semconv.HTTPMethodKey), e.Method)
		a.PutInt(string(semconv.HTTPStatusCodeKey), int64(e.Status))
	}
	r.SetDroppedAttributesCount(0)
}
