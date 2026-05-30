package logqlengine

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

type (
	explainKey    struct{}
	explainTracer struct{}
)

// IsExplainQuery whether if this LogQL query is explained.
func IsExplainQuery(ctx context.Context) bool {
	_, ok := ctx.Value(explainKey{}).(*explainTracer)
	return ok
}

func buildExplainQuery(ctx context.Context, logs *explainLogs) context.Context {
	return startExplainQuery(ctx, logs)
}

func startExplainQuery(ctx context.Context, logs *explainLogs) context.Context {
	ctx = context.WithValue(ctx, explainKey{}, &explainTracer{})
	return logs.InjectLogger(ctx)
}

type explainLogs struct {
	entries []lokiapi.LogEntry
	mux     sync.Mutex
}

func (e *explainLogs) InjectLogger(ctx context.Context) context.Context {
	lg := zctx.From(ctx).WithOptions(
		zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			return zapcore.NewTee(c, e.Core())
		}),
	)
	ctx = zctx.WithOpenTelemetryZap(ctx)
	return zctx.Base(ctx, lg.Named("explain"))
}

func (e *explainLogs) Core() zapcore.Core {
	return &explainCore{
		enc:  zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		with: nil,
		logs: e,
	}
}

func (e *explainLogs) Data() lokiapi.QueryResponseData {
	e.mux.Lock()
	defer e.mux.Unlock()

	stream := lokiapi.Stream{
		Stream: lokiapi.NewOptLabelSet(lokiapi.LabelSet{"log": "explain"}),
		Values: e.entries,
	}

	return lokiapi.QueryResponseData{
		Type: lokiapi.StreamsResultQueryResponseData,
		StreamsResult: lokiapi.StreamsResult{
			Result: lokiapi.Streams{stream},
		},
	}
}

func (e *explainLogs) Push(entry lokiapi.LogEntry) {
	e.mux.Lock()
	defer e.mux.Unlock()

	e.entries = append(e.entries, entry)
}

type explainCore struct {
	enc  zapcore.Encoder
	with []zapcore.Field
	logs *explainLogs
}

var _ zapcore.Core = (*explainCore)(nil)

func (t *explainCore) Enabled(zapcore.Level) bool {
	return true
}

// With adds structured context to the Core.
func (t *explainCore) With(fields []zapcore.Field) zapcore.Core {
	return &explainCore{
		enc:  t.enc.Clone(),
		with: append(slices.Clone(t.with), fields...),
		logs: t.logs,
	}
}

// Check determines whether the supplied Entry should be logged (using the
// embedded LevelEnabler and possibly some extra logic). If the entry
// should be logged, the Core adds itself to the CheckedEntry and returns
// the result.
//
// Callers must use Check before calling Write.
func (t *explainCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(ent, t)
}

// Write serializes the Entry and any Fields supplied at the log site and
// writes them to their destination.
//
// If called, Write should always log the Entry and Fields; it should not
// replicate the logic of Check.
func (t *explainCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	buf, err := t.enc.EncodeEntry(e, fields)
	if err != nil {
		return err
	}
	t.logs.Push(lokiapi.LogEntry{
		T: uint64(e.Time.UnixNano()),
		V: buf.String(),
	})
	return nil
}

// Sync flushes buffered logs (if any).
func (t *explainCore) Sync() error {
	return nil
}

// ExplainQuery is simple Explain expression query.
type ExplainQuery struct {
	Explain Query

	logs *explainLogs
}

var _ Query = (*ExplainQuery)(nil)

// Eval implements [Query].
func (q *ExplainQuery) Eval(ctx context.Context, params EvalParams) (lokiapi.QueryResponseData, error) {
	ctx = startExplainQuery(ctx, q.logs)
	lg := zctx.From(ctx)

	var (
		start       = time.Now()
		resultField zap.Field
	)
	data, err := q.Explain.Eval(ctx, params)
	if err != nil {
		resultField = zap.Error(err)
	} else {
		resultField = zap.String("data_type", string(data.Type))
	}
	lg.Debug("Evaluated query",
		zap.String("query_type", fmt.Sprintf("%T", q.Explain)),
		resultField,
		zap.Duration("took", time.Since(start)),
	)

	return q.logs.Data(), nil
}
