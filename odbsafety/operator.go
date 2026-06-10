package odbsafety

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"go.uber.org/multierr"

	safetyconfig "github.com/oteldb/oteldb/internal/odbsafety"
)

// Transformer is a stanza operator that applies oteldb log safety limiting.
type Transformer struct {
	helper.TransformerOperator

	maxRatePerSecond int
	redactFields     []string
	handler          *safetyconfig.Handler[*entry.Entry]
	now              func() time.Time

	rateWindowStart time.Time
	rateWindowCount int
}

var _ operator.Operator = (*Transformer)(nil)

func newTransformer(base helper.TransformerOperator, cfg Config) *Transformer {
	sampler := func() bool { return rand.Float64() < cfg.SampleRate } //#nosec G404
	return &Transformer{
		TransformerOperator: base,
		maxRatePerSecond:    cfg.MaxRatePerSecond,
		redactFields:        cfg.RedactFields,
		handler:             safetyconfig.NewHandler[*entry.Entry](cfg.Config, sampler, safetyconfig.NoopMetrics{}),
		now:                 time.Now,
	}
}

// ProcessBatch applies safety limiting to a batch of entries.
func (t *Transformer) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	if len(t.redactFields) == 0 && (t.maxRatePerSecond == 0 || t.handler == nil) {
		return t.WriteBatch(ctx, entries)
	}

	out := make([]*entry.Entry, 0, len(entries))
	batch := processBatch{ctx: ctx, output: &out}
	var errs error
	for _, ent := range entries {
		if err := t.process(ctx, ent, &batch); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	errs = multierr.Append(errs, t.WriteBatch(ctx, out))
	return errs
}

// Process applies safety limiting to a single entry.
func (t *Transformer) Process(ctx context.Context, ent *entry.Entry) error {
	if len(t.redactFields) == 0 && (t.maxRatePerSecond == 0 || t.handler == nil) {
		return t.Write(ctx, ent)
	}
	out := make([]*entry.Entry, 0, 1)
	batch := processBatch{ctx: ctx, output: &out}
	if err := t.process(ctx, ent, &batch); err != nil {
		return err
	}
	return t.WriteBatch(ctx, out)
}

func (t *Transformer) process(ctx context.Context, ent *entry.Entry, batch *processBatch) error {
	skip, err := t.Skip(ctx, ent)
	if err != nil {
		return t.HandleEntryErrorWithWrite(ctx, ent, err, batch.write)
	}
	if skip {
		return batch.write(ctx, ent)
	}

	t.redact(ent)
	if !t.exceeded() || t.handler == nil {
		return batch.write(ctx, ent)
	}
	if t.handler.Handle(ctx, batch, ent) {
		return nil
	}
	return batch.write(ctx, ent)
}

func (t *Transformer) redact(ent *entry.Entry) {
	if len(t.redactFields) == 0 || ent.Attributes == nil {
		return
	}
	for _, field := range t.redactFields {
		if _, ok := ent.Attributes[field]; ok {
			ent.Attributes[field] = "<redacted>"
		}
	}
}

func (t *Transformer) exceeded() bool {
	if t.maxRatePerSecond <= 0 {
		return false
	}
	now := t.now()
	if t.rateWindowStart.IsZero() || now.Sub(t.rateWindowStart) >= time.Second || now.Before(t.rateWindowStart) {
		t.rateWindowStart = now.Truncate(time.Second)
		t.rateWindowCount = 0
	}
	t.rateWindowCount++
	return t.rateWindowCount > t.maxRatePerSecond
}

type processBatch struct {
	ctx             context.Context
	output          *[]*entry.Entry
	compacted       map[string]*entry.Entry
	truncatedBySlot map[int64]*entry.Entry
}

func (b *processBatch) write(_ context.Context, ent *entry.Entry) error {
	*b.output = append(*b.output, ent)
	return nil
}

func (b *processBatch) Key(ent *entry.Entry, fields []string) string {
	return entryKey(ent, fields)
}

func (b *processBatch) Time(ent *entry.Entry) time.Time {
	return entryTime(ent)
}

func (b *processBatch) Truncate(slot int64, ent *entry.Entry, windowStart, windowEnd time.Time) {
	if b.truncatedBySlot == nil {
		b.truncatedBySlot = make(map[int64]*entry.Entry, 1)
	}
	out, ok := b.truncatedBySlot[slot]
	if !ok {
		out = ent.Copy()
		out.Body = "<output is truncated>"
		if out.Attributes == nil {
			out.Attributes = make(map[string]any, 3)
		}
		out.Attributes["oteldb.truncated_count"] = int64(0)
		out.Attributes["oteldb.window_start"] = windowStart.Format(time.RFC3339Nano)
		out.Attributes["oteldb.window_end"] = windowEnd.Format(time.RFC3339Nano)
		b.truncatedBySlot[slot] = out
		*b.output = append(*b.output, out)
	}
	out.Attributes["oteldb.truncated_count"] = out.Attributes["oteldb.truncated_count"].(int64) + 1
}

func (b *processBatch) Compact(key string, ent *entry.Entry) {
	if b.compacted == nil {
		b.compacted = make(map[string]*entry.Entry, 1)
	}
	out, ok := b.compacted[key]
	if !ok {
		out = ent.Copy()
		if out.Attributes == nil {
			out.Attributes = make(map[string]any, 1)
		}
		out.Attributes["oteldb.collapsed_count"] = int64(0)
		b.compacted[key] = out
		*b.output = append(*b.output, out)
	}
	out.Attributes["oteldb.collapsed_count"] = out.Attributes["oteldb.collapsed_count"].(int64) + 1
}

func entryTime(ent *entry.Entry) time.Time {
	if !ent.Timestamp.IsZero() {
		return ent.Timestamp
	}
	if !ent.ObservedTimestamp.IsZero() {
		return ent.ObservedTimestamp
	}
	return time.Now()
}

func entryKey(ent *entry.Entry, fields []string) string {
	if len(fields) == 0 {
		return valueKey(ent.Body)
	}
	var b strings.Builder
	for _, field := range fields {
		b.WriteString(field)
		b.WriteByte('=')
		switch field {
		case "body":
			b.WriteString(valueKey(ent.Body))
		default:
			if ent.Attributes != nil {
				b.WriteString(valueKey(ent.Attributes[field]))
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func valueKey(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprint(v)
	}
}
