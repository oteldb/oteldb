package storagebackup_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	siglog "github.com/oteldb/storage/signal/log"
	sigmetric "github.com/oteldb/storage/signal/metric"
	sigtrace "github.com/oteldb/storage/signal/trace"
)

// day is the fixed UTC day every test writes into, so nothing depends on the wall clock.
var day = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

// at returns an instant within [day], as unix nanoseconds.
func at(h, m int) int64 {
	return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute).UnixNano()
}

// newStore opens an ephemeral in-memory engine. tenantFn, when non-nil, is the engine's
// record→tenant routing callback — the seam a cluster's shard placement rides on.
func newStore(tb testing.TB, tenantFn func(signal.Resource, signal.Scope) signal.TenantID) *storage.Storage {
	tb.Helper()

	opts := []storage.Option{
		storage.WithBackend(backend.Memory()),
		storage.WithDurability(storage.DurabilityEphemeral),
	}
	if tenantFn != nil {
		opts = append(opts, storage.WithTenant(tenantFn))
	}

	store, err := storage.Open(tb.Context(), storage.Options{}, opts...)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, store.Close(context.WithoutCancel(tb.Context())))
	})
	return store
}

func attrs(kvs ...signal.KeyValue) signal.Attributes { return signal.NewAttributes(kvs...) }

func str(k, v string) signal.KeyValue {
	return signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v))}
}

func num(k string, v int64) signal.KeyValue {
	return signal.KeyValue{Key: []byte(k), Value: signal.IntValue(v)}
}

// attrMap renders attributes as kind-tagged strings, so a test fails when a value round-trips with
// the right text but the wrong type.
func attrMap(a signal.Attributes) map[string]string {
	if len(a) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(a))
	for _, kv := range a {
		out[string(kv.Key)] = fmt.Sprintf("%d:%s", kv.Value.Kind(), kv.Value.AppendText(nil))
	}
	return out
}

// logRow is one restored log record, flattened for comparison.
type logRow struct {
	Resource     map[string]string
	ScopeName    string
	ScopeVersion string
	ScopeAttrs   map[string]string
	Timestamp    int64
	Observed     int64
	Severity     int32
	SeverityText string
	Body         string
	TraceID      string
	SpanID       string
	Flags        uint32
	Dropped      uint32
	Attrs        map[string]string
}

// spanRow is one restored span, flattened for comparison.
type spanRow struct {
	Resource      map[string]string
	ScopeName     string
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	Kind          int32
	StatusCode    int32
	StatusMessage string
	Start         int64
	End           int64
	Attrs         map[string]string
	Events        []string
	Links         []string
}

// seriesRow is one restored metric series, flattened for comparison.
type seriesRow struct {
	Resource    map[string]string
	Name        string
	Unit        string
	Kind        sigmetric.PointKind
	Temporality sigmetric.Temporality
	Monotonic   bool
	Attrs       map[string]string
	Samples     []string
}

// scan drains every batch of one tenant and signal over the whole test day.
func scan(tb testing.TB, store *storage.Storage, tenant signal.TenantID, sig signal.Signal) []*fetch.Batch {
	tb.Helper()

	var fetcher fetch.Fetcher
	switch sig {
	case signal.Log:
		fetcher = store.LogFetcher(tenant)
	case signal.Trace:
		fetcher = store.TraceFetcher(tenant)
	default:
		fetcher = store.Fetcher(tenant)
	}

	it, err := fetcher.Fetch(tb.Context(), fetch.Request{
		Tenant: tenant,
		Signal: sig,
		Start:  day.UnixNano(),
		End:    day.AddDate(0, 0, 1).UnixNano() - 1,
	})
	require.NoError(tb, err)
	defer func() {
		require.NoError(tb, it.Close())
	}()

	var out []*fetch.Batch
	for {
		b, err := it.Next(tb.Context())
		if err == io.EOF {
			return out
		}
		require.NoError(tb, err)
		out = append(out, b)
	}
}

func collectLogs(tb testing.TB, store *storage.Storage, tenants ...signal.TenantID) []logRow {
	tb.Helper()

	var out []logRow
	for _, tenant := range tenants {
		for _, b := range scan(tb, store, tenant, signal.Log) {
			cols := columns(b)
			for i := range b.Timestamps {
				out = append(out, logRow{
					Resource:     attrMap(b.Series.Resource.Attributes),
					ScopeName:    string(b.Series.Scope.Name),
					ScopeVersion: string(b.Series.Scope.Version),
					ScopeAttrs:   attrMap(b.Series.Scope.Attributes),
					Timestamp:    b.Timestamps[i],
					Observed:     cols.int(siglog.ColObserved, i),
					Severity:     int32(cols.int(siglog.ColSeverity, i)),
					SeverityText: string(cols.raw(siglog.ColSeverityText, i)),
					Body:         string(cols.raw(siglog.ColBody, i)),
					TraceID:      fmt.Sprintf("%x", cols.raw(siglog.ColTraceID, i)),
					SpanID:       fmt.Sprintf("%x", cols.raw(siglog.ColSpanID, i)),
					Flags:        uint32(cols.int(siglog.ColFlags, i)),
					Dropped:      uint32(cols.int(siglog.ColDropped, i)),
					Attrs:        attrMap(decodeAttrs(tb, cols.raw(siglog.ColAttrs, i))),
				})
			}
		}
	}
	slices.SortFunc(out, func(a, b logRow) int { return strings.Compare(a.Body, b.Body) })
	return out
}

func collectSpans(tb testing.TB, store *storage.Storage, tenants ...signal.TenantID) []spanRow {
	tb.Helper()

	var out []spanRow
	for _, tenant := range tenants {
		for _, b := range scan(tb, store, tenant, signal.Trace) {
			cols := columns(b)
			for i := range b.Timestamps {
				row := spanRow{
					Resource:      attrMap(b.Series.Resource.Attributes),
					ScopeName:     string(b.Series.Scope.Name),
					TraceID:       fmt.Sprintf("%x", cols.raw(sigtrace.ColTraceID, i)),
					SpanID:        fmt.Sprintf("%x", cols.raw(sigtrace.ColSpanID, i)),
					ParentSpanID:  fmt.Sprintf("%x", cols.raw(sigtrace.ColParentSpanID, i)),
					Name:          string(cols.raw(sigtrace.ColName, i)),
					Kind:          int32(cols.int(sigtrace.ColKind, i)),
					StatusCode:    int32(cols.int(sigtrace.ColStatusCode, i)),
					StatusMessage: string(cols.raw(sigtrace.ColStatusMsg, i)),
					Start:         b.Timestamps[i],
					End:           b.Timestamps[i] + cols.int(sigtrace.ColDuration, i),
					Attrs:         attrMap(decodeAttrs(tb, cols.raw(sigtrace.ColAttrs, i))),
				}
				if raw := cols.raw(sigtrace.ColEvents, i); len(raw) > 0 {
					evs, err := sigtrace.DecodeEvents(raw)
					require.NoError(tb, err)
					for _, ev := range evs {
						row.Events = append(row.Events, fmt.Sprintf("%d/%s/%v", ev.Time, ev.Name, attrMap(ev.Attributes)))
					}
				}
				if raw := cols.raw(sigtrace.ColLinks, i); len(raw) > 0 {
					links, err := sigtrace.DecodeLinks(raw)
					require.NoError(tb, err)
					for _, ln := range links {
						row.Links = append(row.Links, fmt.Sprintf("%x/%x/%v", ln.TraceID, ln.SpanID, attrMap(ln.Attributes)))
					}
				}
				out = append(out, row)
			}
		}
	}
	slices.SortFunc(out, func(a, b spanRow) int { return strings.Compare(a.SpanID, b.SpanID) })
	return out
}

func collectSeries(tb testing.TB, store *storage.Storage, tenants ...signal.TenantID) []seriesRow {
	tb.Helper()

	var out []seriesRow
	for _, tenant := range tenants {
		for _, b := range scan(tb, store, tenant, signal.Metric) {
			row := seriesRow{Resource: attrMap(b.Series.Resource.Attributes), Attrs: map[string]string{}}
			for _, kv := range b.Series.Attributes {
				switch string(kv.Key) {
				case string(sigmetric.LabelName):
					row.Name = string(kv.Value.Str())
				case string(sigmetric.LabelUnit):
					row.Unit = string(kv.Value.Str())
				case string(sigmetric.LabelKind):
					row.Kind = sigmetric.PointKind(kv.Value.Int())
				case string(sigmetric.LabelTemporality):
					row.Temporality = sigmetric.Temporality(kv.Value.Int())
				case string(sigmetric.LabelMonotonic):
					row.Monotonic = kv.Value.Bool()
				default:
					row.Attrs[string(kv.Key)] = fmt.Sprintf("%d:%s", kv.Value.Kind(), kv.Value.AppendText(nil))
				}
			}
			for i := range b.Timestamps {
				row.Samples = append(row.Samples, fmt.Sprintf("%d=%g", b.Timestamps[i], b.Values[i]))
			}
			slices.Sort(row.Samples)
			out = append(out, row)
		}
	}
	slices.SortFunc(out, func(a, b seriesRow) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return strings.Compare(fmt.Sprint(a.Attrs), fmt.Sprint(b.Attrs))
	})
	return out
}

func decodeAttrs(tb testing.TB, raw []byte) signal.Attributes {
	tb.Helper()

	if len(raw) == 0 {
		return nil
	}
	a, _, err := signal.DecodeAttributes(raw)
	require.NoError(tb, err)
	return a
}

type batchCols struct{ b *fetch.Batch }

func columns(b *fetch.Batch) batchCols { return batchCols{b: b} }

func (c batchCols) int(name string, i int) int64 {
	col, ok := c.b.Column(name)
	if !ok || i >= len(col.Int64) {
		return 0
	}
	return col.Int64[i]
}

func (c batchCols) raw(name string, i int) []byte {
	col, ok := c.b.Column(name)
	if !ok || i >= len(col.Bytes) {
		return nil
	}
	return col.Bytes[i]
}
