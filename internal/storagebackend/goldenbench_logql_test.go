package storagebackend_test

// Golden LogQL benchmarks — the stable set used to gate storage-level read regressions from the
// LogQL front-end. They mirror the contract of storage's own golden_bench_test.go:
//
//   - Deterministic: one fixed canonical corpus, no RNG and no wall-clock (timestamps are derived
//     from a fixed epoch), so run-to-run variance is the machine, not the data.
//   - Flushed: the corpus is written as goldenLogQLParts immutable parts (plus one unflushed head
//     round), so every query exercises the part-scan/decode path and the head+parts merge, not just
//     the in-memory head.
//   - Comparable: b.SetBytes is the LOGICAL (uncompressed) size of the streams the query selects, so
//     MB/s is a real scan speed rather than a function of the codec's compression ratio.
//   - Guarded: every case asserts an exact result count once, outside the timed loop, so a silently
//     empty (and therefore fast) query can never masquerade as an improvement.
//
// Keep this set small and stable: the sub-benchmark names under BenchmarkGoldenLogQL/… are the CI
// baseline keys, and changing the corpus resets the historical baseline.

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

const (
	// goldenLogQLServices is the number of JSON-bodied services; each is one stream.
	goldenLogQLServices = 8
	// goldenLogQLPerRound is the number of records each stream contributes per round.
	goldenLogQLPerRound = 600
	// goldenLogQLParts is the number of rounds flushed to their own immutable part. A ninth round
	// stays in the head, so reads merge parts with the head the way a live store does.
	goldenLogQLParts = 8
	// goldenLogQLNeedleRound is the round that carries the needle records; every other part can be
	// pruned by the body bloom, which is exactly what the needle case measures.
	goldenLogQLNeedleRound = 3
	// goldenLogQLNeedleEvery selects the needle records inside the needle round (svc-0 only).
	goldenLogQLNeedleEvery = 150
	// goldenLogQLStartTS is the fixed corpus epoch (2024-01-01T00:00:00Z), so nothing depends on the
	// wall clock. Records are spaced goldenLogQLSpacing apart.
	goldenLogQLStartTS = int64(1_704_067_200_000_000_000)
	goldenLogQLSpacing = 100 * time.Millisecond
	// goldenLogQLLogfmtService is the one stream whose bodies are logfmt rather than JSON, so the
	// logfmt parser stage has a corpus of its own.
	goldenLogQLLogfmtService = "logfmt"
	// goldenLogQLNeedle is the high-selectivity literal. It is deliberately three words: the storage
	// optimizer derives a bloom token hint from the *interior* tokens of a line-filter literal only
	// (edge tokens may be glued into a larger token by a sub-word match), so a one-word needle would
	// prune nothing.
	goldenLogQLNeedle = "needle deadbeef marker"
)

var (
	goldenLogQLLevels = [6]struct {
		num  plog.SeverityNumber
		text string
	}{
		{plog.SeverityNumberTrace, "TRACE"},
		{plog.SeverityNumberDebug, "DEBUG"},
		{plog.SeverityNumberInfo, "INFO"},
		{plog.SeverityNumberWarn, "WARN"},
		{plog.SeverityNumberError, "ERROR"},
		{plog.SeverityNumberFatal, "FATAL"},
	}
	goldenLogQLMethods  = [6]string{"GET", "POST", "PUT", "HEAD", "DELETE", "PATCH"}
	goldenLogQLStatuses = [6]int64{200, 201, 204, 400, 404, 500}
	goldenLogQLRegions  = [4]string{"eu-west-1", "us-east-1", "us-west-2", "ap-south-1"}
)

// goldenLogQLStreams is the canonical stream set: goldenLogQLServices JSON services (the first half
// in prod, the second in staging) plus one prod logfmt service.
func goldenLogQLStreams() []goldenLogQLStream {
	streams := make([]goldenLogQLStream, 0, goldenLogQLServices+1)
	for s := range goldenLogQLServices {
		env := "prod"
		if s >= goldenLogQLServices/2 {
			env = "staging"
		}
		streams = append(streams, goldenLogQLStream{service: "svc-" + strconv.Itoa(s), env: env})
	}
	return append(streams, goldenLogQLStream{service: goldenLogQLLogfmtService, env: "prod", logfmt: true})
}

// goldenLogQLStream describes one resource (stream) of the corpus.
type goldenLogQLStream struct {
	service string
	env     string
	logfmt  bool
}

// goldenLogQLCorpus is the read-only fixture shared by every sub-benchmark: a flushed store, the
// LogQL engine over it, the query window, and the per-service logical (uncompressed) byte tally used
// for b.SetBytes.
type goldenLogQLCorpus struct {
	engine *logqlengine.Engine
	start  time.Time
	end    time.Time
	// logical maps service name → uncompressed bytes of its records (body + attributes + 16 bytes of
	// timestamp/severity), the denominator for a query's scan throughput.
	logical map[string]int64
}

// bytesFor sums the logical size of the named services.
func (c *goldenLogQLCorpus) bytesFor(services ...string) int64 {
	var n int64
	for _, s := range services {
		n += c.logical[s]
	}
	return n
}

// goldenLogQLRound builds one round of the corpus: every stream's goldenLogQLPerRound records, with
// a JSON or logfmt body, rotating severities, and http.method/http.status_code/region record
// attributes (structured metadata). It accumulates each stream's logical byte count into logical.
// Fully deterministic — round and record index decide everything.
func goldenLogQLRound(round int, logical map[string]int64) plog.Logs {
	ld := plog.NewLogs()
	for _, st := range goldenLogQLStreams() {
		rl := ld.ResourceLogs().AppendEmpty()
		res := rl.Resource().Attributes()
		res.PutStr("service.name", st.service)
		res.PutStr("env", st.env)

		recs := rl.ScopeLogs().AppendEmpty().LogRecords()
		recs.EnsureCapacity(goldenLogQLPerRound)
		for i := range goldenLogQLPerRound {
			lv := goldenLogQLLevels[i%len(goldenLogQLLevels)]
			method := goldenLogQLMethods[i%len(goldenLogQLMethods)]
			status := goldenLogQLStatuses[i%len(goldenLogQLStatuses)]
			region := goldenLogQLRegions[i%len(goldenLogQLRegions)]

			// The needle lives in exactly one part and one service, so the other parts are prunable.
			note := "ok"
			if round == goldenLogQLNeedleRound && st.service == "svc-0" && i%goldenLogQLNeedleEvery == 0 {
				note = goldenLogQLNeedle
			}

			var body string
			if st.logfmt {
				body = fmt.Sprintf("level=%s method=%s status=%d client_ip=10.0.0.%d duration_ms=%d note=%q",
					lv.text, method, status, i%256, i%97, note)
			} else {
				body = fmt.Sprintf(`{"level":%q,"method":%q,"status":%d,"client_ip":"10.0.0.%d","duration_ms":%d,"note":%q}`,
					lv.text, method, status, i%256, i%97, note)
			}

			off := time.Duration(round*goldenLogQLPerRound+i) * goldenLogQLSpacing
			r := recs.AppendEmpty()
			r.SetTimestamp(pcommon.Timestamp(goldenLogQLStartTS + int64(off)))
			r.SetSeverityNumber(lv.num)
			r.SetSeverityText(lv.text)
			r.Body().SetStr(body)
			a := r.Attributes()
			a.PutStr("http.method", method)
			a.PutInt("http.status_code", status)
			a.PutStr("region", region)

			// body + attribute keys/values + timestamp/severity, matching the storage golden bench's
			// accounting. http.status_code is an int64 ⇒ 8 bytes.
			logical[st.service] += int64(len(body)) +
				int64(len("http.method")+len(method)) +
				int64(len("http.status_code")+8) +
				int64(len("region")+len(region)) +
				16
		}
	}
	return ld
}

// goldenLogQLFixture builds the canonical corpus once: goldenLogQLParts rounds each flushed to its
// own immutable part, plus one final round left in the head. Reads therefore merge many parts with a
// live head — the shape a running store actually serves. No compaction is forced, so the cross-part
// merge and per-part pruning stay in the measured path.
func goldenLogQLFixture(tb testing.TB) *goldenLogQLCorpus {
	tb.Helper()

	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = store.Close(ctx) })

	backend := storagebackend.New(store)
	logical := make(map[string]int64)

	for round := range goldenLogQLParts {
		require.NoError(tb, backend.ConsumeLogs(ctx, goldenLogQLRound(round, logical)))
		require.NoError(tb, store.Admin().Flush(ctx, "", signal.Log))
	}
	// One more round stays in the head.
	require.NoError(tb, backend.ConsumeLogs(ctx, goldenLogQLRound(goldenLogQLParts, logical)))

	engine, err := logqlengine.NewEngine(backend.Logs(), logqlengine.Options{
		Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}},
	})
	require.NoError(tb, err)

	rounds := goldenLogQLParts + 1
	start := time.Unix(0, goldenLogQLStartTS)
	end := start.Add(time.Duration(rounds*goldenLogQLPerRound) * goldenLogQLSpacing)

	return &goldenLogQLCorpus{engine: engine, start: start, end: end, logical: logical}
}

// logqlResultCount counts the samples/entries in a LogQL result, regardless of its shape. It is the
// single number each case asserts on, so an accidentally-empty query fails loudly instead of
// benchmarking nothing.
func logqlResultCount(data lokiapi.QueryResponseData) int {
	var n int
	switch data.Type {
	case lokiapi.StreamsResultQueryResponseData:
		for _, s := range data.StreamsResult.Result {
			n += len(s.Values)
		}
	case lokiapi.MatrixResultQueryResponseData:
		for _, s := range data.MatrixResult.Result {
			n += len(s.Values)
		}
	case lokiapi.VectorResultQueryResponseData:
		n = len(data.VectorResult.Result)
	case lokiapi.ScalarResultQueryResponseData:
		n = 1
	}
	return n
}

// goldenLogQLCase is one sub-benchmark: a LogQL query, the eval shape it runs with, the logical
// bytes it scans, and the exact result count it must produce.
type goldenLogQLCase struct {
	name string
	// query is the LogQL expression.
	query string
	// step, when non-zero, makes this a range (metric) query.
	step time.Duration
	// limit bounds a log query's entry count (Grafana passes one); <=0 means unbounded.
	limit int
	// scans lists the services the selector reaches, for b.SetBytes. Empty ⇒ no throughput number.
	scans []string
	// want is the exact number of entries/samples the query must return.
	want int
}

// BenchmarkGoldenLogQL is the definitive LogQL-over-storage read set. Sub-benchmarks:
//
//	full_scan             — regex over every stream, unbounded: no pruning, everything materialized
//	select_service        — bare stream selector on one stream (postings pruning + part scan)
//	select_multi_stream   — bare selector matching half the streams (wide scan, many streams merged)
//	select_regexp         — regexp stream matcher (pushed to the postings index, not post-filtered)
//	line_filter           — `|= "GET"` (offloaded to a storage body condition; one word ⇒ no bloom hint)
//	line_filter_negated   — `!= "GET"` (negation is NOT offloaded: the engine filters after materializing)
//	label_filter          — `| http_method="GET"` structured-metadata equality (offloaded per record)
//	json_parser           — `| json | status>=400` (full body materialization + engine-side parse)
//	logfmt_parser         — the same shape over the logfmt stream
//	needle                — high-selectivity multi-word literal present in a single part (bloom pruning)
//	limit_backward        — the Grafana shape: wide selector, backward, Limit 100
//	metric_count_by_level — `sum by (level) (count_over_time(…[1m]))` (bucketed sampling pushdown)
//	metric_rate_by_service— `sum by (service_name) (rate(…[1m]))` (grouping the pushdown can't honor)
func BenchmarkGoldenLogQL(b *testing.B) {
	corpus := goldenLogQLFixture(b)

	// The prod half of the corpus: the wide-scan selector's stream set.
	prod := []string{"svc-0", "svc-1", "svc-2", "svc-3", goldenLogQLLogfmtService}

	// Every stream, derived from the same source the corpus is built from.
	all := make([]string, 0, goldenLogQLServices+1)
	for _, st := range goldenLogQLStreams() {
		all = append(all, st.service)
	}

	for _, tc := range []goldenLogQLCase{
		{
			// The worst case, mirroring storage's own query/promql_full_scan_count: a regex over
			// every stream prunes nothing in the postings index, and no limit bounds the result, so
			// the whole window is fetched and materialized. It is the ceiling the other log cases
			// are read against.
			name:  "full_scan",
			query: `{service_name=~".+"}`,
			scans: all,
			want:  (goldenLogQLServices + 1) * goldenLogQLPerRound * (goldenLogQLParts + 1),
		},
		{
			name:  "select_service",
			query: `{service_name="svc-0"}`,
			limit: 1000,
			scans: []string{"svc-0"},
			want:  1000,
		},
		{
			name:  "select_multi_stream",
			query: `{env="prod"}`,
			limit: 1000,
			scans: prod,
			want:  1000,
		},
		{
			name:  "select_regexp",
			query: `{service_name=~"svc-[0-3]"}`,
			limit: 1000,
			scans: []string{"svc-0", "svc-1", "svc-2", "svc-3"},
			want:  1000,
		},
		{
			name:  "line_filter",
			query: `{service_name="svc-0"} |= "\"method\":\"GET\""`,
			limit: 1000,
			scans: []string{"svc-0"},
			want:  900,
		},
		{
			name:  "line_filter_negated",
			query: `{service_name="svc-0"} != "\"method\":\"GET\""`,
			limit: 1000,
			scans: []string{"svc-0"},
			want:  1000,
		},
		{
			name:  "label_filter",
			query: `{service_name="svc-0"} | http_method="GET"`,
			limit: 1000,
			scans: []string{"svc-0"},
			want:  900,
		},
		{
			name:  "json_parser",
			query: `{service_name="svc-0"} | json | status>=400`,
			limit: 1000,
			scans: []string{"svc-0"},
			want:  1000,
		},
		{
			name:  "logfmt_parser",
			query: `{service_name="logfmt"} | logfmt | status>=400`,
			limit: 1000,
			scans: []string{goldenLogQLLogfmtService},
			want:  1000,
		},
		{
			name:  "needle",
			query: `{env="prod"} |= "` + goldenLogQLNeedle + `"`,
			limit: 1000,
			scans: prod,
			want:  4,
		},
		{
			name:  "limit_backward",
			query: `{env="prod"}`,
			limit: 100,
			scans: prod,
			want:  100,
		},
		{
			name:  "metric_count_by_level",
			query: `sum by (level) (count_over_time({env="prod"}[1m]))`,
			step:  30 * time.Second,
			scans: prod,
			want:  115,
		},
		{
			name:  "metric_rate_by_service",
			query: `sum by (service_name) (rate({env="prod"}[1m]))`,
			step:  30 * time.Second,
			scans: prod,
			want:  100,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			goldenLogQLRun(b, corpus, tc)
		})
	}
}

// goldenLogQLRun evaluates one case: it parses+plans once (planning is not the storage signal),
// checks the result count once outside the timed loop, then times repeated evaluations.
func goldenLogQLRun(b *testing.B, corpus *goldenLogQLCorpus, tc goldenLogQLCase) {
	b.Helper()

	ctx := context.Background()
	params := logqlengine.EvalParams{
		Start:     corpus.start,
		End:       corpus.end,
		Direction: logqlengine.DirectionBackward,
		Limit:     tc.limit,
		Step:      tc.step,
	}

	q, err := corpus.engine.NewQuery(ctx, tc.query)
	require.NoError(b, err)

	data, err := q.Eval(ctx, params)
	require.NoError(b, err)
	require.Equal(b, tc.want, logqlResultCount(data), "query %q returned an unexpected result count", tc.query)

	if n := corpus.bytesFor(tc.scans...); n > 0 {
		b.SetBytes(n)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := q.Eval(ctx, params); err != nil {
			b.Fatal(err)
		}
	}
}
