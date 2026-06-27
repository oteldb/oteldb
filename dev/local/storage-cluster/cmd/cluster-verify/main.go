// Command cluster-verify is an end-to-end check for the storage cluster: it pushes one deterministic
// metric, log, and trace via OTLP to one node, then queries them back through the PromQL, LogQL, and
// TraceQL APIs of *other* nodes. Success proves a write to one node is routed/replicated through the
// ring and served by another — across all three signals.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// service is the resource service.name carried by every pushed signal; the queries select on it.
const service = "cluster-e2e"

// metricName is the gauge pushed and queried back via PromQL.
const metricName = "cluster_e2e_value"

func main() {
	var (
		otlp    = flag.String("otlp", "localhost:4317", "OTLP gRPC endpoint to push signals to (the ingest node)")
		prom    = flag.String("prometheus", "http://localhost:9090", "PromQL base URL to query (a different node)")
		loki    = flag.String("loki", "http://localhost:3100", "LogQL base URL to query (a different node)")
		tempo   = flag.String("tempo", "http://localhost:3200", "TraceQL base URL to query (a different node)")
		timeout = flag.Duration("timeout", 90*time.Second, "max time to wait for each query to return the data")
	)
	flag.Parse()

	if err := run(*otlp, *prom, *loki, *tempo, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("OK: metric, log, and trace ingested on one node and served by another")
}

func run(otlp, prom, loki, tempo string, timeout time.Duration) error {
	ctx := context.Background()

	// Wait until every query node is serving before writing, so the ring is fully formed and the
	// write replicates to the right owners (otherwise it could land before a node has joined).
	ready := map[string]string{
		"PromQL " + prom:   prom + "/api/v1/query?query=" + url.QueryEscape("vector(1)"),
		"LogQL " + loki:    loki + "/loki/api/v1/labels",
		"TraceQL " + tempo: tempo + "/api/echo",
	}
	for name, u := range ready {
		if err := retry(func() error { return get2xx(ctx, u) }, timeout); err != nil {
			return fmt.Errorf("%s not ready: %w", name, err)
		}
	}
	fmt.Println(">> all query nodes ready")

	now := time.Now()
	if err := retry(func() error { return push(ctx, otlp, now) }, timeout); err != nil {
		return fmt.Errorf("push signals to %s: %w", otlp, err)
	}
	fmt.Printf(">> pushed metric/log/trace to %s (service.name=%s)\n", otlp, service)

	start, end := now.Add(-time.Hour), now.Add(time.Hour)
	checks := []struct {
		name string
		fn   func() error
	}{
		{"PromQL " + prom, func() error { return checkProm(ctx, prom) }},
		{"LogQL " + loki, func() error { return checkLoki(ctx, loki, start, end) }},
		{"TraceQL " + tempo, func() error { return checkTempo(ctx, tempo, start, end) }},
	}
	for _, c := range checks {
		if err := retry(c.fn, timeout); err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		fmt.Printf(">> %s served the data\n", c.name)
	}
	return nil
}

// get2xx GETs a URL and returns an error unless the response is 2xx (readiness probe).
func get2xx(ctx context.Context, rawURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16))
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", res.StatusCode)
	}
	return nil
}

// retry runs fn until it succeeds or the timeout elapses (signals need a moment to be queryable).
func retry(fn func() error, timeout time.Duration) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = 3 * time.Second
	bo.MaxElapsedTime = timeout
	return backoff.Retry(fn, bo)
}

func push(ctx context.Context, target string, ts time.Time) error {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	if _, err := pmetricotlp.NewGRPCClient(conn).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(buildMetric(ts))); err != nil {
		return fmt.Errorf("export metrics: %w", err)
	}
	if _, err := plogotlp.NewGRPCClient(conn).Export(ctx, plogotlp.NewExportRequestFromLogs(buildLog(ts))); err != nil {
		return fmt.Errorf("export logs: %w", err)
	}
	if _, err := ptraceotlp.NewGRPCClient(conn).Export(ctx, ptraceotlp.NewExportRequestFromTraces(buildTrace(ts))); err != nil {
		return fmt.Errorf("export traces: %w", err)
	}
	return nil
}

func buildMetric(ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", service)
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(metricName)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetDoubleValue(42)
	return md
}

func buildLog(ts time.Time) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(ts))
	lr.Body().SetStr("cluster-e2e log line")
	return ld
}

func buildTrace(ts time.Time) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", service)
	s := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetTraceID(pcommon.TraceID{0x0c, 0x1e, 0x57, 0x2e, 0xe2, 0xe0, 0x0c, 0x1e, 0x57, 0x2e, 0xe2, 0xe0, 0x0c, 0x1e, 0x57, 0x2e})
	s.SetSpanID(pcommon.SpanID{0x0c, 0x1e, 0x57, 0x2e, 0xe2, 0xe0, 0x0c, 0x1e})
	s.SetName("cluster-e2e-span")
	s.SetKind(ptrace.SpanKindServer)
	s.SetStartTimestamp(pcommon.NewTimestampFromTime(ts))
	s.SetEndTimestamp(pcommon.NewTimestampFromTime(ts.Add(time.Millisecond)))
	return td
}

func checkProm(ctx context.Context, base string) error {
	// No explicit time: evaluate at the server's "now", which is always at or after the just-pushed
	// sample (and within the lookback window), so the gauge resolves.
	q := base + "/api/v1/query?query=" + url.QueryEscape(metricName)
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := getJSON(ctx, q, &resp); err != nil {
		return err
	}
	if resp.Status != "success" || len(resp.Data.Result) == 0 {
		return fmt.Errorf("no samples for %q (status %q)", metricName, resp.Status)
	}
	return nil
}

func checkLoki(ctx context.Context, base string, start, end time.Time) error {
	q := base + "/loki/api/v1/query_range?query=" + url.QueryEscape(`{service_name="`+service+`"}`) +
		"&start=" + strconv.FormatInt(start.UnixNano(), 10) +
		"&end=" + strconv.FormatInt(end.UnixNano(), 10) + "&limit=5"
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := getJSON(ctx, q, &resp); err != nil {
		return err
	}
	if len(resp.Data.Result) == 0 {
		return fmt.Errorf("no log streams for service %q", service)
	}
	return nil
}

func checkTempo(ctx context.Context, base string, start, end time.Time) error {
	q := base + "/api/search?q=" + url.QueryEscape(`{ resource.service.name = "`+service+`" }`) +
		"&start=" + strconv.FormatInt(start.Unix(), 10) +
		"&end=" + strconv.FormatInt(end.Unix(), 10) + "&limit=5"
	var resp struct {
		Traces []json.RawMessage `json:"traces"`
	}
	if err := getJSON(ctx, q, &resp); err != nil {
		return err
	}
	if len(resp.Traces) == 0 {
		return fmt.Errorf("no traces for service %q", service)
	}
	return nil
}

func getJSON(ctx context.Context, rawURL string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("status %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, dst)
}
