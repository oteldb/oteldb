// Package main generates rate-controlled synthetic OTLP traces.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type totals struct {
	Elapsed       float64 `json:"elapsed"`
	PayloadBytes  int64   `json:"payload_bytes"`
	AcceptedSpans int64   `json:"accepted_spans"`
	RejectedSpans int64   `json:"rejected_spans"`
	FailedSpans   int64   `json:"failed_spans"`
	Requests      int64   `json:"requests"`
	Rate          float64 `json:"payload_bytes_per_second"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	endpoint := flag.String("endpoint", "http://127.0.0.1:24318/v1/traces", "OTLP HTTP endpoint")
	rate := flag.Int64("bytes-per-second", 1_000_000, "uncompressed protobuf request bytes per second")
	duration := flag.Duration("duration", 5*time.Minute, "ingest duration")
	batch := flag.Int("batch", 256, "spans per request (multiple of 4)")
	services := flag.Int("services", 16, "stable service identities")
	payload := flag.Int("payload", 512, "unique hexadecimal attribute bytes per span")
	age := flag.Duration("age", 0, "fixed span timestamp offset")
	flag.Parse()
	if *rate <= 0 || *duration <= 0 || *batch <= 0 || *batch%4 != 0 || *services <= 0 || *payload < 0 {
		return errors.New("rate, duration, services and batch must be positive; batch must be a multiple of four; payload must be nonnegative")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	rnd := rand.New(rand.NewPCG(101, 103)) //nolint:gosec // Reproducible synthetic data, not security material.
	begin := time.Now()
	var seq uint64
	var total totals
	encoder := json.NewEncoder(os.Stdout)
	nextReport := begin
	report := func() error {
		total.Elapsed = time.Since(begin).Seconds()
		total.Rate = float64(total.PayloadBytes) / total.Elapsed
		return encoder.Encode(total)
	}

	for time.Since(begin) < *duration && ctx.Err() == nil {
		due := begin.Add(time.Duration(float64(total.PayloadBytes) / float64(*rate) * float64(time.Second)))
		if delay := time.Until(due); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		if time.Since(begin) >= *duration {
			break
		}

		td := makeTraces(seq, *batch, *services, *payload, time.Now().Add(-*age), rnd)
		seq += uint64(*batch)
		request := ptraceotlp.NewExportRequestFromTraces(td)
		raw, err := request.MarshalProto()
		if err != nil {
			return errors.Wrap(err, "marshal traces")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, *endpoint, bytes.NewReader(raw))
		if err != nil {
			return errors.Wrap(err, "create export request")
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		resp, err := client.Do(req)
		if err != nil {
			total.FailedSpans += int64(*batch)
			_ = report()
			return errors.Wrap(err, "export traces")
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		closeErr := resp.Body.Close()
		if readErr != nil || closeErr != nil || resp.StatusCode != http.StatusOK {
			total.FailedSpans += int64(*batch)
			_ = report()
			return errors.Errorf("export status=%d read=%v close=%v response=%q", resp.StatusCode, readErr, closeErr, body)
		}
		response := ptraceotlp.NewExportResponse()
		if err := response.UnmarshalProto(body); err != nil {
			return errors.Wrap(err, "decode export response")
		}
		rejected := response.PartialSuccess().RejectedSpans()
		total.PayloadBytes += int64(len(raw))
		total.AcceptedSpans += int64(*batch) - rejected
		total.RejectedSpans += rejected
		total.Requests++
		if rejected > 0 {
			_ = report()
			return errors.Errorf("receiver rejected %d spans: %s", rejected, response.PartialSuccess().ErrorMessage())
		}
		if time.Now().After(nextReport) {
			if err := report(); err != nil {
				return errors.Wrap(err, "write counters")
			}
			nextReport = time.Now().Add(5 * time.Second)
		}
	}

	return report()
}

func makeTraces(seq uint64, count, services, payloadBytes int, now time.Time, rnd *rand.Rand) ptrace.Traces {
	td := ptrace.NewTraces()
	groups := make([]ptrace.SpanSlice, services)
	for i := range groups {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("tracebench-%02d", i))
		rs.Resource().Attributes().PutStr("service.instance.id", fmt.Sprintf("instance-%02d", i))
		rs.Resource().Attributes().PutStr("deployment.environment.name", "benchmark")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("trace-memory-repro")
		ss.Scope().SetVersion("1")
		groups[i] = ss.Spans()
	}

	for i := range count {
		n := seq + uint64(i)
		sp := groups[int(n%uint64(services))].AppendEmpty()
		var traceID pcommon.TraceID
		binary.BigEndian.PutUint64(traceID[:8], 0x123456789abcdef0)
		binary.BigEndian.PutUint64(traceID[8:], n/4+1)
		var spanID pcommon.SpanID
		binary.BigEndian.PutUint64(spanID[:], n+1)
		sp.SetTraceID(traceID)
		sp.SetSpanID(spanID)
		if n%4 != 0 {
			var parent pcommon.SpanID
			binary.BigEndian.PutUint64(parent[:], n-n%4+1)
			sp.SetParentSpanID(parent)
		}
		sp.SetName(fmt.Sprintf("GET /api/%d", n%32))
		sp.SetKind(ptrace.SpanKindServer)
		start := now.Add(-200*time.Millisecond + time.Duration(n%4)*time.Millisecond)
		sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
		sp.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(500 * time.Microsecond)))
		sp.Status().SetCode(ptrace.StatusCodeOk)
		attrs := sp.Attributes()
		attrs.PutStr("http.request.method", "GET")
		attrs.PutStr("http.route", fmt.Sprintf("/api/%d", n%32))
		attrs.PutInt("http.response.status_code", 200)
		attrs.PutStr("server.address", "upstream.example")
		attrs.PutStr("db.system", "postgresql")
		attrs.PutStr("db.statement", "SELECT id, name FROM users WHERE id = $1")
		attrs.PutStr("request.id", fmt.Sprintf("%016x", n))
		raw := make([]byte, (payloadBytes+1)/2)
		for j := range raw {
			raw[j] = byte(rnd.Uint64())
		}
		attrs.PutStr("bench.payload", hex.EncodeToString(raw)[:payloadBytes])
	}

	return td
}
