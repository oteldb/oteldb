package lokie2e_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"sigs.k8s.io/yaml"

	"github.com/oteldb/oteldb/integration/lokie2e"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func appendAttributes(target pcommon.Map, attrs []attribute.KeyValue) {
	for _, attr := range attrs {
		k := string(attr.Key)
		switch attr.Value.Type() {
		case attribute.BOOL:
			target.PutBool(k, attr.Value.AsBool())
		case attribute.INT64:
			target.PutInt(k, attr.Value.AsInt64())
		case attribute.FLOAT64:
			target.PutDouble(k, attr.Value.AsFloat64())
		case attribute.STRING:
			target.PutStr(k, attr.Value.AsString())
		}
	}
}

type httpLog struct {
	ServiceName      string
	ServiceNamespace string

	Severity plog.SeverityNumber
	Time     time.Time
	Method   string
	Status   int
	Bytes    int
	Protocol string
	User     string
	URL      string
	IP       string
	Ref      string
	SpanID   otelstorage.SpanID
	TraceID  otelstorage.TraceID
}

func (l httpLog) Append(s *lokie2e.BatchSet) error {
	var (
		ld = plog.NewLogs()
		rl = ld.ResourceLogs().AppendEmpty()
	)
	appendAttributes(rl.Resource().Attributes(), []attribute.KeyValue{
		semconv.ServiceName(l.ServiceName),
		semconv.ServiceVersion("testVersion"),
		semconv.ServiceNamespace(l.ServiceNamespace),
	})
	rl.SetSchemaUrl(semconv.SchemaURL)
	il := rl.ScopeLogs().AppendEmpty()
	{
		sc := il.Scope()
		sc.SetName("name")
		sc.SetVersion("version")
		sc.Attributes().PutStr("oteldb.name", "testDB")
		sc.SetDroppedAttributesCount(1)
	}
	il.SetSchemaUrl(semconv.SchemaURL)
	lg := il.LogRecords().AppendEmpty()
	lg.Body().SetStr(fmt.Sprintf("%s %s %d %d - 0.000 ms", l.Method, l.URL, l.Status, l.Bytes))
	lg.SetTimestamp(pcommon.NewTimestampFromTime(l.Time))
	lg.SetObservedTimestamp(pcommon.NewTimestampFromTime(l.Time))
	lg.SetTraceID(pcommon.TraceID(l.TraceID))
	lg.SetSpanID(pcommon.SpanID(l.SpanID))
	lg.SetSeverityNumber(l.Severity)
	appendAttributes(lg.Attributes(), []attribute.KeyValue{
		semconv.HTTPMethod(l.Method),
		semconv.HTTPStatusCode(l.Status),
		semconv.ClientAddress(l.IP),
		attribute.String("protocol", l.Protocol),
	})
	lg.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	if err := s.Append(ld); err != nil {
		return errors.Wrap(err, "append log")
	}
	return nil
}

func generateLogs(now time.Time, mul int) (*lokie2e.BatchSet, error) {
	type httpLogBatch struct {
		Method   string
		Status   int
		Count    int
		IP       string
		Protocol string
	}
	var lines []httpLog
	for j, b := range []httpLogBatch{
		{Method: "GET", Status: 200, Count: 11 * mul, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "GET", Status: 200, Count: 10 * mul, IP: "200.1.1.1", Protocol: "HTTP/1.1"},
		{Method: "DELETE", Status: 200, Count: 20 * mul, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "POST", Status: 200, Count: 21 * mul, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "PATCH", Status: 200, Count: 19 * mul, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "HEAD", Status: 200, Count: 15 * mul, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "HEAD", Status: 200, Count: 4 * mul, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "HEAD", Status: 200, Count: 1 * mul, IP: "236.7.233.166", Protocol: "HTTP/2.0"},
		{Method: "HEAD", Status: 500, Count: 2 * mul, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "PUT", Status: 200, Count: 20 * mul, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
	} {
		for i := 0; i < b.Count; i++ {
			var (
				spanID  otelstorage.SpanID
				traceID otelstorage.TraceID
			)
			{
				// Predictable IDs for testing.
				binary.PutUvarint(spanID[:], uint64(i+1056123959+j*100))
				spanID[7] = byte(j)
				spanID[6] = byte(i)
				binary.PutUvarint(traceID[:], uint64(i+3959+j*1000))
				binary.PutUvarint(traceID[8:], uint64(i+13+j*1000))
				traceID[15] = byte(j)
				traceID[14] = byte(i)
			}
			now = now.Add(time.Millisecond * 120)
			severity := plog.SeverityNumberInfo
			switch b.Status / 100 {
			case 2:
				severity = plog.SeverityNumberInfo
			case 3:
				severity = plog.SeverityNumberWarn
			case 4:
				severity = plog.SeverityNumberError
			case 5:
				severity = plog.SeverityNumberFatal
			}
			serviceName := "testService"
			serviceNamespace := "testNamespace"
			if i%2 == 0 {
				serviceName = "fooService"
				serviceNamespace = "fooNamespace"
			}
			lines = append(lines, httpLog{
				ServiceName:      serviceName,
				ServiceNamespace: serviceNamespace,
				Severity:         severity,
				SpanID:           spanID,
				TraceID:          traceID,
				Time:             now,
				Method:           b.Method,
				Status:           b.Status,
				Bytes:            250,
				Protocol:         b.Protocol,
				IP:               b.IP,
				URL:              "/api/v1/series",
				Ref:              "https://api.go-faster.org",
			})
		}
	}

	s := lokie2e.NewBatchSet()
	for _, l := range lines {
		if err := l.Append(s); err != nil {
			return s, err
		}
	}

	return s, nil
}

func TestGenerateLogs(t *testing.T) {
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now, 1)
	require.NoError(t, err)
	logEncoder := plog.JSONMarshaler{}
	var out bytes.Buffer
	for _, b := range set.Batches {
		data, err := logEncoder.MarshalLogs(b)
		require.NoError(t, err)
		outData, err := yaml.JSONToYAML(data)
		require.NoError(t, err)
		out.WriteString("---\n")
		out.Write(outData)
	}

	gold.Str(t, out.String(), "logs.yml")
}
