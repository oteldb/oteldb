package chtracker

import (
	"context"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/oteldb/internal/tempoapi"
)

// QueryReport is a Clickhouse query stats retrieved from trace.
type QueryReport struct {
	DurationNanos int64  `json:"duration_nanos,omitempty" yaml:"duration_nanos,omitempty"`
	Query         string `json:"query,omitempty" yaml:"query,omitempty"`
	ReadBytes     int64  `json:"read_bytes,omitempty" yaml:"read_bytes,omitempty"`
	ReadRows      int64  `json:"read_rows,omitempty" yaml:"read_rows,omitempty"`
	MemoryUsage   int64  `json:"memory_usage,omitempty" yaml:"memory_usage,omitempty"`

	ReceivedRows int64 `json:"recevied_rows,omitempty" yaml:"recevied_rows,omitempty"`
}

func (t *Tracker[Q]) retrieveReports(ctx context.Context, tq TrackedQuery[Q]) (reports []QueryReport, _ error) {
	if !t.trace {
		return reports, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	bo := backoff.NewConstantBackOff(2 * time.Second)
	res, err := backoff.RetryWithData(func() (v ptrace.Traces, err error) {
		reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		res, err := t.tempo.TraceByID(reqCtx, tempoapi.TraceByIDParams{
			TraceID: tq.TraceID,
			Accept:  "application/protobuf",
		})
		if err != nil {
			return v, errors.Wrap(err, "query Tempo API")
		}
		switch r := res.(type) {
		case *tempoapi.TraceByIDNotFound:
			return v, errors.Errorf("trace %q not found", tq.TraceID)
		case *tempoapi.TraceByID:
			var um ptrace.ProtoUnmarshaler
			buf, err := io.ReadAll(r.Data)
			if err != nil {
				return v, backoff.Permanent(errors.Wrap(err, "read data"))
			}
			traces, err := um.UnmarshalTraces(buf)
			if err != nil {
				return v, backoff.Permanent(errors.Wrap(err, "unmarshal traces"))
			}

			services := make(map[string]int)
			list := traces.ResourceSpans()
			for i := 0; i < list.Len(); i++ {
				rs := list.At(i)
				attrValue, ok := rs.Resource().Attributes().Get("service.name")
				if !ok {
					return v, backoff.Permanent(errors.New("service name not found"))
				}
				services[attrValue.AsString()]++
			}
			for _, svc := range []string{
				"otelbench." + t.senderName,
				"oteldb",
				"clickhouse",
			} {
				if _, ok := services[svc]; !ok {
					return v, errors.Errorf("service %q not found", svc)
				}
			}

			return traces, nil
		default:
			return v, backoff.Permanent(errors.Errorf("unknown response type %T", res))
		}
	}, backoff.WithContext(bo, ctx))
	if err != nil {
		return nil, errors.Wrap(err, "get trace")
	}
	if res.SpanCount() < 1 {
		return nil, errors.New("response is empty, no spans returned")
	}

	// For each clickhouse query ID, save query.
	type queryReport struct {
		// "query" span coming from Clickhouse
		clickhouseSpan ptrace.Span
		// "Do" span coming from ch-go
		chgoSpan ptrace.Span
	}
	var (
		rsl     = res.ResourceSpans()
		queries = map[string]queryReport{}
	)
	for i := 0; i < rsl.Len(); i++ {
		rs := rsl.At(i)
		spansSlices := rs.ScopeSpans()
		for j := 0; j < spansSlices.Len(); j++ {
			spans := spansSlices.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				attrs := span.Attributes()
				if _, ok := attrs.Get("db.statement"); !ok {
					continue
				}

				switch span.Name() {
				case "query":
					queryIDVal, ok := attrs.Get("clickhouse.query_id")
					if !ok {
						continue
					}
					queryID := queryIDVal.AsString()

					report := queries[queryID]
					report.clickhouseSpan = span
					queries[queryID] = report
				case "Do":
					queryIDVal, ok := attrs.Get("ch.query.id")
					if !ok {
						continue
					}
					queryID := queryIDVal.AsString()

					report := queries[queryID]
					report.chgoSpan = span
					queries[queryID] = report
				default:
					continue
				}
			}
		}
	}

	for _, r := range queries {
		var report QueryReport

		if span := r.clickhouseSpan; span != (ptrace.Span{}) {
			attrs := span.Attributes()

			if statement, ok := attrs.Get("db.statement"); ok {
				report.Query = statement.AsString()
			}
			if readBytes, ok := attrs.Get("clickhouse.read_bytes"); ok {
				report.ReadBytes = readBytes.Int()
			}
			if readRows, ok := attrs.Get("clickhouse.read_rows"); ok {
				report.ReadRows = readRows.Int()
			}
			if memoryUsage, ok := attrs.Get("clickhouse.memory_usage"); ok {
				report.MemoryUsage = memoryUsage.Int()
			}
			report.DurationNanos = span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds()
		} else {
			continue
		}

		if span := r.chgoSpan; span != (ptrace.Span{}) {
			attrs := span.Attributes()
			if receivedRows, ok := attrs.Get("ch.rows_received"); ok {
				report.ReceivedRows = receivedRows.Int()
			}
		}

		reports = append(reports, report)
	}
	return reports, nil
}
