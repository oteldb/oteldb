package otlpdirect

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// Field numbers of logs.proto and collector/logs/v1/logs_service.proto.
const (
	// opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest
	fieldExportResourceLogs = 1

	// opentelemetry.proto.logs.v1.ResourceLogs
	fieldResourceLogsResource  = 1
	fieldResourceLogsScope     = 2
	fieldResourceLogsSchemaURL = 3

	// opentelemetry.proto.logs.v1.ScopeLogs
	fieldScopeLogsScope     = 1
	fieldScopeLogsRecords   = 2
	fieldScopeLogsSchemaURL = 3

	// opentelemetry.proto.logs.v1.LogRecord
	fieldLogTime         = 1
	fieldLogSeverityNum  = 2
	fieldLogSeverityText = 3
	fieldLogBody         = 5
	fieldLogAttributes   = 6
	fieldLogDropped      = 7
	fieldLogFlags        = 8
	fieldLogTraceID      = 9
	fieldLogSpanID       = 10
	fieldLogObservedTime = 11
)

// LogsConverter decodes an OTLP ExportLogsServiceRequest into [log.Logs]. It retains the batch and
// the scratch it is built from, so a converter reused across requests allocates nothing in steady
// state. It is not safe for concurrent use; pool one per in-flight request.
type LogsConverter struct {
	batch log.Logs
	dec   decoder

	// kvScratch collects a record's attribute submessages during its walk. A record's attributes
	// are decoded before the next record starts and never retained, so one buffer serves them all
	// — which is what keeps the per-record cost off the allocator.
	kvScratch [][]byte
}

// Convert decodes a serialized ExportLogsServiceRequest.
//
// The returned batch aliases src: every key, string value, body and id is a sub-slice of it. It
// stays valid until the next Convert on this converter, and src must not be recycled until the
// write consuming the batch has returned.
func (c *LogsConverter) Convert(src []byte) (*log.Logs, error) {
	c.batch.Reset()
	c.dec.reset()

	resources, err := collect(src, fieldExportResourceLogs, "resource logs")
	if err != nil {
		return nil, err
	}

	for _, data := range resources {
		if err := c.resourceLogs(data); err != nil {
			return nil, err
		}
	}

	return &c.batch, nil
}

func (c *LogsConverter) resourceLogs(src []byte) error {
	var (
		fc     easyproto.FieldContext
		res    signal.Resource
		scopes [][]byte
		err    error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read resource logs field")
		}

		switch fc.FieldNum {
		case fieldResourceLogsResource:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read resource")
			}

			if res.Attributes, err = c.dec.resource(data); err != nil {
				return err
			}
		case fieldResourceLogsScope:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read scope logs")
			}

			scopes = append(scopes, data)
		case fieldResourceLogsSchemaURL:
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read resource schema url")
			}

			res.SchemaURL = v
		}
	}

	rl := c.batch.AddResource()
	rl.Resource = res

	for _, data := range scopes {
		if err := c.scopeLogs(rl, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *LogsConverter) scopeLogs(rl *log.ResourceLogs, src []byte) error {
	var (
		fc        easyproto.FieldContext
		scopeData []byte
		schemaURL []byte
		records   [][]byte
		err       error
	)

	// Fields arrive in whatever order the producer wrote them — pdata's marshaler emits them
	// descending, so schema_url lands before scope. Nothing here may depend on the order, which is
	// why the scope submessage is decoded after the walk rather than during it: decoding it in
	// place would overwrite a schema_url already read.
	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read scope logs field")
		}

		switch fc.FieldNum {
		case fieldScopeLogsScope:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read scope")
			}

			scopeData = data
		case fieldScopeLogsRecords:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read log record")
			}

			records = append(records, data)
		case fieldScopeLogsSchemaURL:
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read scope schema url")
			}

			schemaURL = v
		}
	}

	sc, err := c.dec.scope(scopeData)
	if err != nil {
		return err
	}

	sc.SchemaURL = schemaURL

	sl := rl.AddScope()
	sl.Scope = sc

	for _, data := range records {
		if err := c.record(sl, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *LogsConverter) record(sl *log.ScopeLogs, src []byte) error {
	var (
		fc  easyproto.FieldContext
		rec log.Record
		err error
	)

	kvs := c.kvScratch[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read log record field")
		}

		switch fc.FieldNum {
		case fieldLogTime:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read log timestamp")
			}

			rec.Timestamp = int64(v)
		case fieldLogObservedTime:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read log observed timestamp")
			}

			rec.ObservedTimestamp = int64(v)
		case fieldLogSeverityNum:
			v, ok := fc.Enum()
			if !ok {
				return errors.New("read log severity number")
			}

			rec.SeverityNumber = v
		case fieldLogSeverityText:
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read log severity text")
			}

			rec.SeverityText = v
		case fieldLogBody:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read log body")
			}

			body, err := c.dec.anyValue(data)
			if err != nil {
				return err
			}

			// The model stores a body as text, so a non-string body is rendered the way the pdata
			// path renders it.
			rec.Body = c.dec.renderText(body)
		case fieldLogAttributes:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read log attribute")
			}

			kvs = append(kvs, data)
		case fieldLogDropped:
			v, ok := fc.Uint32()
			if !ok {
				return errors.New("read log dropped count")
			}

			rec.Dropped = v
		case fieldLogFlags:
			v, ok := fc.Fixed32()
			if !ok {
				return errors.New("read log flags")
			}

			rec.Flags = v
		case fieldLogTraceID:
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read log trace id")
			}

			rec.TraceID = v
		case fieldLogSpanID:
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read log span id")
			}

			rec.SpanID = v
		}
	}

	c.kvScratch = kvs // keep the grown capacity for the next record

	if rec.Attributes, err = c.dec.attributes(kvs); err != nil {
		return err
	}

	*sl.AddRecord() = rec

	return nil
}
