// Package logparser parses logs.
package logparser

import (
	"encoding/hex"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/jx"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

var formatRegistry = new(sync.Map)

// Record is a log record.
type Record struct {
	Timestamp         otelstorage.Timestamp `json:"timestamp"`
	ObservedTimestamp otelstorage.Timestamp `json:"observed_timestamp"`
	TraceID           otelstorage.TraceID   `json:"trace_id"`
	SpanID            otelstorage.SpanID    `json:"span_id"`
	Flags             plog.LogRecordFlags   `json:"flags"`
	SeverityText      string                `json:"severity_text"`
	SeverityNumber    plog.SeverityNumber   `json:"severity_number"`
	Body              string                `json:"body"`
	Attrs             otelstorage.Attrs     `json:"attrs"`

	ResourceAttrs otelstorage.Attrs `json:"resource_attrs"`

	ScopeName    string            `json:"scope_name"`
	ScopeVersion string            `json:"scope_version"`
	ScopeAttrs   otelstorage.Attrs `json:"scope_attrs"`
}

// Reset resets fields.
func (r *Record) Reset() {
	var a struct {
		Timestamp         otelstorage.Timestamp `json:"timestamp"`
		ObservedTimestamp otelstorage.Timestamp `json:"observed_timestamp"`
		TraceID           otelstorage.TraceID   `json:"trace_id"`
		SpanID            otelstorage.SpanID    `json:"span_id"`
		Flags             plog.LogRecordFlags   `json:"flags"`
		SeverityText      string                `json:"severity_text"`
		SeverityNumber    plog.SeverityNumber   `json:"severity_number"`
		Body              string                `json:"body"`
		Attrs             otelstorage.Attrs     `json:"attrs"`

		ResourceAttrs otelstorage.Attrs `json:"resource_attrs"`

		ScopeName    string            `json:"scope_name"`
		ScopeVersion string            `json:"scope_version"`
		ScopeAttrs   otelstorage.Attrs `json:"scope_attrs"`
	}
	*r = a
}

// String implements [fmt.Stringer].
func (r Record) String() string {
	e := &jx.Encoder{}
	e.SetIdent(2)
	r.Encode(e)
	return e.String()
}

// Encode line as json.
func (r Record) Encode(e *jx.Encoder) {
	e.Obj(func(e *jx.Encoder) {
		hasSeverity := r.SeverityNumber != 0 || r.SeverityText != ""
		if hasSeverity {
			e.Field("severity", func(e *jx.Encoder) {
				e.Obj(func(e *jx.Encoder) {
					if r.SeverityNumber != 0 {
						e.Field("str", func(e *jx.Encoder) {
							e.Str(r.SeverityNumber.String())
						})
						e.Field("number", func(e *jx.Encoder) {
							e.Int64(int64(r.SeverityNumber))
						})
					}
					if r.SeverityText != "" {
						e.Field("text", func(e *jx.Encoder) {
							e.Str(r.SeverityText)
						})
					}
				})
			})
		}
		if r.Body != "" {
			e.Field("body", func(e *jx.Encoder) {
				e.Str(r.Body)
			})
		}
		writeTimestamp := func(e *jx.Encoder, name string, value pcommon.Timestamp) {
			t := value.AsTime()
			if t.IsZero() || value == 0 {
				return
			}
			e.Field(name, func(e *jx.Encoder) {
				e.Str(t.Format(time.RFC3339Nano))
			})
		}
		writeTimestamp(e, "timestamp", r.Timestamp)
		writeTimestamp(e, "observed_timestamp", r.ObservedTimestamp)
		if !r.TraceID.IsEmpty() {
			e.Field("trace_id", func(e *jx.Encoder) {
				e.Str(hex.EncodeToString(r.TraceID[:]))
			})
		}
		if !r.SpanID.IsEmpty() {
			e.Field("span_id", func(e *jx.Encoder) {
				e.Str(hex.EncodeToString(r.SpanID[:]))
			})
		}
		writeAttrs := func(e *jx.Encoder, name string, attrs otelstorage.Attrs) {
			if attrs.IsZero() {
				return
			}
			e.Field(name, func(e *jx.Encoder) {
				e.Obj(func(e *jx.Encoder) {
					for k, v := range attrs.AsMap().All() {
						e.Field(k, func(e *jx.Encoder) {
							encodeValue(v, e)
						})
					}
				})
			})
		}
		writeAttrs(e, "attrs", r.Attrs)
		writeAttrs(e, "resource_attrs", r.ResourceAttrs)
		if r.ScopeName != "" || r.ScopeVersion != "" || !r.ScopeAttrs.IsZero() {
			e.Field("scope", func(e *jx.Encoder) {
				if r.ScopeName != "" {
					e.Field("name", func(e *jx.Encoder) {
						e.Str(r.ScopeName)
					})
				}
				if r.ScopeVersion != "" {
					e.Field("version", func(e *jx.Encoder) {
						e.Str(r.ScopeVersion)
					})
				}
				writeAttrs(e, "scope_attrs", r.ScopeAttrs)
			})
		}
	})
}

// RegisterFormat register a [Parser] for specified format name.
func RegisterFormat(f string, p Parser) {
	formatRegistry.Store(f, p)
}

// LookupFormat returns a [Parser] instance for specified format name.
func LookupFormat(f string) (Parser, bool) {
	v, ok := formatRegistry.Load(f)
	if !ok {
		return nil, false
	}
	return v.(Parser), ok
}

// Parser parses logs.
type Parser interface {
	// Parse parses data and returns a line.
	//
	// TODO: refactor to `Parse(data []byte, target *logstorage.Record) error`
	Parse(data string, target *Record) error
	// Detect whether data is parsable.
	//
	// TODO: refactor to `Detect(data []byte) bool`
	Detect(line string) bool
	String() string
}
