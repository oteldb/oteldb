package chstorage

import (
	"sync"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/ddl"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/xsync"
)

var (
	logColumnsPool        = xsync.NewPool(newLogColumns)
	logAttrMapColumnsPool = xsync.NewPool(newLogAttrMapColumns)
)

type logColumns struct {
	serviceInstanceID *proto.ColLowCardinality[string]
	serviceName       *proto.ColLowCardinality[string]
	serviceNamespace  *proto.ColLowCardinality[string]

	timestamp *proto.ColDateTime64

	severityText   *proto.ColLowCardinality[string]
	severityNumber proto.ColUInt8

	traceFlags proto.ColUInt8
	traceID    proto.ColRawOf[otelstorage.TraceID]
	spanID     proto.ColRawOf[otelstorage.SpanID]

	body       proto.ColStr
	attributes *Attributes
	resource   *Attributes

	scopeName       *proto.ColLowCardinality[string]
	scopeVersion    *proto.ColLowCardinality[string]
	scopeAttributes *Attributes

	columns func() Columns
	Input   func() proto.Input
	Body    func(table string) string
}

func newLogColumns() *logColumns {
	c := &logColumns{
		serviceName:       new(proto.ColStr).LowCardinality(),
		serviceInstanceID: new(proto.ColStr).LowCardinality(),
		serviceNamespace:  new(proto.ColStr).LowCardinality(),
		timestamp:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		severityText:      new(proto.ColStr).LowCardinality(),
		attributes:        NewAttributes(colAttrs, WithLowCardinality(false)),
		resource:          NewAttributes(colResource),
		scopeName:         new(proto.ColStr).LowCardinality(),
		scopeVersion:      new(proto.ColStr).LowCardinality(),
		scopeAttributes:   NewAttributes(colScope),
	}
	c.columns = sync.OnceValue(func() Columns {
		return MergeColumns(Columns{
			{Name: "service_instance_id", Data: c.serviceInstanceID},
			{Name: "service_name", Data: c.serviceName},
			{Name: "service_namespace", Data: c.serviceNamespace},

			{Name: "timestamp", Data: c.timestamp},

			{Name: "severity_number", Data: &c.severityNumber},
			{Name: "severity_text", Data: c.severityText},

			{Name: "trace_id", Data: &c.traceID},
			{Name: "span_id", Data: &c.spanID},
			{Name: "trace_flags", Data: &c.traceFlags},

			{Name: "body", Data: &c.body},

			{Name: "scope_name", Data: c.scopeName},
			{Name: "scope_version", Data: c.scopeVersion},
		},
			c.attributes.Columns(),
			c.scopeAttributes.Columns(),
			c.resource.Columns(),
		)
	})
	c.Input = sync.OnceValue(func() proto.Input {
		return c.columns().Input()
	})
	c.Body = xsync.KeyOnce(func(table string) string {
		return c.Input().Into(table)
	})
	return c
}

// DDL of the log table.
func (c *logColumns) DDL() ddl.Table {
	table := ddl.Table{
		Engine:      ddl.Engine{Type: "MergeTree"},
		PartitionBy: "toYYYYMMDD(timestamp)",
		PrimaryKey:  []string{"severity_number", "service_namespace", "service_name", "resource"},
		OrderBy:     []string{"severity_number", "service_namespace", "service_name", "resource", "timestamp"},
		TTL:         ddl.TTL{Field: "timestamp"},
		Indexes: []ddl.Index{
			{
				Name:        "idx_trace_id",
				Target:      "trace_id",
				Type:        "bloom_filter",
				Params:      []string{"0.001"},
				Granularity: 1,
			},
			{
				Name:        "idx_body",
				Target:      "body",
				Type:        "tokenbf_v1",
				Params:      []string{"32768", "3", "0"},
				Granularity: 1,
			},
			{
				Name:        "idx_ts",
				Target:      "timestamp",
				Type:        "minmax",
				Granularity: 8192,
			},
		},
		Columns: []ddl.Column{
			{
				Name:    "service_instance_id",
				Type:    c.serviceInstanceID.Type(),
				Comment: "service.instance.id",
			},
			{
				Name:    "service_name",
				Type:    c.serviceName.Type(),
				Comment: "service.name",
			},
			{
				Name:    "service_namespace",
				Type:    c.serviceNamespace.Type(),
				Comment: "service.namespace",
			},
			{
				Name:  "timestamp",
				Type:  c.timestamp.Type(),
				Codec: "Delta, ZSTD(1)",
			},
			{
				Name: "severity_number",
				Type: c.severityNumber.Type(),
			},
			{
				Name: "severity_text",
				Type: c.severityText.Type(),
			},
			{
				Name: "trace_id",
				Type: c.traceID.Type(),
			},
			{
				Name: "span_id",
				Type: c.spanID.Type(),
			},
			{
				Name: "trace_flags",
				Type: c.traceFlags.Type(),
			},
			{
				Name: "body",
				Type: c.body.Type(),
			},
		},
	}

	c.attributes.DDL(&table)
	c.resource.DDL(&table)

	table.Columns = append(table.Columns,
		ddl.Column{
			Name: "scope_name",
			Type: c.scopeName.Type(),
		},
		ddl.Column{
			Name: "scope_version",
			Type: c.scopeVersion.Type(),
		},
	)
	c.scopeAttributes.DDL(&table)

	return table
}

func (c *logColumns) StaticColumns() []string {
	var (
		input = c.Input()
		cols  = make([]string, len(input))
	)
	for i, col := range input {
		cols[i] = col.Name
	}
	return cols
}

func setStrOrEmpty(col proto.ColumnOf[string], m pcommon.Map, k string) {
	v, ok := m.Get(k)
	if !ok {
		col.Append("")
		return
	}
	col.Append(v.AsString())
}

func (c *logColumns) ForEach(f func(r logstorage.Record) error) error {
	for i := 0; i < c.timestamp.Rows(); i++ {
		r := logstorage.Record{
			Timestamp:      otelstorage.NewTimestampFromTime(c.timestamp.Row(i)),
			SeverityText:   c.severityText.Row(i),
			SeverityNumber: plog.SeverityNumber(c.severityNumber.Row(i)),
			TraceID:        c.traceID.Row(i),
			SpanID:         c.spanID.Row(i),
			Flags:          plog.LogRecordFlags(c.traceFlags.Row(i)),
			Body:           c.body.Row(i),

			ScopeVersion: c.scopeVersion.Row(i),
			ScopeName:    c.scopeName.Row(i),
		}
		{
			a := c.resource.Row(i)
			v := a.AsMap()
			if s := c.serviceInstanceID.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceInstanceIDKey), s)
			}
			if s := c.serviceName.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceNameKey), s)
			}
			if s := c.serviceNamespace.Row(i); s != "" {
				v.PutStr(string(semconv.ServiceNamespaceKey), s)
			}
			r.ResourceAttrs = a
		}
		{
			r.Attrs = c.attributes.Row(i)
		}
		{
			r.ScopeAttrs = c.scopeAttributes.Row(i)
		}
		{
			// Default just to timestamp.
			r.ObservedTimestamp = r.Timestamp
		}
		if err := f(r); err != nil {
			return err
		}
	}
	return nil
}

func (c *logColumns) AddRow(r logstorage.Record) {
	{
		m := r.ResourceAttrs.AsMap()
		setStrOrEmpty(c.serviceInstanceID, m, string(semconv.ServiceInstanceIDKey))
		setStrOrEmpty(c.serviceName, m, string(semconv.ServiceNameKey))
		setStrOrEmpty(c.serviceNamespace, m, string(semconv.ServiceNamespaceKey))
	}
	// NOTE(tdakkota): otelcol filelog receiver sends entries
	// 	with zero value timestamp
	//	Probably, this is the wrong way to handle it.
	//	Probably, we should not accept records if both timestamps are zero.
	ts := r.Timestamp
	if ts == 0 {
		ts = r.ObservedTimestamp
	}
	c.timestamp.Append(ts.AsTime())

	c.severityNumber.Append(uint8(r.SeverityNumber))
	c.severityText.Append(r.SeverityText)

	c.traceID.Append(r.TraceID)
	c.spanID.Append(r.SpanID)
	c.traceFlags.Append(uint8(r.Flags))

	c.body.Append(r.Body)
	c.attributes.Append(r.Attrs)
	c.resource.Append(r.ResourceAttrs)

	c.scopeName.Append(r.ScopeName)
	c.scopeVersion.Append(r.ScopeVersion)
	c.scopeAttributes.Append(r.ScopeAttrs)
}

func (c *logColumns) Result() proto.Results             { return c.columns().Result() }
func (c *logColumns) ChsqlResult() []chsql.ResultColumn { return c.columns().ChsqlResult() }
func (c *logColumns) Reset()                            { c.columns().Reset() }

type logAttrMapColumns struct {
	name proto.ColStr // http_method
	key  proto.ColStr // http.method

	columns func() Columns
	Input   func() proto.Input
	Body    func(table string) string
}

func newLogAttrMapColumns() *logAttrMapColumns {
	c := &logAttrMapColumns{}
	c.columns = sync.OnceValue(func() Columns {
		return []Column{
			{Name: "name", Data: &c.name},
			{Name: "key", Data: &c.key},
		}
	})
	c.Input = sync.OnceValue(func() proto.Input {
		return c.columns().Input()
	})
	c.Body = xsync.KeyOnce(func(table string) string {
		return c.Input().Into(table)
	})
	return c
}

func (c *logAttrMapColumns) Result() proto.Results             { return c.columns().Result() }
func (c *logAttrMapColumns) ChsqlResult() []chsql.ResultColumn { return c.columns().ChsqlResult() }
func (c *logAttrMapColumns) Reset()                            { c.columns().Reset() }

func (c *logAttrMapColumns) ForEach(f func(name, key string)) {
	for i := 0; i < c.name.Rows(); i++ {
		f(c.name.Row(i), c.key.Row(i))
	}
}

func (c *logAttrMapColumns) AddAttrs(attrs otelstorage.Attrs) {
	buf := make([]byte, 0, 128)
	attrs.AsMap().Range(func(k string, _ pcommon.Value) bool {
		c.AddRow(otelstorage.AppendKeyToLabel(buf, k), k)
		return true
	})
}

func (c *logAttrMapColumns) AddRow(name []byte, key string) {
	c.name.AppendBytes(name)
	c.key.Append(key)
}

func (c *logAttrMapColumns) DDL() ddl.Table {
	return ddl.Table{
		OrderBy: []string{"name"},
		Engine:  ddl.Engine{Type: "ReplacingMergeTree"},
		Columns: []ddl.Column{
			{
				Name:    "name",
				Type:    c.name.Type(),
				Comment: "foo_bar",
			},
			{
				Name:    "key",
				Type:    c.key.Type(),
				Comment: "foo.bar",
			},
		},
	}
}
