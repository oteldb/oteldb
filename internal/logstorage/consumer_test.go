package logstorage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/logparser"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type mockInserter struct {
	records []Record
}

var _ Inserter = (*mockInserter)(nil)

// RecordWriter implements [Inserter].
func (m *mockInserter) RecordWriter(ctx context.Context) (RecordWriter, error) {
	return &mockRecordWriter{inserter: m}, nil
}

type mockRecordWriter struct {
	inserter *mockInserter
	records  []Record
}

var _ RecordWriter = (*mockRecordWriter)(nil)

// Add implements [RecordWriter].
func (m *mockRecordWriter) Add(record Record) error {
	m.records = append(m.records, record)
	return nil
}

// Close implements [RecordWriter].
func (m *mockRecordWriter) Close() error {
	return nil
}

// Submit implements [RecordWriter].
func (m *mockRecordWriter) Submit(ctx context.Context) error {
	m.inserter.records = append(m.inserter.records, m.records...)
	m.records = nil
	return nil
}

func TestConsumeLogs(t *testing.T) {
	ctx := t.Context()

	i := &mockInserter{}
	c, err := NewConsumer(i, ConsumerOptions{
		TriggerAttributes: []AttributeRef{
			{Name: "log.msg", Location: AttributesLocation},
		},
		FormatAttributes: []AttributeRef{
			{Name: "log.format", Location: AnyLocation},
		},
		DetectFormats: []logparser.Parser{
			logparser.GenericJSONParser{},
			logparser.LogFmtParser{},
		},
	})
	require.NoError(t, err)

	logs := plog.NewLogs()
	resLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resLogs.ScopeLogs().AppendEmpty()
	records := scopeLogs.LogRecords()
	{
		record := records.AppendEmpty()
		record.SetTimestamp(1)
		record.Body().SetStr(`{"msg": "json-message", "level": "INFO"}`)
		record.Attributes().PutStr("log.format", "json")
	}
	{
		record := records.AppendEmpty()
		record.SetTimestamp(2)
		record.Body().SetStr(`msg=logfmt level=debug`)
		record.Attributes().PutStr("log.msg", "hello")
	}
	{
		record := records.AppendEmpty()
		record.SetTimestamp(3)
		record.Body().SetStr(`<aboba>`)
		record.Attributes().PutStr("log.msg", "clearly-weird-format")
	}
	{
		record := records.AppendEmpty()
		record.SetTimestamp(4)
		record.Body().SetStr(`{bad json message`)
		record.Attributes().PutStr("log.format", "json")
	}
	{
		record := records.AppendEmpty()
		record.SetTimestamp(5)
		record.Body().SetStr(`{detected bad json message`)
		record.Attributes().PutStr("log.msg", "clearly-bad-format")
	}
	{
		record := records.AppendEmpty()
		record.SetTimestamp(6)
		record.SetSeverityNumber(plog.SeverityNumberDebug)
		record.Body().SetStr(`well cooked OTEL record`)
		record.Attributes().PutStr("method", "GET")
	}

	err = c.ConsumeLogs(ctx, logs)
	require.NoError(t, err)

	expected := []Record{
		{
			Timestamp: 1, Body: "json-message",
			SeverityText: "INFO", SeverityNumber: plog.SeverityNumberInfo,
			Attrs:         attrMap("log.format", "json"),
			ScopeAttrs:    otelstorage.Attrs(pcommon.NewMap()),
			ResourceAttrs: otelstorage.Attrs(pcommon.NewMap()),
		},
		{
			Timestamp: 2, Body: "logfmt",
			SeverityText: "debug", SeverityNumber: plog.SeverityNumberDebug,
			Attrs:         attrMap("log.msg", "hello", "logparser.type", "logfmt"),
			ScopeAttrs:    otelstorage.Attrs(pcommon.NewMap()),
			ResourceAttrs: otelstorage.Attrs(pcommon.NewMap()),
		},
		{
			Timestamp: 3, Body: "<aboba>",
			Attrs:         attrMap("log.msg", "clearly-weird-format"),
			ScopeAttrs:    otelstorage.Attrs(pcommon.NewMap()),
			ResourceAttrs: otelstorage.Attrs(pcommon.NewMap()),
		},
		{
			Timestamp: 4, Body: "{bad json message",
			Attrs:         attrMap("log.format", "json"),
			ScopeAttrs:    otelstorage.Attrs(pcommon.NewMap()),
			ResourceAttrs: otelstorage.Attrs(pcommon.NewMap()),
		},
		{
			Timestamp: 5, Body: "{detected bad json message",
			Attrs:         attrMap("log.msg", "clearly-bad-format"),
			ScopeAttrs:    otelstorage.Attrs(pcommon.NewMap()),
			ResourceAttrs: otelstorage.Attrs(pcommon.NewMap()),
		},
		{
			Timestamp: 6, Body: "well cooked OTEL record",
			SeverityText: "Debug", SeverityNumber: plog.SeverityNumberDebug,
			Attrs:         attrMap("method", "GET"),
			ScopeAttrs:    otelstorage.Attrs(pcommon.NewMap()),
			ResourceAttrs: otelstorage.Attrs(pcommon.NewMap()),
		},
	}
	require.Equal(t, expected, i.records)
}

func attrMap(kv ...string) otelstorage.Attrs {
	m := pcommon.NewMap()
	for i := 0; i < len(kv); i += 2 {
		m.PutStr(kv[i], kv[i+1])
	}
	return otelstorage.Attrs(m)
}

func Test_normalizeSeverity(t *testing.T) {
	tests := []struct {
		text       string
		number     plog.SeverityNumber
		wantText   string
		wantNumber plog.SeverityNumber
	}{
		{text: "", number: plog.SeverityNumberDebug, wantText: "Debug", wantNumber: plog.SeverityNumberDebug},
		{text: "debug", number: 0, wantText: "debug", wantNumber: plog.SeverityNumberDebug},
		{text: "debug", number: plog.SeverityNumberDebug, wantText: "debug", wantNumber: plog.SeverityNumberDebug},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			number, text := normalizeSeverity(tt.number, tt.text)
			assert.Equal(t, tt.wantNumber, number)
			assert.Equal(t, tt.wantText, text)
		})
	}
}
