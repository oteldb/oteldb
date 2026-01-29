// Package lokie2e provides scripts for E2E testing Loki API implementation.
package lokie2e

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// BatchSet is a set of batches.
type BatchSet struct {
	Batches []plog.Logs
	Labels  map[string][]logstorage.Label
	Records map[pcommon.Timestamp]plog.LogRecord

	Start otelstorage.Timestamp
	End   otelstorage.Timestamp
}

// NewBatchSet creates and initializes a new BatchSet.
func NewBatchSet() *BatchSet {
	s := &BatchSet{}
	// Init common labels.
	// Should return blank values.
	s.Labels = map[string][]logstorage.Label{}
	for _, v := range []string{
		logstorage.LabelBody,
		logstorage.LabelTraceID,
		logstorage.LabelSpanID,
		logstorage.LabelServiceInstanceID,
		logstorage.LabelServiceName,
		logstorage.LabelServiceNamespace,
	} {
		s.Labels[v] = []logstorage.Label{}
	}
	for _, i := range []plog.SeverityNumber{
		plog.SeverityNumberUnspecified,
		plog.SeverityNumberTrace,
		plog.SeverityNumberDebug,
		plog.SeverityNumberInfo,
		plog.SeverityNumberWarn,
		plog.SeverityNumberError,
		plog.SeverityNumberFatal,
	} {
		s.addLabel(logstorage.Label{
			Name:  logstorage.LabelSeverity,
			Value: i.String(),
		})
		s.addLabel(logstorage.Label{
			Name:  logstorage.LabelDetectedLevel,
			Value: i.String(),
		})
	}

	return s
}

// Append appends a batch to the set.
func (s *BatchSet) Append(raw plog.Logs) error {
	s.Batches = append(s.Batches, raw)

	resLogs := raw.ResourceLogs()
	for i := 0; i < resLogs.Len(); i++ {
		resLog := resLogs.At(i)
		res := resLog.Resource()
		s.addLabels(res.Attributes())

		scopeLogs := resLog.ScopeLogs()
		for i := 0; i < scopeLogs.Len(); i++ {
			scopeLog := scopeLogs.At(i)
			records := scopeLog.LogRecords()
			for i := 0; i < records.Len(); i++ {
				record := records.At(i)
				if err := s.addRecord(record); err != nil {
					return errors.Wrap(err, "add record")
				}
			}
		}
	}
	return nil
}

func (s *BatchSet) addRecord(record plog.LogRecord) error {
	ts := record.Timestamp()

	if _, ok := s.Records[ts]; ok {
		return errors.Errorf("duplicate record with timestamp %v", ts)
	}

	if s.Start == 0 || ts < s.Start {
		s.Start = ts
	}
	if ts > s.End {
		s.End = ts
	}

	if s.Records == nil {
		s.Records = map[pcommon.Timestamp]plog.LogRecord{}
	}
	s.Records[ts] = record
	return nil
}

func (s *BatchSet) addLabels(m pcommon.Map) {
	m.Range(func(k string, v pcommon.Value) bool {
		switch t := v.Type(); t {
		case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		default:
			s.addLabel(logstorage.Label{
				Name:  k,
				Value: v.AsString(),
			})
		}
		return true
	})
}

func (s *BatchSet) addLabel(label logstorage.Label) {
	if s.Labels == nil {
		s.Labels = map[string][]logstorage.Label{}
	}
	name := otelstorage.KeyToLabel(label.Name)
	s.Labels[name] = append(s.Labels[name], label)
}
