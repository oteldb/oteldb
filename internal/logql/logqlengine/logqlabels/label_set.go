package logqlabels

import (
	"slices"
	"strconv"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LabelSet is a log record's label set.
type LabelSet struct {
	labels map[logql.Label]pcommon.Value
}

// NewLabelSet creates new [LabelSet].
func NewLabelSet() LabelSet {
	return LabelSet{
		labels: map[logql.Label]pcommon.Value{},
	}
}

// AllowDots whether if dots in labels are allowed.
func (l *LabelSet) AllowDots() bool {
	return true
}

// Reset resets internal state of [LabelSet].
func (l *LabelSet) Reset() {
	if l.labels == nil {
		l.labels = map[logql.Label]pcommon.Value{}
	}
	maps.Clear(l.labels)
}

// AsLokiAPI returns lokiapi.LabelSet
func (l *LabelSet) AsLokiAPI() lokiapi.LabelSet {
	return lokiapi.LabelSet(l.AsMap())
}

// AsMap returns labels as strings map.
func (l *LabelSet) AsMap() map[string]string {
	set := make(map[string]string, len(l.labels))
	for k, v := range l.labels {
		set[string(k)] = v.AsString()
	}
	return set
}

// AppendString appends text representation of labels.
func (l *LabelSet) AppendString(buf []byte) []byte {
	buf = append(buf, '{')

	const stackThreshold = 24
	var keys []logql.Label
	if len(l.labels) < stackThreshold {
		keys = make([]logql.Label, 0, stackThreshold)
	} else {
		keys = make([]logql.Label, 0, len(l.labels))
	}

	for key := range l.labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	i := 0
	for _, k := range keys {
		v := l.labels[k]
		if i != 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, k...)
		buf = append(buf, '=')
		buf = strconv.AppendQuote(buf, v.AsString())
		i++
	}
	buf = append(buf, '}')
	return buf
}

// String returns text representation of labels.
func (l *LabelSet) String() string {
	return string(l.AppendString(nil))
}

var severityStrings = func() (r map[plog.SeverityNumber]pcommon.Value) {
	r = map[plog.SeverityNumber]pcommon.Value{}
	for _, n := range []plog.SeverityNumber{
		plog.SeverityNumberUnspecified,
		plog.SeverityNumberTrace,
		plog.SeverityNumberTrace2,
		plog.SeverityNumberTrace3,
		plog.SeverityNumberTrace4,
		plog.SeverityNumberDebug,
		plog.SeverityNumberDebug2,
		plog.SeverityNumberDebug3,
		plog.SeverityNumberDebug4,
		plog.SeverityNumberInfo,
		plog.SeverityNumberInfo2,
		plog.SeverityNumberInfo3,
		plog.SeverityNumberInfo4,
		plog.SeverityNumberWarn,
		plog.SeverityNumberWarn2,
		plog.SeverityNumberWarn3,
		plog.SeverityNumberWarn4,
		plog.SeverityNumberError,
		plog.SeverityNumberError2,
		plog.SeverityNumberError3,
		plog.SeverityNumberError4,
		plog.SeverityNumberFatal,
		plog.SeverityNumberFatal2,
		plog.SeverityNumberFatal3,
		plog.SeverityNumberFatal4,
	} {
		r[n] = pcommon.NewValueStr(n.String())
	}
	return r
}()

// SetFromRecord sets labels from given log record.
func (l *LabelSet) SetFromRecord(record logstorage.Record) {
	l.Reset()

	if traceID := record.TraceID; !traceID.IsEmpty() {
		l.Set(logstorage.LabelTraceID, pcommon.NewValueStr(traceID.Hex()))
	}
	if spanID := record.SpanID; !spanID.IsEmpty() {
		l.Set(logstorage.LabelSpanID, pcommon.NewValueStr(spanID.Hex()))
	}
	if severity := record.SeverityNumber; severity != plog.SeverityNumberUnspecified {
		s := severityStrings[severity]
		l.Set(logstorage.LabelSeverity, s)
		l.Set(logstorage.LabelDetectedLevel, s)
	} else if severityText := record.SeverityText; severityText != "" {
		s := pcommon.NewValueStr(severityText)
		l.Set(logstorage.LabelSeverity, s)
		l.Set(logstorage.LabelDetectedLevel, s)
	}
	l.SetAttrs(record.Attrs, record.ScopeAttrs, record.ResourceAttrs)
}

// Len returns set length
func (l *LabelSet) Len() int {
	return len(l.labels)
}

// SetAttrs sets labels from attrs.
func (l *LabelSet) SetAttrs(attrMaps ...otelstorage.Attrs) {
	for _, attrs := range attrMaps {
		m := attrs.AsMap()
		if m == (pcommon.Map{}) {
			continue
		}
		m.Range(func(k string, v pcommon.Value) bool {
			k = otelstorage.KeyToLabel(k)
			l.Set(logql.Label(k), v)
			return true
		})
	}
}

// Set sets label.
func (l *LabelSet) Set(s logql.Label, val pcommon.Value) {
	l.labels[s] = val
}

// Delete deletes label.
func (l *LabelSet) Delete(s logql.Label) {
	delete(l.labels, s)
}

// Range iterates over label set.
func (l *LabelSet) Range(cb func(logql.Label, pcommon.Value)) {
	for k, v := range l.labels {
		cb(k, v)
	}
}

// Get returns attr value.
func (l *LabelSet) Get(name logql.Label) (v pcommon.Value, ok bool) {
	v, ok = l.labels[name]
	return v, ok
}

// GetString returns stringified attr value.
func (l *LabelSet) GetString(name logql.Label) (string, bool) {
	v, ok := l.Get(name)
	if ok {
		return v.AsString(), true
	}
	return "", false
}

// GetFloat returns number attr value.
func (l *LabelSet) GetFloat(name logql.Label) (_ float64, ok bool, err error) {
	v, ok := l.Get(name)
	if !ok {
		return 0, false, nil
	}
	switch t := v.Type(); t {
	case pcommon.ValueTypeStr:
		v, err := strconv.ParseFloat(v.Str(), 64)
		return v, true, err
	case pcommon.ValueTypeInt:
		// TODO(tdakkota): check for overflow.
		return float64(v.Int()), true, nil
	case pcommon.ValueTypeDouble:
		return v.Double(), true, nil
	default:
		return 0, false, errors.Errorf("can't convert %q to float", t)
	}
}

// SetError sets special error label.
func (l *LabelSet) SetError(typ string, err error) {
	if _, ok := l.labels[logql.ErrorLabel]; ok {
		// Do not override old error.
		return
	}
	if err != nil {
		l.labels[logql.ErrorLabel] = pcommon.NewValueStr(typ)
		l.labels[logql.ErrorDetailsLabel] = pcommon.NewValueStr(err.Error())
	}
}

// GetError returns error label.
func (l *LabelSet) GetError() (string, bool) {
	return l.GetString(logql.ErrorLabel)
}
