package hareceiver

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// serviceNamespace is set on every resource emitted by this receiver.
const serviceNamespace = "homeassistant"

type resourceKey struct {
	hostname   string
	identifier string
}

// translateEntries converts journal entries of a single source into OTLP logs,
// grouping records into one ResourceLogs per distinct resource.
func translateEntries(entries []Entry, src Source, cfg *Config, observed time.Time) plog.Logs {
	logs := plog.NewLogs()
	if len(entries) == 0 {
		return logs
	}

	scopes := make(map[resourceKey]plog.LogRecordSlice, 1)
	for _, e := range entries {
		key := resourceKey{hostname: e.Hostname, identifier: e.Identifier}
		records, ok := scopes[key]
		if !ok {
			rl := logs.ResourceLogs().AppendEmpty()
			res := rl.Resource().Attributes()
			res.PutStr(string(semconv.ServiceNamespaceKey), serviceNamespace)
			res.PutStr(string(semconv.ServiceNameKey), serviceName(key, src))
			if key.hostname != "" {
				res.PutStr(string(semconv.HostNameKey), key.hostname)
			}
			records = rl.ScopeLogs().AppendEmpty().LogRecords()
			scopes[key] = records
		}

		r := records.AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(e.Timestamp))
		r.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))
		attrs := r.Attributes()

		body := e.Message
		app, isApp := appMessage{}, false
		if cfg.ParseMessage {
			app, isApp = parseAppMessage(e.Message)
		}
		switch {
		case isApp:
			// Core and Supervisor embed their own structure in MESSAGE; lifting
			// it out gives an exact level and a queryable logger name, and
			// leaves the body as just the message.
			body = app.Message
			attrs.PutStr("ha.logger", app.Logger)
			if app.Thread != "" {
				attrs.PutStr("ha.thread", app.Thread)
			}
			r.SetSeverityText(app.Level)
			if sev, ok := severityLevels[app.Level]; ok {
				r.SetSeverityNumber(sev)
			}
		case cfg.SeverityFromMessage:
			if sev, text, ok := detectSeverity(e.Message); ok {
				r.SetSeverityNumber(sev)
				r.SetSeverityText(text)
			}
		}
		r.Body().SetStr(body)

		attrs.PutStr("ha.source", string(src.Kind))
		if src.Addon != "" {
			attrs.PutStr("ha.addon", src.Addon)
		}
		if e.HasPID {
			attrs.PutInt(string(semconv.ProcessPIDKey), e.PID)
		}
	}
	return logs
}

// serviceName falls back to the source name when the entry carries no syslog
// identifier, so that records are never emitted without a service.
func serviceName(key resourceKey, src Source) string {
	if key.identifier != "" {
		return key.identifier
	}
	return src.Name()
}
