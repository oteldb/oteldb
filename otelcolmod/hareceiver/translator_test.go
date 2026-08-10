package hareceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestTranslateEntriesEmpty(t *testing.T) {
	logs := translateEntries(nil, Source{Kind: SourceKindHost}, &Config{}, observedAt)
	require.Equal(t, 0, logs.LogRecordCount())
	require.Equal(t, 0, logs.ResourceLogs().Len())
}

func TestTranslateEntriesResource(t *testing.T) {
	entries := ParseEntries(
		"2026-08-09 12:00:00.000 ha kernel[1]: a\n" +
			"2026-08-09 12:00:01.000 ha systemd[2]: b\n" +
			"2026-08-09 12:00:02.000 ha kernel[3]: c\n",
	)
	logs := translateEntries(entries, Source{Kind: SourceKindHost}, &Config{}, observedAt)

	require.Equal(t, 2, logs.ResourceLogs().Len(), "one ResourceLogs per identifier")
	require.Equal(t, 3, logs.LogRecordCount())

	rl := logs.ResourceLogs().At(0)
	attrs := rl.Resource().Attributes()
	requireStr(t, attrs, "service.namespace", "homeassistant")
	requireStr(t, attrs, "service.name", "kernel")
	requireStr(t, attrs, "host.name", "ha")
	require.Equal(t, 2, rl.ScopeLogs().At(0).LogRecords().Len())
}

func TestTranslateEntriesRecord(t *testing.T) {
	entries := ParseEntries("2026-08-09 12:00:00.500 ha kernel[42]: hello\n")
	logs := translateEntries(entries, Source{Kind: SourceKindHost}, &Config{}, observedAt)

	r := onlyRecord(t, logs)
	require.Equal(t, "hello", r.Body().Str())
	require.Equal(t,
		pcommon.NewTimestampFromTime(time.Date(2026, 8, 9, 12, 0, 0, 5e8, time.UTC)),
		r.Timestamp(),
	)
	require.Equal(t, pcommon.NewTimestampFromTime(observedAt), r.ObservedTimestamp())
	require.Equal(t, plog.SeverityNumberUnspecified, r.SeverityNumber())

	requireStr(t, r.Attributes(), "ha.source", "host")
	pid, ok := r.Attributes().Get("process.pid")
	require.True(t, ok)
	require.Equal(t, int64(42), pid.Int())
	_, ok = r.Attributes().Get("ha.addon")
	require.False(t, ok)
}

func TestTranslateEntriesAddon(t *testing.T) {
	entries := ParseEntries("2026-08-09 12:00:00.000 ha addon_ssh: up\n")
	src := Source{Kind: SourceKindAddon, Addon: "core_ssh"}
	logs := translateEntries(entries, src, &Config{}, observedAt)

	r := onlyRecord(t, logs)
	requireStr(t, r.Attributes(), "ha.source", "addon")
	requireStr(t, r.Attributes(), "ha.addon", "core_ssh")
	_, ok := r.Attributes().Get("process.pid")
	require.False(t, ok, "no PID attribute when the entry has none")
}

func TestTranslateEntriesIdentifierFallback(t *testing.T) {
	entries := ParseEntries("2026-08-09 12:00:00.000 ha _UNKNOWN_: orphan\n")
	src := Source{Kind: SourceKindAddon, Addon: "core_ssh"}
	logs := translateEntries(entries, src, &Config{}, observedAt)

	attrs := logs.ResourceLogs().At(0).Resource().Attributes()
	requireStr(t, attrs, "service.name", "addon/core_ssh")
}

func TestTranslateEntriesNoHostname(t *testing.T) {
	entries := ParseEntries("2026-08-09 12:00:00.000  systemd[1]: up\n")
	logs := translateEntries(entries, Source{Kind: SourceKindHost}, &Config{}, observedAt)

	_, ok := logs.ResourceLogs().At(0).Resource().Attributes().Get("host.name")
	require.False(t, ok)
}

func TestTranslateEntriesSeverity(t *testing.T) {
	body := "2026-08-09 12:00:00.000 ha homeassistant[7]: " +
		"2026-08-09 12:00:00.000 WARNING (MainThread) [x] slow\n"
	entries := ParseEntries(body)

	t.Run("Disabled", func(t *testing.T) {
		logs := translateEntries(entries, Source{Kind: SourceKindCore}, &Config{}, observedAt)
		r := onlyRecord(t, logs)
		require.Equal(t, plog.SeverityNumberUnspecified, r.SeverityNumber())
		require.Empty(t, r.SeverityText())
	})
	t.Run("Enabled", func(t *testing.T) {
		cfg := &Config{SeverityFromMessage: true}
		logs := translateEntries(entries, Source{Kind: SourceKindCore}, cfg, observedAt)
		r := onlyRecord(t, logs)
		require.Equal(t, plog.SeverityNumberWarn, r.SeverityNumber())
		require.Equal(t, "WARNING", r.SeverityText())
	})
}

func TestTranslateEntriesParseMessage(t *testing.T) {
	body := "2026-08-09 12:00:00.000 ha hassio_supervisor[655]: " +
		"2026-08-09 15:26:04.893 INFO (SyncWorker_2) [supervisor.backups.backup] Backing up folder ssl\n"
	entries := ParseEntries(body)
	cfg := &Config{ParseMessage: true}
	logs := translateEntries(entries, Source{Kind: SourceKindSupervisor}, cfg, observedAt)

	r := onlyRecord(t, logs)
	require.Equal(t, "Backing up folder ssl", r.Body().Str(),
		"the redundant timestamp, level, thread and logger must leave the body")
	require.Equal(t, plog.SeverityNumberInfo, r.SeverityNumber())
	require.Equal(t, "INFO", r.SeverityText())
	requireStr(t, r.Attributes(), "ha.logger", "supervisor.backups.backup")
	requireStr(t, r.Attributes(), "ha.thread", "SyncWorker_2")
}

func TestTranslateEntriesParseMessageUnknownLevel(t *testing.T) {
	body := "2026-08-09 12:00:00.000 ha homeassistant[7]: " +
		"2026-08-09 15:26:04.893 VERBOSE (MainThread) [a.b] hi\n"
	cfg := &Config{ParseMessage: true}
	logs := translateEntries(ParseEntries(body), Source{Kind: SourceKindCore}, cfg, observedAt)

	r := onlyRecord(t, logs)
	require.Equal(t, "VERBOSE", r.SeverityText(), "the level is reported even when unmapped")
	require.Equal(t, plog.SeverityNumberUnspecified, r.SeverityNumber())
}

func TestTranslateEntriesParseMessageFallsBack(t *testing.T) {
	// Host logs are not in the application format; the body must survive intact
	// and the heuristic still applies when enabled.
	body := "2026-08-09 12:00:00.000 ha systemd[1]: Started libcontainer container 617dc0c8.\n"
	cfg := &Config{ParseMessage: true, SeverityFromMessage: true}
	logs := translateEntries(ParseEntries(body), Source{Kind: SourceKindHost}, cfg, observedAt)

	r := onlyRecord(t, logs)
	require.Equal(t, "Started libcontainer container 617dc0c8.", r.Body().Str())
	require.Equal(t, plog.SeverityNumberUnspecified, r.SeverityNumber())
	_, ok := r.Attributes().Get("ha.logger")
	require.False(t, ok)
}

func TestTranslateEntriesParseMessageDisabled(t *testing.T) {
	raw := "2026-08-09 15:26:04.893 INFO (SyncWorker_2) [supervisor.backups.backup] Backing up folder ssl"
	body := "2026-08-09 12:00:00.000 ha hassio_supervisor[655]: " + raw + "\n"
	logs := translateEntries(ParseEntries(body), Source{Kind: SourceKindSupervisor}, &Config{}, observedAt)

	r := onlyRecord(t, logs)
	require.Equal(t, raw, r.Body().Str())
	_, ok := r.Attributes().Get("ha.logger")
	require.False(t, ok)
}

func TestTranslateEntriesMultiline(t *testing.T) {
	body := "2026-08-09 12:00:00.000 ha homeassistant[7]: Traceback:\n" +
		"    raise Error\n"
	logs := translateEntries(ParseEntries(body), Source{Kind: SourceKindCore}, &Config{}, observedAt)

	require.Equal(t, 1, logs.LogRecordCount())
	require.Equal(t, "Traceback:\n    raise Error", onlyRecord(t, logs).Body().Str())
}

func onlyRecord(t *testing.T, logs plog.Logs) plog.LogRecord {
	t.Helper()
	require.Equal(t, 1, logs.LogRecordCount())
	return logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
}

func requireStr(t *testing.T, m pcommon.Map, key, want string) {
	t.Helper()
	v, ok := m.Get(key)
	require.Truef(t, ok, "attribute %q is missing", key)
	require.Equal(t, want, v.Str())
}
