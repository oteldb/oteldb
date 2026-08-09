package hareceiver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/jx"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

// observedAt is fixed so that ObservedTimestamp is reproducible in golden files.
var observedAt = time.Date(2026, time.January, 20, 1, 2, 3, 4, time.UTC)

// testDataFile is one captured log API response.
type testDataFile struct {
	Name string
	Body string
}

// GoldenFile returns the golden file name for the given prefix.
func (f testDataFile) GoldenFile(prefix string) string {
	name := strings.TrimSuffix(f.Name, filepath.Ext(f.Name))
	return fmt.Sprintf("%s_%s.json", prefix, name)
}

// Source returns the source a response was captured from, taken from its name.
func (f testDataFile) Source() Source {
	switch strings.TrimSuffix(f.Name, filepath.Ext(f.Name)) {
	case "core":
		return Source{Kind: SourceKindCore}
	case "supervisor":
		return Source{Kind: SourceKindSupervisor}
	case "addon":
		return Source{Kind: SourceKindAddon, Addon: "core_ssh"}
	default:
		return Source{Kind: SourceKindHost}
	}
}

func readTestData(t require.TestingT) iter.Seq[testDataFile] {
	return func(yield func(testDataFile) bool) {
		dir := filepath.Join("_testdata", "journal")

		files, err := os.ReadDir(dir)
		require.NoError(t, err, "read testdata directory")

		for _, file := range files {
			data, err := os.ReadFile(filepath.Join(dir, file.Name()))
			require.NoError(t, err, "read testdata file")

			if !yield(testDataFile{Name: file.Name(), Body: string(data)}) {
				return
			}
		}
	}
}

// encodeEntries renders parsed entries as stable JSON.
func encodeEntries(entries []Entry) string {
	e := &jx.Encoder{}
	e.SetIdent(2)
	e.Arr(func(e *jx.Encoder) {
		for _, entry := range entries {
			e.Obj(func(e *jx.Encoder) {
				e.Field("timestamp", func(e *jx.Encoder) {
					e.Str(entry.Timestamp.Format(time.RFC3339Nano))
				})
				if entry.Hostname != "" {
					e.Field("hostname", func(e *jx.Encoder) { e.Str(entry.Hostname) })
				}
				if entry.Identifier != "" {
					e.Field("identifier", func(e *jx.Encoder) { e.Str(entry.Identifier) })
				}
				if entry.HasPID {
					e.Field("pid", func(e *jx.Encoder) { e.Int64(entry.PID) })
				}
				e.Field("message", func(e *jx.Encoder) { e.Str(entry.Message) })
			})
		}
	})
	return e.String() + "\n"
}

// encodeLogs renders translated logs as indented OTLP JSON.
func encodeLogs(t *testing.T, logs plog.Logs) string {
	t.Helper()
	data, err := (&plog.JSONMarshaler{}).MarshalLogs(logs)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, json.Indent(&buf, data, "", "  "))
	return buf.String() + "\n"
}

func TestParseEntries(t *testing.T) {
	cfg := &Config{ParseMessage: true, SeverityFromMessage: true}

	for file := range readTestData(t) {
		t.Run(file.Name, func(t *testing.T) {
			entries := ParseEntries(file.Body)

			t.Run("Entries", func(t *testing.T) {
				for _, e := range entries {
					require.False(t, e.Timestamp.IsZero(), "entry must have a timestamp")
				}
				gold.Str(t, encodeEntries(entries), file.GoldenFile("journal"))
			})
			t.Run("Logs", func(t *testing.T) {
				logs := translateEntries(entries, file.Source(), cfg, observedAt)
				require.Equal(t, len(entries), logs.LogRecordCount(),
					"every entry must produce exactly one record")
				gold.Str(t, encodeLogs(t, logs), file.GoldenFile("logs"))
			})
		})
	}
}

func FuzzParseEntries(f *testing.F) {
	for file := range readTestData(f) {
		f.Add(file.Body)
	}

	f.Fuzz(func(t *testing.T, body string) {
		for _, e := range ParseEntries(body) {
			require.False(t, e.Timestamp.IsZero(), "entry must have a timestamp")
		}
	})
}
