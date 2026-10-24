package logparser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const klogLocalTimeLine = `W1024 18:00:00.123456 243509 logging.go:55] grpc failed`

func TestKLogParserTimestamp(t *testing.T) {
	for _, tt := range []struct {
		name      string
		timestamp string
		want      string
	}{
		{"UTC", "2026-10-24T18:00:00.123456789Z", "2026-10-24T18:00:00.123456789Z"},
		{"UTC+3", "2026-10-24T18:00:00.123456789+03:00", "2026-10-24T15:00:00.123456789Z"},
		{"UTC-4", "2026-10-24T18:00:00.123456789-04:00", "2026-10-24T22:00:00.123456789Z"},
		{"no event timestamp", "", "2026-10-24T18:00:00.123456Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			observed := pcommon.NewTimestampFromTime(time.Date(2026, time.October, 24, 23, 0, 0, 0, time.UTC))
			record := Record{ObservedTimestamp: observed}
			if tt.timestamp != "" {
				ts, err := time.Parse(time.RFC3339Nano, tt.timestamp)
				require.NoError(t, err)
				record.Timestamp = pcommon.NewTimestampFromTime(ts)
			}
			want, err := time.Parse(time.RFC3339Nano, tt.want)
			require.NoError(t, err)

			require.NoError(t, (KLogParser{}).Parse(klogLocalTimeLine, &record))
			assert.Equal(t, pcommon.NewTimestampFromTime(want), record.Timestamp)
			assert.Equal(t, observed, record.ObservedTimestamp)
			assert.Equal(t, "grpc failed", record.Body)
		})
	}
}

func TestKLogParser(t *testing.T) {
	// heavily relies on time.Now.
	testParser("klog")(t)
}

func FuzzKLogParser(f *testing.F) {
	fuzzParser(f, "klog")
}

func FuzzKLogParserPreservesTimestamp(f *testing.F) {
	f.Add(klogLocalTimeLine, uint64(1792854000123456789))
	f.Add(klogLocalTimeLine, uint64(0))
	f.Fuzz(func(t *testing.T, line string, timestamp uint64) {
		record := Record{
			Timestamp:         pcommon.Timestamp(timestamp),
			ObservedTimestamp: pcommon.Timestamp(1792854001000000000),
		}
		if err := (KLogParser{}).Parse(line, &record); err != nil {
			return
		}
		if timestamp != 0 && uint64(record.Timestamp) != timestamp {
			t.Fatalf("timestamp changed: got %d, want %d", record.Timestamp, timestamp)
		}
	})
}
