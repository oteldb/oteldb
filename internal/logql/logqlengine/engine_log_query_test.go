package logqlengine

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/lokiapi"
)

func labelSet(keyval ...string) logqlabels.LabelSet {
	set := logqlabels.NewLabelSet()
	for i := 0; i < len(keyval); i += 2 {
		set.Set(logql.Label(keyval[i]), pcommon.NewValueStr(keyval[i+1]))
	}
	return set
}

func apiLabels(keyval ...string) lokiapi.OptLabelSet {
	set := lokiapi.LabelSet{}
	for i := 0; i < len(keyval); i += 2 {
		set[keyval[i]] = keyval[i+1]
	}
	return lokiapi.NewOptLabelSet(set)
}

func Test_groupEntries(t *testing.T) {
	tests := []struct {
		entries []Entry
		wantS   lokiapi.Streams
		wantErr bool
	}{
		{
			nil,
			lokiapi.Streams{},
			false,
		},
		{
			[]Entry{
				{1, "line 1", labelSet("key", "a")},
				{2, "line 2", labelSet("key", "a")},
				{4, "line 4", labelSet("key", "a")},
				{3, "line 3", labelSet("key", "b")},
				{5, "line 5", labelSet("key", "a", "key2", "a")},
			},
			lokiapi.Streams{
				{
					Stream: apiLabels("key", "a"),
					Values: []lokiapi.LogEntry{
						{T: 1, V: "line 1"},
						{T: 2, V: "line 2"},
						{T: 4, V: "line 4"},
					},
				},
				{
					Stream: apiLabels("key", "b"),
					Values: []lokiapi.LogEntry{
						{T: 3, V: "line 3"},
					},
				},
				{
					Stream: apiLabels("key", "a", "key2", "a"),
					Values: []lokiapi.LogEntry{
						{T: 5, V: "line 5"},
					},
				},
			},
			false,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, gotTotal, err := groupEntries(iterators.Slice(tt.entries))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tt.wantS, got)
			require.Equal(t, len(tt.entries), gotTotal)

			for _, stream := range got {
				require.True(t,
					slices.IsSortedFunc(stream.Values, func(a, b lokiapi.LogEntry) int {
						return cmp.Compare(a.T, b.T)
					}),
					"Entries must be sorted",
				)
			}
		})
	}
}

func generateLogs(now time.Time) (entries []Entry) {
	type httpLogBatch struct {
		Method   string
		Status   int
		Count    int
		IP       string
		Protocol string
	}
	for _, b := range []httpLogBatch{
		{Method: "GET", Status: 200, Count: 11, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "GET", Status: 200, Count: 10, IP: "200.1.1.1", Protocol: "HTTP/1.1"},
		{Method: "DELETE", Status: 200, Count: 20, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "POST", Status: 200, Count: 21, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "PATCH", Status: 200, Count: 19, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "HEAD", Status: 200, Count: 15, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "HEAD", Status: 200, Count: 4, IP: "200.1.1.1", Protocol: "HTTP/1.0"},
		{Method: "HEAD", Status: 200, Count: 1, IP: "236.7.233.166", Protocol: "HTTP/2.0"},
		{Method: "HEAD", Status: 500, Count: 2, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
		{Method: "PUT", Status: 200, Count: 20, IP: "200.1.1.1", Protocol: "HTTP/2.0"},
	} {
		for i := 0; i < b.Count; i++ {
			now = now.Add(time.Millisecond * 120)
			severity := plog.SeverityNumberInfo
			switch b.Status / 100 {
			case 2:
				severity = plog.SeverityNumberInfo
			case 3:
				severity = plog.SeverityNumberWarn
			case 4:
				severity = plog.SeverityNumberError
			case 5:
				severity = plog.SeverityNumberFatal
			}
			entries = append(entries, Entry{
				Timestamp: pcommon.Timestamp(now.UnixNano()),
				Line:      "line",
				Set: labelSet(
					"level", strings.ToLower(severity.String()),
					"method", b.Method,
					"status", fmt.Sprint(b.Status),
					"bytes", fmt.Sprint(250),
					"protocol", b.Protocol,
					"ip", b.IP,
					"url", "/api/v1/series",
					"ref", "https://api.go-faster.org",
				),
			})
		}
	}
	return entries
}

func Benchmark_groupEntries(b *testing.B) {
	var (
		entries = generateLogs(time.Now())

		iter = iterators.Slice(entries)
		sink lokiapi.Streams
	)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter.Reset(entries)
		sink, _, _ = groupEntries(iter)
	}

	if len(sink) == 0 {
		b.Fatal("at least one stream expected")
	}
}
