package main

import (
	"bytes"
	"io"
	"slices"
	"strconv"
	"unicode"
	"unicode/utf8"

	"golang.org/x/perf/benchfmt"

	"github.com/oteldb/oteldb/cmd/otelbench/chtracker"
)

// appendChValues returns sum values of metrics of traced Clickhouse queries.
func appendChValues(s []benchfmt.Value, reports []chtracker.QueryReport) []benchfmt.Value {
	var chDurationNanos, memUsage, readBytes, readRows int64
	for _, v := range reports {
		chDurationNanos += v.DurationNanos
		memUsage += v.MemoryUsage
		readBytes += v.ReadBytes
		readRows += v.ReadRows
	}
	// NOTE(tdakkota): it is important to keep 'ns/op', `bytes/op`
	// 	suffix in Unit, because it lets benchstat to figure out measurement unit.
	return append(s,
		benchfmt.Value{
			Value: float64(chDurationNanos),
			Unit:  "ch-ns/op",
		},
		benchfmt.Value{
			Value: float64(memUsage),
			Unit:  "ch-mem-bytes/op",
		},
		benchfmt.Value{
			Value: float64(readBytes),
			Unit:  "ch-read-bytes/op",
		},
		benchfmt.Value{
			Value: float64(readRows),
			Unit:  "ch-read-rows/op",
		},
	)
}

func writeBenchstat(w io.Writer, r []benchfmt.Result) error {
	slices.SortFunc(r, func(a, b benchfmt.Result) int {
		return bytes.Compare(a.Name, b.Name)
	})

	fw := benchfmt.NewWriter(w)
	for i := range r {
		if err := fw.Write(&r[i]); err != nil {
			return err
		}
	}
	return nil
}

func normalizeBenchName(s string) (name []byte) {
	// Normalize title as benchmark name.
	for _, r := range s {
		switch {
		case unicode.IsSpace(r):
			name = append(name, '_')
		case !strconv.IsPrint(r):
			s := strconv.QuoteRune(r)
			name = append(name, s[1:len(s)-1]...)
		default:
			name = utf8.AppendRune(name, r)
		}
	}
	return name
}
