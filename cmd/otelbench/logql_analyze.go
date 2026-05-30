package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/spf13/cobra"
	"golang.org/x/perf/benchfmt"

	"github.com/oteldb/oteldb/cmd/otelbench/logqlbench"
)

type LogQLAnalyze struct {
	Input  string
	Format string
}

func (a LogQLAnalyze) Run() error {
	data, err := os.ReadFile(a.Input)
	if err != nil {
		return errors.Wrap(err, "read file")
	}
	var report logqlbench.LogQLReport
	if err := yamlx.Unmarshal(data, &report); err != nil {
		return errors.Wrap(err, "unmarshal yaml")
	}

	switch a.Format {
	case "pretty":
		return a.renderPretty(report, os.Stdout)
	case "benchstat":
		return a.renderBenchstat(report, os.Stdout)
	default:
		return errors.Errorf("unknown format %q", a.Format)
	}
}

func (a LogQLAnalyze) renderPretty(report logqlbench.LogQLReport, w io.Writer) error {
	var buf bytes.Buffer
	for _, q := range report.Queries {
		if q.ID != 0 {
			fmt.Fprintf(&buf, "[Q%d] ", q.ID)
		}
		if q.Query != "" {
			fmt.Fprintln(&buf, q.Query)
		} else {
			fmt.Fprintln(&buf, q.Matchers)
		}

		formatNanos := func(nanos int64) string {
			d := time.Duration(nanos) * time.Nanosecond
			return d.Round(time.Millisecond / 20).String()
		}
		fmt.Fprint(&buf, " duration:", formatNanos(q.DurationNanos))
		if q.Timeout {
			fmt.Fprint(&buf, " (timeout)")
		}
		fmt.Fprintln(&buf)

		if len(q.Queries) > 0 {
			fmt.Fprintln(&buf, " sql queries:", len(q.Queries))

			var memUsage, readBytes, readRows int64
			for _, v := range q.Queries {
				memUsage += v.MemoryUsage
				readBytes += v.ReadBytes
				readRows += v.ReadRows
			}

			fmt.Fprintln(&buf, " memory usage:", humanize.Bytes(uint64(memUsage)))
			fmt.Fprintln(&buf, " read bytes:", humanize.Bytes(uint64(readBytes)))
			fmt.Fprintln(&buf, " read rows:", fmtInt(int(readRows)))
		}
	}
	_, err := buf.WriteTo(w)
	return err
}

func (a LogQLAnalyze) renderBenchstat(report logqlbench.LogQLReport, w io.Writer) error {
	var recs []benchfmt.Result
	for _, q := range report.Queries {
		if q.ReportError != "" {
			continue
		}

		name := normalizeBenchName(q.Title)
		if len(name) == 0 {
			name = fmt.Appendf(name, "Query%d", q.ID)
		}
		recs = append(recs, benchfmt.Result{
			Name: bytes.Join(
				[][]byte{
					[]byte(`LogQL`),
					[]byte(q.Type),
					name,
				},
				[]byte{'/'},
			),
			Values: appendChValues(
				[]benchfmt.Value{
					{
						Value: float64(q.DurationNanos),
						Unit:  "ns/op",
					},
				},
				q.Queries,
			),
		})
	}
	return writeBenchstat(w, recs)
}

func newLogQLAnalyzeCommand() *cobra.Command {
	p := &LogQLAnalyze{}
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Run LogQL queries",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return p.Run()
		},
	}
	f := cmd.Flags()
	f.StringVarP(&p.Input, "input", "i", "report.yml", "Input file")
	f.StringVarP(&p.Format, "format", "f", "pretty", "Output format")
	return cmd
}
