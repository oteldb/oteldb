// Package logqlbench defines utilities to benchmark LogQL queries.
package logqlbench

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"github.com/oteldb/oteldb/cmd/otelbench/chtracker"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/lokihandler"
)

type LogQLBenchmark struct {
	Addr   string
	Count  int
	Warmup int

	StartTime  string
	EndTime    string
	AllowEmpty bool

	TrackerOptions chtracker.SetupOptions

	Input          string
	Output         string
	RequestTimeout time.Duration

	tracker *chtracker.Tracker[Query]

	client *lokiapi.Client
	start  time.Time
	end    time.Time
	limit  int
}

// Setup setups benchmark using given flags.
func (p *LogQLBenchmark) Setup(cmd *cobra.Command) error {
	ctx := cmd.Context()

	var err error
	if p.start, err = lokihandler.ParseTimestamp(p.StartTime, time.Time{}); err != nil {
		return errors.Wrap(err, "parse start time")
	}
	if p.end, err = lokihandler.ParseTimestamp(p.EndTime, time.Time{}); err != nil {
		return errors.Wrap(err, "parse end time")
	}
	p.limit = 1000

	p.tracker, err = chtracker.Setup[Query](ctx, "logql", p.TrackerOptions)
	if err != nil {
		return errors.Wrap(err, "create tracker")
	}

	p.client, err = lokiapi.NewClient(p.Addr,
		lokiapi.WithTracerProvider(p.tracker.TracerProvider()),
		lokiapi.WithClient(p.tracker.HTTPClient()),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}
	return nil
}

// Run starts the benchmark.
func (p *LogQLBenchmark) Run(ctx context.Context) error {
	fmt.Println("sending LogQL queries from", p.Input, "to", p.Addr)
	if !p.start.IsZero() {
		fmt.Println("start time override:", p.start.Format(time.RFC3339))
	}
	if !p.end.IsZero() {
		fmt.Println("end time override:", p.end.Format(time.RFC3339))
	}

	var total int
	if err := p.each(ctx, func(ctx context.Context, q Query) error {
		total += p.Count
		total += p.Warmup
		return nil
	}); err != nil {
		return errors.Wrap(err, "count total")
	}

	pb := progressbar.Default(int64(total))
	start := time.Now()
	if err := p.each(ctx, func(ctx context.Context, q Query) (rerr error) {
		// Warmup.
		for i := 0; i < p.Warmup; i++ {
			if err := p.send(ctx, q); err != nil {
				return errors.Wrap(err, "send (warmup)")
			}
			if err := pb.Add(1); err != nil {
				return errors.Wrap(err, "update progress bar")
			}
		}
		// Run.
		for i := 0; i < p.Count; i++ {
			if err := p.sendAndRecord(ctx, q); err != nil {
				return errors.Wrap(err, "send")
			}
			if err := pb.Add(1); err != nil {
				return errors.Wrap(err, "update progress bar")
			}
		}
		return nil
	}); err != nil {
		_ = pb.Exit()
		return errors.Wrap(err, "send queries")
	}
	if err := pb.Finish(); err != nil {
		return errors.Wrap(err, "finish progress bar")
	}
	fmt.Println("done in", time.Since(start).Round(time.Millisecond))

	if p.TrackerOptions.Trace {
		fmt.Println("waiting for traces")
		if err := p.tracker.Flush(ctx); err != nil {
			fmt.Println("warning: flush traces:", err)
		}
	} else {
		fmt.Println("saving")
	}

	var reports []LogQLReportQuery
	if err := p.tracker.Report(ctx,
		func(ctx context.Context, tq chtracker.TrackedQuery[Query], queries []chtracker.QueryReport, retriveErr error) error {
			var errMsg string
			if retriveErr != nil {
				if errors.Is(retriveErr, context.DeadlineExceeded) {
					errMsg = "no queries"
				} else {
					errMsg = retriveErr.Error()
				}
			}

			header := tq.Meta.Header()
			reports = append(reports, LogQLReportQuery{
				ID:            header.ID,
				Type:          string(tq.Meta.Type()),
				Title:         header.Title,
				Description:   header.Description,
				Query:         tq.Meta.Query(),
				Matchers:      tq.Meta.Matchers(),
				DurationNanos: tq.Duration.Nanoseconds(),
				Queries:       queries,
				Timeout:       tq.Timeout,
				ReportError:   errMsg,
			})
			return nil
		},
	); err != nil {
		return err
	}

	report := LogQLReport{
		Queries: reports,
	}
	slices.SortFunc(report.Queries, func(a, b LogQLReportQuery) int {
		if a.DurationNanos == b.DurationNanos {
			return strings.Compare(a.Query, b.Query)
		}
		if a.DurationNanos > b.DurationNanos {
			return -1
		}
		return 1
	})
	buf := new(bytes.Buffer)
	enc := yamlx.NewEncoder(buf)
	enc.SetIndent(2)
	if err := enc.Encode(report); err != nil {
		return errors.Wrap(err, "encode report")
	}
	reportData := buf.Bytes()

	if err := os.WriteFile(p.Output, reportData, 0o644); err != nil {
		return errors.Wrap(err, "write report")
	}
	fmt.Println("done")
	return nil
}
