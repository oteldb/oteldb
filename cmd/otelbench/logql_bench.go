package main

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/oteldb/oteldb/cmd/otelbench/logqlbench"
)

func newLogQLBenchmarkCommand() *cobra.Command {
	p := &logqlbench.LogQLBenchmark{}
	cmd := &cobra.Command{
		Use:     "bench",
		Aliases: []string{"benchmark"},
		Short:   "Run LogQL queries",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			if err := p.Setup(cmd); err != nil {
				return errors.Wrap(err, "setup")
			}
			return p.Run(ctx)
		},
	}
	f := cmd.Flags()
	f.StringVar(&p.Addr, "addr", "http://127.0.0.1:3100", "Loki address")
	f.StringVarP(&p.Input, "input", "i", "logql.yml", "Input file")
	f.StringVarP(&p.Output, "output", "o", "report.yml", "Output report file")
	f.DurationVar(&p.RequestTimeout, "request-timeout", time.Second*10, "Request timeout")
	f.StringVar(&p.StartTime, "start", "", "Start time override (RFC3339 or unix timestamp)")
	f.StringVar(&p.EndTime, "end", "", "End time override (RFC3339 or unix timestamp)")
	f.BoolVar(&p.AllowEmpty, "allow-empty", true, "Allow empty results")

	f.StringVar(&p.TrackerOptions.TempoAddr, "tempo-addr", "http://127.0.0.1:3200", "Tempo endpoint")
	f.BoolVar(&p.TrackerOptions.Trace, "trace", false, "Trace queries")

	f.IntVar(&p.Count, "count", 1, "Number of times to run each query (only for sequential)")
	f.IntVar(&p.Warmup, "warmup", 0, "Number of warmup runs (only for sequential)")

	return cmd
}
