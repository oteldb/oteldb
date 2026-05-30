package main

import (
	"encoding/json"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/oteldb/oteldb/internal/promproxy"
)

type PromQLConvert struct{}

func (p PromQLConvert) Run() error {
	var report promproxy.Record
	d := json.NewDecoder(os.Stdin)

	var (
		seriesQueries  []promproxy.SeriesQuery
		instantQueries []promproxy.InstantQuery
		rangeQueries   []promproxy.RangeQuery
	)
	steps := make(map[int]int)
	for {
		var query promproxy.Query
		if err := d.Decode(&query); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.Wrap(err, "decode")
		}
		switch query.Type {
		case promproxy.SeriesQueryQuery:
			seriesQueries = append(seriesQueries, query.SeriesQuery)
		case promproxy.InstantQueryQuery:
			instantQueries = append(instantQueries, query.InstantQuery)
		case promproxy.RangeQueryQuery:
			steps[query.RangeQuery.Step.Value]++
			rangeQueries = append(rangeQueries, query.RangeQuery)
		default:
			report.Queries = append(report.Queries, query)
		}
	}

	// Simplify.
	var step int
	{
		var maxValue int
		for k, v := range steps {
			if v > maxValue {
				maxValue = v
				step = k
			}
		}
	}
	var start, end time.Time
	for _, q := range rangeQueries {
		if start.IsZero() {
			start = q.Start.Value
		}
		if end.IsZero() {
			end = q.End.Value
		}
		if !start.IsZero() && q.Start.Value.Equal(start) {
			q.Start.Set = false
		}
		if !end.IsZero() && q.End.Value.Equal(end) {
			q.End.Set = false
		}
		if q.Step.Value == step && step != 0 {
			q.Step.Set = false
		}
		report.Range = append(report.Range, q)
	}
	if !start.IsZero() {
		report.Start = promproxy.NewOptDateTime(start)
	}
	if !end.IsZero() {
		report.End = promproxy.NewOptDateTime(end)
	}
	if step != 0 {
		report.Step = promproxy.NewOptInt(step)
	}

	for _, q := range seriesQueries {
		if !q.Start.Value.IsZero() && q.Start.Value.Equal(start) {
			q.Start.Set = false
		}
		if !q.End.Value.IsZero() && q.End.Value.Equal(end) {
			q.End.Set = false
		}
		report.Series = append(report.Series, q)
	}
	report.Instant = instantQueries

	slices.SortFunc(report.Range, func(a, b promproxy.RangeQuery) int {
		return strings.Compare(a.Query, b.Query)
	})
	slices.SortFunc(report.Instant, func(a, b promproxy.InstantQuery) int {
		return strings.Compare(a.Query, b.Query)
	})

	data, err := report.MarshalJSON()
	if err != nil {
		return errors.Wrap(err, "marshal")
	}
	output, err := yaml.JSONToYAML(data)
	if err != nil {
		return errors.Wrap(err, "yaml")
	}
	if _, err := os.Stdout.Write(output); err != nil {
		return errors.Wrap(err, "write")
	}
	return nil
}

func newPromQLConvertCommand() *cobra.Command {
	p := PromQLConvert{}
	cmd := &cobra.Command{
		Use:   "convert",
		Short: "Convert json line query export (stdin) to yaml (stdout) structured record document",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return p.Run()
		},
	}
	return cmd
}
