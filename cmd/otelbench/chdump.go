package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func newChDumpCommand() *cobra.Command {
	var (
		dsn      string
		query    string
		startStr string
		endStr   string
		stepStr  string
		output   string
	)
	cmd := &cobra.Command{
		Use:   "chdump",
		Short: "Dump raw points from ClickHouse using PromQL",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			client, err := chstorage.Dial(ctx, dsn, chstorage.DialOptions{})
			if err != nil {
				return errors.Wrap(err, "dial")
			}

			start := time.Now().Add(-time.Hour)
			if startStr != "" {
				start, err = time.Parse(time.RFC3339, startStr)
				if err != nil {
					return errors.Wrap(err, "parse start time")
				}
			}
			end := time.Now()
			if endStr != "" {
				end, err = time.Parse(time.RFC3339, endStr)
				if err != nil {
					return errors.Wrap(err, "parse end time")
				}
			}
			step, err := time.ParseDuration(stepStr)
			if err != nil {
				return errors.Wrap(err, "parse step")
			}

			querier, err := chstorage.NewQuerier(client, chstorage.QuerierOptions{})
			if err != nil {
				return errors.Wrap(err, "create querier")
			}

			capturer := &dataCapturer{
				queryable: querier,
			}

			engine := promql.NewEngine(promql.EngineOpts{
				MaxSamples:    1_000_000,
				Timeout:       time.Minute,
				LookbackDelta: 5 * time.Minute,
			})

			q, err := engine.NewRangeQuery(ctx, capturer, nil, query, start, end, step)
			if err != nil {
				return errors.Wrap(err, "create range query")
			}
			defer q.Close()

			res := q.Exec(ctx)
			if res.Err != nil {
				return errors.Wrap(res.Err, "query execution")
			}

			f, err := os.Create(output)
			if err != nil {
				return errors.Wrap(err, "create file")
			}
			defer f.Close()

			enc := json.NewEncoder(f)
			if err := enc.Encode(capturer.points); err != nil {
				return errors.Wrap(err, "encode points")
			}

			fmt.Printf("Dumped %d points to %s\n", len(capturer.points), output)
			return nil
		},
	}
	cmd.Flags().StringVar(&dsn, "dsn", "clickhouse://localhost:9000", "ClickHouse DSN")
	cmd.Flags().StringVar(&query, "query", "", "PromQL query")
	cmd.Flags().StringVar(&startStr, "start", "", "Start time (RFC3339)")
	cmd.Flags().StringVar(&endStr, "end", "", "End time (RFC3339)")
	cmd.Flags().StringVar(&stepStr, "step", "15s", "Step duration")
	cmd.Flags().StringVar(&output, "output", "dump.json", "Output file")
	_ = cmd.MarkFlagRequired("query")

	return cmd
}

type Point struct {
	Hash      [16]byte `json:"hash"`
	Value     float64  `json:"value"`
	Timestamp int64    `json:"timestamp"`
}

type dataCapturer struct {
	queryable storage.Queryable
	points    []Point
}

func (c *dataCapturer) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := c.queryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &querierCapturer{
		Querier:  q,
		capturer: c,
	}, nil
}

type querierCapturer struct {
	storage.Querier
	capturer *dataCapturer
}

func (q *querierCapturer) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	set := q.Querier.Select(ctx, sortSeries, hints, matchers...)
	return &seriesSetCapturer{
		SeriesSet: set,
		capturer:  q.capturer,
	}
}

type seriesSetCapturer struct {
	storage.SeriesSet
	capturer *dataCapturer
}

func (s *seriesSetCapturer) Next() bool {
	if !s.SeriesSet.Next() {
		return false
	}
	series := s.SeriesSet.At()
	ls := series.Labels()
	hash := ls.Hash()
	var hash16 [16]byte
	binary.LittleEndian.PutUint64(hash16[:8], hash)

	it := series.Iterator(nil)
	for it.Next() == chunkenc.ValFloat {
		t, v := it.At()
		s.capturer.points = append(s.capturer.points, Point{
			Hash:      hash16,
			Value:     v,
			Timestamp: t,
		})
	}

	return true
}
