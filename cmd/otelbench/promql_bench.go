package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"sigs.k8s.io/yaml"

	"github.com/go-faster/oteldb/cmd/otelbench/chtracker"
	"github.com/go-faster/oteldb/internal/logparser"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promproxy"
)

type promQLQuery struct {
	ID int
	promproxy.Query
}

type PromQL struct {
	Addr   string
	Trace  bool
	Count  int
	Warmup int

	// For concurrent benchmarks.
	Jobs       int
	Duration   time.Duration
	Concurrent bool

	StartTime  string
	EndTime    string
	AllowEmpty bool

	TrackerOptions chtracker.SetupOptions

	Input          string
	Output         string
	RequestTimeout time.Duration

	tracker *chtracker.Tracker[promQLQuery]

	client *promapi.Client
	start  time.Time
	end    time.Time
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	for _, format := range []string{
		time.RFC3339,
		time.RFC3339Nano,
	} {
		t, err := time.Parse(format, s)
		if err == nil {
			return t, nil
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "parse int")
	}
	nanos, ok := logparser.DeduceNanos(v)
	if !ok {
		return time.Time{}, errors.Errorf("invalid unix timestamp %d", v)
	}
	return time.Unix(0, nanos), nil
}

func (p *PromQL) Setup(cmd *cobra.Command) error {
	ctx := cmd.Context()

	var err error
	if p.start, err = parseTime(p.StartTime); err != nil {
		return errors.Wrap(err, "parse start time")
	}
	if p.end, err = parseTime(p.EndTime); err != nil {
		return errors.Wrap(err, "parse end time")
	}

	if p.Concurrent {
		if p.Trace {
			return errors.New("concurrent mode is not supported with tracing")
		}
		if p.client, err = promapi.NewClient(p.Addr); err != nil {
			return errors.Wrap(err, "create client")
		}
		return nil
	}

	p.tracker, err = chtracker.Setup[promQLQuery](ctx, "promql", p.TrackerOptions)
	if err != nil {
		return errors.Wrap(err, "create tracker")
	}
	p.client, err = promapi.NewClient(p.Addr,
		promapi.WithTracerProvider(p.tracker.TracerProvider()),
		promapi.WithClient(p.tracker.HTTPClient()),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}
	return nil
}

func toPrometheusTimestamp(t time.Time) promapi.PrometheusTimestamp {
	return promapi.PrometheusTimestamp(strconv.FormatInt(t.Unix(), 10))
}

func (p *PromQL) sendRangeQuery(ctx context.Context, q promproxy.RangeQuery) error {
	res, err := p.client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
		Query: q.Query,
		Step:  promapi.NewOptString(strconv.Itoa(q.Step.Value)),
		Start: toPrometheusTimestamp(q.Start.Value),
		End:   toPrometheusTimestamp(q.End.Value),
	})
	if err != nil {
		return errors.Wrap(err, "get query range")
	}
	if len(res.Data.Matrix.Result) == 0 && !p.AllowEmpty {
		return errors.Errorf("no results for range query %q", q.Query)
	}
	return nil
}

func toOptPrometheusTimestamp(t promproxy.OptDateTime) promapi.OptPrometheusTimestamp {
	if !t.IsSet() {
		return promapi.OptPrometheusTimestamp{}
	}
	return promapi.NewOptPrometheusTimestamp(toPrometheusTimestamp(t.Value))
}

func (p *PromQL) sendInstantQuery(ctx context.Context, q promproxy.InstantQuery) error {
	res, err := p.client.GetQuery(ctx, promapi.GetQueryParams{
		Query: q.Query,
		Time:  toOptPrometheusTimestamp(q.Time),
	})
	if err != nil {
		return errors.Wrap(err, "get query")
	}
	if len(res.Data.Vector.Result) == 0 && !p.AllowEmpty {
		return errors.Errorf("no results for query %q", q.Query)
	}
	return nil
}

func (p *PromQL) sendSeriesQuery(ctx context.Context, query promproxy.SeriesQuery) error {
	res, err := p.client.GetSeries(ctx, promapi.GetSeriesParams{
		Start: toOptPrometheusTimestamp(query.Start),
		End:   toOptPrometheusTimestamp(query.End),
		Match: query.Matchers,
	})
	if err != nil {
		return errors.Wrap(err, "get series")
	}
	if len(res.Data) == 0 && !p.AllowEmpty {
		return errors.Errorf("no results for series query %q", query.Matchers)
	}
	return nil
}

func (p *PromQL) sendAndRecord(ctx context.Context, q promQLQuery) (rerr error) {
	return p.tracker.Track(ctx, q, p.send)
}

func (p *PromQL) send(ctx context.Context, pq promQLQuery) error {
	ctx, cancel := context.WithTimeout(ctx, p.RequestTimeout)
	defer cancel()

	switch q := pq.Query; q.Type {
	case promproxy.InstantQueryQuery:
		return p.sendInstantQuery(ctx, q.InstantQuery)
	case promproxy.RangeQueryQuery:
		return p.sendRangeQuery(ctx, q.RangeQuery)
	case promproxy.SeriesQueryQuery:
		return p.sendSeriesQuery(ctx, q.SeriesQuery)
	default:
		return errors.Errorf("unknown query type %q", q.Type)
	}
}

func (p *PromQL) eachFromReport(ctx context.Context, f *os.File, fn func(ctx context.Context, id int, q promproxy.Query) error) error {
	data, err := io.ReadAll(f)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return errors.Wrap(err, "convert yaml to json")
	}
	var report promproxy.Record
	if err := report.UnmarshalJSON(jsonData); err != nil {
		return errors.Wrap(err, "decode report")
	}
	var id int
	for _, q := range report.Range {
		if !q.Start.Set {
			q.Start = report.Start
		}
		if !q.End.Set {
			q.End = report.End
		}
		if !q.Step.Set {
			q.Step = report.Step
		}
		if !p.start.IsZero() {
			q.Start.Value = p.start
		}
		if !p.end.IsZero() {
			q.End.Value = p.end
		}
		id++
		if err := fn(ctx, id, promproxy.NewRangeQueryQuery(q)); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	for _, q := range report.Series {
		if !q.End.Set {
			q.End = report.End
		}
		if !q.Start.Set {
			q.Start = report.Start
		}
		if !p.start.IsZero() {
			q.Start.Value = p.start
		}
		if !p.end.IsZero() {
			q.End.Value = p.end
		}
		id++
		if err := fn(ctx, id, promproxy.NewSeriesQueryQuery(q)); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	for _, q := range report.Instant {
		id++
		if err := fn(ctx, id, promproxy.NewInstantQueryQuery(q)); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	return nil
}

func (p *PromQL) each(ctx context.Context, fn func(ctx context.Context, id int, q promproxy.Query) error) error {
	f, err := os.Open(p.Input)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	defer func() {
		_ = f.Close()
	}()
	if filepath.Ext(p.Input) == ".yaml" || filepath.Ext(p.Input) == ".yml" {
		return p.eachFromReport(ctx, f, fn)
	}
	d := json.NewDecoder(f)
	id := 0
	for {
		id++ // start from 1
		var q promproxy.Query
		if err := d.Decode(&q); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.Wrap(err, "decode query")
		}
		if err := fn(ctx, id, q); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	return nil
}

type PromQLReportQuery struct {
	ID            int                     `yaml:"id,omitempty"`
	Query         string                  `yaml:"query,omitempty"`
	Title         string                  `yaml:"title,omitempty"`
	Description   string                  `yaml:"description,omitempty"`
	DurationNanos int64                   `yaml:"duration_nanos,omitempty"`
	Matchers      []string                `yaml:"matchers,omitempty"`
	Queries       []chtracker.QueryReport `yaml:"queries,omitempty"`
}

type PromQLReport struct {
	Queries []PromQLReportQuery `json:"queries"`
}

func (p *PromQL) runConcurrentBenchmark(ctx context.Context) error {
	// Load queries.
	var queries []promQLQuery
	if err := p.each(ctx, func(ctx context.Context, id int, q promproxy.Query) error {
		queries = append(queries, promQLQuery{
			ID:    id,
			Query: q,
		})
		return nil
	}); err != nil {
		return errors.Wrap(err, "load queries")
	}

	// Spawn workers and execute random queries until time is up.
	g, ctx := errgroup.WithContext(ctx)
	tasks := make(chan promQLQuery, p.Jobs)
	for i := 0; i < p.Jobs; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case q, ok := <-tasks:
					if !ok {
						return nil
					}
					if err := p.send(ctx, q); err != nil {
						return errors.Wrap(err, "send")
					}
				}
			}
		})
	}
	g.Go(func() error {
		// Generate load.
		// #nosec G404
		rnd := rand.New(rand.NewSource(1))
		defer close(tasks)
		for {
			q := queries[rnd.Intn(len(queries))]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case tasks <- q:
				continue
			}
		}
	})
	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "wait")
	}
	return nil
}

func (p *PromQL) waitForSeries(ctx context.Context, query promproxy.SeriesQuery) error {
	var (
		attempts   = 5
		sleepDelay = 5 * time.Second
	)
	for {
		res, err := p.client.GetSeries(ctx, promapi.GetSeriesParams{
			Start: toOptPrometheusTimestamp(query.Start),
			End:   toOptPrometheusTimestamp(query.End),
			Match: query.Matchers,
		})
		if err != nil {
			return errors.Wrap(err, "get series")
		}

		if len(res.Data) > 0 {
			return nil
		}

		if attempts == 0 {
			return errors.New("no series found after retries")
		}
		attempts--

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDelay):
			continue
		}
	}
}

func (p *PromQL) Run(ctx context.Context) error {
	fmt.Println("sending promql queries from", p.Input, "to", p.Addr)
	if !p.start.IsZero() {
		fmt.Println("start time override:", p.start.Format(time.RFC3339))
	}
	if !p.end.IsZero() {
		fmt.Println("end time override:", p.end.Format(time.RFC3339))
	}
	if p.Concurrent {
		return p.runConcurrentBenchmark(ctx)
	}

	var (
		total       int
		seriesQuery *promproxy.SeriesQuery
	)
	if err := p.each(ctx, func(ctx context.Context, _ int, q promproxy.Query) error {
		total += p.Count
		total += p.Warmup

		if q.Type == promproxy.SeriesQueryQuery && seriesQuery == nil {
			sq := q.SeriesQuery
			seriesQuery = &sq
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "count total")
	}

	if seriesQuery != nil {
		start := time.Now()
		fmt.Println("waiting for series to appear")
		if err := p.waitForSeries(ctx, *seriesQuery); err != nil {
			return errors.Wrap(err, "waiting for series to appear")
		}
		fmt.Println("done in", time.Since(start).Round(time.Millisecond))
	} else {
		fmt.Println("no series query found")
	}

	pb := progressbar.Default(int64(total))
	start := time.Now()
	if err := p.each(ctx, func(ctx context.Context, id int, q promproxy.Query) (rerr error) {
		pq := promQLQuery{ID: id, Query: q}

		// Warmup.
		for i := 0; i < p.Warmup; i++ {
			if err := p.send(ctx, pq); err != nil {
				return errors.Wrap(err, "send")
			}
			if err := pb.Add(1); err != nil {
				return errors.Wrap(err, "update progress bar")
			}
		}
		// Run.
		for i := 0; i < p.Count; i++ {
			if err := p.sendAndRecord(ctx, pq); err != nil {
				return errors.Wrap(err, "send")
			}
			if err := pb.Add(1); err != nil {
				return errors.Wrap(err, "update progress bar")
			}
		}
		return nil
	}); err != nil {
		_ = pb.Exit()
		return errors.Wrap(err, "send")
	}
	if err := pb.Finish(); err != nil {
		return errors.Wrap(err, "finish progress bar")
	}
	fmt.Println("done in", time.Since(start).Round(time.Millisecond))

	if p.Trace {
		fmt.Println("waiting for traces")
		if err := p.tracker.Flush(ctx); err != nil {
			return errors.Wrap(err, "flush traces")
		}
	} else {
		fmt.Println("saving")
	}

	var reports []PromQLReportQuery
	if err := p.tracker.Report(ctx,
		func(ctx context.Context, tq chtracker.TrackedQuery[promQLQuery], queries []chtracker.QueryReport, retriveErr error) error {
			if retriveErr != nil {
				return retriveErr
			}

			entry := PromQLReportQuery{
				ID:            tq.Meta.ID,
				DurationNanos: tq.Duration.Nanoseconds(),
				Queries:       queries,
			}
			switch q := tq.Meta.Query; q.Type {
			case promproxy.InstantQueryQuery:
				entry.Query = q.InstantQuery.Query
				entry.Title = q.InstantQuery.Title.Value
				entry.Description = q.InstantQuery.Description.Value
			case promproxy.RangeQueryQuery:
				entry.Query = q.RangeQuery.Query
				entry.Title = q.RangeQuery.Title.Value
				entry.Description = q.RangeQuery.Description.Value
			case promproxy.SeriesQueryQuery:
				entry.Matchers = q.SeriesQuery.Matchers
				entry.Title = q.SeriesQuery.Title.Value
				entry.Description = q.SeriesQuery.Description.Value
			default:
				return errors.Errorf("unknown query type %q", q.Type)
			}
			reports = append(reports, entry)
			return nil
		},
	); err != nil {
		return err
	}

	report := PromQLReport{
		Queries: reports,
	}
	slices.SortFunc(report.Queries, func(a, b PromQLReportQuery) int {
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

func newPromQLBenchmarkCommand() *cobra.Command {
	p := &PromQL{}
	cmd := &cobra.Command{
		Use:     "bench",
		Aliases: []string{"benchmark"},
		Short:   "Run promql queries",
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
	f.StringVar(&p.Addr, "addr", "http://localhost:9090", "Prometheus address")
	f.StringVarP(&p.Input, "input", "i", "queries.jsonl", "Input file")
	f.StringVarP(&p.Output, "output", "o", "report.yml", "Output report file")
	f.DurationVar(&p.RequestTimeout, "request-timeout", time.Second*10, "Request timeout")
	f.StringVar(&p.StartTime, "start", "", "Start time override (RFC3339 or unix timestamp)")
	f.StringVar(&p.EndTime, "end", "", "End time override (RFC3339 or unix timestamp)")
	f.BoolVar(&p.AllowEmpty, "allow-empty", true, "Allow empty results")

	f.StringVar(&p.TrackerOptions.TempoAddr, "tempo-addr", "http://127.0.0.1:3200", "Tempo endpoint")
	f.BoolVar(&p.TrackerOptions.Trace, "trace", false, "Trace queries")

	f.IntVar(&p.Count, "count", 1, "Number of times to run each query (only for sequential)")
	f.IntVar(&p.Warmup, "warmup", 0, "Number of warmup runs (only for sequential)")

	f.IntVar(&p.Jobs, "jobs", 1, "Number of concurrent jobs (only for concurrent)")
	f.DurationVarP(&p.Duration, "duration", "d", time.Minute*5, "Duration of benchmark (only for concurrent)")
	f.BoolVarP(&p.Concurrent, "concurrent", "c", false, "Run queries concurrently")

	return cmd
}
