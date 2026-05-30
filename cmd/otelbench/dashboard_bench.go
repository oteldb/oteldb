package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/promapi"
	"github.com/oteldb/oteldb/internal/tempoapi"
)

type DashboardBenchmark struct {
	Input  string
	Output string

	PromAddr  string
	LokiAddr  string
	TempoAddr string

	Count  int
	Warmup int

	StartTime string
	EndTime   string
	Step      string

	Interval     string
	RateInterval string
	Range        string

	Variables []string

	promClient  *promapi.Client
	lokiClient  *lokiapi.Client
	tempoClient *tempoapi.Client

	start time.Time
	end   time.Time

	vars map[string]string
}

func (b *DashboardBenchmark) Run(ctx context.Context) error {
	if err := b.setup(ctx); err != nil {
		return errors.Wrap(err, "setup")
	}

	dashboard, err := b.loadDashboard()
	if err != nil {
		return errors.Wrap(err, "load dashboard")
	}

	queries := b.extractQueries(dashboard)
	if len(queries) == 0 {
		return errors.New("no queries found in dashboard")
	}

	fmt.Printf("found %d queries in %d panels\n", len(queries), len(dashboard.Panels))

	total := len(queries) * (b.Count + b.Warmup)
	pb := progressbar.Default(int64(total))

	var results []queryResult
	for _, q := range queries {
		res := queryResult{
			Query: q,
		}

		// Warmup
		for i := range b.Warmup {
			start := time.Now()
			isEmpty, n, err := b.execute(ctx, q)
			if err != nil {
				return errors.Wrapf(err, "warmup query %q", q.Expr)
			}
			res.Empty = isEmpty
			res.N = n
			dur := time.Since(start)
			if i == 0 {
				res.First = dur
			}
			_ = pb.Add(1)
		}

		// Benchmark
		var durations []time.Duration
		for i := range b.Count {
			start := time.Now()
			isEmpty, n, err := b.execute(ctx, q)
			if err != nil {
				return errors.Wrapf(err, "execute query %q", q.Expr)
			}
			res.Empty = isEmpty
			res.N = n
			dur := time.Since(start)
			if i == 0 && b.Warmup == 0 {
				res.First = dur
			}
			durations = append(durations, dur)
			_ = pb.Add(1)
		}

		if len(durations) > 0 {
			var totalDur time.Duration
			minDur := durations[0]
			maxDur := durations[0]
			for _, d := range durations {
				totalDur += d
				if d < minDur {
					minDur = d
				}
				if d > maxDur {
					maxDur = d
				}
			}
			res.Avg = totalDur / time.Duration(len(durations))
			res.Min = minDur
			res.Max = maxDur

			slices.Sort(durations)
			res.P99 = durations[len(durations)*99/100]
		}

		results = append(results, res)
	}
	_ = pb.Finish()

	return b.report(results)
}

func (b *DashboardBenchmark) setup(_ context.Context) (err error) {
	if b.promClient, err = promapi.NewClient(b.PromAddr); err != nil {
		return errors.Wrap(err, "setup prometheus client")
	}
	if b.lokiClient, err = lokiapi.NewClient(b.LokiAddr); err != nil {
		return errors.Wrap(err, "setup loki client")
	}
	if b.tempoClient, err = tempoapi.NewClient(b.TempoAddr); err != nil {
		return errors.Wrap(err, "setup tempo client")
	}

	if b.start, err = parseTime(b.StartTime); err != nil {
		return errors.Wrap(err, "parse start time")
	}
	if b.end, err = parseTime(b.EndTime); err != nil {
		return errors.Wrap(err, "parse end time")
	}

	b.vars = map[string]string{
		"__interval":      b.Interval,
		"__rate_interval": b.RateInterval,
		"__range":         b.Range,
	}
	for _, v := range b.Variables {
		key, val, ok := strings.Cut(v, "=")
		if !ok {
			return errors.Errorf("invalid variable %q", v)
		}
		b.vars[key] = val
	}

	return nil
}

type queryType string

const (
	promQL  queryType = "promql"
	logQL   queryType = "logql"
	traceQL queryType = "traceql"
)

type extractedQuery struct {
	Panel string
	Expr  string
	Type  queryType
}

type queryResult struct {
	Query extractedQuery
	Empty bool
	N     int
	First time.Duration
	Avg   time.Duration
	Min   time.Duration
	Max   time.Duration
	P99   time.Duration
}

func (b *DashboardBenchmark) loadDashboard() (*grafanaDashboard, error) {
	f, err := os.Open(b.Input)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()

	var d grafanaDashboard
	if err := json.NewDecoder(f).Decode(&d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (b *DashboardBenchmark) extractQueries(d *grafanaDashboard) []extractedQuery {
	var queries []extractedQuery
	for _, p := range d.Panels {
		for _, t := range p.Targets {
			typ := b.identifyType(t)
			if typ == "" {
				continue
			}
			expr := t.Expr
			if expr == "" {
				expr = t.Query
			}
			if expr == "" {
				continue
			}

			expr = b.interpolate(expr)
			queries = append(queries, extractedQuery{
				Panel: p.Title,
				Expr:  expr,
				Type:  typ,
			})
		}
	}
	return queries
}

func (b *DashboardBenchmark) identifyType(t grafanaTarget) queryType {
	dsType := strings.ToLower(t.Datasource.Type)
	if dsType == "" {
		dsType = strings.ToLower(fmt.Sprint(t.Datasource))
	}

	switch {
	case strings.Contains(dsType, "prometheus"):
		return promQL
	case strings.Contains(dsType, "loki"):
		return logQL
	case strings.Contains(dsType, "tempo"):
		return traceQL
	default:
		return ""
	}
}

func (b *DashboardBenchmark) interpolate(s string) string {
	return os.Expand(s, func(s string) string {
		val, ok := b.vars[s]
		if !ok {
			fmt.Printf("warning: query %q contains unreplaced variables\n", s)
		}
		return val
	})
}

func (b *DashboardBenchmark) execute(ctx context.Context, q extractedQuery) (isEmpty bool, n int, err error) {
	switch q.Type {
	case promQL:
		return b.executePromQL(ctx, q)
	case logQL:
		return b.executeLogQL(ctx, q)
	case traceQL:
		return b.executeTraceQL(ctx, q)
	default:
		return false, 0, errors.Errorf("unknown query type %q", q.Type)
	}
}

func (b *DashboardBenchmark) executePromQL(ctx context.Context, q extractedQuery) (isEmpty bool, n int, err error) {
	if b.start.IsZero() || b.end.IsZero() {
		resp, err := b.promClient.GetQuery(ctx, promapi.GetQueryParams{
			Query: q.Expr,
		})
		if err != nil {
			return false, 0, err
		}
		n := 0
		switch resp.Data.Type {
		case promapi.MatrixData:
			n = len(resp.Data.Matrix.Result)
		case promapi.VectorData:
			n = len(resp.Data.Vector.Result)
		}
		return n == 0, n, nil
	}
	resp, err := b.promClient.GetQueryRange(ctx, promapi.GetQueryRangeParams{
		Query: q.Expr,
		Start: toPrometheusTimestamp(b.start),
		End:   toPrometheusTimestamp(b.end),
		Step:  promapi.NewOptString(b.Step),
	})
	if err != nil {
		return false, 0, err
	}
	n = 0
	switch resp.Data.Type {
	case promapi.MatrixData:
		n = len(resp.Data.Matrix.Result)
	case promapi.VectorData:
		n = len(resp.Data.Vector.Result)
	}
	return n == 0, n, nil
}

func (b *DashboardBenchmark) executeLogQL(ctx context.Context, q extractedQuery) (isEmpty bool, n int, err error) {
	if b.start.IsZero() || b.end.IsZero() {
		resp, err := b.lokiClient.Query(ctx, lokiapi.QueryParams{
			Query: q.Expr,
		})
		if err != nil {
			return false, 0, err
		}
		n := 0
		switch resp.Data.Type {
		case lokiapi.StreamsResultQueryResponseData:
			n = len(resp.Data.StreamsResult.Result)
		case lokiapi.MatrixResultQueryResponseData:
			n = len(resp.Data.MatrixResult.Result)
		case lokiapi.VectorResultQueryResponseData:
			n = len(resp.Data.VectorResult.Result)
		}
		return n == 0, n, nil
	}
	resp, err := b.lokiClient.QueryRange(ctx, lokiapi.QueryRangeParams{
		Query: q.Expr,
		Start: lokiapi.NewOptLokiTime(lokiapi.LokiTime(b.start.Format(time.RFC3339Nano))),
		End:   lokiapi.NewOptLokiTime(lokiapi.LokiTime(b.end.Format(time.RFC3339Nano))),
		Step:  lokiapi.NewOptPrometheusDuration(lokiapi.PrometheusDuration(b.Step)),
	})
	if err != nil {
		return false, 0, err
	}
	n = 0
	switch resp.Data.Type {
	case lokiapi.StreamsResultQueryResponseData:
		n = len(resp.Data.StreamsResult.Result)
	case lokiapi.MatrixResultQueryResponseData:
		n = len(resp.Data.MatrixResult.Result)
	case lokiapi.VectorResultQueryResponseData:
		n = len(resp.Data.VectorResult.Result)
	}
	return n == 0, n, nil
}

func (b *DashboardBenchmark) executeTraceQL(ctx context.Context, q extractedQuery) (isEmpty bool, n int, err error) {
	resp, err := b.tempoClient.Search(ctx, tempoapi.SearchParams{
		Q:     tempoapi.NewOptString(q.Expr),
		Start: tempoapi.NewOptTempoTime(tempoapi.TempoTime(b.start.Format(time.RFC3339Nano))),
		End:   tempoapi.NewOptTempoTime(tempoapi.TempoTime(b.end.Format(time.RFC3339Nano))),
	})
	if err != nil {
		return false, 0, err
	}
	n = len(resp.Traces)
	return n == 0, n, nil
}

type DashboardReportEntry struct {
	Panel string        `yaml:"panel"`
	Query string        `yaml:"query"`
	Type  string        `yaml:"type"`
	Empty bool          `yaml:"empty"`
	N     int           `yaml:"n"`
	First time.Duration `yaml:"first"`
	Avg   time.Duration `yaml:"avg"`
	Min   time.Duration `yaml:"min"`
	Max   time.Duration `yaml:"max"`
	P99   time.Duration `yaml:"p99"`
}

func (b *DashboardBenchmark) report(results []queryResult) error {
	fmt.Println("\nBenchmark results:")
	slices.SortFunc(results, func(a, b queryResult) int {
		return cmp.Compare(b.Avg, a.Avg)
	})

	for i, res := range results {
		if i >= 10 {
			break
		}
		empty := ""
		if res.Empty {
			empty = " [EMPTY]"
		} else {
			empty = fmt.Sprintf(" [N=%d]", res.N)
		}
		fmt.Printf("%d. %s: %s (avg: %v, p99: %v, first: %v)%s\n", i+1, res.Query.Panel, res.Query.Expr, res.Avg, res.P99, res.First, empty)
	}

	var report []DashboardReportEntry
	for _, res := range results {
		report = append(report, DashboardReportEntry{
			Panel: res.Query.Panel,
			Query: res.Query.Expr,
			Type:  string(res.Query.Type),
			Empty: res.Empty,
			N:     res.N,
			First: res.First,
			Avg:   res.Avg,
			Min:   res.Min,
			Max:   res.Max,
			P99:   res.P99,
		})
	}

	out, err := yaml.Marshal(report)
	if err != nil {
		return err
	}

	outputPath := filepath.Clean(b.Output)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(b.Output, out, 0o600)
}

type grafanaDashboard struct {
	Panels []grafanaPanel `json:"panels"`
}

type grafanaPanel struct {
	Title   string          `json:"title"`
	Targets []grafanaTarget `json:"targets"`
}

type grafanaTarget struct {
	Datasource grafanaDatasource `json:"datasource"`
	Expr       string            `json:"expr"`
	Query      string            `json:"query"`
	RefID      string            `json:"refId"`
}

type grafanaDatasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

func (d *grafanaDatasource) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		d.Type = s
		return nil
	}
	type alias grafanaDatasource
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*d = grafanaDatasource(a)
	return nil
}

func newDashboardBenchmarkCommand() *cobra.Command {
	b := &DashboardBenchmark{}
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Run dashboard benchmarks",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return b.Run(cmd.Context())
		},
	}

	f := cmd.Flags()
	f.StringVarP(&b.Input, "input", "i", "dashboard.json", "Input dashboard JSON file")
	f.StringVarP(&b.Output, "output", "o", "report.yml", "Output report file")
	f.StringVar(&b.PromAddr, "prom-addr", "http://localhost:9090", "Prometheus address")
	f.StringVar(&b.LokiAddr, "loki-addr", "http://localhost:3100", "Loki address")
	f.StringVar(&b.TempoAddr, "tempo-addr", "http://localhost:3200", "Tempo address")
	f.IntVar(&b.Count, "count", 1, "Number of times to run each query")
	f.IntVar(&b.Warmup, "warmup", 0, "Number of warmup runs")
	f.StringVar(&b.StartTime, "start", "", "Start time override")
	f.StringVar(&b.EndTime, "end", "", "End time override")
	f.StringVar(&b.Step, "step", "15s", "Step override")
	f.StringVar(&b.Interval, "interval", "15s", "Default $__interval variable")
	f.StringVar(&b.RateInterval, "rate-interval", "5m", "Default $__rate_interval variable")
	f.StringVar(&b.Range, "range", "1h", "Default $__range variable")
	f.StringSliceVar(&b.Variables, "template-var", nil, "Template variables (key=value)")

	return cmd
}
