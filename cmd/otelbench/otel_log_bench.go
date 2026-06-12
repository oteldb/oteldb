package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oteldb/oteldb/internal/lokicompliance"
	"github.com/oteldb/oteldb/internal/lokihandler"
)

type LogsBench struct {
	seed int64

	resourceCount   int
	entriesPerBatch int
	rate            time.Duration
	limit           int64
	targets         []logsBenchTarget
	start           lokiTimeVar
	sourceDir       string
	repeat          int
	reportPath      string

	clickhouseAddr string
	writtenLines   atomic.Int64
	writtenBytes   atomic.Int64
	storageInfo    atomic.Pointer[ClickhouseStats]
	stop           chan struct{}
	windowStart    time.Time
	windowEnd      time.Time
	services       []string
}

type logsBenchTarget struct {
	plogotlp.GRPCClient
	target string
}

func (b *LogsBench) Run(ctx context.Context) error {
	b.stop = make(chan struct{})
	started := time.Now()
	g, ctx := errgroup.WithContext(ctx)

	if b.clickhouseAddr != "" {
		g.Go(func() error {
			return b.RunClickhouseReporter(ctx)
		})
	}
	g.Go(func() error {
		return b.RunReporter(ctx)
	})
	g.Go(func() error {
		return b.run(ctx)
	})
	if err := g.Wait(); err != nil {
		return err
	}
	if b.reportPath != "" {
		if err := b.writeReport(ctx, started, time.Now()); err != nil {
			return errors.Wrap(err, "write report")
		}
	}
	return nil
}

func (b *LogsBench) RunClickhouseReporter(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.stop:
			return nil
		case <-ticker.C:
			info, err := fetchClickhouseStats(ctx, b.clickhouseAddr, "logs")
			if err != nil {
				zctx.From(ctx).Error("cannot fetch clickhouse stats", zap.Error(err))
			}
			b.storageInfo.Store(&info)
		}
	}
}

func (b *LogsBench) RunReporter(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	var (
		old                = time.Now()
		oldLines, oldBytes int64
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.stop:
			return nil
		case now := <-ticker.C:
			var (
				lines = b.writtenLines.Load()
				bytes = b.writtenBytes.Load()

				deltaSeconds = now.Sub(old).Seconds()
				deltaLines   = float64(lines - oldLines)
				deltaBytes   = float64(bytes - oldBytes)

				sb strings.Builder
			)

			fmt.Fprintf(&sb, "lines=%v/s bytes=%v/s",
				fmtInt(int(deltaLines/deltaSeconds)),
				compactBytes(int(deltaBytes/deltaSeconds)),
			)
			if v := b.storageInfo.Load(); v != nil && b.clickhouseAddr != "" {
				v.WriteInfo(&sb, now)
			}
			fmt.Println(sb.String())

			old, oldLines, oldBytes = now, lines, bytes
		}
	}
}

var errLogsLimit = errors.New("limit reached")

func (b *LogsBench) run(ctx context.Context) error {
	r := rand.New(rand.NewSource(b.seed)) // #nosec: G404
	source, err := b.newSource()
	if err != nil {
		return err
	}
	defer func() {
		if closer, ok := source.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()

	ticker := time.NewTicker(b.rate)
	defer ticker.Stop()

	now := b.start.Value
	if now.IsZero() {
		now = time.Now()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.stop:
			return nil
		case <-ticker.C:
			now = now.Add(b.rate)

			batch, lines, bytes, exhausted, err := source.Next(r, now)
			if err != nil {
				return err
			}
			if lines > 0 {
				b.observeWindow(batch)
				b.send(ctx, batch)
				b.writtenLines.Add(lines)
				b.writtenBytes.Add(bytes)
			}
			if exhausted || (b.limit > 0 && b.writtenLines.Load() >= b.limit) {
				close(b.stop)
				return nil
			}
		}
	}
}

func (b *LogsBench) observeWindow(logs plog.Logs) {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resource := logs.ResourceLogs().At(i)
		for j := 0; j < resource.ScopeLogs().Len(); j++ {
			scope := resource.ScopeLogs().At(j)
			for k := 0; k < scope.LogRecords().Len(); k++ {
				ts := scope.LogRecords().At(k).Timestamp().AsTime()
				if ts.IsZero() {
					continue
				}
				if b.windowStart.IsZero() || ts.Before(b.windowStart) {
					b.windowStart = ts
				}
				if b.windowEnd.IsZero() || ts.After(b.windowEnd) {
					b.windowEnd = ts
				}
			}
		}
	}
}

type logsBatchSource interface {
	Next(r *rand.Rand, ts time.Time) (plog.Logs, int64, int64, bool, error)
}

func (b *LogsBench) newSource() (logsBatchSource, error) {
	if b.sourceDir == "" {
		return randomLogsSource{bench: b}, nil
	}
	source, err := newFileLogsSource(b.sourceDir, b.entriesPerBatch, b.repeat, b.limit)
	if err != nil {
		return nil, err
	}
	b.services = source.Services()
	return source, nil
}

type randomLogsSource struct {
	bench *LogsBench
}

func (s randomLogsSource) Next(r *rand.Rand, ts time.Time) (plog.Logs, int64, int64, bool, error) {
	logs, lines, bytes := s.bench.generateBatch(r, ts)
	return logs, lines, bytes, false, nil
}

func (b *LogsBench) send(ctx context.Context, logs plog.Logs) {
	var wg sync.WaitGroup
	wg.Add(len(b.targets))
	for _, conn := range b.targets {
		go func() {
			defer wg.Done()

			req := plogotlp.NewExportRequestFromLogs(logs)
			_, err := conn.Export(ctx, req)
			if err != nil {
				zctx.From(ctx).Warn("Send failed", zap.String("target", conn.target), zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

func (b *LogsBench) generateBatch(r *rand.Rand, now time.Time) (logs plog.Logs, lines, bytes int64) {
	logs = plog.NewLogs()
	resLogs := logs.ResourceLogs()
	for i := 0; i < b.resourceCount; i++ {
		resLog := resLogs.AppendEmpty()
		resLog.Resource().Attributes().PutInt("otelbench.resource", int64(i))
		resLog.ScopeLogs().AppendEmpty()
	}

	rt := now
	for i := 0; i < b.entriesPerBatch; i++ {
		if b.limit > 0 && b.writtenLines.Load()+1 >= b.limit {
			break
		}
		rt = rt.Add(100 * time.Microsecond)
		entry := lokicompliance.NewLogEntry(r, rt)

		resource := resLogs.At(r.Intn(resLogs.Len()))
		scope := resource.ScopeLogs().At(0)
		record := scope.LogRecords().AppendEmpty()
		entry.OTEL(record)

		lines++
		bytes += int64(len(record.Body().AsString()))
	}
	return logs, lines, bytes
}

type logsBenchReport struct {
	StartedAt  time.Time              `json:"started_at"`
	FinishedAt time.Time              `json:"finished_at"`
	Duration   string                 `json:"duration"`
	Throughput logsBenchThroughput    `json:"throughput"`
	Window     logsBenchWindow        `json:"window"`
	Dataset    logsBenchDatasetConfig `json:"dataset"`
	Storage    *ClickhouseStats       `json:"storage,omitempty"`
}

type logsBenchThroughput struct {
	Lines          int64   `json:"lines"`
	Bytes          int64   `json:"bytes"`
	LinesPerSecond float64 `json:"lines_per_second"`
	BytesPerSecond float64 `json:"bytes_per_second"`
}

type logsBenchWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type logsBenchDatasetConfig struct {
	SourceDir string   `json:"source_dir,omitempty"`
	Repeat    int      `json:"repeat"`
	Services  []string `json:"services,omitempty"`
}

func (b *LogsBench) writeReport(ctx context.Context, started, finished time.Time) error {
	duration := finished.Sub(started)
	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	var storage *ClickhouseStats
	if b.clickhouseAddr != "" {
		info, err := fetchClickhouseStats(ctx, b.clickhouseAddr, "logs")
		if err == nil {
			storage = &info
		} else if v := b.storageInfo.Load(); v != nil {
			storage = v
		}
	} else if v := b.storageInfo.Load(); v != nil {
		storage = v
	}
	report := logsBenchReport{
		StartedAt:  started,
		FinishedAt: finished,
		Duration:   duration.String(),
		Throughput: logsBenchThroughput{
			Lines:          b.writtenLines.Load(),
			Bytes:          b.writtenBytes.Load(),
			LinesPerSecond: float64(b.writtenLines.Load()) / seconds,
			BytesPerSecond: float64(b.writtenBytes.Load()) / seconds,
		},
		Window: logsBenchWindow{
			Start: b.windowStart,
			End:   b.windowEnd,
		},
		Dataset: logsBenchDatasetConfig{
			SourceDir: b.sourceDir,
			Repeat:    b.repeat,
			Services:  b.services,
		},
		Storage: storage,
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal")
	}
	if err := os.WriteFile(b.reportPath, append(data, '\n'), 0o644); err != nil {
		return errors.Wrap(err, "write file")
	}
	return nil
}

func (b *LogsBench) prepareTargets(ctx context.Context, args []string) error {
	for _, arg := range args {
		client, err := b.prepareTarget(ctx, arg)
		if err != nil {
			return errors.Wrapf(err, "prepare %q", arg)
		}
		b.targets = append(b.targets, logsBenchTarget{
			GRPCClient: client,
			target:     arg,
		})
	}
	if len(b.targets) == 0 {
		return errors.New("no targets")
	}
	return nil
}

func (b *LogsBench) prepareTarget(ctx context.Context, target string) (plogotlp.GRPCClient, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "new client")
	}
	client := plogotlp.NewGRPCClient(conn)

	var (
		log = zctx.From(ctx).With(zap.String("target", target))
		eb  = backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(5*time.Second),
			backoff.WithMaxElapsedTime(time.Minute),
		)
	)
	log.Info("Waiting for receiver")
	if err := backoff.RetryNotify(
		func() error {
			_, err := client.Export(ctx, plogotlp.NewExportRequest())
			if err != nil {
				if cerr := ctx.Err(); cerr != nil {
					return backoff.Permanent(cerr)
				}
				return err
			}
			return nil
		},
		eb,
		func(err error, _ time.Duration) {
			log.Debug("Retry ping request",
				zap.Error(err),
			)
		},
	); err != nil {
		return nil, err
	}
	log.Info("Receiver is ready")

	return client, nil
}

func newOtelLogsBenchCommand() *cobra.Command {
	var b LogsBench
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Start OpenTelemetry logs benchmark",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := zap.NewDevelopment()
			if err != nil {
				return errors.Wrap(err, "create logger")
			}
			defer func() {
				_ = log.Sync()
			}()
			ctx = zctx.Base(ctx, log)

			if err := b.prepareTargets(ctx, args); err != nil {
				return err
			}

			err = b.Run(ctx)
			if errors.Is(err, errLogsLimit) {
				err = nil
			}
			return err
		},
	}
	f := cmd.Flags()
	f.Int64Var(&b.seed, "seed", time.Now().UnixNano(), "Seed of random generator")
	f.IntVar(&b.resourceCount, "resources", 3, "The number of resources")
	f.IntVar(&b.entriesPerBatch, "entries", 5, "The number of entries per batch")
	f.Int64Var(&b.limit, "total", 0, "The total number of generated entries (0 to disable limit)")
	f.DurationVar(&b.rate, "rate", time.Second, "Rate of log emitter")
	f.StringVar(&b.sourceDir, "source", "", "Directory with .log files to replay instead of random logs")
	f.IntVar(&b.repeat, "repeat", 0, "Number of extra dataset replay loops for --source (0 replays once)")
	f.StringVar(&b.reportPath, "report", "", "Write ingest summary JSON report to path")
	f.StringVar(&b.clickhouseAddr, "clickhouseAddr", "", "clickhouse tcp protocol addr to get actual stats from")
	f.Var(&b.start, "start", "Set starting point for log timestamps")
	return cmd
}

type lokiTimeVar struct {
	Value time.Time
}

var _ pflag.Value = (*lokiTimeVar)(nil)

func (v *lokiTimeVar) String() string {
	if v.Value.IsZero() {
		return "<zero>"
	}
	return v.Value.String()
}

func (v *lokiTimeVar) Set(s string) error {
	if s == "" {
		return errors.New("empty value")
	}
	ts, err := lokihandler.ParseTimestamp(s, time.Time{})
	if err != nil {
		return err
	}
	v.Value = ts
	return nil
}

func (v *lokiTimeVar) Type() string {
	return "string"
}
