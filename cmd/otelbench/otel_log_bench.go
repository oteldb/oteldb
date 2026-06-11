package main

import (
	"context"
	"fmt"
	"math/rand"
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

	clickhouseAddr string
	writtenLines   atomic.Int64
	writtenBytes   atomic.Int64
	storageInfo    atomic.Pointer[ClickhouseStats]
	stop           chan struct{}
}

type logsBenchTarget struct {
	plogotlp.GRPCClient
	target string
}

func (b *LogsBench) Run(ctx context.Context) error {
	b.stop = make(chan struct{})
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
	return g.Wait()
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

			batch, lines, bytes := b.generateBatch(r, now)
			b.send(ctx, batch)
			totalLines := b.writtenLines.Add(lines)
			b.writtenBytes.Add(bytes)
			if b.limit > 0 && totalLines >= b.limit {
				close(b.stop)
				return nil
			}
		}
	}
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
