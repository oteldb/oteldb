package chreceiver

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chotel"
)

// Receiver reads ClickHouse internal OpenTelemetry spans.
type Receiver struct {
	cfg      *Config
	reader   *chotel.Reader
	consumer consumer.Traces
	logger   *zap.Logger

	mu        sync.Mutex
	client    *ch.Client
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	stopOnce  sync.Once
}

// NewReceiver creates a new Receiver.
func NewReceiver(params receiver.Settings, cfg *Config, tconsumer consumer.Traces) (*Receiver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Receiver{
		cfg:      cfg,
		consumer: tconsumer,
		logger:   params.Logger,
	}, nil
}

var _ component.Component = (*Receiver)(nil)

// Start implements [component.Component].
func (r *Receiver) Start(ctx context.Context, host component.Host) (err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.startOnce.Do(func() {
		var client *ch.Client
		client, err = dial(ctx, r.cfg.DSN)
		if err != nil {
			return
		}
		r.client = client
		r.reader = chotel.NewReader(client)
		if err = r.reader.Setup(ctx); err != nil {
			_ = client.Close()
			r.client = nil
			return
		}

		var runCtx context.Context
		runCtx, r.cancel = context.WithCancel(context.Background())
		r.wg.Go(func() {
			if runErr := r.run(runCtx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(runErr))
			}
		})
	})
	return err
}

func (r *Receiver) run(ctx context.Context) error {
	if err := r.poll(ctx, time.Now()); err != nil {
		r.logger.Warn("ClickHouse poll failed", zap.Error(err))
	}
	ticker := time.NewTicker(r.cfg.PollRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if err := r.poll(ctx, now); err != nil {
				r.logger.Warn("ClickHouse poll failed", zap.Error(err))
			}
		}
	}
}

func (r *Receiver) poll(ctx context.Context, now time.Time) error {
	spans, err := r.reader.Read(ctx, now)
	if err != nil {
		return err
	}
	filtered := chotel.Filter(spans, r.cfg.Filter)
	if len(filtered) > 0 {
		traces := chotel.ToTraces(filtered)
		if err := r.consumer.ConsumeTraces(ctx, traces); err != nil {
			return errors.Wrap(err, "consume traces")
		}
	}
	if err := r.reader.MarkExported(ctx, spans, now); err != nil {
		return errors.Wrap(err, "mark exported")
	}
	r.logger.Debug("Consumed ClickHouse spans", zap.Int("count", len(filtered)))
	return nil
}

// Shutdown implements [component.Component].
func (r *Receiver) Shutdown(context.Context) (err error) {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		cancel := r.cancel
		client := r.client
		r.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		r.wg.Wait()
		if client != nil {
			err = client.Close()
		}
	})
	return err
}

func dial(ctx context.Context, dsn string) (*ch.Client, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, errors.Wrap(err, "parse dsn")
	}
	pass, _ := u.User.Password()
	client, err := ch.Dial(ctx, ch.Options{
		Address:     u.Host,
		Compression: ch.CompressionZSTD,
		User:        u.User.Username(),
		Password:    pass,
		Database:    strings.TrimPrefix(u.Path, "/"),

		OpenTelemetryInstrumentation: false,
	})
	if err != nil {
		return nil, errors.Wrap(err, "clickhouse")
	}
	if err := client.Ping(ctx); err != nil {
		_ = client.Close()
		return nil, errors.Wrap(err, "clickhouse ping")
	}
	return client, nil
}
