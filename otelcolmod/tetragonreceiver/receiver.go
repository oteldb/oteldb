package tetragonreceiver

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/tetragon/api/v1/tetragon"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// Receiver streams events from Tetragon.
type Receiver struct {
	cfg      *Config
	params   receiver.Settings
	consumer consumer.Logs
	logger   *zap.Logger

	mu        sync.Mutex
	host      component.Host
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	stopOnce  sync.Once
}

// NewReceiver creates a new Receiver.
func NewReceiver(params receiver.Settings, cfg *Config, lc consumer.Logs) (*Receiver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Receiver{cfg: cfg, params: params, consumer: lc, logger: params.Logger}, nil
}

var _ component.Component = (*Receiver)(nil)

// Start implements [component.Component].
func (r *Receiver) Start(_ context.Context, host component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startOnce.Do(func() {
		r.host = host
		var runCtx context.Context
		runCtx, r.cancel = context.WithCancel(context.Background())
		r.wg.Go(func() {
			if err := r.run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
			}
		})
	})
	return nil
}

func (r *Receiver) run(ctx context.Context) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	return backoff.RetryNotify(
		func() error { return r.stream(ctx) },
		backoff.WithContext(bo, ctx),
		func(err error, d time.Duration) {
			r.logger.Warn("Tetragon stream disconnected, reconnecting",
				zap.Error(err), zap.Duration("delay", d))
		},
	)
}

func (r *Receiver) stream(ctx context.Context) error {
	conn, err := r.cfg.ClientConfig.ToClientConn(ctx, r.host.GetExtensions(), r.params.TelemetrySettings)
	if err != nil {
		return backoff.Permanent(errors.Wrap(err, "dial"))
	}
	defer func() { _ = conn.Close() }()

	client := tetragon.NewFineGuidanceSensorsClient(conn)
	stream, err := client.GetEvents(ctx, &tetragon.GetEventsRequest{})
	if err != nil {
		return errors.Wrap(err, "GetEvents")
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return errors.Wrap(err, "recv")
		}
		if logs, ok := translateEvent(resp, r.cfg.ClusterID); ok {
			if err := r.consumer.ConsumeLogs(ctx, logs); err != nil {
				r.logger.Warn("ConsumeLogs failed", zap.Error(err))
			}
		} else {
			r.logger.Debug("Unknown Tetragon event type, skipping")
		}
	}
}

// Shutdown implements [component.Component].
func (r *Receiver) Shutdown(ctx context.Context) error {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		cancel := r.cancel
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		done := make(chan struct{})
		go func() { r.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
		}
	})
	return nil
}
