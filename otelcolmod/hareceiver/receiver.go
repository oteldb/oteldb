package hareceiver

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// Receiver polls journal logs from a Home Assistant instance.
type Receiver struct {
	cfg      *Config
	params   receiver.Settings
	consumer consumer.Logs
	logger   *zap.Logger

	mu        sync.Mutex
	storage   storage.Client
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
func (r *Receiver) Start(ctx context.Context, host component.Host) error {
	var err error
	r.startOnce.Do(func() {
		err = r.start(ctx, host)
	})
	return err
}

func (r *Receiver) start(ctx context.Context, host component.Host) error {
	client, err := r.cfg.ClientConfig.ToClient(ctx, host.GetExtensions(), r.params.TelemetrySettings)
	if err != nil {
		return errors.Wrap(err, "create client")
	}
	st, err := r.storageClient(ctx, host)
	if err != nil {
		return errors.Wrap(err, "create storage client")
	}

	pollers := make([]*poller, 0, len(r.cfg.Sources))
	for _, src := range r.cfg.Sources {
		p, err := newPoller(src, r.cfg, client, r.consumer, st, r.logger)
		if err != nil {
			return errors.Wrapf(err, "source %q", src.Name())
		}
		pollers = append(pollers, p)
	}

	r.mu.Lock()
	r.storage = st
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.mu.Unlock()

	for _, p := range pollers {
		r.wg.Go(func() {
			if err := p.run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
			}
		})
	}
	return nil
}

func (r *Receiver) storageClient(ctx context.Context, host component.Host) (storage.Client, error) {
	if r.cfg.StorageID == nil {
		r.logger.Warn("No storage extension configured, cursors are lost on restart")
		return storage.NewNopClient(), nil
	}
	ext, ok := host.GetExtensions()[*r.cfg.StorageID]
	if !ok {
		return nil, errors.Errorf("extension %q not found", r.cfg.StorageID)
	}
	se, ok := ext.(storage.Extension)
	if !ok {
		return nil, errors.Errorf("extension %q is not a storage extension", r.cfg.StorageID)
	}
	return se.GetClient(ctx, component.KindReceiver, r.params.ID, "")
}

// Shutdown implements [component.Component].
func (r *Receiver) Shutdown(ctx context.Context) error {
	var err error
	r.stopOnce.Do(func() {
		r.mu.Lock()
		cancel, st := r.cancel, r.storage
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
		if st != nil {
			err = st.Close(ctx)
		}
	})
	return err
}
