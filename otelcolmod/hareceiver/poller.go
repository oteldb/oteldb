package hareceiver

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

// firstCursorHeader is set by Home Assistant to the journal cursor of the first
// entry of the response.
const firstCursorHeader = "X-First-Cursor"

// maxErrorBody bounds how much of an error response is kept for the message.
const maxErrorBody = 1024

// poller ingests a single [Source].
type poller struct {
	src      Source
	cfg      *Config
	url      string
	client   *http.Client
	consumer consumer.Logs
	storage  storage.Client
	logger   *zap.Logger
	now      func() time.Time

	state  cursorState
	loaded bool
}

func newPoller(
	src Source,
	cfg *Config,
	client *http.Client,
	lc consumer.Logs,
	st storage.Client,
	logger *zap.Logger,
) (*poller, error) {
	u, err := url.JoinPath(cfg.Endpoint, src.Path())
	if err != nil {
		return nil, errors.Wrap(err, "build url")
	}
	// Home Assistant renders journal entries to text; "verbose" selects the
	// format that carries the timestamp, hostname and identifier, and
	// "no_colors" makes it strip ANSI escapes server-side.
	u += "?verbose&no_colors"
	return &poller{
		src:      src,
		cfg:      cfg,
		url:      u,
		client:   client,
		consumer: lc,
		storage:  st,
		logger:   logger.With(zap.String("source", src.Name())),
		now:      time.Now,
	}, nil
}

func (p *poller) run(ctx context.Context) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.MaxInterval = p.cfg.PollInterval * 10

	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if err := p.poll(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var permanent *backoff.PermanentError
			if errors.As(err, &permanent) {
				return permanent.Err
			}
			d := bo.NextBackOff()
			p.logger.Warn("Poll failed, retrying", zap.Error(err), zap.Duration("delay", d))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
			continue
		}
		bo.Reset()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// poll performs one cursor-advancing request.
//
// The cursor advances only after [consumer.Logs.ConsumeLogs] returns nil, which
// makes delivery at-least-once: a failure re-reads the same window rather than
// skipping it.
func (p *poller) poll(ctx context.Context) error {
	if !p.loaded {
		state, err := loadCursor(ctx, p.storage, p.src.StorageKey())
		if err != nil {
			return errors.Wrap(err, "load cursor")
		}
		p.state, p.loaded = state, true
	}
	if p.state.Anchor == "" {
		return p.anchorAtTail(ctx)
	}

	body, firstCursor, err := p.fetch(ctx, p.state.rangeHeader(p.cfg.BatchSize))
	if err != nil {
		return err
	}
	entries := ParseEntries(body)
	if len(entries) == 0 {
		return nil
	}

	logs := translateEntries(entries, p.src, p.cfg, p.now())
	if err := p.consumer.ConsumeLogs(ctx, logs); err != nil {
		return errors.Wrap(err, "consume logs")
	}

	state := p.state.advance(firstCursor, len(entries))
	if err := saveCursor(ctx, p.storage, p.src.StorageKey(), state); err != nil {
		return errors.Wrap(err, "save cursor")
	}
	p.state = state
	return nil
}

// anchorAtTail positions a source with no stored cursor at the most recent
// entry without emitting it.
func (p *poller) anchorAtTail(ctx context.Context) error {
	body, firstCursor, err := p.fetch(ctx, tailRangeHeader)
	if err != nil {
		return err
	}
	if firstCursor == "" {
		p.logger.Debug("Journal is empty, retrying")
		return nil
	}

	state := cursorState{Anchor: firstCursor, Skip: len(ParseEntries(body))}
	if err := saveCursor(ctx, p.storage, p.src.StorageKey(), state); err != nil {
		return errors.Wrap(err, "save cursor")
	}
	p.state = state
	return nil
}

func (p *poller) fetch(ctx context.Context, rangeHeader string) (body, firstCursor string, _ error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, http.NoBody)
	if err != nil {
		return "", "", errors.Wrap(err, "create request")
	}
	req.Header.Set("Authorization", "Bearer "+string(p.cfg.Token))
	req.Header.Set("Range", rangeHeader)

	resp, err := p.client.Do(req)
	if err != nil {
		return "", "", errors.Wrap(err, "do request")
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		err := errors.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(data)))
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			// The token is either invalid or does not belong to an admin user;
			// retrying will not fix either.
			return "", "", backoff.Permanent(err)
		}
		return "", "", err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", errors.Wrap(err, "read body")
	}
	return string(data), resp.Header.Get(firstCursorHeader), nil
}
