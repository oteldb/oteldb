package logqlbench

import (
	"context"
	"strconv"
	"time"

	"github.com/oteldb/oteldb/internal/lokiapi"
)

func (p *LogQLBenchmark) sendAndRecord(ctx context.Context, q Query) (rerr error) {
	return p.tracker.Track(ctx, q, p.send)
}

func toLokiTimestamp(t time.Time) (opt lokiapi.OptLokiTime) {
	if t.IsZero() {
		return opt
	}
	s := strconv.FormatInt(t.UnixNano(), 10)
	opt.SetTo(lokiapi.LokiTime(s))
	return opt
}

func toLokiDuration(t time.Duration) (opt lokiapi.OptPrometheusDuration) {
	opt.SetTo(lokiapi.PrometheusDuration(t.String()))
	return opt
}

func (p *LogQLBenchmark) send(ctx context.Context, q Query) error {
	ctx, cancel := context.WithTimeout(ctx, p.RequestTimeout)
	defer cancel()

	return q.Execute(ctx, p.client, p)
}
