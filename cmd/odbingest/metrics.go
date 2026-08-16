package main

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/oteldb/oteldb/internal/promrw"
)

// reasonKey labels a rejected point with why the shard primary refused it.
const reasonKey = attribute.Key("reason")

// observer records what the remote write endpoint ingested. Its counters are the ones an operator
// alerts on: a sender that stopped, and points refused as too old.
type observer struct {
	requests  metric.Int64Counter
	series    metric.Int64Counter
	points    metric.Int64Counter
	dropped   metric.Int64Counter
	byteCount metric.Int64Counter
}

func newObserver(mp metric.MeterProvider) (*observer, error) {
	meter := mp.Meter("github.com/oteldb/oteldb/cmd/odbingest")

	var (
		o   observer
		err error
	)
	if o.requests, err = meter.Int64Counter("odbingest.remote_write.requests",
		metric.WithDescription("Accepted remote write requests."),
	); err != nil {
		return nil, errors.Wrap(err, "create requests counter")
	}
	if o.series, err = meter.Int64Counter("odbingest.remote_write.series",
		metric.WithDescription("Timeseries received."),
	); err != nil {
		return nil, errors.Wrap(err, "create series counter")
	}
	if o.points, err = meter.Int64Counter("odbingest.remote_write.points",
		metric.WithDescription("Points written to the storage engine."),
	); err != nil {
		return nil, errors.Wrap(err, "create points counter")
	}
	if o.dropped, err = meter.Int64Counter("odbingest.remote_write.dropped_points",
		metric.WithDescription("Points dropped as older than the time threshold."),
	); err != nil {
		return nil, errors.Wrap(err, "create dropped counter")
	}
	if o.byteCount, err = meter.Int64Counter("odbingest.remote_write.decoded_bytes",
		metric.WithDescription("Decompressed request bytes."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, errors.Wrap(err, "create bytes counter")
	}
	return &o, nil
}

func (o *observer) observe(s promrw.Stats) {
	ctx := context.Background()
	o.requests.Add(ctx, 1)
	o.series.Add(ctx, int64(s.Series))
	o.points.Add(ctx, int64(s.Points))
	o.byteCount.Add(ctx, int64(s.Bytes))
	if s.Dropped > 0 {
		o.dropped.Add(ctx, int64(s.Dropped))
	}
}
