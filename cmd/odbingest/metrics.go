package main

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/oteldb/oteldb/internal/otlpdirect"
	"github.com/oteldb/oteldb/internal/promrw"
)

// reasonKey labels a rejected record with why the shard primary refused it, and signalKey with
// which signal it belonged to.
const (
	reasonKey = attribute.Key("reason")
	signalKey = attribute.Key("signal")
)

// observer records what the remote write endpoint ingested. Its counters are the ones an operator
// alerts on: a sender that stopped, and the points refused along with why.
type observer struct {
	requests  metric.Int64Counter
	series    metric.Int64Counter
	points    metric.Int64Counter
	rejected  metric.Int64Counter
	byteCount metric.Int64Counter

	otlpRequests metric.Int64Counter
	otlpItems    metric.Int64Counter
	otlpRejected metric.Int64Counter
	otlpBytes    metric.Int64Counter
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
	if o.rejected, err = meter.Int64Counter("odbingest.remote_write.rejected_points",
		metric.WithDescription("Points the conversion did not ingest, by reason."),
	); err != nil {
		return nil, errors.Wrap(err, "create rejected counter")
	}
	if o.byteCount, err = meter.Int64Counter("odbingest.remote_write.decoded_bytes",
		metric.WithDescription("Decompressed request bytes."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, errors.Wrap(err, "create bytes counter")
	}
	if o.otlpRequests, err = meter.Int64Counter("odbingest.otlp.requests",
		metric.WithDescription("Accepted OTLP export requests, by signal."),
	); err != nil {
		return nil, errors.Wrap(err, "create otlp requests counter")
	}
	if o.otlpItems, err = meter.Int64Counter("odbingest.otlp.items",
		metric.WithDescription("Records, spans, points or samples received over OTLP, by signal."),
	); err != nil {
		return nil, errors.Wrap(err, "create otlp items counter")
	}
	if o.otlpRejected, err = meter.Int64Counter("odbingest.otlp.rejected_items",
		metric.WithDescription("Items an OTLP request carried that could not be represented, by signal."),
	); err != nil {
		return nil, errors.Wrap(err, "create otlp rejected counter")
	}
	if o.otlpBytes, err = meter.Int64Counter("odbingest.otlp.decoded_bytes",
		metric.WithDescription("Decompressed OTLP request bytes, by signal."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, errors.Wrap(err, "create otlp bytes counter")
	}

	return &o, nil
}

func (o *observer) observeOTLP(s otlpdirect.Stats) {
	ctx := context.Background()
	attr := metric.WithAttributes(signalKey.String(s.Signal.String()))

	o.otlpRequests.Add(ctx, 1, attr)
	o.otlpItems.Add(ctx, int64(s.Items), attr)
	o.otlpBytes.Add(ctx, int64(s.Bytes), attr)

	if s.Rejected > 0 {
		o.otlpRejected.Add(ctx, int64(s.Rejected), attr)
	}
}

func (o *observer) observe(s promrw.Stats) {
	ctx := context.Background()
	o.requests.Add(ctx, 1)
	o.series.Add(ctx, int64(s.Series))
	o.points.Add(ctx, int64(s.Points))
	o.byteCount.Add(ctx, int64(s.Bytes))
	for _, r := range []struct {
		reason string
		count  int
	}{
		{"too_old", s.Rejected.Old},
		{"invalid_labels", s.Rejected.Invalid},
		{"unsupported_histogram", s.Rejected.Unsupported},
	} {
		if r.count > 0 {
			o.rejected.Add(ctx, int64(r.count), metric.WithAttributes(reasonKey.String(r.reason)))
		}
	}
}
