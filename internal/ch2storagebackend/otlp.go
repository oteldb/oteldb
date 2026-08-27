package ch2storagebackend

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// otlpDest exports over OTLP gRPC instead of writing an engine directly.
//
// Pointed at odbingest, this is the only way to load a cluster: data enters through the same
// routing and replication as live traffic, rather than landing in one node's backend where the
// ring never assigned it.
type otlpDest struct {
	conn    *grpc.ClientConn
	logs    plogotlp.GRPCClient
	traces  ptraceotlp.GRPCClient
	metrics pmetricotlp.GRPCClient
}

// dialOTLP connects to an OTLP gRPC endpoint (host:port).
func dialOTLP(endpoint string, maxSendBytes int) (*otlpDest, error) {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxSendBytes)),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "dial %q", endpoint)
	}

	return &otlpDest{
		conn:    conn,
		logs:    plogotlp.NewGRPCClient(conn),
		traces:  ptraceotlp.NewGRPCClient(conn),
		metrics: pmetricotlp.NewGRPCClient(conn),
	}, nil
}

func (d *otlpDest) WriteLogs(ctx context.Context, records []logstorage.Record) error {
	req := plogotlp.NewExportRequestFromLogs(logstorage.RecordsToLogs(records))
	resp, err := d.logs.Export(ctx, req)
	if err != nil {
		return errors.Wrap(err, "export logs")
	}
	if n := resp.PartialSuccess().RejectedLogRecords(); n > 0 {
		return errors.Errorf("rejected %d log records: %s", n, resp.PartialSuccess().ErrorMessage())
	}

	return nil
}

func (d *otlpDest) WriteTraces(ctx context.Context, spans []tracestorage.Span) error {
	req := ptraceotlp.NewExportRequestFromTraces(tracestorage.SpansToTraces(spans))
	resp, err := d.traces.Export(ctx, req)
	if err != nil {
		return errors.Wrap(err, "export traces")
	}
	if n := resp.PartialSuccess().RejectedSpans(); n > 0 {
		return errors.Errorf("rejected %d spans: %s", n, resp.PartialSuccess().ErrorMessage())
	}

	return nil
}

func (d *otlpDest) WriteNumberPoints(ctx context.Context, points []metricstorage.NumberPoint) error {
	return d.WriteMetrics(ctx, metricstorage.NumberPointsToMetrics(points))
}

func (d *otlpDest) WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	resp, err := d.metrics.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md))
	if err != nil {
		return errors.Wrap(err, "export metrics")
	}
	if n := resp.PartialSuccess().RejectedDataPoints(); n > 0 {
		return errors.Errorf("rejected %d data points: %s", n, resp.PartialSuccess().ErrorMessage())
	}

	return nil
}

func (d *otlpDest) Close() error {
	if err := d.conn.Close(); err != nil {
		return errors.Wrap(err, "close otlp connection")
	}

	return nil
}
