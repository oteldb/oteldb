package chdump

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"golang.org/x/sync/errgroup"
)

// IngestLogs loads logs from dump and sends them to the collector.
type IngestLogs struct {
	// Workers is the number of workers to use.
	//
	// Defaults to 1.
	Workers int
}

// Run ingests logs.
func (lg IngestLogs) Run(ctx context.Context, client plogotlp.GRPCClient, tr TableReader) error {
	var (
		workers = max(lg.Workers, 1)
		batcnCh = make(chan plog.Logs, workers)
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		defer close(batcnCh)
		return Consume(tr, ConsumeOptions{
			OnLogs: func(t *Logs) error {
				ctx := grpCtx

				batch := plog.NewLogs()
				t.ToOTLP(batch)

				select {
				case <-ctx.Done():
					return ctx.Err()
				case batcnCh <- batch:
					return nil
				}
			},
		})
	})
	for range workers {
		grp.Go(func() error {
			ctx := grpCtx
			for batch := range batcnCh {
				if _, err := client.Export(ctx, plogotlp.NewExportRequestFromLogs(batch)); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return grp.Wait()
}
