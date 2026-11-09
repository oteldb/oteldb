package main

import (
	"context"
	"time"

	"go.uber.org/zap"
)

func monitorShutdown(ctx context.Context, completed <-chan struct{}, log *zap.Logger, running func() []string) {
	select {
	case <-completed:
		return
	case <-ctx.Done():
	}
	started := time.Now()
	log.Info("Application shutdown started", zap.Strings("running_services", running()), zap.NamedError("context_error", ctx.Err()))
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-completed:
			log.Info("Application shutdown completed", zap.Duration("elapsed", time.Since(started)))
			return
		case <-ticker.C:
			log.Warn("Application still shutting down", zap.Duration("elapsed", time.Since(started)), zap.Strings("running_services", running()))
		}
	}
}
