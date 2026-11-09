package main

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-faster/sdk/zctx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestShutdownProgressStopsWithServices(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		ctx, cancel := context.WithCancel(zctx.Base(context.Background(), zap.New(core)))
		defer cancel()
		release := make(chan struct{})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()
		storageStopped := make(chan struct{})
		app := &App{services: map[string]func(context.Context) error{
			"storage": func(ctx context.Context) error { <-ctx.Done(); close(storageStopped); <-release; return nil },
			"http":    func(ctx context.Context) error { <-ctx.Done(); return nil },
		}}
		returned := make(chan error, 1)
		go func() { returned <- app.Run(ctx) }()
		synctest.Wait()
		cancel()
		<-storageStopped
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		waiting := logs.FilterMessage("Application still shutting down").All()
		require.Len(t, waiting, 1)
		require.ElementsMatch(t, []string{"storage"}, waiting[0].ContextMap()["running_services"])
		close(release)
		require.NoError(t, <-returned)
		count := logs.Len()
		time.Sleep(20 * time.Second)
		synctest.Wait()
		require.Equal(t, count, logs.Len(), "monitor must stop after Run returns")
	})
}

func TestRunWithoutServicesStopsMonitor(t *testing.T) {
	app := &App{services: map[string]func(context.Context) error{}}
	require.NoError(t, app.Run(t.Context()))
}
