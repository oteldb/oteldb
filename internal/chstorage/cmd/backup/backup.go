package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func run(ctx context.Context) error {
	lg, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrap(err, "create logger")
	}
	defer func() {
		_ = lg.Sync()
	}()

	b := chstorage.NewBackup(
		chstorage.NewDialingClickhouseClient(ch.Options{
			Address:  "10.42.0.21:9000",
			Database: "default",
		}),
		chstorage.DefaultTables(),
		lg,
	)
	return b.Create(ctx, "backupdir")
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v", err)
		os.Exit(1)
	}
}
