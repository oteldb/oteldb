package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func run(ctx context.Context) error {
	var (
		path = flag.String("path", "./restore", "Backup directory")
		dsn  = flag.String("dsn", "clickhouse://localhost:9000", "Clickhouse connection URL")
	)
	flag.Parse()

	lg, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrap(err, "create logger")
	}
	defer func() {
		_ = lg.Sync()
	}()
	ctx = zctx.Base(ctx, lg)

	d, err := chstorage.Dial(ctx, *dsn, chstorage.DialOptions{})
	if err != nil {
		return errors.Wrap(err, "dial clickhouse")
	}
	restore := chstorage.NewRestore(d, chstorage.DefaultTables(), lg.Named("restore"))

	return restore.Restore(ctx, *path)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
