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

	b := chstorage.NewRestore(
		chstorage.NewDialingClickhouseClient(ch.Options{
			Address:  "localhost:9000",
			User:     "default",
			Password: "default",
			Database: "default",
		}),
		chstorage.DefaultTables(),
		lg,
	)
	return b.Restore(ctx, "../backup/backupdir")
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v", err)
		os.Exit(1)
	}
}
