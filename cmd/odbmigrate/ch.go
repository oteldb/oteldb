package main

import (
	"context"
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func chFlag() (set func(*cobra.Command), dial func(ctx context.Context) (chstorage.ClickHouseClient, error)) {
	var dsn string
	set = func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&dsn, "dsn", os.Getenv("CH_DSN"), "ClickHouse DSN (or set CH_DSN)")
	}
	dial = func(ctx context.Context) (chstorage.ClickHouseClient, error) {
		if dsn == "" {
			dsn = os.Getenv("CH_DSN")
		}
		if dsn == "" {
			return nil, errors.New("--dsn is required (or set CH_DSN)")
		}

		return chstorage.Dial(ctx, dsn, chstorage.DialOptions{})
	}
	return set, dial
}
