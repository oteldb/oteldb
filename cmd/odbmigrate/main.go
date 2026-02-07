package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
)

func run(ctx context.Context) error {
	cmd := cobra.Command{
		Use:   "odbmigrate",
		Short: "ClickHouse schema migrator for oteldb",
		Long:  "Tooling to create, drop and migrate ClickHouse schema used by oteldb.",
	}
	cmd.AddCommand(newCreateCmd(), newStatusCmd(), newDropCmd())
	return cmd.ExecuteContext(ctx)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
