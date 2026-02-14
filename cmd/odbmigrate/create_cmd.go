package main

import (
	"fmt"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func newCreateCmd() *cobra.Command {
	setChFlag, dial := chFlag()

	var opts chstorage.MigratorOptions

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create ClickHouse schema.",
		Long:  "Create necessary ClickHouse tables and schema for oteldb.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := dial(cmd.Context())
			if err != nil {
				return errors.Wrap(err, "dial clickhouse")
			}

			migrator := chstorage.NewMigrator(client, opts)
			if err := migrator.Create(cmd.Context()); err != nil {
				return fmt.Errorf("create schema: %w", err)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "create: schema applied")
			return nil
		},
	}
	opts.AddFlags(cmd.Flags())
	setChFlag(cmd)
	return cmd
}
