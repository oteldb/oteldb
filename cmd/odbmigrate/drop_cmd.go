package main

import (
	"fmt"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func newDropCmd() *cobra.Command {
	setChFlag, dial := chFlag()
	var opts chstorage.MigratorOptions

	cmd := &cobra.Command{
		Use:   "drop",
		Short: "Drop ClickHouse schema.",
		Long:  "Drop ClickHouse tables and schema used by oteldb.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := dial(cmd.Context())
			if err != nil {
				return errors.Wrap(err, "dial clickhouse")
			}
			migrator := chstorage.NewMigrator(client, opts)
			if err := migrator.Drop(cmd.Context(), func(database, table string) {
				fmt.Fprintf(cmd.OutOrStdout(), "dropping %q.%q\n", database, table)
			}); err != nil {
				return fmt.Errorf("drop schema: %w", err)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "drop: schema dropped")
			return nil
		},
	}
	opts.AddFlags(cmd.Flags())
	setChFlag(cmd)
	return cmd
}
