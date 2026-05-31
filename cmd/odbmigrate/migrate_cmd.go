package main

import (
	"fmt"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func newMigrateCmd() *cobra.Command {
	setChFlag, dial := chFlag()
	var opts chstorage.MigratorOptions
	var yes bool
	var defaultTenant string

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate ClickHouse schema (e.g. to multitenancy).",
		Long:  "Migrate existing oteldb schema to new schema versions. This may rename tables, recreate them, and copy data.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes {
				return errors.New("migration requires --yes flag to confirm")
			}
			ctx := cmd.Context()
			client, err := dial(ctx)
			if err != nil {
				return errors.Wrap(err, "dial clickhouse")
			}
			out := cmd.OutOrStdout()
			return chstorage.NewMigrator(client, opts).Migrate(
				ctx,
				chstorage.MigrateOptions{DefaultTenant: defaultTenant},
				func(format string, args ...any) {
					_, _ = fmt.Fprintf(out, format+"\n", args...)
				},
			)
		},
	}
	opts.AddFlags(cmd.Flags())
	setChFlag(cmd)
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm migration")
	cmd.Flags().StringVar(&defaultTenant, "default-tenant", "", "Default tenant ID for migrated records")
	return cmd
}
