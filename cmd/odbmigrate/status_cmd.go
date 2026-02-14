package main

import (
	"fmt"
	"text/tabwriter"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func newStatusCmd() *cobra.Command {
	setChFlag, dial := chFlag()
	var opts chstorage.MigratorOptions

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Check connection and migration status.",
		Long:  "Verify ClickHouse connectivity and show applied/available migrations.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := dial(cmd.Context())
			if err != nil {
				return errors.Wrap(err, "dial clickhouse")
			}
			migrator := chstorage.NewMigrator(client, opts)
			diff, err := migrator.Diff(cmd.Context())
			if err != nil {
				return fmt.Errorf("diff: %w", err)
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 7, 7, 1, ' ', tabwriter.TabIndent)
			defer func() {
				_ = w.Flush()
			}()
			_, _ = fmt.Fprintf(w, "%s \t%s \t\n", "TABLE", "STATUS")
			for _, d := range diff {
				_, _ = fmt.Fprintf(w, "%s \t%s \t\n", d.Table, d.Status.ColorString())
			}

			return nil
		},
	}
	opts.AddFlags(cmd.Flags())
	setChFlag(cmd)
	return cmd
}
