package main

import (
	"fmt"
	"text/tabwriter"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func newCleanupCmd() *cobra.Command {
	setChFlag, dial := chFlag()
	var opts chstorage.MigratorOptions
	var yes bool

	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Drop backup tables left by a migrate run.",
		Long: "Lists (or drops with --yes) <table>_backup tables created by 'odbmigrate migrate'.\n" +
			"Without --yes this is a dry run: it prints backup tables and their row counts.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			client, err := dial(ctx)
			if err != nil {
				return errors.Wrap(err, "dial clickhouse")
			}
			migrator := chstorage.NewMigrator(client, opts)

			backups, err := migrator.ListBackups(ctx)
			if err != nil {
				return errors.Wrap(err, "list backups")
			}

			if len(backups) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "cleanup: no backup tables found")
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 7, 7, 1, ' ', tabwriter.TabIndent)
			_, _ = fmt.Fprintf(w, "%s \t%s \t\n", "TABLE", "ROWS")
			for _, b := range backups {
				_, _ = fmt.Fprintf(w, "%s \t%d \t\n", b.Name, b.Rows)
			}
			_ = w.Flush()

			if !yes {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "\nDry run — pass --yes to drop these tables.")
				return nil
			}

			// Strip _backup suffix to get base names for DropBackups.
			const suffix = "_backup"
			baseNames := make([]string, len(backups))
			for i, b := range backups {
				baseNames[i] = b.Name[:len(b.Name)-len(suffix)]
			}
			out := cmd.OutOrStdout()
			if err := migrator.DropBackups(ctx, baseNames, func(table string) {
				_, _ = fmt.Fprintf(out, "dropped %s\n", table)
			}); err != nil {
				return errors.Wrap(err, "drop backup tables")
			}

			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "cleanup: done")
			return nil
		},
	}
	opts.AddFlags(cmd.Flags())
	setChFlag(cmd)
	cmd.Flags().BoolVar(&yes, "yes", false, "Drop the backup tables (default is dry-run)")
	return cmd
}
