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

			tables := opts.Tables
			if tables == (chstorage.Tables{}) {
				tables = chstorage.DefaultTables()
			}

			// Collect candidate backup table names from the known table set.
			var candidates []string
			_ = tables.Each(func(name *string) error {
				if *name != "" {
					base := *name + "_backup"
					candidates = append(candidates, base)
					if opts.Replicated {
						candidates = append(candidates, *name+"_replicated_backup")
						candidates = append(candidates, *name+"_distributed_backup")
					}
				}
				return nil
			})

			type backupTable struct {
				name string
				rows uint64
			}
			var found []backupTable

			for _, candidate := range candidates {
				exists, err := tableExists(ctx, client, candidate)
				if err != nil {
					return errors.Wrapf(err, "check %s", candidate)
				}
				if !exists {
					continue
				}
				rows, err := getRowCount(ctx, client, candidate)
				if err != nil {
					return errors.Wrapf(err, "count rows in %s", candidate)
				}
				found = append(found, backupTable{name: candidate, rows: rows})
			}

			if len(found) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "cleanup: no backup tables found")
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 7, 7, 1, ' ', tabwriter.TabIndent)
			_, _ = fmt.Fprintf(w, "%s \t%s \t\n", "TABLE", "ROWS")
			for _, t := range found {
				_, _ = fmt.Fprintf(w, "%s \t%d \t\n", t.name, t.rows)
			}
			_ = w.Flush()

			if !yes {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "\nDry run — pass --yes to drop these tables.")
				return nil
			}

			// Extract base names for DropBackups (it appends _backup itself).
			// For replicated/distributed variants we pass the full name and use the
			// Migrator's cluster-aware dropTable via DropBackups with the suffixed names.
			// Simpler: just drop each found table by its full name directly.
			baseNames := make([]string, len(found))
			for i, t := range found {
				// Strip the _backup suffix to get the base name.
				baseNames[i] = t.name[:len(t.name)-len("_backup")]
			}

			migrator := chstorage.NewMigrator(client, opts)
			if err := migrator.DropBackups(ctx, baseNames, func(table string) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "dropped %s\n", table)
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
