package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/chstorage"
)

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func newStatusCmd() *cobra.Command {
	setChFlag, dial := chFlag()
	var opts chstorage.MigratorOptions
	var showDiff bool

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Check connection and migration status.",
		Long:  "Verify ClickHouse connectivity and show applied/available migrations.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			client, err := dial(ctx)
			if err != nil {
				return errors.Wrap(err, "dial clickhouse")
			}
			migrator := chstorage.NewMigrator(client, opts)

			// Run inspect and diff concurrently; cancel both if either fails.
			var info []chstorage.TableInfo
			var diff []chstorage.MigrationDiff
			g, gctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				var err error
				info, err = migrator.Inspect(gctx)
				if err != nil {
					return errors.Wrap(err, "inspect schema")
				}
				return nil
			})
			g.Go(func() error {
				var err error
				diff, err = migrator.Diff(gctx)
				if err != nil {
					return errors.Wrap(err, "diff schema")
				}
				return nil
			})
			if err := g.Wait(); err != nil {
				return err
			}

			// Index diff by table name for O(1) lookup.
			diffByTable := make(map[string]chstorage.MigrationDiff, len(diff))
			for _, d := range diff {
				diffByTable[d.Table] = d
			}

			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "Engine:  %s\n\n", detectEngineSummary(info))

			const (
				colTable    = 30
				colEngine   = 22
				colTenantID = 10
				colStatus   = 10
			)
			_, _ = fmt.Fprintf(out, "%-*s  %-*s  %-*s  %-*s  %s\n",
				colTable, "TABLE",
				colEngine, "ENGINE",
				colTenantID, "TENANT_ID",
				colStatus, "STATUS",
				"REASON",
			)
			_, _ = fmt.Fprintf(out, "%s  %s  %s  %s  %s\n",
				strings.Repeat("-", colTable),
				strings.Repeat("-", colEngine),
				strings.Repeat("-", colTenantID),
				strings.Repeat("-", colStatus),
				strings.Repeat("-", 6),
			)
			for _, t := range info {
				engine := t.Engine
				if engine == "" {
					engine = "(absent)"
				}
				status := chstorage.MigrationCreate // table is missing
				var reason string
				if d, ok := diffByTable[t.Name]; ok {
					status = d.Status
					reason = d.Reason
				}
				_, _ = fmt.Fprintf(out, "%-*s  %-*s  %-*s  %s  %s\n",
					colTable, t.Name,
					colEngine, engine,
					colTenantID, yesNo(t.HasTenantID),
					status.ColorPaddedString(colStatus),
					reason,
				)
			}

			if showDiff {
				for _, d := range diff {
					if d.Diff == "" {
						continue
					}
					_, _ = fmt.Fprintf(out, "\n--- %s ---\n%s\n", d.Table, d.Diff)
				}
			}

			return nil
		},
	}
	opts.AddFlags(cmd.Flags())
	setChFlag(cmd)
	cmd.Flags().BoolVar(&showDiff, "diff", false, "Print SQL diff for tables in UPGRADE state")
	return cmd
}

// detectEngineSummary derives a one-line engine description from the live tables.
func detectEngineSummary(info []chstorage.TableInfo) string {
	counts := map[string]int{}
	for _, t := range info {
		if t.Engine != "" {
			counts[t.Engine]++
		}
	}
	if len(counts) == 0 {
		return "(no tables)"
	}
	if len(counts) == 1 {
		for e := range counts {
			if strings.Contains(e, "Replicated") {
				return e + " (replicated)"
			}
			return e + " (standalone)"
		}
	}
	parts := make([]string, 0, len(counts))
	for e := range counts {
		parts = append(parts, e)
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ") + " (mixed)"
}
