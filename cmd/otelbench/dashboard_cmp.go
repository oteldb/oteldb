package main

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"
	"github.com/spf13/cobra"
)

type DashboardCmp struct {
	Markdown bool
}

func (c *DashboardCmp) Run(basePath, newPath string) error {
	base, err := c.loadReport(basePath)
	if err != nil {
		return errors.Wrap(err, "load base report")
	}
	current, err := c.loadReport(newPath)
	if err != nil {
		return errors.Wrap(err, "load new report")
	}

	return c.runCompare(os.Stdout, base, current)
}

func (c *DashboardCmp) runCompare(out io.Writer, base, current []DashboardReportEntry) error {
	baseMap := make(map[string]DashboardReportEntry)
	for _, e := range base {
		baseMap[e.Panel+e.Query] = e
	}
	currentMap := make(map[string]DashboardReportEntry)
	for _, e := range current {
		currentMap[e.Panel+e.Query] = e
	}

	allKeys := make(map[string]struct{})
	for k := range baseMap {
		allKeys[k] = struct{}{}
	}
	for k := range currentMap {
		allKeys[k] = struct{}{}
	}

	type row struct {
		key   string
		panel string
		query string
	}
	var rows []row
	for k := range allKeys {
		var panel, query string
		if e, ok := baseMap[k]; ok {
			panel, query = e.Panel, e.Query
		} else {
			e := currentMap[k]
			panel, query = e.Panel, e.Query
		}
		rows = append(rows, row{key: k, panel: panel, query: query})
	}

	// Sort by panel, then query.
	slices.SortFunc(rows, func(a, b row) int {
		if a.panel != b.panel {
			return strings.Compare(a.panel, b.panel)
		}
		return strings.Compare(a.query, b.query)
	})

	if c.Markdown {
		fmt.Fprintln(out, "| PANEL | QUERY | OLD AVG | NEW AVG | DELTA | P99 OLD | P99 NEW | DELTA |")
		fmt.Fprintln(out, "|-------|-------|---------|---------|-------|---------|---------|-------|")
	}
	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	if !c.Markdown {
		fmt.Fprintln(w, "PANEL\tQUERY\tOLD AVG\tNEW AVG\tDELTA\tP99 OLD\tP99 NEW\tDELTA")
	}

	for _, r := range rows {
		old, hasOld := baseMap[r.key]
		curr, hasCurr := currentMap[r.key]

		if c.Markdown {
			if !hasOld {
				fmt.Fprintf(out, "| %s | %s | - | %v | [NEW] | - | %v | [NEW] |\n",
					r.panel,
					curr.Query,
					curr.Avg.Round(time.Microsecond),
					curr.P99.Round(time.Microsecond),
				)
				continue
			}
			if !hasCurr {
				fmt.Fprintf(out, "| %s | %s | %v | - | [REMOVED] | %v | - | [REMOVED] |\n",
					r.panel,
					old.Query,
					old.Avg.Round(time.Microsecond),
					old.P99.Round(time.Microsecond),
				)
				continue
			}

			fmt.Fprintf(out, "| %s | %s | %v | %v | %s | %v | %v | %s |\n",
				r.panel,
				curr.Query,
				old.Avg.Round(time.Microsecond),
				curr.Avg.Round(time.Microsecond),
				c.formatDelta(old.Avg, curr.Avg),
				old.P99.Round(time.Microsecond),
				curr.P99.Round(time.Microsecond),
				c.formatDelta(old.P99, curr.P99),
			)
			continue
		}

		if !hasOld {
			fmt.Fprintf(w, "%s\t%s\t-\t%v\t[NEW]\t-\t%v\t[NEW]\n",
				r.panel,
				c.truncate(r.query, 32),
				curr.Avg.Round(time.Microsecond),
				curr.P99.Round(time.Microsecond),
			)
			continue
		}
		if !hasCurr {
			fmt.Fprintf(w, "%s\t%s\t%v\t-\t[REMOVED]\t%v\t-\t[REMOVED]\n",
				r.panel,
				c.truncate(r.query, 32),
				old.Avg.Round(time.Microsecond),
				old.P99.Round(time.Microsecond),
			)
			continue
		}

		fmt.Fprintf(w, "%s\t%s\t%v\t%v\t%s\t%v\t%v\t%s\n",
			r.panel,
			c.truncate(r.query, 32),
			old.Avg.Round(time.Microsecond),
			curr.Avg.Round(time.Microsecond),
			c.formatDelta(old.Avg, curr.Avg),
			old.P99.Round(time.Microsecond),
			curr.P99.Round(time.Microsecond),
			c.formatDelta(old.P99, curr.P99),
		)
	}

	return w.Flush()
}

func (c *DashboardCmp) truncate(s string, l int) string {
	if len(s) <= l {
		return s
	}
	return s[:l-3] + "..."
}

func (c *DashboardCmp) loadReport(path string) ([]DashboardReportEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var report []DashboardReportEntry
	if err := yaml.Unmarshal(data, &report); err != nil {
		return nil, err
	}
	return report, nil
}

func (c *DashboardCmp) formatDelta(old, new time.Duration) string {
	if old == 0 {
		return "-"
	}
	diff := new - old
	pct := float64(diff) / float64(old) * 100
	s := fmt.Sprintf("%+v (%.2f%%)", diff.Round(time.Microsecond), pct)
	if c.Markdown {
		return s
	}
	if pct > 10 {
		return color.RedString(s)
	}
	if pct < -10 {
		return color.GreenString(s)
	}
	return s
}

func newDashboardCmpCommand() *cobra.Command {
	c := &DashboardCmp{}
	cmd := &cobra.Command{
		Use:   "cmp <base.yml> <new.yml>",
		Short: "Compare dashboard benchmark reports",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args[0], args[1])
		},
	}
	f := cmd.Flags()
	f.BoolVar(&c.Markdown, "markdown", false, "Output as markdown table")
	return cmd
}
