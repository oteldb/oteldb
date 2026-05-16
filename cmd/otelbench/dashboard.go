package main

import "github.com/spf13/cobra"

func newDashboardCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Suite for dashboard benchmarks",
	}
	cmd.AddCommand(
		newDashboardBenchmarkCommand(),
		newDashboardCmpCommand(),
	)
	return cmd
}
