package main

import "github.com/spf13/cobra"

func newLogsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "logs is a self-benchmarking suite for oteldb logs",
	}
	cmd.AddCommand(
		newLogsDumpAssetsCommand(),
		newLogsReportCommand(),
		newLogsSuiteCommand(),
	)
	return cmd
}
