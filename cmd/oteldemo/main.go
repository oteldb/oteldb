// Binary oteldemo generates demo telemetry (logs, metrics, traces, profiles) for oteldb.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "oteldemo",
		Short: "oteldemo generates demo telemetry for oteldb",

		SilenceUsage:  true,
		SilenceErrors: true,
	}
	rootCmd.AddCommand(
		newServerCommand(),
		newClientCommand(),
		newProfilesCommand(),
	)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
