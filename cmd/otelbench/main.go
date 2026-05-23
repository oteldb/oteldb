// Binary otelbench implements benchmarking suite for oteldb.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "otelbench",
		Short: "otelbench is a benchmarking suite for oteldb",

		SilenceUsage:  true,
		SilenceErrors: true,
	}
	rootCmd.AddCommand(
		newPromQLCommand(),
		newLogQLCommand(),
		newPrometheusRemoteWriteCommand(),
		newOtelCommand(),
		newDumpCommand(),
		newDashboardCommand(),
		newChDumpCommand(),
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		if errors.Is(err, ctx.Err()) {
			fmt.Fprintln(os.Stderr, "interrupted")
			os.Exit(1)
		}

		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
