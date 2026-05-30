// Command main implements Docker CLI plugin to run LogQL queries.
package main

import (
	"fmt"

	"github.com/docker/cli/cli-plugins/metadata"
	"github.com/docker/cli/cli-plugins/plugin"
	"github.com/docker/cli/cli/command"
	"github.com/spf13/cobra"

	"github.com/oteldb/oteldb/internal/cliversion"
)

func rootCmd(dcli command.Cli) *cobra.Command {
	root := &cobra.Command{
		Use: "logql",
	}
	root.AddCommand(queryCmd(dcli))
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print plugin version",
		Run: func(cmd *cobra.Command, _ []string) {
			info, _ := cliversion.GetInfo("github.com/oteldb/oteldb")
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "docker-logql %s\n", info)
		},
	})
	return root
}

func getVersion() string {
	info, _ := cliversion.GetInfo("github.com/oteldb/oteldb")
	switch {
	case info.Version != "":
		return info.Version
	case info.Commit != "":
		return "dev-" + info.Commit
	default:
		return "unknown"
	}
}

func main() {
	meta := metadata.Metadata{
		SchemaVersion:    "0.1.0",
		Vendor:           "go-faster",
		Version:          getVersion(),
		ShortDescription: "A simple Docker CLI plugin to run LogQL queries over docker container logs.",
		URL:              "https://github.com/oteldb/oteldb/cmd/docker-logql",
	}
	plugin.Run(rootCmd, meta)
}
