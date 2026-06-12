package main

import (
	"fmt"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/oteldb/oteldb/cmd/otelbench/logsbench"
)

func newLogsDumpAssetsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump-assets <dir>",
		Short: "Write embedded logs benchmark assets to a directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if err := logsbench.WriteAssets(args[0]); err != nil {
				return errors.Wrap(err, "write assets")
			}
			fmt.Println("assets written to", args[0])
			return nil
		},
	}
	return cmd
}
