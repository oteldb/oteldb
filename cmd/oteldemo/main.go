package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-faster/sdk/app"
)

func main() {
	flag.Parse()
	switch mode := flag.Arg(0); mode {
	case "server":
		app.Run(server)
	case "client":
		app.Run(client)
	case "profiles":
		app.Run(profiles)
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", mode)
		fmt.Fprintf(os.Stderr, "Usage: %s [server|client|profiles]\n", os.Args[0])
		os.Exit(1)
	}
}
