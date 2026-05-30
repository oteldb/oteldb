package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/ogen-go/ogen/json"

	"github.com/oteldb/oteldb/internal/promcompliance/output"
)

func main() {
	var arg struct {
		minimumPercentage float64
	}
	flag.Float64Var(&arg.minimumPercentage, "target", 99.5, "minimum percentage")
	flag.Parse()
	var (
		data []byte
		err  error
	)
	if filePath := flag.Arg(0); filePath == "" {
		// Read all from stdin.
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(filePath) // #nosec G304
	}
	if err != nil {
		panic(err)
	}
	var info output.JSONResult
	if err := json.Unmarshal(data, &info); err != nil {
		panic(err)
	}
	successes := 0
	unsupported := 0
	for _, res := range info.Results {
		if res.Success() {
			successes++
		}
		if res.Unsupported {
			unsupported++
		}
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("General query tweaks:")
	if len(info.QueryTweaks) == 0 {
		fmt.Println("None.")
	}
	for _, t := range info.QueryTweaks {
		fmt.Println("* ", t.Note)
	}
	fmt.Println(strings.Repeat("=", 80))
	successPercentage := 100 * float64(successes) / float64(len(info.Results))
	fmt.Printf("Total: %d / %d (%.2f%%) passed, %d unsupported\n",
		successes, len(info.Results), successPercentage, unsupported,
	)
	fmt.Printf("Target: %.2f%%\n", arg.minimumPercentage)

	if successPercentage < arg.minimumPercentage {
		fmt.Println(color.RedString("FAILED"))
		os.Exit(1)
	}

	fmt.Println(color.GreenString("PASSED"))
}
