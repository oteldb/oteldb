package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/lokicompliance"
)

func printOutput(results []*lokicompliance.Result, cfg OutputConfig) error {
	if p := cfg.OutputFile; p != "" {
		p = filepath.Clean(p)

		f, err := os.Create(p)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()

		if err := outp(f, results, cfg); err != nil {
			return err
		}
	}

	return verifyTargetCompliance(results, cfg.MinimumPercentage)
}

func outp(output io.Writer, results []*lokicompliance.Result, cfg OutputConfig) error {
	if cfg.PrintFailed {
		for _, r := range results {
			if d := r.Diff; d != "" && !r.Unsupported {
				fmt.Printf("%q:\n%s\n", r.TestCase.Query, d)
			}
		}
	}

	// JSONResult is the JSON output format.
	type JSONResult struct {
		TotalResults       int                      `json:"totalResults"`
		Results            []*lokicompliance.Result `json:"results,omitempty"`
		IncludePassing     bool                     `json:"includePassing"`
		IncludeUnsupported bool                     `json:"includeUnsupported"`
	}
	total := len(results)
	if !cfg.OutputPassing || !cfg.OutputUnsupported {
		var filter []*lokicompliance.Result
		for _, r := range results {
			if r.Success() && !cfg.OutputPassing {
				// Exclude passing.
				continue
			}
			if r.Unsupported && !cfg.OutputUnsupported {
				// Exclude unsupported.
				continue
			}
			filter = append(filter, r)
		}
		results = filter
	}

	buf, err := json.MarshalIndent(JSONResult{
		TotalResults:       total,
		Results:            results,
		IncludePassing:     cfg.OutputPassing,
		IncludeUnsupported: cfg.OutputUnsupported,
	}, "", "\t")
	if err != nil {
		return err
	}

	_, err = output.Write(buf)
	return err
}

func verifyTargetCompliance(results []*lokicompliance.Result, target float64) error {
	var successes, differ, unsupported int
	for _, res := range results {
		switch {
		case res.Success() && !res.Unsupported:
			successes++
		case res.Unsupported:
			unsupported++
		default:
			differ++
		}
	}

	fmt.Println(strings.Repeat("=", 80))
	successPercentage := 100 * float64(successes) / float64(len(results))

	fmt.Printf("Total: %d / %d (%.2f%%) passed",
		successes, len(results), successPercentage,
	)
	if differ > 0 {
		fmt.Print(", ")
		fmt.Print(color.RedString("%d differ", differ))
	}
	if unsupported > 0 {
		fmt.Print(", ")
		fmt.Print(color.YellowString("%d unsupported", unsupported))
	}
	fmt.Println()

	if math.IsNaN(target) {
		return nil
	}
	fmt.Printf("Target: %.2f%%\n", target)

	if successPercentage < target {
		fmt.Println(color.RedString("FAILED"))
		return errors.Errorf("target is %v, passed is %.2f", target, successPercentage)
	}

	fmt.Println(color.GreenString("PASSED"))
	return nil
}
