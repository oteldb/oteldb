package main

import (
	"flag"
	"math"
	"strings"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/lokicompliance"
	"github.com/oteldb/oteldb/internal/lokihandler"
)

type Config struct {
	Start, End          time.Time
	Step                time.Duration
	Parallelism         int
	SkipReferenceErrors bool

	ReferenceTarget lokicompliance.TargetConfig
	TestTarget      lokicompliance.TargetConfig
	TestCases       []*lokicompliance.TestCase

	Output OutputConfig
}

type OutputConfig struct {
	OutputFile        string
	OutputFormat      string
	OutputPassing     bool
	OutputUnsupported bool
	PrintFailed       bool
	MinimumPercentage float64
}

type Flags struct {
	ConfigFiles arrayFlags

	QueryParallelism    int
	SkipReferenceErrors bool
	EndDelta            time.Duration
	RangeDuration       time.Duration
	StepDuration        time.Duration
	OutputFile          string
	OutputFormat        string
	OutputPassing       bool
	OutputUnsupported   bool
	PrintFailed         bool
	MinimumPercentage   float64
}

func (f *Flags) Validate() error {
	if v := f.OutputFormat; v != "json" {
		return errors.Errorf("unknown output format %q", v)
	}
	return nil
}

func (f *Flags) Register(set *flag.FlagSet) {
	set.Var(&f.ConfigFiles, "config-file",
		"The path to the configuration file. If repeated, the specified files will be concatenated before YAML parsing.")

	set.IntVar(&f.QueryParallelism, "query-parallelism", 20, "Maximum number of comparison queries to run in parallel.")
	set.BoolVar(&f.SkipReferenceErrors, "skip-reference-errors", false,
		"Record queries the reference API rejects as unsupported instead of aborting the run.")

	set.DurationVar(&f.EndDelta, "end", 12*time.Minute, "The delta between the end time and current time, negated")
	set.DurationVar(&f.RangeDuration, "range", 10*time.Minute, "The duration of the query range.")
	set.DurationVar(&f.StepDuration, "step", 10*time.Second, "The step of the query.")

	set.StringVar(&f.OutputFile, "output-file", "", "Path to output file")
	set.StringVar(&f.OutputFormat, "output-format", "json", "The comparison output format. Valid values: [json]")
	set.BoolVar(&f.OutputPassing, "output-passing", false, "Whether to also include passing test cases in the output.")
	set.BoolVar(&f.OutputUnsupported, "output-unsupported", false, "Whether to also include unsupported test cases in the output.")
	set.BoolVar(&f.PrintFailed, "print-failed", false, "Whether to print diff to stdout.")
	set.Float64Var(&f.MinimumPercentage, "target", math.NaN(), "Minimum compliance percentage")
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	if i == nil {
		return ""
	}
	return strings.Join(*i, ",")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func parseConfig() (c Config, _ error) {
	var flags Flags
	flags.Register(flag.CommandLine)

	flag.Parse()
	if err := flags.Validate(); err != nil {
		return c, err
	}

	c.Parallelism = flags.QueryParallelism
	c.SkipReferenceErrors = flags.SkipReferenceErrors
	c.Output = OutputConfig{
		OutputFile:        flags.OutputFile,
		OutputFormat:      flags.OutputFormat,
		OutputPassing:     flags.OutputPassing,
		OutputUnsupported: flags.OutputUnsupported,
		PrintFailed:       flags.PrintFailed,
		MinimumPercentage: flags.MinimumPercentage,
	}

	cfg, err := lokicompliance.LoadFromFiles(flags.ConfigFiles)
	if err != nil {
		return c, errors.Wrap(err, "loading configuration file")
	}
	c.ReferenceTarget = cfg.ReferenceTargetConfig
	c.TestTarget = cfg.TestTargetConfig

	c.End, err = lokihandler.ParseTimestamp(cfg.QueryParameters.EndTime, time.Now().Add(-flags.EndDelta))
	if err != nil {
		return c, errors.Wrap(err, "parse end")
	}
	c.Start = c.End.Add(-getNonZeroDuration(
		cfg.QueryParameters.RangeInSeconds,
		flags.RangeDuration,
	))
	c.Step = getNonZeroDuration(cfg.QueryParameters.StepInSeconds, flags.StepDuration)

	c.TestCases, err = lokicompliance.ExpandQuery(cfg, c.Start, c.End, c.Step)
	if err != nil {
		return c, errors.Wrap(err, "expand test cases")
	}
	return c, nil
}

func getNonZeroDuration(
	seconds float64,
	defaultDuration time.Duration,
) time.Duration {
	if seconds == 0.0 {
		return defaultDuration
	}
	return time.Duration(seconds * float64(time.Second))
}
