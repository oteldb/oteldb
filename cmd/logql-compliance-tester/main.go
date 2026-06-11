package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/cheggaaa/pb/v3"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/lokicompliance"
)

func run(ctx context.Context) error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrap(err, "create logger")
	}
	defer func() {
		_ = log.Sync()
	}()
	ctx = zctx.Base(ctx, log)

	cfg, err := parseConfig()
	if err != nil {
		return err
	}

	comp, err := setup(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "setup")
	}

	var (
		results     = make([]*lokicompliance.Result, len(cfg.TestCases))
		progressBar = pb.StartNew(len(results))
	)
	// Progress bar messes up Github Actions logs, so keep it static.
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		progressBar.SetTemplate(pb.Default + `{{ "\n" }}`)
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	if n := cfg.Parallelism; n > 0 {
		grp.SetLimit(n)
	}

	for i, tc := range cfg.TestCases {
		grp.Go(func() error {
			ctx := grpCtx

			res, err := comp.Compare(ctx, tc)
			if err != nil {
				return errors.Wrapf(err, "compare %q", tc.Query)
			}
			results[i] = res

			progressBar.Increment()
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return errors.Wrap(err, "run queries")
	}
	progressBar.Finish()

	return printOutput(results, cfg.Output)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
