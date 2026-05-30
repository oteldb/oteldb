package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/oteldb/oteldb/internal/otelbench"
	"github.com/oteldb/oteldb/internal/prompb"
)

type Replay struct {
	Target     string
	Source     string
	Encoding   string
	ReportPath string
	Workers    int

	points atomic.Uint64
}

func (r *Replay) report(compressedData []byte) error {
	if r.ReportPath == "" {
		return nil
	}
	var (
		data []byte
		err  error
	)
	switch r.Encoding {
	case "zstd":
		reader, err := zstd.NewReader(bytes.NewReader(compressedData))
		if err != nil {
			return errors.Wrap(err, "create decoder")
		}
		defer reader.Close()
		if data, err = io.ReadAll(reader); err != nil {
			return errors.Wrap(err, "read data")
		}
	case "snappy":
		if data, err = snappy.Decode(data, compressedData); err != nil {
			return errors.Wrap(err, "decode data")
		}
	default:
		return errors.Errorf("unsupported encoding %q", r.Encoding)
	}
	var writeRequest prompb.WriteRequest
	if err := writeRequest.Unmarshal(data); err != nil {
		return errors.Wrap(err, "unmarshal request")
	}
	r.points.Add(uint64(len(writeRequest.Timeseries)))
	return nil
}

type ReplayReport struct {
	Points          int   `yaml:"points" json:"points"`
	DurationNanos   int64 `yaml:"duration_nanos" json:"durationNanos"`
	PointsPerSecond int   `yaml:"points_per_second" json:"pointsPerSecond"`
}

func (r *Replay) Run(ctx context.Context) error {
	f, err := os.Open(r.Source)
	if err != nil {
		return errors.Wrap(err, "open file")
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(">> close:", err)
		}
	}()
	stat, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "stat file")
	}

	pb := progressbar.DefaultBytes(
		stat.Size(),
		"sending",
	)
	pbr := progressbar.NewReader(f, pb)
	d := otelbench.NewReader(&pbr)
	client := &http.Client{
		Timeout: time.Minute,
	}
	fn := func(data []byte) error {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Target, bytes.NewReader(data))
		if err != nil {
			return errors.Wrap(err, "create request")
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", r.Encoding)
		res, err := client.Do(req)
		if err != nil {
			return errors.Wrap(err, "do request")
		}
		defer func() {
			_ = res.Body.Close()
		}()
		out, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		if len(out) == 0 {
			out = []byte("empty")
		}

		switch code := res.StatusCode; code {
		case http.StatusAccepted, http.StatusNoContent, http.StatusOK:
			return nil
		default:
			return errors.Errorf("%s: %s", res.Status, out)
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	inputs := make(chan []byte)
	start := time.Now()
	for i := 0; i < r.Workers; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case data, ok := <-inputs:
					if !ok {
						return nil
					}
					bo := backoff.NewExponentialBackOff()
					bo.MaxElapsedTime = time.Minute
					do := func() error { return fn(data) }
					if err := backoff.RetryNotify(do, backoff.WithContext(bo, ctx), func(err error, d time.Duration) {
						fmt.Printf("Retry after %s: %v\n", d, err)
					}); err != nil {
						return errors.Wrap(err, "retry")
					}
					if err := r.report(data); err != nil {
						return errors.Wrap(err, "report")
					}
				}
			}
		})
	}
	g.Go(func() error {
		defer close(inputs)
		for d.Decode() {
			data := d.Data()
			select {
			case <-ctx.Done():
				return nil
			case inputs <- slices.Clone(data):
				continue
			}
		}
		return d.Err()
	})
	if err := g.Wait(); err != nil {
		if errors.Is(err, ctx.Err()) {
			_ = pb.Exit()
			fmt.Println("Canceled")
			return nil
		}
		return err
	}
	_ = pb.Finish()
	fmt.Println("Done")
	duration := time.Since(start).Round(time.Millisecond)
	fmt.Println("Duration:", duration)
	if r.ReportPath != "" {
		fmt.Println("Points:", fmtInt(int(r.points.Load())))
		pointsPerSecond := float64(r.points.Load()) / duration.Seconds()
		fmt.Println("Per second:", fmtInt(int(pointsPerSecond)))
		rep := ReplayReport{
			Points:          int(r.points.Load()),
			DurationNanos:   duration.Nanoseconds(),
			PointsPerSecond: int(pointsPerSecond),
		}
		data, err := yaml.Marshal(rep)
		if err != nil {
			return errors.Wrap(err, "marshal report")
		}
		if err := os.WriteFile(r.ReportPath, data, 0o644); err != nil {
			return errors.Wrap(err, "write report")
		}
	}
	return nil
}

func newReplayCommand() *cobra.Command {
	var replay Replay
	cmd := &cobra.Command{
		Use:     "replay",
		Aliases: []string{"send"},
		Short:   "Send recorded requests to remote write server",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return replay.Run(cmd.Context())
		},
	}
	f := cmd.Flags()
	f.StringVar(&replay.Target, "target", "http://127.0.0.1:19291", "Target server")
	f.StringVarP(&replay.Source, "input", "i", "requests.rwq", "Source file")
	f.IntVarP(&replay.Workers, "workers", "j", 8, "Number of workers")
	f.StringVar(&replay.Encoding, "encoding", "zstd", "Encoding")
	f.StringVar(&replay.ReportPath, "report", "", "Report path")
	return cmd
}
