// Package chdump provides utilities to read ClickHouse data dumps created by oteldb.
package chdump

import (
	"io"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
)

// ConsumeOptions defines options for [Consume].
type ConsumeOptions struct {
	OnTraces        func(*Spans) error
	OnPoints        func(*Points) error
	OnExpHistograms func(*ExpHistograms) error
	OnLogs          func(*Logs) error
}

// Consume reads dump and calls callbacks.
func Consume(iter TableReader, opts ConsumeOptions) error {
	for {
		name, file, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.Wrap(err, "next")
		}
		if err := func() error {
			defer func() {
				_ = file.Close()
			}()

			table := strings.TrimSuffix(name, ".bin.zstd")
			switch table {
			case "traces_spans":
				return readBlock(file, NewSpans(), opts.OnTraces)
			case "metrics_points":
				return readBlock(file, NewPoints(), opts.OnPoints)
			case "metrics_exp_histograms":
				return readBlock(file, NewExpHistograms(), opts.OnExpHistograms)
			case "logs":
				return readBlock(file, NewLogs(), opts.OnLogs)
			}

			return nil
		}(); err != nil {
			return errors.Wrapf(err, "read %q", name)
		}
	}
}

const protoVersion = 51902

func readBlock[
	TableColumns interface {
		Reset()
		Result() proto.Results
	},
](
	r io.Reader,
	cols TableColumns,
	cb func(TableColumns) error,
) error {
	if cb == nil {
		return nil
	}

	zr, err := zstd.NewReader(r)
	if err != nil {
		return errors.Wrap(err, "open zstd dump")
	}
	defer zr.Close()

	var (
		chr   = proto.NewReader(zr)
		block = &proto.Block{}

		blockIdx int
		results  = cols.Result()
	)
	for {
		cols.Reset()

		if err := block.DecodeBlock(chr, protoVersion, results); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.Wrapf(err, "decode block %d", blockIdx)
		}

		if err := cb(cols); err != nil {
			return errors.Wrap(err, "callback")
		}
		blockIdx++
	}
}
