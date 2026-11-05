package main

import (
	"math/rand/v2"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

func BenchmarkTraceRequestDecode(b *testing.B) {
	td := makeTraces(0, 256, 16, 512, time.Unix(1_790_000_000, 0), rand.New(rand.NewPCG(101, 103)))
	raw, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"pdata", "pdata_to_native", "direct_cold", "direct_to_native"} {
		b.Run(mode, func(b *testing.B) {
			var direct otlpdirect.TracesConverter
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for b.Loop() {
				if mode == "direct_cold" {
					var cold otlpdirect.TracesConverter
					if _, err := cold.Convert(raw); err != nil {
						b.Fatal(err)
					}
					continue
				}
				if mode == "direct_to_native" {
					if _, err := direct.Convert(raw); err != nil {
						b.Fatal(err)
					}
					continue
				}
				request := ptraceotlp.NewExportRequest()
				if err := request.UnmarshalProto(raw); err != nil {
					b.Fatal(err)
				}
				if mode == "pdata_to_native" {
					var batch trace.Traces
					if dropped := pdataconv.AppendTraces(&batch, request.Traces()); dropped != 0 {
						b.Fatalf("dropped %d spans", dropped)
					}
				}
			}
		})
	}
}
