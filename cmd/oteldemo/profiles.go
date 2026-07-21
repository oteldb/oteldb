package main

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// profileFrames are the function names making up the synthetic call stacks (root-most last).
var profileFrames = []string{"main", "serveHTTP", "queryDB", "encodeJSON", "gcAssist"}

// profileStacks are leaf-first call stacks expressed as indices into profileFrames.
var profileStacks = [][]int{
	{2, 1, 0}, // queryDB <- serveHTTP <- main
	{3, 1, 0}, // encodeJSON <- serveHTTP <- main
	{4, 0},    // gcAssist <- main
}

// profileServices are the services the generator emits CPU profiles for.
var profileServices = []string{"client", "server"}

// profiles emits synthetic OTLP CPU profiles to the OTLP gRPC endpoint on a fixed interval, so the
// demo exercises the profiles signal (served by oteldb's Pyroscope API) alongside logs, metrics, and
// traces. The OTel Go SDK has no stable profiles exporter, so this dials the collector directly and
// pushes pprofile batches built with the pdata API (the same shape the storage engine ingests).
func newProfilesCommand() *cobra.Command {
	var endpoint string
	cmd := &cobra.Command{
		Use:   "profiles",
		Short: "Emit synthetic OTLP CPU profiles",
		Args:  cobra.NoArgs,
		Run: func(*cobra.Command, []string) {
			app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Telemetry) error {
				return profiles(ctx, lg, m, endpoint)
			})
		},
	}
	cmd.Flags().StringVar(&endpoint, "endpoint", "",
		"OTLP gRPC endpoint (defaults to OTEL_EXPORTER_OTLP_ENDPOINT, then oteldb:4317)")
	return cmd
}

func profiles(ctx context.Context, lg *zap.Logger, _ *app.Telemetry, endpoint string) error {
	target := otlpEndpoint(endpoint)
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.Wrap(err, "dial otlp")
	}
	defer func() { _ = conn.Close() }()
	client := pprofileotlp.NewGRPCClient(conn)

	lg.Info("Emitting profiles", zap.String("endpoint", target), zap.Strings("services", profileServices))
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		req := pprofileotlp.NewExportRequestFromProfiles(buildProfiles(time.Now()))
		if _, err := client.Export(ctx, req); err != nil {
			lg.Error("export profiles", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// otlpEndpoint resolves the OTLP gRPC target from the given override or OTEL_EXPORTER_OTLP_ENDPOINT,
// stripping any scheme (gRPC dials host:port). It defaults to the in-cluster oteldb endpoint used by
// the demo compose.
func otlpEndpoint(ep string) string {
	if ep == "" {
		ep = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if ep == "" {
		ep = "oteldb:4317"
	}
	if i := strings.Index(ep, "://"); i >= 0 {
		ep = ep[i+3:]
	}
	return strings.TrimSuffix(ep, "/")
}

// buildProfiles constructs a CPU profiles batch stamped at ts: one profile per service, each carrying
// a sample on every call stack with a jittered value.
func buildProfiles(ts time.Time) pprofile.Profiles {
	pd := pprofile.NewProfiles()
	dict := pd.Dictionary()

	st := dict.StringTable()
	st.Append("")            // 0: reserved empty.
	st.Append("cpu")         // 1: sample type.
	st.Append("nanoseconds") // 2: sample unit.
	for _, name := range profileFrames {
		st.Append(name)
	}
	frameStr := func(i int) int32 { return int32(3 + i) }

	dict.MappingTable().AppendEmpty().SetFilenameStrindex(0)
	for i := range profileFrames {
		dict.FunctionTable().AppendEmpty().SetNameStrindex(frameStr(i))
		loc := dict.LocationTable().AppendEmpty()
		loc.SetMappingIndex(0)
		line := loc.Lines().AppendEmpty()
		line.SetFunctionIndex(int32(i))
		line.SetLine(int64(10 * (i + 1)))
	}
	for _, frameRefs := range profileStacks {
		stk := dict.StackTable().AppendEmpty()
		for _, fr := range frameRefs {
			stk.LocationIndices().Append(int32(fr))
		}
	}

	for _, svc := range profileServices {
		rp := pd.ResourceProfiles().AppendEmpty()
		rp.Resource().Attributes().PutStr("service.name", svc)
		scp := rp.ScopeProfiles().AppendEmpty()

		p := scp.Profiles().AppendEmpty()
		p.SampleType().SetTypeStrindex(1)
		p.SampleType().SetUnitStrindex(2)
		p.PeriodType().SetTypeStrindex(1)
		p.PeriodType().SetUnitStrindex(2)
		p.SetTime(pcommon.Timestamp(ts.UnixNano()))

		for si := range profileStacks {
			s := p.Samples().AppendEmpty()
			s.SetStackIndex(int32(si))
			s.Values().Append(int64(100*(si+1) + rand.Intn(100))) // #nosec G404 -- demo jitter.
			s.TimestampsUnixNano().Append(uint64(ts.UnixNano()))
		}
	}
	return pd
}
