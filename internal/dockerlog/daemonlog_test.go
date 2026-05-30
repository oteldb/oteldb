package dockerlog

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

func TestParseLog(t *testing.T) {
	f, err := os.Open("_testdata/dockerlog.bin")
	require.NoError(t, err)
	defer f.Close()

	iter := ParseLog(f, otelstorage.Attrs(pcommon.NewMap()))
	defer iter.Close()

	expected := []logqlengine.Entry{
		{Timestamp: 1707644252033031260, Line: "time=\"2024-02-11T09:37:32.032946602Z\" level=warning msg=\"No HTTP secret provided - generated random secret. This may cause problems with uploads if multiple registries are behind a load-balancer. To provide a shared secret, fill in http.secret in the configuration file or set the REGISTRY_HTTP_SECRET environment variable.\" go.version=go1.20.8 instance.id=3482d08d-d782-4c47-b0e0-37af45c9b495 service=registry version=2.8.3 \n"},
		{Timestamp: 1707644252033058840, Line: "time=\"2024-02-11T09:37:32.032982092Z\" level=info msg=\"redis not configured\" go.version=go1.20.8 instance.id=3482d08d-d782-4c47-b0e0-37af45c9b495 service=registry version=2.8.3 \n"},
		{Timestamp: 1707644252033079609, Line: "time=\"2024-02-11T09:37:32.03304416Z\" level=info msg=\"using inmemory blob descriptor cache\" go.version=go1.20.8 instance.id=3482d08d-d782-4c47-b0e0-37af45c9b495 service=registry version=2.8.3 \n"},
		{Timestamp: 1707644252033097289, Line: "time=\"2024-02-11T09:37:32.03303576Z\" level=info msg=\"Starting upload purge in 4m0s\" go.version=go1.20.8 instance.id=3482d08d-d782-4c47-b0e0-37af45c9b495 service=registry version=2.8.3 \n"},
		{Timestamp: 1707644252033198626, Line: "time=\"2024-02-11T09:37:32.033175887Z\" level=info msg=\"listening on [::]:5000\" go.version=go1.20.8 instance.id=3482d08d-d782-4c47-b0e0-37af45c9b495 service=registry version=2.8.3 \n"},
	}

	var (
		r logqlengine.Entry
		i int
	)
	for iter.Next(&r) {
		require.Equal(t, expected[i].Timestamp, r.Timestamp)
		require.Equal(t, expected[i].Line, r.Line)
		v, ok := r.Set.GetString("stream")
		require.True(t, ok)
		require.Equal(t, stderr.String(), v)

		i++
	}
	require.NoError(t, iter.Err())
}

func BenchmarkParseLog(b *testing.B) {
	data, err := os.ReadFile("_testdata/dockerlog.bin")
	require.NoError(b, err)

	var (
		rd = bytes.NewReader(data)
		rc = io.NopCloser(rd)

		attrs = otelstorage.Attrs(pcommon.NewMap())
		e     logqlengine.Entry
	)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd.Reset(data)

		iter := ParseLog(rc, attrs)
		for iter.Next(&e) {
			continue
		}
		iter.Close()
	}
}
