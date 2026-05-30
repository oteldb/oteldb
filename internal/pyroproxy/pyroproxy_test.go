package pyroproxy_test

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/grafana/pyroscope-go/upstream"
	"github.com/grafana/pyroscope-go/upstream/remote"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/pyroscopeapi"
)

type ingestInput struct {
	params pyroscopeapi.IngestParams
	ct     string
	data   []byte
}

type testIngest struct {
	pyroscopeapi.UnimplementedHandler
	got chan<- ingestInput
}

func (s *testIngest) Ingest(ctx context.Context, req *pyroscopeapi.IngestReqWithContentType, params pyroscopeapi.IngestParams) error {
	data, err := io.ReadAll(req.Content)
	if err != nil {
		return errors.Wrap(err, "read data")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.got <- ingestInput{
		params: params,
		ct:     req.ContentType,
		data:   data,
	}:
		return nil
	}
}

type testLogger struct {
	t interface {
		Logf(string, ...any)
	}
}

func (l *testLogger) Infof(a string, b ...any)  { l.t.Logf("[INFO]  "+a+"\n", b...) }
func (l *testLogger) Debugf(a string, b ...any) { l.t.Logf("[DEBUG] "+a+"\n", b...) }
func (l *testLogger) Errorf(a string, b ...any) { l.t.Logf("[ERROR] "+a+"\n", b...) }

func TestGoSDK(t *testing.T) {
	ctx := context.Background()
	ch := make(chan ingestInput, 1)

	api, err := pyroscopeapi.NewServer(&testIngest{got: ch})
	require.NoError(t, err)

	s := httptest.NewServer(api)
	t.Cleanup(func() {
		s.Close()
	})

	r, err := remote.NewRemote(remote.Config{
		Address: s.URL,
		Timeout: 15 * time.Second,
		Threads: 1,
		Logger:  &testLogger{t: t},
	})
	require.NoError(t, err)

	r.Start()
	defer r.Stop()

	data := []byte("data")
	now := time.Now()
	start := now.Add(-5 * time.Minute)
	end := now.Add(+5 * time.Minute)
	job := &upstream.UploadJob{
		Name:            "test-profile",
		StartTime:       start,
		EndTime:         end,
		SpyName:         "spyName",
		SampleRate:      10,
		Units:           "samples",
		AggregationType: "average",
		Format:          upstream.FormatPprof,
		Profile:         data,
	}

	r.Upload(job)
	r.Flush()

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatal(ctx.Done())
	case r := <-ch:
		require.Equal(t, job.Name, string(r.params.Name))
		require.Equal(t, job.SpyName, r.params.SpyName.Value)
		require.Equal(t, job.SampleRate, r.params.SampleRate.Value)
		require.Equal(t, job.Units, r.params.Units.Value)
		require.Equal(t, job.AggregationType, r.params.AggregationType.Value)
	}
}
