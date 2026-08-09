package hareceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap/zaptest"
)

// memStorage is an in-memory [storage.Client].
type memStorage struct {
	data map[string][]byte
}

func newMemStorage() *memStorage {
	return &memStorage{data: map[string][]byte{}}
}

func (s *memStorage) Get(_ context.Context, key string) ([]byte, error) {
	return s.data[key], nil
}

func (s *memStorage) Set(_ context.Context, key string, value []byte) error {
	s.data[key] = value
	return nil
}

func (s *memStorage) Delete(_ context.Context, key string) error {
	delete(s.data, key)
	return nil
}

func (s *memStorage) Batch(context.Context, ...*storage.Operation) error {
	return errors.New("not implemented")
}

func (*memStorage) Close(context.Context) error { return nil }

// response is a canned reply of the fake Home Assistant instance.
type response struct {
	status      int
	firstCursor string
	body        string
}

type fakeHA struct {
	t         *testing.T
	responses []response
	requests  []*http.Request
	server    *httptest.Server
}

func newFakeHA(t *testing.T, responses ...response) *fakeHA {
	ha := &fakeHA{t: t, responses: responses}
	ha.server = httptest.NewServer(http.HandlerFunc(ha.handle))
	t.Cleanup(ha.server.Close)
	return ha
}

func (ha *fakeHA) handle(w http.ResponseWriter, r *http.Request) {
	ha.requests = append(ha.requests, r)
	if len(ha.requests) > len(ha.responses) {
		ha.t.Errorf("unexpected request %d: %s", len(ha.requests), r.Header.Get("Range"))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	resp := ha.responses[len(ha.requests)-1]
	if resp.firstCursor != "" {
		w.Header().Set(firstCursorHeader, resp.firstCursor)
	}
	status := resp.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(resp.body))
}

// ranges returns the Range header of every request received so far.
func (ha *fakeHA) ranges() []string {
	out := make([]string, 0, len(ha.requests))
	for _, r := range ha.requests {
		out = append(out, r.Header.Get("Range"))
	}
	return out
}

func newTestPoller(t *testing.T, ha *fakeHA, lc consumer.Logs, st storage.Client) *poller {
	t.Helper()
	cfg := testConfig(func(c *Config) {
		c.Endpoint = ha.server.URL
		c.BatchSize = 100
	})
	p, err := newPoller(
		Source{Kind: SourceKindHost}, cfg,
		ha.server.Client(), lc, st, zaptest.NewLogger(t),
	)
	require.NoError(t, err)
	return p
}

func storedCursor(t *testing.T, st *memStorage) string {
	t.Helper()
	return string(st.data["cursor/host"])
}

func TestPollerRequest(t *testing.T) {
	ha := newFakeHA(t, response{firstCursor: "s=1", body: "2026-08-09 12:00:00.000 ha x[1]: a\n"})
	p := newTestPoller(t, ha, consumertest.NewNop(), newMemStorage())
	require.NoError(t, p.poll(context.Background()))

	require.Len(t, ha.requests, 1)
	req := ha.requests[0]
	require.Equal(t, "/api/hassio/host/logs", req.URL.Path)
	require.Equal(t, "Bearer token", req.Header.Get("Authorization"))
	require.Equal(t, tailRangeHeader, req.Header.Get("Range"))

	q, err := url.ParseQuery(req.URL.RawQuery)
	require.NoError(t, err)
	require.Contains(t, q, "verbose")
	require.Contains(t, q, "no_colors")
	require.NotContains(t, q, "lines", "lines would override the Range header")
}

func TestPollerColdStartDoesNotEmit(t *testing.T) {
	// "entries=:-1:2" returns the last two entries; anchoring on the first of
	// them with skip=2 puts the next read past the last, so neither is emitted.
	ha := newFakeHA(t,
		response{firstCursor: "s=1", body: "" +
			"2026-08-09 12:00:00.000 ha x[1]: second to last\n" +
			"2026-08-09 12:00:01.000 ha x[1]: last\n"},
		response{firstCursor: "s=3", body: "2026-08-09 12:00:02.000 ha x[1]: new\n"},
	)
	sink := new(consumertest.LogsSink)
	st := newMemStorage()
	p := newTestPoller(t, ha, sink, st)

	require.NoError(t, p.poll(context.Background()))
	require.Zero(t, sink.LogRecordCount(), "tail anchoring must not emit pre-existing entries")
	require.JSONEq(t, `{"anchor":"s=1","skip":2}`, storedCursor(t, st))

	require.NoError(t, p.poll(context.Background()))
	require.Equal(t, 1, sink.LogRecordCount())
	require.Equal(t, "new", sink.AllLogs()[0].
		ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
	require.Equal(t, []string{tailRangeHeader, "entries=s=1:2:100"}, ha.ranges())
	require.JSONEq(t, `{"anchor":"s=3","skip":1}`, storedCursor(t, st))
}

func TestPollerColdStartShortJournal(t *testing.T) {
	// A journal holding a single entry returns one line instead of two; the
	// skip must follow the response, not the requested count.
	ha := newFakeHA(t,
		response{firstCursor: "s=1", body: "2026-08-09 12:00:00.000 ha x[1]: only\n"},
	)
	sink := new(consumertest.LogsSink)
	st := newMemStorage()
	p := newTestPoller(t, ha, sink, st)

	require.NoError(t, p.poll(context.Background()))
	require.Zero(t, sink.LogRecordCount())
	require.JSONEq(t, `{"anchor":"s=1","skip":1}`, storedCursor(t, st))
}

func TestPollerAdvancesCursor(t *testing.T) {
	ha := newFakeHA(t,
		response{firstCursor: "s=2", body: "" +
			"2026-08-09 12:00:01.000 ha x[1]: a\n" +
			"2026-08-09 12:00:02.000 ha x[1]: b\n" +
			"2026-08-09 12:00:03.000 ha x[1]: c\n"},
		response{firstCursor: "s=5", body: "2026-08-09 12:00:04.000 ha x[1]: d\n"},
	)
	sink := new(consumertest.LogsSink)
	st := newMemStorage()
	require.NoError(t, st.Set(context.Background(), "cursor/host", []byte(`{"anchor":"s=1","skip":1}`)))
	p := newTestPoller(t, ha, sink, st)

	require.NoError(t, p.poll(context.Background()))
	require.NoError(t, p.poll(context.Background()))

	require.Equal(t, []string{"entries=s=1:1:100", "entries=s=2:3:100"}, ha.ranges(),
		"skip must equal the number of entries of the anchoring response")
	require.Equal(t, 4, sink.LogRecordCount())
	require.JSONEq(t, `{"anchor":"s=5","skip":1}`, storedCursor(t, st))
}

func TestPollerMultilineCountsOneEntry(t *testing.T) {
	ha := newFakeHA(t,
		response{firstCursor: "s=2", body: "" +
			"2026-08-09 12:00:01.000 ha x[1]: Traceback:\n" +
			"    raise Error\n" +
			"2026-08-09 12:00:02.000 ha x[1]: b\n"},
		response{firstCursor: "s=4", body: "2026-08-09 12:00:03.000 ha x[1]: c\n"},
	)
	st := newMemStorage()
	require.NoError(t, st.Set(context.Background(), "cursor/host", []byte(`{"anchor":"s=1","skip":1}`)))
	p := newTestPoller(t, ha, consumertest.NewNop(), st)

	require.NoError(t, p.poll(context.Background()))
	require.NoError(t, p.poll(context.Background()))

	require.Equal(t, []string{"entries=s=1:1:100", "entries=s=2:2:100"}, ha.ranges(),
		"a continuation line is not a journal entry and must not be skipped over")
}

func TestPollerRecombineKeepsCursorOnEntries(t *testing.T) {
	// Four journal entries collapse into two records; the cursor must advance
	// by four, or the next poll would re-read the fragments it already emitted.
	ha := newFakeHA(t,
		response{firstCursor: "s=2", body: "" +
			"2026-08-09 12:00:01.000 ha app[1]: 2026-08-09 12:00:01.000 ERROR (MainThread) [a.b] boom\n" +
			"2026-08-09 12:00:01.001 ha app[1]: Traceback (most recent call last):\n" +
			"2026-08-09 12:00:01.002 ha app[1]:   File \"x.py\", line 1\n" +
			"2026-08-09 12:00:01.003 ha app[1]: 2026-08-09 12:00:01.003 INFO (MainThread) [a.b] done\n"},
		response{firstCursor: "s=6", body: "2026-08-09 12:00:02.000 ha app[1]: next\n"},
	)
	sink := new(consumertest.LogsSink)
	st := newMemStorage()
	require.NoError(t, st.Set(context.Background(), "cursor/host", []byte(`{"anchor":"s=1","skip":1}`)))
	p := newTestPoller(t, ha, sink, st)

	require.NoError(t, p.poll(context.Background()))
	require.Equal(t, 2, sink.LogRecordCount(), "the traceback joins the error record")
	require.JSONEq(t, `{"anchor":"s=2","skip":4}`, storedCursor(t, st),
		"skip counts journal entries, not emitted records")

	require.NoError(t, p.poll(context.Background()))
	require.Equal(t, []string{"entries=s=1:1:100", "entries=s=2:4:100"}, ha.ranges())
}

func TestPollerEmptyResponseKeepsCursor(t *testing.T) {
	ha := newFakeHA(t,
		response{body: ""},
		response{body: ""},
	)
	st := newMemStorage()
	require.NoError(t, st.Set(context.Background(), "cursor/host", []byte(`{"anchor":"s=1","skip":1}`)))
	p := newTestPoller(t, ha, consumertest.NewNop(), st)

	require.NoError(t, p.poll(context.Background()))
	require.NoError(t, p.poll(context.Background()))

	require.Equal(t, []string{"entries=s=1:1:100", "entries=s=1:1:100"}, ha.ranges())
	require.JSONEq(t, `{"anchor":"s=1","skip":1}`, storedCursor(t, st))
}

func TestPollerEmptyJournalRetriesAnchoring(t *testing.T) {
	ha := newFakeHA(t,
		response{body: ""},
		response{firstCursor: "s=1", body: "2026-08-09 12:00:00.000 ha x[1]: a\n"},
	)
	st := newMemStorage()
	p := newTestPoller(t, ha, consumertest.NewNop(), st)

	require.NoError(t, p.poll(context.Background()))
	require.Empty(t, storedCursor(t, st), "nothing to anchor on yet")

	require.NoError(t, p.poll(context.Background()))
	require.Equal(t, []string{tailRangeHeader, tailRangeHeader}, ha.ranges())
	require.JSONEq(t, `{"anchor":"s=1","skip":1}`, storedCursor(t, st))
}

func TestPollerConsumeErrorDoesNotAdvance(t *testing.T) {
	ha := newFakeHA(t,
		response{firstCursor: "s=2", body: "2026-08-09 12:00:01.000 ha x[1]: a\n"},
		response{firstCursor: "s=2", body: "2026-08-09 12:00:01.000 ha x[1]: a\n"},
	)
	st := newMemStorage()
	require.NoError(t, st.Set(context.Background(), "cursor/host", []byte(`{"anchor":"s=1","skip":1}`)))
	p := newTestPoller(t, ha, consumertest.NewErr(errors.New("downstream")), st)

	require.ErrorContains(t, p.poll(context.Background()), "downstream")
	require.JSONEq(t, `{"anchor":"s=1","skip":1}`, storedCursor(t, st),
		"cursor must not advance past entries that were never delivered")

	require.ErrorContains(t, p.poll(context.Background()), "downstream")
	require.Equal(t, []string{"entries=s=1:1:100", "entries=s=1:1:100"}, ha.ranges(),
		"the same window must be re-read")
}

func TestPollerResumesFromStoredCursor(t *testing.T) {
	ha := newFakeHA(t, response{firstCursor: "s=9", body: "2026-08-09 12:00:01.000 ha x[1]: a\n"})
	st := newMemStorage()
	require.NoError(t, st.Set(context.Background(), "cursor/host", []byte(`{"anchor":"s=7","skip":4}`)))

	p := newTestPoller(t, ha, consumertest.NewNop(), st)
	require.NoError(t, p.poll(context.Background()))
	require.Equal(t, []string{"entries=s=7:4:100"}, ha.ranges())
}

func TestPollerUnauthorizedIsPermanent(t *testing.T) {
	ha := newFakeHA(t, response{status: http.StatusUnauthorized, body: "unauthorized"})
	p := newTestPoller(t, ha, consumertest.NewNop(), newMemStorage())

	err := p.poll(context.Background())
	require.Error(t, err)
	var permanent *backoff.PermanentError
	require.ErrorAs(t, err, &permanent, "a non-admin token is not worth retrying")
	require.ErrorContains(t, err, "401")
}

func TestPollerServerErrorIsRetryable(t *testing.T) {
	ha := newFakeHA(t, response{status: http.StatusBadGateway, body: "bad gateway"})
	p := newTestPoller(t, ha, consumertest.NewNop(), newMemStorage())

	err := p.poll(context.Background())
	require.Error(t, err)
	var permanent *backoff.PermanentError
	require.NotErrorAs(t, err, &permanent)
}
