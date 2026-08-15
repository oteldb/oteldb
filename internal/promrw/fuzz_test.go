package promrw_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"

	"github.com/oteldb/oteldb/internal/promrw"
)

// FuzzHandler drives the whole ingest path — snappy, protobuf, conversion — on arbitrary bodies:
// a malformed request must be rejected, never panic.
func FuzzHandler(f *testing.F) {
	f.Add(snappy.Encode(nil, readCorpus(f)))
	f.Add(snappy.Encode(nil, nil))
	f.Add([]byte("not snappy"))
	f.Add([]byte{})

	h := promrw.NewHandler(nopSink{}, promrw.HandlerConfig{MaxBodyBytes: 1 << 20})

	f.Fuzz(func(t *testing.T, body []byte) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if code := rec.Code; code != http.StatusAccepted && code != http.StatusBadRequest {
			t.Fatalf("unexpected code %d", code)
		}
	})
}
