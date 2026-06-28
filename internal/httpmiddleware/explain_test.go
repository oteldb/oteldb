package httpmiddleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/profile"
)

func TestExplain(t *testing.T) {
	// A handler that records into the collector exactly like a storage operator.
	var seen bool
	h := Explain()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pf := profile.Begin(r.Context(), "fake-fetch")
		seen = profile.Active(r.Context())
		pf.Add("rows", 3)
		pf.End()
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("HeaderInstallsCollector", func(t *testing.T) {
		seen = false
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query", http.NoBody)
		req.Header.Set(ProfileHeader, "1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.True(t, seen, "collector must be active when header is set")
	})

	t.Run("NoHeaderIsNoop", func(t *testing.T) {
		seen = true
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query", http.NoBody)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.False(t, seen, "no collector when header is absent")
	})
}

func TestProfileRequested(t *testing.T) {
	for _, v := range []string{"1", "true", "yes", "on", "TRUE"} {
		require.True(t, profileRequested(v), v)
	}
	for _, v := range []string{"", "0", "false", "no", "off", "  "} {
		require.False(t, profileRequested(v), v)
	}
}
