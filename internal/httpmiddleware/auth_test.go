package httpmiddleware_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/httpmiddleware"
)

func TestBasicAuth(t *testing.T) {
	var (
		dir        = t.TempDir()
		secretFile = filepath.Join(dir, "password")
	)
	err := os.WriteFile(secretFile, []byte("qwerty"), 0o600)
	require.NoError(t, err)

	auth, err := httpmiddleware.BasicAuth([]httpmiddleware.UserCredentials{
		{User: "alice", Password: "secret"},
		{User: "bob", PasswordFile: secretFile},
	})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/", http.NoBody)
	require.NoError(t, err)

	verdict := auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Unauthenticated("missing or invalid Authorization header"), verdict)

	req.SetBasicAuth("alice", "secret")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Authenticated(), verdict)

	req.SetBasicAuth("alice", "hello")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Unauthenticated("unauthorized"), verdict)

	req.SetBasicAuth("alice", "")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Unauthenticated("unauthorized"), verdict)

	req.SetBasicAuth("bob", "qwerty")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Authenticated(), verdict)

	req.SetBasicAuth("admin", "admin")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Unauthenticated("unauthorized"), verdict)
}

func TestBearerToken(t *testing.T) {
	var (
		dir        = t.TempDir()
		secretFile = filepath.Join(dir, "token")
	)
	err := os.WriteFile(secretFile, []byte("def"), 0o600)
	require.NoError(t, err)

	auth, err := httpmiddleware.BearerToken([]httpmiddleware.Token{
		{Token: "abc"},
		{TokenFile: secretFile},
	})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/", http.NoBody)
	require.NoError(t, err)

	verdict := auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Unauthenticated("missing or invalid Authorization header"), verdict)

	req.Header.Set("Authorization", "Bearer abc")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Authenticated(), verdict)

	req.Header.Set("Authorization", "Bearer 123")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Unauthenticated("unauthorized"), verdict)

	req.Header.Set("Authorization", "Bearer def")
	verdict = auth.Authenticate(req)
	require.Equal(t, httpmiddleware.Authenticated(), verdict)
}

func TestAuth(t *testing.T) {
	testHandler := func(h http.Handler, r *http.Request) string {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Body.String()
	}

	first, err := httpmiddleware.BearerToken([]httpmiddleware.Token{
		{Token: "abc"},
		{Token: "def"},
	})
	require.NoError(t, err)
	second, err := httpmiddleware.BasicAuth([]httpmiddleware.UserCredentials{
		{User: "alice", Password: "secret"},
		{User: "bob", Password: "qwerty"},
	})
	require.NoError(t, err)
	auths := []httpmiddleware.Authenticator{first, second}

	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	})

	h := httpmiddleware.Auth(auths, nil)(ok)
	{
		req, err := http.NewRequest(http.MethodPost, "/", http.NoBody)
		require.NoError(t, err)

		testHandler(h, req)
		require.Equal(t, `{"error":"missing or invalid Authorization header"}`+"\n", testHandler(h, req))

		req.SetBasicAuth("alice", "secret")
		testHandler(h, req)
		require.Equal(t, `ok`, testHandler(h, req))
	}
	{
		req, err := http.NewRequest(http.MethodPost, "/", http.NoBody)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer abc")

		testHandler(h, req)
		require.Equal(t, `ok`, testHandler(h, req))
	}

	h = httpmiddleware.Auth(auths, func(w http.ResponseWriter, req *http.Request, msg string) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, msg)
	})(ok)
	{
		req, err := http.NewRequest(http.MethodPost, "/", http.NoBody)
		require.NoError(t, err)

		testHandler(h, req)
		require.Equal(t, `missing or invalid Authorization header`, testHandler(h, req))

		req.SetBasicAuth("alice", "secret")
		testHandler(h, req)
		require.Equal(t, `ok`, testHandler(h, req))
	}
	{
		req, err := http.NewRequest(http.MethodPost, "/", http.NoBody)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer abc")

		testHandler(h, req)
		require.Equal(t, `ok`, testHandler(h, req))
	}
}
