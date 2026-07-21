package adminhandler

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

// distFS holds the built admin single-page app produced by the frontend package
// (internal/adminhandler/frontend, TypeScript + Vite + Orval). The build output
// (dist/) is NOT committed — it is produced by CI and on release — so only a
// placeholder is embedded during a plain `go build`. Build it with:
//
//	cd internal/adminhandler/frontend && bun install && bun run build
//
//go:embed all:frontend/dist
var distFS embed.FS

// uiFS is distFS rooted at the build output directory.
var uiFS = func() fs.FS {
	sub, err := fs.Sub(distFS, "frontend/dist")
	if err != nil {
		panic(err)
	}
	return sub
}()

// notBuiltPage is served when the SPA has not been built (dist/ holds only the
// placeholder). The JSON API remains fully functional.
var notBuiltPage = []byte(`<!doctype html><html><head><meta charset="utf-8">` +
	`<title>oteldb admin</title></head><body style="font-family:sans-serif;padding:2rem">` +
	`<h1>oteldb admin</h1><p>The admin web UI has not been built into this binary.</p>` +
	`<p>Build it with <code>cd internal/adminhandler/frontend &amp;&amp; bun install &amp;&amp; bun run build</code>, ` +
	`then rebuild oteldb. The JSON API under <code>/api/v1</code> is available regardless.</p>` +
	`</body></html>`)

// UIMiddleware serves the embedded admin SPA for non-API requests, delegating
// everything under /api/ to the next handler (the ogen server). Unknown paths
// fall back to index.html so client-side routing works. When the SPA was not
// built, a placeholder page is served instead.
func UIMiddleware() httpmiddleware.Middleware {
	fileServer := http.FileServer(http.FS(uiFS))
	index, err := fs.ReadFile(uiFS, "index.html")
	if err != nil {
		// dist/ holds only the placeholder (SPA not built): serve a static notice.
		index = notBuiltPage
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				next.ServeHTTP(w, r)
				return
			}
			// Serve a real asset when it exists, otherwise the SPA shell.
			if p := strings.TrimPrefix(r.URL.Path, "/"); p != "" {
				if f, err := uiFS.Open(p); err == nil {
					_ = f.Close()
					fileServer.ServeHTTP(w, r)
					return
				}
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(index)
		})
	}
}
