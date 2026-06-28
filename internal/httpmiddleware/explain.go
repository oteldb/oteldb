package httpmiddleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/query/profile"
)

// ProfileHeader, when set to a truthy value ("1", "true", "yes", "on") on a
// request, turns on EXPLAIN ANALYZE for that query: the storage fetch operators
// record a profiled tree with per-node timing and I/O counters. This matches the
// X-Oteldb-Profile convention the storage cluster read path uses between peers.
const ProfileHeader = "X-Oteldb-Profile"

// Explain enables EXPLAIN ANALYZE for requests carrying the X-Oteldb-Profile
// header. It installs a storage profile collector into the request context and,
// after the handler runs, renders the fetch-operator tree to the context logger.
//
// It is zero-overhead for requests without the header: the storage operators'
// profile.Begin is a no-op when no collector is in the context, so the normal
// query path makes no extra timing reads or allocations. Install it after
// InjectLogger so the rendered tree reaches the request logger.
func Explain() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !profileRequested(r.Header.Get(ProfileHeader)) {
				next.ServeHTTP(w, r)
				return
			}

			ctx, coll := profile.WithCollector(r.Context())
			start := time.Now()
			next.ServeHTTP(w, r.WithContext(ctx))

			root := coll.Root()
			if root == nil {
				return
			}
			// The collector's root node is never End()ed by the fetch tier, so
			// stamp it with the wall time of the whole request for the total.
			root.Dur = time.Since(start)

			zctx.From(ctx).Info("EXPLAIN ANALYZE",
				zap.String("method", r.Method),
				zap.Stringer("url", r.URL),
				zap.String("plan", "\n"+root.Render()),
			)
		})
	}
}

// profileRequested reports whether the X-Oteldb-Profile header value asks for
// profiling. Empty, "0", "false", "no" and "off" are treated as off.
func profileRequested(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
