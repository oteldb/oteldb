package httpmiddleware

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// UserCredentials holds username and password.
type UserCredentials struct {
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
}

// BasicAuthMiddleware provides an authentication middleware that checks for Basic Auth credentials.
func BasicAuthMiddleware(users []UserCredentials) Middleware {
	creds := make(map[string]string, len(users))
	for _, u := range users {
		creds[u.User] = u.Password
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			expectedPass, userExists := creds[user]
			if !userExists || subtle.ConstantTimeCompare([]byte(pass), []byte(expectedPass)) != 1 {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// BearerTokenMiddleware provides an authentication middleware that checks for Bearer tokens.
func BearerTokenMiddleware(tokens []string) Middleware {
	isValid := func(token string) bool {
		for _, t := range tokens {
			if subtle.ConstantTimeCompare([]byte(token), []byte(t)) == 1 {
				return true
			}
		}
		return false
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")

			token, ok := strings.CutPrefix(authHeader, "Bearer ")
			if !ok {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			token = strings.TrimSpace(token)

			if !isValid(token) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
