package httpmiddleware

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// UserCredentials holds username and password.
type UserCredentials struct {
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
}

// BearerToken provides an  that checks for Bearer tokens.
func BasicAuth(users []UserCredentials) Authenticator {
	creds := make(map[string]string, len(users))
	for _, u := range users {
		creds[u.User] = u.Password
	}
	return AuthenticatorFunc(func(r *http.Request) AuthVerdict {
		user, pass, ok := r.BasicAuth()
		if !ok {
			return Unauthenticated("missing or invalid Authorization header")
		}
		expectedPass, userExists := creds[user]
		if !userExists {
			return Unauthenticated("unauthorized")
		}
		if subtle.ConstantTimeCompare([]byte(pass), []byte(expectedPass)) != 1 {
			return Unauthenticated("unauthorized")
		}
		return Authenticated()
	})
}

// BearerToken provides an [Authenticator] that checks for Bearer tokens.
func BearerToken(tokens []string) Authenticator {
	isValid := func(token string) bool {
		for _, t := range tokens {
			if subtle.ConstantTimeCompare([]byte(token), []byte(t)) == 1 {
				return true
			}
		}
		return false
	}
	return AuthenticatorFunc(func(r *http.Request) AuthVerdict {
		authHeader := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(authHeader, "Bearer ")
		if !ok {
			return Unauthenticated("missing or invalid Authorization header")
		}
		token = strings.TrimSpace(token)

		if !isValid(token) {
			return Unauthenticated("unauthorized")
		}

		return Authenticated()
	})
}

// Authenticator provides an interface for authentication mechanisms.
type Authenticator interface {
	Authenticate(r *http.Request) AuthVerdict
}

// AuthenticatorFunc provides a function wrapper for [Authenticator] interface.
type AuthenticatorFunc func(r *http.Request) AuthVerdict

var _ Authenticator = AuthenticatorFunc(nil)

// Authenticate implements [Authenticator] interface.
func (f AuthenticatorFunc) Authenticate(r *http.Request) AuthVerdict {
	return f(r)
}

// AuthVerdict represents the result of an authentication attempt.
type AuthVerdict struct {
	Ok      bool
	Message string
}

// Authenticated creates a successful [AuthVerdict].
func Authenticated() AuthVerdict {
	return AuthVerdict{
		Ok:      true,
		Message: "success",
	}
}

// Unauthenticated creates an [AuthVerdict] with a message.
func Unauthenticated(msg string) AuthVerdict {
	return AuthVerdict{
		Ok:      false,
		Message: msg,
	}
}

// Auth provides a middleware that tries multiple authenticators in sequence.
func Auth(sets []Authenticator, onError func(w http.ResponseWriter, req *http.Request, msg string)) Middleware {
	if onError == nil {
		onError = defaultOnError
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var lastVerdict AuthVerdict
			for _, a := range sets {
				lastVerdict = a.Authenticate(r)
				if lastVerdict.Ok {
					next.ServeHTTP(w, r)
					return
				}
			}
			onError(w, r, lastVerdict.Message)
		})
	}
}

func defaultOnError(w http.ResponseWriter, _ *http.Request, msg string) {
	resp := struct {
		Error string `json:"error"`
	}{
		Error: msg,
	}
	data, _ := json.Marshal(resp)
	http.Error(w, string(data), http.StatusUnauthorized)
}
