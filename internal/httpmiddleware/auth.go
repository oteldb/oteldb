package httpmiddleware

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
)

// UserCredentials holds username and password.
type UserCredentials struct {
	User         string `json:"user" yaml:"user"`
	Password     string `json:"password" yaml:"password"`
	PasswordFile string `json:"password_file" yaml:"password_file"`
}

// GetPassword returns password value.
func (u UserCredentials) GetPassword() (string, error) {
	if u.Password != "" && u.PasswordFile != "" {
		return "", errors.New("password and password_file are both set")
	}
	if u.PasswordFile != "" {
		data, err := os.ReadFile(u.PasswordFile)
		if err != nil {
			return "", err
		}
		password := strings.TrimSpace(string(data))
		clear(data)
		if password == "" {
			return "", errors.New("password file is empty")
		}
		return password, nil
	}
	if u.Password == "" {
		return "", errors.New("password is empty")
	}
	return u.Password, nil
}

// BasicAuth provides an [Authenticator] that checks for HTTP Basic Auth.
func BasicAuth(users []UserCredentials) (Authenticator, error) {
	creds := make(map[string]string, len(users))
	for _, u := range users {
		p, err := u.GetPassword()
		if err != nil {
			return nil, err
		}
		creds[u.User] = p
	}
	auth := AuthenticatorFunc(func(r *http.Request) AuthVerdict {
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
	return auth, nil
}

// Token defines token config.
type Token struct {
	Token     string `json:"token" yaml:"token"`
	TokenFile string `json:"token_file" yaml:"token_file"`
}

// Get returns token value.
func (t Token) Get() (string, error) {
	if t.TokenFile != "" && t.Token != "" {
		return "", errors.New("token_file and token are both set")
	}

	if t.TokenFile != "" {
		data, err := os.ReadFile(t.TokenFile)
		if err != nil {
			return "", err
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return "", errors.New("token_file is empty")
		}
		clear(data)

		return token, nil
	}
	if t.Token == "" {
		return "", errors.New("token is empty")
	}
	return t.Token, nil
}

// BearerToken provides an [Authenticator] that checks for Bearer tokens.
func BearerToken(tokens []Token) (Authenticator, error) {
	values := make([]string, 0, len(tokens))
	for _, t := range tokens {
		v, err := t.Get()
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}

	isValid := func(token string) bool {
		for _, t := range values {
			if subtle.ConstantTimeCompare([]byte(token), []byte(t)) == 1 {
				return true
			}
		}
		return false
	}
	auth := AuthenticatorFunc(func(r *http.Request) AuthVerdict {
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
	return auth, nil
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
