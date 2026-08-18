package config

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

// AuthType defines authentication method type.
type AuthType string

// Supported [AuthType] values.
const (
	AuthTypeNone        AuthType = "none"
	AuthTypeBasic       AuthType = "basicauth"
	AuthTypeBearerToken AuthType = "bearertoken"
)

// IsValid checks if auth type is valid.
func (t AuthType) IsValid() bool {
	switch t {
	case AuthTypeNone, AuthTypeBasic, AuthTypeBearerToken:
		return true
	default:
		return false
	}
}

// Auth is authentication config.
type Auth struct {
	Type   AuthType                         `json:"type" yaml:"type"`
	Tokens []httpmiddleware.Token           `json:"tokens" yaml:"tokens"`
	Users  []httpmiddleware.UserCredentials `json:"users" yaml:"users"`
}

// SetDefaults implements [Defaulter].
func (cfg *Auth) SetDefaults() {
	if cfg.Type == "" {
		cfg.Type = AuthTypeNone
	}
}

// AuthMiddleware builds the authentication middleware for auth, returning nil middleware when no
// authentication is configured.
func AuthMiddleware(auth []Auth) (httpmiddleware.Middleware, error) {
	if len(auth) == 0 {
		return nil, nil
	}

	r := make([]httpmiddleware.Authenticator, 0, len(auth))
	for _, a := range auth {
		if !a.Type.IsValid() {
			return nil, errors.Errorf("invalid auth type %q", a.Type)
		}

		a.SetDefaults()
		switch a.Type {
		case AuthTypeBasic:
			m, err := httpmiddleware.BasicAuth(a.Users)
			if err != nil {
				return nil, errors.Wrap(err, "setup basic auth")
			}
			r = append(r, m)
		case AuthTypeBearerToken:
			m, err := httpmiddleware.BearerToken(a.Tokens)
			if err != nil {
				return nil, errors.Wrap(err, "setup bearer token auth")
			}
			r = append(r, m)
		}
	}

	return httpmiddleware.Auth(r, nil), nil
}
