package multitenancy

import (
	"strings"

	"github.com/go-faster/errors"
)

// MaxTenantLen bounds a tenant id. A tenant id becomes a shard key, which becomes a backend path
// segment and an etcd key component, so an unbounded one is a way to make keys nobody can address.
const MaxTenantLen = 150

// ParseTenantID validates a tenant id, returning it trimmed.
//
// The character set is what stays safe as a backend path segment and an etcd key, and excludes
// cluster.ShardSep so a tenant id can never be mistaken for the shard key derived from it. It is
// the same rule the ingest side applies, because the constraint is about the storage layer rather
// than about where the id came from: any id reaching it, from either direction, must satisfy it.
func ParseTenantID(s string) (string, error) {
	s = strings.TrimSpace(s)

	switch {
	case s == "":
		return "", errors.New("empty tenant id")
	case len(s) > MaxTenantLen:
		return "", errors.Errorf("tenant id longer than %d bytes", MaxTenantLen)
	case s == "." || s == "..":
		return "", errors.Errorf("invalid tenant id %q", s)
	}

	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
		default:
			return "", errors.Errorf("invalid character %q in tenant id", string(c))
		}
	}

	return s, nil
}
