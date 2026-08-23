// Package etcdtest starts embedded etcd servers for tests.
package etcdtest

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

// freeAddrs returns n loopback addresses that were unbound when probed.
//
// Each listener is held open until every address is allocated, so no two results collide.
// They are unbound on return, though, so a caller that loses a race to bind one must retry.
func freeAddrs(n int) ([]string, error) {
	var (
		lc        net.ListenConfig
		listeners = make([]net.Listener, 0, n)
	)
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()

	addrs := make([]string, 0, n)
	for range n {
		l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return nil, errors.Wrapf(err, "listen %d/%d", len(addrs)+1, n)
		}

		listeners = append(listeners, l)
		addrs = append(addrs, l.Addr().String())
	}

	return addrs, nil
}

// FreeAddr returns a loopback address that was unbound when probed.
func FreeAddr(tb testing.TB) string {
	tb.Helper()

	addrs, err := freeAddrs(1)
	require.NoError(tb, err, "allocate port")

	return addrs[0]
}

// Start runs an embedded etcd and returns its client endpoint.
func Start(tb testing.TB) string {
	tb.Helper()

	// Nothing holds the ports between [freeAddrs] and etcd binding them, so anything else on the
	// machine can take one first. That is a lost race rather than a broken environment, so retry
	// with fresh ports instead of failing the test.
	const attempts = 5

	var err error
	for range attempts {
		// Running out of ephemeral ports is an exhausted machine rather than a lost race,
		// so it fails the test instead of feeding the retry.
		addrs, allocErr := freeAddrs(2)
		require.NoError(tb, allocErr, "allocate ports")

		var endpoint string
		if endpoint, err = tryStart(tb, addrs); err == nil {
			return endpoint
		}
	}
	require.NoError(tb, err, "start embedded etcd")

	return ""
}

func tryStart(tb testing.TB, addrs []string) (string, error) {
	tb.Helper()

	client := url.URL{Scheme: "http", Host: addrs[0]}
	peer := url.URL{Scheme: "http", Host: addrs[1]}

	cfg := embed.NewConfig()
	cfg.Dir = tb.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{client}
	cfg.AdvertiseClientUrls = []url.URL{client}
	cfg.ListenPeerUrls = []url.URL{peer}
	cfg.AdvertisePeerUrls = []url.URL{peer}
	cfg.InitialCluster = cfg.Name + "=" + peer.String()

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return "", err
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		e.Close()
		tb.Fatal("embedded etcd did not become ready")
	}
	tb.Cleanup(e.Close)

	return client.String(), nil
}
