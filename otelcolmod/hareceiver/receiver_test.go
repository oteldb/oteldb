package hareceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// storageExtension is a [storage.Extension] backed by [memStorage].
type storageExtension struct {
	component.StartFunc
	component.ShutdownFunc
	client *memStorage
}

func (e *storageExtension) GetClient(
	context.Context, component.Kind, component.ID, string,
) (storage.Client, error) {
	return e.client, nil
}

type testHost struct {
	extensions map[component.ID]component.Component
}

func (h testHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

func newReceiver(t *testing.T, cfg *Config) *Receiver {
	t.Helper()
	r, err := NewReceiver(receivertest.NewNopSettings(typ), cfg, consumertest.NewNop())
	require.NoError(t, err)
	return r
}

// idleEndpoint serves an empty journal, so that a started poller makes no
// progress and needs no canned responses.
func idleEndpoint(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestReceiverLifecycle(t *testing.T) {
	cfg := testConfig(func(c *Config) {
		c.Endpoint = idleEndpoint(t)
		c.Sources = []Source{
			{Kind: SourceKindHost},
			{Kind: SourceKindAddon, Addon: "core_ssh"},
		}
	})
	r := newReceiver(t, cfg)

	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, r.Shutdown(context.Background()))
	require.NoError(t, r.Shutdown(context.Background()), "shutdown is idempotent")
}

func TestReceiverStorageExtension(t *testing.T) {
	id := component.MustNewID("file_storage")
	cfg := testConfig(func(c *Config) {
		c.Endpoint = idleEndpoint(t)
		c.StorageID = &id
	})

	t.Run("Found", func(t *testing.T) {
		ext := &storageExtension{client: newMemStorage()}
		host := testHost{extensions: map[component.ID]component.Component{id: ext}}

		r := newReceiver(t, cfg)
		require.NoError(t, r.Start(context.Background(), host))
		require.NoError(t, r.Shutdown(context.Background()))
	})
	t.Run("Missing", func(t *testing.T) {
		r := newReceiver(t, cfg)
		err := r.Start(context.Background(), componenttest.NewNopHost())
		require.ErrorContains(t, err, "not found")
	})
	t.Run("NotStorage", func(t *testing.T) {
		host := testHost{extensions: map[component.ID]component.Component{
			id: extension.Extension(nopExtension{}),
		}}
		r := newReceiver(t, cfg)
		err := r.Start(context.Background(), host)
		require.ErrorContains(t, err, "not a storage extension")
	})
}

type nopExtension struct {
	component.StartFunc
	component.ShutdownFunc
}

func TestReceiverInvalidConfig(t *testing.T) {
	_, err := NewReceiver(
		receivertest.NewNopSettings(typ),
		testConfig(func(c *Config) { c.Sources = nil }),
		consumertest.NewNop(),
	)
	require.ErrorContains(t, err, "at least one source is required")
}
