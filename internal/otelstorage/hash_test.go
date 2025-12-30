package otelstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestHash(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("foo", "bar")
	require.NotEqual(t, AttrHash(m), Hash{})
}

func TestHashNotEqual(t *testing.T) {
	a := pcommon.NewMap()
	a.PutStr("cpu", "1")

	b := pcommon.NewMap()
	b.PutStr("quantile", "1")

	require.NotEqual(t, AttrHash(a), AttrHash(b))
}

func BenchmarkAttrHash(b *testing.B) {
	m := pcommon.NewMap()
	m.PutStr("net.transport", "ip_tcp")
	m.PutStr("net.sock.family", "inet")
	m.PutStr("net.sock.host.addr", "192.168.210.83")
	m.PutStr("net.host.name", "shop-backend.local")
	m.PutStr("http.flavor", "1.1")
	m.PutStr("http.method", "PUT")
	m.PutInt("http.status_code", 204)
	m.PutStr("http.url", "https://shop-backend.local:8409/article-to-cart")
	m.PutStr("http.scheme", "https")
	m.PutStr("http.target", "/article-to-cart")
	m.PutInt("http.response_content_length", 937939)
	m.PutInt("http.request_content_length", 39543)

	var sink Hash
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sink = AttrHash(m)
	}

	if sink == (Hash{}) {
		b.Fatal("hash is zero")
	}
}
