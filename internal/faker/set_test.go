package faker

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/xpdata/xhash"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

func Value(v attribute.Value) string {
	s, _ := v.MarshalJSON()
	d := jx.DecodeBytes(s)
	var out string
	_ = d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "Value":
			r, err := d.Raw()
			if err != nil {
				return err
			}
			out = r.String()
			return nil
		default:
			return d.Skip()
		}
	})
	return out
}

func TestResource(t *testing.T) {
	res, err := resource.New(context.Background(),
		resource.WithHost(),
		resource.WithHostID(),
		resource.WithOS(),
		resource.WithOSType(),
		resource.WithOSDescription(),
	)
	require.NoError(t, err)
	for _, a := range res.Attributes() {
		require.NoError(t, err)
		t.Logf("%s=%s", a.Key, Value(a.Value))
	}
}

// Hash is PoC hash of attribute set.
func Hash(set attribute.Set) [sha256.Size]byte {
	return sha256.Sum256([]byte(set.Encoded(attribute.DefaultEncoder())))
}

func TestSet(t *testing.T) {
	set := attribute.NewSet(
		attribute.Int("foo", 1),
		attribute.String("bar", "baz"),
	)
	t.Logf("%s %x", set.Encoded(attribute.DefaultEncoder()), Hash(set))

	m := pcommon.NewMap()
	require.NoError(t,
		m.FromRaw(map[string]any{
			"foo": 1,
			"bar": "baz",
		}),
	)
	t.Logf("%x", xhash.MapHash(m))
}

func BenchmarkMapHash(b *testing.B) {
	m := pcommon.NewMap()
	require.NoError(b,
		m.FromRaw(map[string]any{
			"foo": 1,
			"bar": "baz",
		}),
	)
	for i := 0; i < b.N; i++ {
		xhash.MapHash(m)
	}
}
