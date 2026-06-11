package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttribute_String(t *testing.T) {
	tests := []struct {
		attr Attribute
		want string
	}{
		{Attribute{Prop: SpanDuration}, "duration"},
		{Attribute{Prop: SpanChildCount}, "childCount"},
		{Attribute{Prop: SpanName}, "name"},
		{Attribute{Prop: SpanStatus}, "status"},
		{Attribute{Prop: SpanKind}, "kind"},
		{Attribute{Prop: SpanParent}, "parent"},
		{Attribute{Prop: RootSpanName}, "rootName"},
		{Attribute{Prop: RootServiceName}, "rootServiceName"},
		{Attribute{Prop: TraceDuration}, "traceDuration"},

		{Attribute{Name: "foo.bar", Scope: ScopeNone}, ".foo.bar"},
		{Attribute{Name: "foo.bar", Scope: ScopeSpan}, "span.foo.bar"},
		{Attribute{Name: "foo.bar", Scope: ScopeResource}, "resource.foo.bar"},

		{Attribute{Name: "foo.bar", Scope: ScopeNone, Parent: true}, "parent.foo.bar"},
		{Attribute{Name: "foo.bar", Scope: ScopeSpan, Parent: true}, "parent.span.foo.bar"},
		{Attribute{Name: "foo.bar", Scope: ScopeResource, Parent: true}, "parent.resource.foo.bar"},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, tt.attr.String())
		})
	}
}
