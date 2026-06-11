package traceql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var typeTests = []struct {
	input   string
	want    Expr
	wantErr bool
}{
	// Spanset expression must evaluate to boolean.
	// Static.
	{`{ "foo" }`, nil, true},
	{`{ 1 }`, nil, true},
	{`{ 3.14 }`, nil, true},
	{`{ nil }`, nil, true},
	{`{ 5h }`, nil, true},
	{`{ ok }`, nil, true},
	{`{ client }`, nil, true},
	// Attribute.
	{`{ duration }`, nil, true},
	{`{ childCount }`, nil, true},
	{`{ name }`, nil, true},
	{`{ status }`, nil, true},
	{`{ kind }`, nil, true},
	{`{ parent }`, nil, true},
	{`{ rootName }`, nil, true},
	{`{ rootServiceName }`, nil, true},
	{`{ traceDuration }`, nil, true},
	// Aggregate expression must evaluate to number.
	{`{ .a } | sum(true) > 10`, nil, true},
	{`{ .a } | sum("foo") > 10`, nil, true},
	// Illegal operand.
	{`{ -"foo" =~ "foo" }`, nil, true},
	{`{ !"foo" =~ "foo" }`, nil, true},
	{`{ -(.a = 1) }`, nil, true},
	{`{ 1 + "foo" }`, nil, true},
	{`{ 1 + true }`, nil, true},
	{`{ 1 + nil }`, nil, true},
	{`{ 1 + ok }`, nil, true},
	{`{ 1 + client }`, nil, true},
	{`{ "foo" > 10 }`, nil, true},
	{`{ "foo" =~ 10 }`, nil, true},
	// Illegal operation.
	// String.
	{`{ "foo" && "foo" }`, nil, true},
	{`{ "foo" || "foo" }`, nil, true},
	{`{ "foo" + "foo" }`, nil, true},
	{`{ "foo" % "foo" }`, nil, true},
	// Int.
	{`{ 1 && 1 }`, nil, true},
	{`{ 1 || 1 }`, nil, true},
	{`{ 1 =~ 1 }`, nil, true},
	{`{ 1 !~ 1 }`, nil, true},
	// Number.
	{`{ 3.14 && 3.14 }`, nil, true},
	{`{ 3.14 || 3.14 }`, nil, true},
	{`{ 3.14 =~ 3.14 }`, nil, true},
	{`{ 3.14 !~ 3.14 }`, nil, true},
	// Bool.
	{`{ true / true }`, nil, true},
	{`{ true < true }`, nil, true},
	{`{ true =~ true }`, nil, true},
	{`{ true !~ true }`, nil, true},
	// Nil.
	{`{ nil - nil }`, nil, true},
	{`{ nil =~ nil }`, nil, true},
	{`{ nil !~ nil }`, nil, true},
	{`{ nil < nil }`, nil, true},
	// Duration.
	{`{ 5h && 5h }`, nil, true},
	{`{ 5h || 5h }`, nil, true},
	{`{ 5h =~ 5h }`, nil, true},
	{`{ 5h !~ 5h }`, nil, true},
	// Status.
	{`{ ok && ok }`, nil, true},
	{`{ ok || ok }`, nil, true},
	{`{ ok ^ ok }`, nil, true},
	{`{ ok =~ ok }`, nil, true},
	{`{ ok !~ ok }`, nil, true},
	{`{ ok <= ok }`, nil, true},
	// Kind.
	{`{ client && client }`, nil, true},
	{`{ client || client }`, nil, true},
	{`{ client * client }`, nil, true},
	{`{ client =~ client }`, nil, true},
	{`{ client !~ client }`, nil, true},
	{`{ client >= client }`, nil, true},

	// Pattern must be a static string.
	{`{ .foo =~ .dynamic_regex }`, nil, true},
	{`{ .foo !~ .dynamic_regex }`, nil, true},
	{`{ .foo =~ (.dynamic_regex + 10) }`, nil, true},
}

func TestTypeCheck(t *testing.T) {
	for i, tt := range typeTests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Input:\n%s", tt.input)
				}
			}()

			got, err := Parse(tt.input)
			if err != nil {
				t.Logf("Error: \n%+v", err)
			}
			if tt.wantErr {
				var se *TypeError
				require.ErrorAs(t, err, &se)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
