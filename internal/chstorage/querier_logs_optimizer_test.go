package chstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/logql"
)

func TestClickhouseOptimizer_offloadLabelFilters(t *testing.T) {
	var o ClickhouseOptimizer

	tests := []struct {
		input string
		want  []string
	}{
		// No label filters: nothing to offload.
		{`{job=".+"}`, nil},
		{`{job=".+"} |= "HEAD"`, nil},

		// Parsing label filters: nothing to offload.
		{`{job=".+"} | method == 10`, nil},
		{`{job=".+"} | method == 10s`, nil},
		{`{job=".+"} | method == 10mb`, nil},
		{`{job=".+"} | method = ip("127.0.0.1")`, nil},
		{`{job=".+"} | method = "HEAD" or method == 10`, nil},
		// Ensure that integer label filter affects __error__.
		{
			`{job=".+"}
			| method = "HEAD"
			| __error__ = "error" | __error_details__ = "error details"
			| method = "GET" or code == 10
			| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`,
				`__error__="error"`, `__error_details__="error details"`,
			},
		},

		// Simple cases.
		{`{job=".+"} | method = "HEAD"`, []string{`method="HEAD"`}},
		{`{job=".+"} | method = "HEAD" or method = "GET"`, []string{`method="HEAD" or method="GET"`}},
		{`{job=".+"} | method = "HEAD", method = "GET"`, []string{`method="HEAD" and method="GET"`}},

		// Line filter.
		//
		// Line filters does not affect labels.
		{
			`{job=".+"}
			| method = "HEAD"
			|= "error"
			| method = "GET"`,
			[]string{
				`method="HEAD"`,
				`method="GET"`,
			},
		},
		{
			`{job=".+"}
			| method = "HEAD"
			|= ip("127.0.0.1")
			| method = "GET"`,
			[]string{
				`method="HEAD"`,
				`method="GET"`,
			},
		},
		{
			`{job=".+"}
			| method = "HEAD"
			|> "<_> foo <_>"
			| method = "GET"`,
			[]string{
				`method="HEAD"`,
				`method="GET"`,
			},
		},

		// Parsing.
		//
		// JSON parser.
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| json
				| method != "GET" | status != "error"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`, `status="ok"`,
			},
		},
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| json status
				| method != "GET" | status != "error"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`, `status="ok"`,
				`method!="GET"`,
			},
		},
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| json protocol="request.protocol"
				| method != "GET" | status != "error"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`, `status="ok"`,
				`method!="GET"`, `status!="error"`,
			},
		},
		// Logfmt parser.
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| logfmt
				| method != "GET" | status != "error"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`, `status="ok"`,
			},
		},
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| logfmt status
				| method != "GET" | status != "error"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`, `status="ok"`,
				`method!="GET"`,
			},
		},
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| logfmt protocol="request.protocol"
				| method != "GET" | status != "error"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`, `status="ok"`,
				`method!="GET"`, `status!="error"`,
			},
		},
		// Regexp parser.
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| regexp "(?P<method>\\w+)"
				| method != "GET" | status != "error"`,
			[]string{
				`method="HEAD"`, `status="ok"`,
				`status!="error"`,
			},
		},
		// Pattern parser.
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| pattern "<method> <path>"
				| method != "GET" | status != "error"`,
			[]string{
				`method="HEAD"`, `status="ok"`,
				`status!="error"`,
			},
		},
		// Unpack parser.
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| unpack
				| method != "GET" | status != "error"`,
			[]string{
				`method="HEAD"`, `status="ok"`,
			},
		},

		// Line format.
		//
		// line_format affects `__error__` and `__error_details__`.
		{
			`{job=".+"}
				| method = "HEAD"
				| line_format "{{ . }}"
				| method != "GET"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`,
				`method!="GET"`,
			},
		},

		// Decolorize.
		//
		// Decolorize does not affect labels.
		{
			`{job=".+"}
			| method = "HEAD"
			| decolorize
			| method = "GET"`,
			[]string{
				`method="HEAD"`,
				`method="GET"`,
			},
		},

		// Label format.
		{
			`{job=".+"}
				| method = "HEAD"
				| label_format method="{{ . }}"
				| method != "GET"`,
			[]string{
				`method="HEAD"`,
			},
		},
		{
			`{job=".+"}
				| method = "HEAD" | status = "ok"
				| label_format method=status
				| method != "GET" | status != "error"`,
			[]string{
				`method="HEAD"`, `status="ok"`,
				`status!="error"`,
			},
		},
		// label_format affects __error__.
		{
			`{job=".+"}
				| method = "HEAD"
				| label_format status="{{ . }}"
				| method != "GET"
				| __error__ != "" | __error_details__ != ""`,
			[]string{
				`method="HEAD"`,
				`method!="GET"`,
			},
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Query:\n%s", tt.input)
				}
			}()

			expr, err := logql.Parse(tt.input, logql.ParseOptions{AllowDots: false})
			require.NoError(t, err)
			logExpr := expr.(*logql.LogExpr)

			var (
				offloaded = o.offloadLabelFilters(logExpr.Pipeline)
				got       = make([]string, len(offloaded))
			)
			for i, pred := range offloaded {
				got[i] = pred.String()
			}
			if len(offloaded) == 0 {
				got = nil
			}

			require.Equal(t, tt.want, got)
		})
	}
}
