package promrw_test

import (
	"fmt"
	"strings"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// dump renders a batch as text, so a difference between the direct converter and the pdata path
// reads as a diff of the data instead of a diff of two deep struct dumps.
func dump(m *metric.Metrics) string {
	var sb strings.Builder
	for i := range m.Resources {
		rm := &m.Resources[i]
		fmt.Fprintf(&sb, "resource %s %s\n", rm.Resource.SchemaURL, dumpAttrs(rm.Resource.Attributes))

		for j := range rm.Scopes {
			sm := &rm.Scopes[j]
			fmt.Fprintf(&sb, " scope %s %s %s %s\n",
				sm.Scope.Name, sm.Scope.Version, sm.Scope.SchemaURL, dumpAttrs(sm.Scope.Attributes))

			for k := range sm.Metrics {
				mt := &sm.Metrics[k]
				fmt.Fprintf(&sb, "  metric %s unit=%s kind=%d temporality=%d monotonic=%v\n",
					mt.Name, mt.Unit, mt.Kind, mt.Temporality, mt.Monotonic)

				for p := range mt.Points {
					pt := &mt.Points[p]
					fmt.Fprintf(&sb, "   point start=%d ts=%d value=%v %s\n",
						pt.StartTs, pt.Ts, pt.Value, dumpAttrs(pt.Attributes))
				}
			}
		}
	}
	return sb.String()
}

func dumpAttrs(attrs signal.Attributes) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, kv := range attrs {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.Write(kv.Key)
		sb.WriteByte('=')
		sb.Write(kv.Value.AppendText(nil))
	}
	sb.WriteByte('}')
	return sb.String()
}
