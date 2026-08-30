package otelstorage

import (
	"strconv"
	"strings"
)

// UnescapeLabelName decodes a Prometheus value-encoded label name, turning
// U__k8s_2e_node_2e_name back into k8s.node.name. A name without the U__ prefix is returned
// unchanged, and so is one whose escape sequences do not decode — the scheme is not
// round-trippable, so a name that merely looks escaped must survive intact.
//
// Grafana escapes every label name it puts in a URL path or a selector, so a store holding OTel
// attribute names (which are dotted) never matches unless the name is decoded first.
func UnescapeLabelName(v string) string {
	if !strings.HasPrefix(v, "U__") {
		return v
	}

	var (
		sb    strings.Builder
		runes = []rune(v[3:])
	)
	for i := 0; i < len(runes); i++ {
		if runes[i] == '_' && i+3 < len(runes) && runes[i+3] == '_' {
			hexNumber := string([]rune{runes[i+1], runes[i+2]})
			if b, err := strconv.ParseUint(hexNumber, 16, 8); err == nil {
				sb.WriteByte(byte(b))
				i += 3
				continue
			}
		}
		sb.WriteRune(runes[i])
	}

	return sb.String()
}
