package profileqlengine

import (
	"bytes"
	"html/template"

	"github.com/go-faster/errors"
)

// HTML renders the result into a standalone HTML flamegraph page with the
// flamebearer profile embedded as JSON.
func (r *Result) HTML() ([]byte, error) {
	profile := r.Flamebearer()
	data, err := profile.MarshalJSON()
	if err != nil {
		return nil, errors.Wrap(err, "marshal flamebearer")
	}

	var buf bytes.Buffer
	if err := standaloneTemplate.Execute(&buf, map[string]any{
		"Title":      r.Type.String(),
		"Flamegraph": template.JS(data), //nolint:gosec // data is generated JSON, not user input.
	}); err != nil {
		return nil, errors.Wrap(err, "execute template")
	}
	return buf.Bytes(), nil
}

// standaloneTemplate renders a self-contained flamegraph page. The inline
// renderer decodes the flamebearer "single" format (per-level chunks of
// [xOffset delta, total, self, nameIndex]).
var standaloneTemplate = template.Must(template.New("standalone").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{ .Title }}</title>
<style>
  body { font: 12px monospace; margin: 0; padding: 8px; }
  #flamegraph { position: relative; }
  .frame {
    position: absolute; height: 17px; line-height: 17px;
    overflow: hidden; white-space: nowrap; box-sizing: border-box;
    border: 1px solid #fff; background: #eb9; padding: 0 2px;
    font-size: 11px; cursor: default;
  }
  .frame:hover { background: #fc9; }
</style>
</head>
<body>
<h3>{{ .Title }}</h3>
<div id="flamegraph"></div>
<script type="text/javascript">
window.flamegraph = {{ .Flamegraph }};
(function () {
  var fb = window.flamegraph.flamebearer;
  var names = fb.names, levels = fb.levels, numTicks = fb.numTicks || 1;
  var root = document.getElementById("flamegraph");
  var rowH = 18;
  root.style.height = (levels.length * rowH) + "px";
  for (var depth = 0; depth < levels.length; depth++) {
    var level = levels[depth], prev = 0;
    for (var i = 0; i < level.length; i += 4) {
      var x = prev + level[i], total = level[i + 1], self = level[i + 2];
      var name = names[level[i + 3]] || "";
      var el = document.createElement("div");
      el.className = "frame";
      el.style.left = (x / numTicks * 100) + "%";
      el.style.width = (total / numTicks * 100) + "%";
      el.style.top = (depth * rowH) + "px";
      el.title = name + " (" + total + " total, " + self + " self)";
      el.textContent = name;
      root.appendChild(el);
      prev = x + total;
    }
  }
})();
</script>
</body>
</html>
`))
