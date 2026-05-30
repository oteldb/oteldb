package output

import (
	_ "embed"
	"html/template"
	"log"
	"os"

	"github.com/oteldb/oteldb/internal/promcompliance/comparer"
	"github.com/oteldb/oteldb/internal/promcompliance/config"
)

var funcMap = map[string]interface{}{
	"include": func(includePassing bool, result *comparer.Result) bool {
		return includePassing || !result.Success()
	},
	"numResults": func(results []*comparer.Result) int {
		return len(results)
	},
	"numPassed": func(results []*comparer.Result) int {
		num := 0
		for _, r := range results {
			if r.Success() {
				num++
			}
		}
		return num
	},
	"numFailed": func(results []*comparer.Result) int {
		num := 0
		for _, r := range results {
			if !r.Success() {
				num++
			}
		}
		return num
	},
	"percent": func(part, total int) float64 {
		return 100 * float64(part) / float64(total)
	},
}

//go:embed example-output.html
var defaultTemplateSource string

func getTemplate(templatePath string) (*template.Template, error) {
	t := template.New("output.html").Funcs(funcMap)
	if templatePath == "" {
		return t.Parse(defaultTemplateSource)
	}
	return t.ParseFiles(templatePath)
}

// HTML produces HTML output for a number of query results.
func HTML(tplFile string) (Outputter, error) {
	t, err := getTemplate(tplFile)
	if err != nil {
		return nil, err
	}

	return func(results []*comparer.Result, includePassing bool, _ []*config.QueryTweak) {
		err := t.Execute(os.Stdout, struct {
			Results        []*comparer.Result
			IncludePassing bool
		}{
			Results:        results,
			IncludePassing: includePassing,
		})
		if err != nil {
			log.Println("executing template:", err)
		}
	}, nil
}
