package logparser

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
)

func TestLogFmtParser(t *testing.T) {
	const name = "logfmt"
	files, err := os.ReadDir(filepath.Join("_testdata", name))
	require.NoError(t, err, "read testdata")

	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("_testdata", name, file.Name()))
			require.NoError(t, err, "read testdata")

			var parser LogFmtParser

			scanner := bufio.NewScanner(bytes.NewReader(data))

			var i int
			for scanner.Scan() {
				s := strings.TrimSpace(scanner.Text())
				if s == "" {
					continue
				}
				i++
				t.Run(fmt.Sprintf("Line%02d", i), func(t *testing.T) {
					t.Logf("%s", s)
					t.Run("Detect", func(t *testing.T) {
						detected := parser.Detect(s)
						require.True(t, detected)
					})
					t.Run("Parse", func(t *testing.T) {
						line, err := parser.Parse([]byte(s))
						require.NoError(t, err, "parse")
						fileName := fmt.Sprintf("%s_%s_%02d.json",
							name, strings.TrimSuffix(file.Name(), filepath.Ext(file.Name())), i,
						)
						gold.Str(t, line.String(), fileName)
					})
				})
			}
		})
	}
}
