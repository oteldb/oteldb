package logparser

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func testParser(name string) func(*testing.T) {
	var (
		observedDate      = time.Date(2026, time.January, 20, 1, 2, 3, 4, time.UTC)
		observedTimestamp = pcommon.NewTimestampFromTime(observedDate)
	)
	return func(t *testing.T) {
		files, err := os.ReadDir(filepath.Join("_testdata", name))
		require.NoError(t, err, "read testdata")

		parser, ok := LookupFormat(name)
		if !ok {
			t.Fatalf("unknown format %q", name)
		}

		for _, file := range files {
			t.Run(file.Name(), func(t *testing.T) {
				data, err := os.ReadFile(filepath.Join("_testdata", name, file.Name()))
				require.NoError(t, err, "read testdata")

				var (
					i       int
					scanner = bufio.NewScanner(bytes.NewReader(data))
				)
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
							target := Record{
								ObservedTimestamp: observedTimestamp,
							}
							err := parser.Parse(s, &target)
							require.NoError(t, err, "parse")
							fileName := fmt.Sprintf("%s_%s_%02d.json",
								name, strings.TrimSuffix(file.Name(), filepath.Ext(file.Name())), i,
							)
							gold.Str(t, target.String(), fileName)
						})
					})
				}
			})
		}
	}
}
