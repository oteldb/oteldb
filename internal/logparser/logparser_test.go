package logparser

import (
	"bufio"
	"bytes"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type testDataLine struct {
	Filename   string
	LineNumber int
	Value      string
}

func (t testDataLine) GoldenFile(format string) string {
	ext := filepath.Ext(t.Filename)
	return fmt.Sprintf("%s_%s_%02d.json",
		format,
		strings.TrimSuffix(t.Filename, ext),
		t.LineNumber,
	)
}

func readTestData(t require.TestingT, format string) iter.Seq[testDataLine] {
	return func(yield func(testDataLine) bool) {
		dir := filepath.Join("_testdata", format)

		files, err := os.ReadDir(dir)
		require.NoError(t, err, "read testdata directory")

		for _, file := range files {
			data, err := os.ReadFile(filepath.Join(dir, file.Name()))
			require.NoError(t, err, "read testdata file")

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

				line := testDataLine{
					Filename:   file.Name(),
					LineNumber: i,
					Value:      s,
				}
				if !yield(line) {
					return
				}
			}
		}
	}
}

func testParser(format string) func(*testing.T) {
	var (
		observedDate      = time.Date(2026, time.January, 20, 1, 2, 3, 4, time.UTC)
		observedTimestamp = pcommon.NewTimestampFromTime(observedDate)
	)
	return func(t *testing.T) {
		parser, ok := LookupFormat(format)
		if !ok {
			t.Fatalf("unknown format %q", format)
		}

		for line := range readTestData(t, format) {
			t.Run(line.Filename, func(t *testing.T) {
				t.Run(fmt.Sprintf("Line%02d", line.LineNumber), func(t *testing.T) {
					s := line.Value

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
						gold.Str(t, target.String(), line.GoldenFile(format))
					})
				})
			})
		}
	}
}

func fuzzParser(f *testing.F, format string) {
	f.Helper()

	for line := range readTestData(f, format) {
		f.Add(line.Value)
	}
	var parser KLogParser
	f.Fuzz(func(t *testing.T, input string) {
		detected := parser.Detect(input)

		var target Record
		if err := parser.Parse(input, &target); err != nil {
			return
		}
		require.True(t, detected)
	})
}
