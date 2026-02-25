package logparser

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZapDevelopmentParser_Parse(t *testing.T) {
	testParser("zap-development")(t)
}

func FuzzZapDevelopmentParser(f *testing.F) {
	fuzzParser(f, "zap-development")
}

func BenchmarkZapDevelopmentParser_Parse(b *testing.B) {
	b.ReportAllocs()
	data, err := os.ReadFile(filepath.Join("_testdata", "zap-development", "zapdev.txt"))
	require.NoError(b, err, "read testdata")

	var parser ZapDevelopmentParser
	scanner := bufio.NewScanner(bytes.NewReader(data))

	var i int
	b.ResetTimer()
	for scanner.Scan() {
		i++
		b.Run(fmt.Sprintf("Line%02d", i), func(b *testing.B) {
			var target Record
			b.ReportAllocs()
			b.SetBytes(int64(len(scanner.Bytes())))
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				target.Reset()
				if err := parser.Parse(scanner.Text(), &target); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
