package logparser

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"
)

func TestGenericJSONParser_Parse(t *testing.T) {
	testParser("generic-json")(t)
}

func BenchmarkGenericJSONParser_Parse(b *testing.B) {
	b.ReportAllocs()
	data, err := os.ReadFile(filepath.Join("_testdata", "generic-json", "zap.jsonl"))
	require.NoError(b, err, "read testdata")

	var parser GenericJSONParser
	scanner := bufio.NewScanner(bytes.NewReader(data))

	var i int
	b.ResetTimer()
	for scanner.Scan() {
		i++
		b.Run(fmt.Sprintf("Line%02d", i), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(scanner.Bytes())))

			for j := 0; j < b.N; j++ {
				_, err := parser.Parse(scanner.Text())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func FuzzGenericJSONParser(f *testing.F) {
	files, err := os.ReadDir(filepath.Join("_testdata", "generic-json"))
	require.NoError(f, err, "read testdata")
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join("_testdata", "generic-json", file.Name()))
		require.NoError(f, err, "read testdata")
		s := bufio.NewScanner(bytes.NewReader(data))
		for s.Scan() {
			f.Add(s.Bytes())
		}
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		{
			d := jx.DecodeBytes(input)
			if d.Next() != jx.Object || !jx.Valid(input) {
				t.Skip()
			}
		}
		var parser GenericJSONParser
		if !parser.Detect(string(input)) {
			t.Error("Should detect")
		}
		line, err := parser.Parse(string(input))
		if err != nil {
			return
		}
		if line == nil {
			t.Fatal("line is nil")
			return
		}
		e := &jx.Encoder{}
		line.Encode(e)
		if !jx.Valid(e.Bytes()) {
			t.Fatal("invalid encoded line")
		}
	})
}
