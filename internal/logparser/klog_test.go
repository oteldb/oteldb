package logparser

import (
	"testing"
)

func TestKLogParser(t *testing.T) {
	// heavily relies on time.Now.
	testParser("klog")(t)
}

func FuzzKLogParser(f *testing.F) {
	fuzzParser(f, "klog")
}
