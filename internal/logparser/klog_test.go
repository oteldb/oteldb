package logparser

import (
	"testing"
)

func TestKLogParser(t *testing.T) {
	// heavily relies on time.Now.
	testParser("klog")(t)
}
