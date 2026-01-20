package prometheusremotewrite

import (
	"os"
	"testing"

	"github.com/go-faster/sdk/gold"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}
