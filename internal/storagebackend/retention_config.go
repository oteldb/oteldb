package storagebackend

import (
	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"
)

type removedSignalBudgets []byte

func removedSignalBudgetError() error {
	return errors.New("retention.max_bytes_per_signal was removed; use retention.signals.<signal>.max_bytes")
}

// UnmarshalYAML rejects the removed config key.
func (*removedSignalBudgets) UnmarshalYAML(*yaml.Node) error { return removedSignalBudgetError() }

// UnmarshalJSON rejects the removed config key.
func (*removedSignalBudgets) UnmarshalJSON([]byte) error { return removedSignalBudgetError() }
