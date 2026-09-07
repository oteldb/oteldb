package storagebackup

import (
	"slices"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// ParseSignals parses a comma-separated signal list ("log,trace,metric") as odbbackup and
// odbrestore accept it. An empty list selects every backed-up signal.
func ParseSignals(list string) ([]signal.Signal, error) {
	list = strings.TrimSpace(list)
	if list == "" {
		return nil, nil
	}

	var out []signal.Signal
	for name := range strings.SplitSeq(list, ",") {
		sig, err := signal.ParseSignal(strings.TrimSpace(name))
		if err != nil {
			return nil, err
		}
		if !slices.Contains(BackupSignals, sig) {
			return nil, errors.Errorf("signal %s is not backed up", sig)
		}
		out = append(out, sig)
	}
	return out, nil
}
