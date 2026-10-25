package scarecrow

import (
	"maps"
	"sync"

	"github.com/prometheus/prometheus/util/annotations"
)

type annotationSink struct {
	mu     sync.Mutex
	values annotations.Annotations
}

func (s *annotationSink) add(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values.Add(err)
}

func (s *annotationSink) snapshot() annotations.Annotations {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.values)
}
