// Package logparser parses logs.
package logparser

import "sync"

var formatRegistry = new(sync.Map)

// RegisterFormat register a [Parser] for specified format name.
func RegisterFormat(f string, p Parser) {
	formatRegistry.Store(f, p)
}

// LookupFormat returns a [Parser] instance for specified format name.
func LookupFormat(f string) (Parser, bool) {
	v, ok := formatRegistry.Load(f)
	if !ok {
		return nil, false
	}
	return v.(Parser), ok
}

// Parser parses logs.
type Parser interface {
	// Parse parses data and returns a line.
	//
	// TODO: refactor to `Parse(data []byte, target *logstorage.Record) error`
	Parse(data []byte) (*Line, error)
	// Detect whether data is parsable.
	//
	// TODO: refactor to `Detect(data []byte) bool`
	Detect(line string) bool
	String() string
}
