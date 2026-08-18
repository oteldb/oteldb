package config

import (
	"os"
	"path/filepath"

	"github.com/go-faster/figureout"
	fyaml "github.com/go-faster/figureout/source/yaml"
)

// Resolve reads a YAML config file into C through its descriptor.
//
// It is [Load] with the descriptor's guarantees: an unknown key is a startup error naming the
// offending path rather than a value silently dropped, and the report says which source set each
// field. Defaults are still not applied — the caller decides whether they run before or after its
// own environment overrides.
func Resolve[C any](d *figureout.Descriptor[C], name string, opts LoadOptions) (cfg C, _ *figureout.Report, _ error) {
	if name == "" {
		name = opts.Fallback
		if opts.Optional {
			if _, err := os.Stat(name); err != nil {
				return cfg, nil, nil
			}
		}
	}

	return d.Resolve(fyaml.File(filepath.Clean(name), fyaml.DisallowUnknownFields()))
}

// Descriptor compiles describe once, reporting a broken description as an error on the path that
// can print it rather than as a panic in package initialization.
//
// There is no type registry: figureout defers to a named scalar's own UnmarshalText, so
// [xbytes.Bytes] and [zapcore.Level] read the spellings every config file already uses without a
// registration of their own.
func Descriptor[C any](describe func(*C, *figureout.Schema[C])) (*figureout.Descriptor[C], error) {
	return figureout.Derive(describe)
}
