package config

import (
	"strconv"
	"sync"

	"github.com/go-faster/errors"
	"github.com/go-faster/figureout"
	fjson "github.com/go-faster/figureout/source/json"
	fyaml "github.com/go-faster/figureout/source/yaml"
	"go.uber.org/zap/zapcore"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// Registry describes the named scalar types the config blocks use.
//
// figureout derives a semantic type from the underlying Go kind, so a named type that parses its
// own text — [xbytes.Bytes] reading "256MiB", [zapcore.Level] reading "debug" — binds as the
// integer underneath it and rejects the spelling every existing config file uses. Worse for
// zapcore.Level: "ch_log_level: 1" is an error under go-faster/yaml and would resolve to warn.
// The registry puts each type's own parser back in front of the binder.
//
// A *bool is registered the same way, because figureout has no pointer presence and a nil pointer
// is what "the operator said nothing" means for these fields.
//
// See go-faster/figureout#36 and go-faster/figureout#34: both are one registration per type per
// source until the library covers them itself.
func Registry() *figureout.TypeRegistry { return registry() }

// sources a config block is read from. A source added without its decoders falls back to the
// derived semantic type, which is what the registry exists to override.
var sources = []figureout.SourceID{fyaml.Source, fjson.Source}

var registry = sync.OnceValue(func() *figureout.TypeRegistry {
	r := figureout.NewTypeRegistry()
	register[xbytes.Bytes](r, decodeBytes, figureout.IntegerType(),
		figureout.Shape{Kind: figureout.ShapeInteger},
		figureout.Shape{Kind: figureout.ShapeString},
	)
	register[zapcore.Level](r, decodeLevel, figureout.StringType(),
		figureout.Shape{Kind: figureout.ShapeString},
	)
	register[*bool](r, decodeBoolPointer, figureout.BooleanType(),
		figureout.Shape{Kind: figureout.ShapeBoolean},
	)
	return r
})

func register[T any](
	r *figureout.TypeRegistry,
	decode func(string) (any, error),
	kind figureout.TypeOption,
	shapes ...figureout.Shape,
) {
	opts := []figureout.TypeOption{kind}
	for _, id := range sources {
		opts = append(opts, figureout.TypeFieldOptions(
			figureout.WithDecoder(id, textDecoder(decode), shapes...),
		))
	}
	figureout.MustRegisterType[T](r, opts...)
}

// textDecoder adapts a text parser to [figureout.Decoder].
//
// A tree source hands over the text it read for a scalar, and env and file hand over their value,
// so every source reaches the parser as a string.
type textDecoder func(string) (any, error)

// DecodeValue implements [figureout.Decoder].
func (d textDecoder) DecodeValue(raw any) (any, error) {
	switch v := raw.(type) {
	case string:
		return d(v)
	case int64:
		return d(strconv.FormatInt(v, 10))
	case bool:
		return d(strconv.FormatBool(v))
	default:
		return nil, errors.Errorf("cannot decode %T", raw)
	}
}

func decodeBytes(text string) (any, error) {
	var v xbytes.Bytes
	if err := v.UnmarshalText([]byte(text)); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeLevel(text string) (any, error) {
	var v zapcore.Level
	if err := v.UnmarshalText([]byte(text)); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeBoolPointer(text string) (any, error) {
	v, err := strconv.ParseBool(text)
	if err != nil {
		return nil, errors.Errorf("invalid boolean %q", text)
	}
	return &v, nil
}
