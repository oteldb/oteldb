package logstorage

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logparser"
)

// ConsumerOptions defines options for telemetry pre-processing.
type ConsumerOptions struct {
	// TriggerAttributes defines a list of attribute references to trigger format detection.
	TriggerAttributes []AttributeRef
	// FormatAttributes defines a list of attribute references to choose format.
	//
	// Format attribute may define multple formats via comma.
	// The first valid would be used to parse the record.
	//
	// If attribute value is absent, have an unknown or invalid value, the record would be passed as-is.
	FormatAttributes []AttributeRef
	// DetectFormats defines list of formats to detect.
	DetectFormats []logparser.Parser

	// MeterProvider provides OpenTelemetry meter.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer.
	TracerProvider trace.TracerProvider
}

func (o *ConsumerOptions) setDefaults() {
	if o.TriggerAttributes == nil {
		o.TriggerAttributes = []AttributeRef{
			{Name: "log", Location: AttributesLocation},
		}
	}
	if o.DetectFormats == nil {
		o.DetectFormats = []logparser.Parser{new(logparser.GenericJSONParser)}
	}
	if o.MeterProvider == nil {
		o.MeterProvider = otel.GetMeterProvider()
	}
	if o.TracerProvider == nil {
		o.TracerProvider = otel.GetTracerProvider()
	}
}

// AttributeRef define an attribute ref.
type AttributeRef struct {
	Name     string
	Location AttributeLocation
}

// ParseAttributeRef parses [AttributeRef] from given string.
func ParseAttributeRef(ref string) (AttributeRef, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return AttributeRef{}, errors.New("attribute ref is empty")
	}
	for loc, prefix := range locations {
		if name, ok := strings.CutPrefix(ref, prefix); ok {
			return AttributeRef{Name: name, Location: loc}, nil
		}
	}
	return AttributeRef{
		Name:     ref,
		Location: AnyLocation,
	}, nil
}

// String implements [fmt.Stringer].
func (r AttributeRef) String() string {
	return r.Location.String() + r.Name
}

// Evaluate evaluates reference on given record.
func (r AttributeRef) Evaluate(body pcommon.Value, record Record) (pcommon.Value, bool) {
	switch r.Location {
	case BodyLocation:
		if body.Type() != pcommon.ValueTypeMap {
			return pcommon.Value{}, false
		}
		return body.Map().Get(r.Name)
	case AttributesLocation:
		return record.Attrs.AsMap().Get(r.Name)
	case ScopeLocation:
		return record.ScopeAttrs.AsMap().Get(r.Name)
	case ResourceLocation:
		return record.ResourceAttrs.AsMap().Get(r.Name)
	default:
		if body.Type() == pcommon.ValueTypeMap {
			if v, ok := body.Map().Get(r.Name); ok {
				return v, ok
			}
		}
		for _, set := range []pcommon.Map{
			record.Attrs.AsMap(),
			record.ScopeAttrs.AsMap(),
			record.ResourceAttrs.AsMap(),
		} {
			if v, ok := set.Get(r.Name); ok {
				return v, ok
			}
		}
		return pcommon.Value{}, false
	}
}

// AttributeLocation defines the location to look for attribute.
type AttributeLocation uint8

const (
	// AnyLocation looks for first occurrence of attribute in the following order:
	// 	record body, record attributes, record scope, record resource.
	AnyLocation = AttributeLocation(iota)
	// BodyLocation looks for a value in record body.
	BodyLocation
	// AttributesLocation looks for a value in record attributes.
	AttributesLocation
	// ScopeLocation looks for a value in scope attributes.
	ScopeLocation
	// ResourceLocation looks for a value in resource attributes.
	ResourceLocation
)

// String implements [fmt.Stringer].
func (l AttributeLocation) String() string {
	if s, ok := locations[l]; ok {
		return s
	}
	return fmt.Sprintf("AttributeLocation(%d)", uint8(l))
}

var locations = map[AttributeLocation]string{
	BodyLocation:       "body.",
	AttributesLocation: "attributes.",
	ScopeLocation:      "scope.",
	ResourceLocation:   "resource.",
}
