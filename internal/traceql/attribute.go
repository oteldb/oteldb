package traceql

import (
	"fmt"
	"strings"

	"github.com/go-faster/errors"
)

// ParseAttribute parses attribute from given string.
func ParseAttribute(attr string) (a Attribute, _ error) {
	p, err := newParser(attr)
	if err != nil {
		return a, err
	}

	a, ok := p.tryAttribute()
	if !ok {
		return a, errors.Errorf("invalid attribute %q", attr)
	}

	return a, nil
}

// Attribute is a span attribute.
type Attribute struct {
	Name   string
	Scope  AttributeScope
	Prop   SpanProperty
	Parent bool // refers to parent
}

// String implements fmt.Stringer.
func (s Attribute) String() string {
	switch s.Prop {
	case SpanDuration:
		return "duration"
	case SpanChildCount:
		return "childCount"
	case SpanName:
		return "name"
	case SpanStatus:
		return "status"
	case SpanKind:
		return "kind"
	case SpanParent:
		return "parent"
	case RootSpanName:
		return "rootName"
	case RootServiceName:
		return "rootServiceName"
	case TraceDuration:
		return "traceDuration"
	case SpanStatusMessage:
		return "statusMessage"
	case NestedSetLeft:
		return "nestedSetLeft"
	case NestedSetRight:
		return "nestedSetRight"
	case NestedSetParent:
		return "nestedSetParent"
	case SpanID:
		return "span:id"
	case ParentID:
		return "span:parentId"
	case TraceID:
		return "trace:id"
	case EventName:
		return "event:name"
	case EventTimeSinceStart:
		return "event:timeSinceStart"
	case LinkTraceID:
		return "link:traceId"
	case LinkSpanID:
		return "link:spanId"
	case InstrumentationName:
		return "instrumentation:name"
	case InstrumentationVersion:
		return "instrumentation:version"
	default:
		// SpanAttribute.
		var (
			sb      strings.Builder
			needDot = true
		)
		if s.Parent {
			sb.WriteString("parent.")
			needDot = false
		}
		switch s.Scope {
		case ScopeResource:
			sb.WriteString("resource")
			needDot = true
		case ScopeSpan:
			sb.WriteString("span")
			needDot = true
		case ScopeInstrumentation:
			sb.WriteString("instrumentation")
			needDot = true
		case ScopeEvent:
			sb.WriteString("event")
			needDot = true
		case ScopeLink:
			sb.WriteString("link")
			needDot = true
		}
		if needDot {
			sb.WriteByte('.')
		}
		sb.WriteString(s.Name)
		return sb.String()
	}
}

// ValueType returns value type of expression.
func (s *Attribute) ValueType() StaticType {
	switch s.Prop {
	case SpanDuration:
		return TypeDuration
	case SpanChildCount:
		return TypeInt
	case SpanName:
		return TypeString
	case SpanStatus:
		return TypeSpanStatus
	case SpanStatusMessage:
		return TypeString
	case SpanKind:
		return TypeSpanKind
	case SpanParent:
		return TypeNil
	case RootSpanName:
		return TypeString
	case RootServiceName:
		return TypeString
	case TraceDuration:
		return TypeDuration
	case NestedSetLeft, NestedSetRight, NestedSetParent:
		return TypeInt
	case SpanID, ParentID, TraceID:
		return TypeString
	case EventName:
		return TypeString
	case EventTimeSinceStart:
		return TypeDuration
	case LinkTraceID, LinkSpanID:
		return TypeString
	case InstrumentationName, InstrumentationVersion:
		return TypeString
	default:
		// Type determined at execution time.
		return TypeAttribute
	}
}

// SpanProperty is a span property.
type SpanProperty uint8

const (
	SpanAttribute SpanProperty = iota
	SpanDuration
	SpanChildCount
	SpanName
	SpanStatus
	SpanKind
	SpanParent
	RootSpanName
	RootServiceName
	TraceDuration
	// Scoped intrinsics added with TraceQL v2.
	SpanStatusMessage
	NestedSetLeft
	NestedSetRight
	NestedSetParent
	SpanID
	ParentID
	TraceID
	EventName
	EventTimeSinceStart
	LinkTraceID
	LinkSpanID
	InstrumentationName
	InstrumentationVersion
)

var intrinsicNames = func() (r []string) {
	r = make([]string, 0, TraceDuration)
	for i := SpanDuration; i <= TraceDuration; i++ {
		r = append(r, Attribute{Prop: i}.String())
	}
	return r
}()

// IntrinsicNames returns a slice of intrinsics.
func IntrinsicNames() (r []string) {
	return intrinsicNames
}

// AttributeScope is an attribute scope.
type AttributeScope uint8

const (
	ScopeNone AttributeScope = iota
	ScopeResource
	ScopeSpan
	ScopeInstrumentation
	ScopeEvent
	ScopeLink
)

// String implements [fmt.Stringer].
func (s AttributeScope) String() string {
	switch s {
	case ScopeNone:
		return "none"
	case ScopeResource:
		return "resource"
	case ScopeSpan:
		return "span"
	case ScopeInstrumentation:
		return "instrumentation"
	case ScopeEvent:
		return "event"
	case ScopeLink:
		return "link"
	default:
		return fmt.Sprintf("unknown scope %d", uint8(s))
	}
}
