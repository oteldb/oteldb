package lexer

import "text/scanner"

// Token is a TraceQL token.
type Token struct {
	Type TokenType
	Text string
	Pos  scanner.Position
}

// TokenType defines TraceQL token type.
type TokenType int

//go:generate go run golang.org/x/tools/cmd/stringer -type=TokenType

const (
	Invalid TokenType = iota
	EOF
	Ident
	// Literals
	String
	Integer
	Number
	Duration

	Comma
	Dot
	OpenBrace
	CloseBrace
	OpenParen
	CloseParen
	Eq
	NotEq
	Re
	NotRe
	Gt
	Gte
	Lt
	Lte
	Add
	Sub
	Div
	Mod
	Mul
	Pow
	True
	False
	Nil
	StatusOk
	StatusError
	StatusUnset
	KindUnspecified
	KindInternal
	KindServer
	KindClient
	KindProducer
	KindConsumer
	And
	Or
	Not
	Pipe
	Desc
	Tilde
	SpanDuration
	ChildCount
	Name
	Status
	Kind
	RootName
	RootServiceName
	TraceDuration
	Parent
	Count
	Avg
	Max
	Min
	Sum
	By
	Coalesce
	Select

	// Scoped intrinsic colon prefixes.
	TraceColon
	SpanColon
	EventColon
	LinkColon
	InstrumentationColon

	// New intrinsic keywords.
	StatusMessage   // statusMessage
	RootService     // rootService
	NestedSetLeft   // nestedSetLeft
	NestedSetRight  // nestedSetRight
	NestedSetParent // nestedSetParent
	ID              // id
	TraceID         // traceId
	SpanID          // spanId
	ParentID        // parentId
	TimeSinceStart  // timeSinceStart
	Version         // version
)

var tokens = map[string]TokenType{
	",":               Comma,
	".":               Dot,
	"{":               OpenBrace,
	"}":               CloseBrace,
	"(":               OpenParen,
	")":               CloseParen,
	"=":               Eq,
	"!=":              NotEq,
	"=~":              Re,
	"!~":              NotRe,
	">":               Gt,
	">=":              Gte,
	"<":               Lt,
	"<=":              Lte,
	"+":               Add,
	"-":               Sub,
	"/":               Div,
	"%":               Mod,
	"*":               Mul,
	"^":               Pow,
	"true":            True,
	"false":           False,
	"nil":             Nil,
	"ok":              StatusOk,
	"error":           StatusError,
	"unset":           StatusUnset,
	"unspecified":     KindUnspecified,
	"internal":        KindInternal,
	"server":          KindServer,
	"client":          KindClient,
	"producer":        KindProducer,
	"consumer":        KindConsumer,
	"&&":              And,
	"||":              Or,
	"!":               Not,
	"|":               Pipe,
	">>":              Desc,
	"~":               Tilde,
	"duration":        SpanDuration,
	"childCount":      ChildCount,
	"name":            Name,
	"status":          Status,
	"kind":            Kind,
	"rootName":        RootName,
	"rootServiceName": RootServiceName,
	"traceDuration":   TraceDuration,
	"parent":          Parent,
	"count":           Count,
	"avg":             Avg,
	"max":             Max,
	"min":             Min,
	"sum":             Sum,
	"by":              By,
	"coalesce":        Coalesce,
	"select":          Select,
	"statusMessage":   StatusMessage,
	"rootService":     RootService,
	"nestedSetLeft":   NestedSetLeft,
	"nestedSetRight":  NestedSetRight,
	"nestedSetParent": NestedSetParent,
	"id":              ID,
	"traceId":         TraceID,
	"spanId":          SpanID,
	"parentId":        ParentID,
	"timeSinceStart":  TimeSinceStart,
	"version":         Version,
}
