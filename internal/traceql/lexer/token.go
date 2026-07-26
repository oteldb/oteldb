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

//go:generate go tool stringer -type=TokenType

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
	TraceID         // traceID
	SpanID          // spanID
	ParentID        // parentID
	TimeSinceStart  // timeSinceStart
	Version         // version

	// Structural spanset operators.
	//
	// NOTE: child is [Gt], parent is [Lt], descendant is [Desc],
	// sibling is [Tilde] and not-sibling is [NotRe].
	Ance         // <<
	NotChild     // !>
	NotParent    // !<
	NotDesc      // !>>
	NotAnce      // !<<
	UnionChild   // &>
	UnionParent  // &<
	UnionDesc    // &>>
	UnionAnce    // &<<
	UnionSibling // &~

	// Metrics aggregations.
	Rate              // rate
	CountOverTime     // count_over_time
	MinOverTime       // min_over_time
	MaxOverTime       // max_over_time
	SumOverTime       // sum_over_time
	AvgOverTime       // avg_over_time
	QuantileOverTime  // quantile_over_time
	HistogramOverTime // histogram_over_time

	// Metrics second stage functions.
	TopK    // topk
	BottomK // bottomk

	// NOTE: keep this block append-only, the generated stringer indexes
	// constants by position.
	Compare // compare
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
	"<<":              Ance,
	"!>":              NotChild,
	"!<":              NotParent,
	"!>>":             NotDesc,
	"!<<":             NotAnce,
	"&>":              UnionChild,
	"&<":              UnionParent,
	"&>>":             UnionDesc,
	"&<<":             UnionAnce,
	"&~":              UnionSibling,
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
	"traceID":         TraceID,
	"spanID":          SpanID,
	"parentID":        ParentID,
	"timeSinceStart":  TimeSinceStart,
	"version":         Version,

	"rate":                Rate,
	"count_over_time":     CountOverTime,
	"min_over_time":       MinOverTime,
	"max_over_time":       MaxOverTime,
	"sum_over_time":       SumOverTime,
	"avg_over_time":       AvgOverTime,
	"quantile_over_time":  QuantileOverTime,
	"histogram_over_time": HistogramOverTime,

	"compare": Compare,

	"topk":    TopK,
	"bottomk": BottomK,
}
