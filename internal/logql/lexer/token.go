package lexer

import "text/scanner"

// Token is a LogQL token.
type Token struct {
	Type TokenType
	Text string
	Pos  scanner.Position
}

// TokenType defines LogQL token type.
type TokenType int

//go:generate go tool stringer -type=TokenType

// IsFunction returns true if token is function name.
func (tt TokenType) IsFunction() bool {
	switch tt {
	case Rate,
		RateCounter,
		CountOverTime,
		BytesRate,
		BytesOverTime,
		AvgOverTime,
		SumOverTime,
		MinOverTime,
		MaxOverTime,
		StdvarOverTime,
		StddevOverTime,
		QuantileOverTime,
		FirstOverTime,
		LastOverTime,
		AbsentOverTime,
		Vector,
		Sum,
		Avg,
		Max,
		Min,
		Count,
		Stddev,
		Stdvar,
		Bottomk,
		Topk,
		Sort,
		SortDesc,
		LabelReplace,
		BytesConv,
		DurationConv,
		DurationSecondsConv,
		IP:
		return true
	default:
		return false
	}
}

const (
	Invalid TokenType = iota
	EOF
	Ident
	// Literals
	String
	Number
	Duration
	Bytes

	Comma
	Dot
	OpenBrace
	CloseBrace
	Eq
	NotEq
	Re
	NotRe
	PipeExact
	PipeMatch
	PipePattern
	NotPipePattern
	Pipe
	Unwrap
	OpenParen
	CloseParen
	By
	Without
	Bool
	OpenBracket
	CloseBracket
	Offset
	On
	Ignoring
	GroupLeft
	GroupRight

	// Binary operations
	Or
	And
	Unless
	Add
	Sub
	Mul
	Div
	Mod
	Pow
	// Comparison operations
	CmpEq
	Gt
	Gte
	Lt
	Lte

	JSON
	Regexp
	Logfmt
	Unpack
	Pattern
	LabelFormat
	LineFormat
	IP
	Decolorize
	Distinct
	Drop
	Keep

	Range
	Rate
	RateCounter
	CountOverTime
	BytesRate
	BytesOverTime
	AvgOverTime
	SumOverTime
	MinOverTime
	MaxOverTime
	StdvarOverTime
	StddevOverTime
	QuantileOverTime
	FirstOverTime
	LastOverTime
	AbsentOverTime
	Vector

	Sum
	Avg
	Max
	Min
	Count
	Stddev
	Stdvar
	Bottomk
	Topk
	Sort
	SortDesc
	LabelReplace

	BytesConv
	DurationConv
	DurationSecondsConv

	ParserFlag

	// Extension: for query debugging purposes.
	Explain
)

var tokens = map[string]TokenType{
	",":           Comma,
	".":           Dot,
	"{":           OpenBrace,
	"}":           CloseBrace,
	"=":           Eq,
	"!=":          NotEq,
	"=~":          Re,
	"!~":          NotRe,
	"|=":          PipeExact,
	"|~":          PipeMatch,
	"|>":          PipePattern,
	"!>":          NotPipePattern,
	"|":           Pipe,
	"unwrap":      Unwrap,
	"(":           OpenParen,
	")":           CloseParen,
	"by":          By,
	"without":     Without,
	"bool":        Bool,
	"[":           OpenBracket,
	"]":           CloseBracket,
	"offset":      Offset,
	"on":          On,
	"ignoring":    Ignoring,
	"group_left":  GroupLeft,
	"group_right": GroupRight,

	// Binary operations.
	"or":     Or,
	"and":    And,
	"unless": Unless,
	"+":      Add,
	"-":      Sub,
	"*":      Mul,
	"/":      Div,
	"%":      Mod,
	"^":      Pow,
	// Comparison operations.
	"==": CmpEq,
	">":  Gt,
	">=": Gte,
	"<":  Lt,
	"<=": Lte,

	"json":         JSON,
	"regexp":       Regexp,
	"logfmt":       Logfmt,
	"unpack":       Unpack,
	"pattern":      Pattern,
	"label_format": LabelFormat,
	"line_format":  LineFormat,
	"ip":           IP,
	"decolorize":   Decolorize,
	"distinct":     Distinct,
	"drop":         Drop,
	"keep":         Keep,

	"rate":               Rate,
	"rate_counter":       RateCounter,
	"count_over_time":    CountOverTime,
	"bytes_rate":         BytesRate,
	"bytes_over_time":    BytesOverTime,
	"avg_over_time":      AvgOverTime,
	"sum_over_time":      SumOverTime,
	"min_over_time":      MinOverTime,
	"max_over_time":      MaxOverTime,
	"stdvar_over_time":   StdvarOverTime,
	"stddev_over_time":   StddevOverTime,
	"quantile_over_time": QuantileOverTime,
	"first_over_time":    FirstOverTime,
	"last_over_time":     LastOverTime,
	"absent_over_time":   AbsentOverTime,
	"vector":             Vector,

	"sum":           Sum,
	"avg":           Avg,
	"max":           Max,
	"min":           Min,
	"count":         Count,
	"stddev":        Stddev,
	"stdvar":        Stdvar,
	"bottomk":       Bottomk,
	"topk":          Topk,
	"sort":          Sort,
	"sort_desc":     SortDesc,
	"label_replace": LabelReplace,

	"bytes":            BytesConv,
	"duration":         DurationConv,
	"duration_seconds": DurationSecondsConv,

	"@explain": Explain,
}
