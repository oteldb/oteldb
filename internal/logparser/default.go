package logparser

// DefaultFormats returns default detection order, from strictest to loosest.
//
// [GenericJSONParser.Detect] is a strict "first token is a JSON object" check and is mutually
// exclusive with the rest. [LogFmtParser.Detect] is the most permissive one and falsely matches
// zap-development lines, so it goes last.
func DefaultFormats() []Parser {
	return []Parser{
		new(GenericJSONParser),
		new(KLogParser),
		new(ZapDevelopmentParser),
		new(LogFmtParser),
	}
}

// DefaultFormatNames returns names of [DefaultFormats], in the same order.
func DefaultFormatNames() []string {
	formats := DefaultFormats()
	names := make([]string, 0, len(formats))
	for _, p := range formats {
		names = append(names, p.String())
	}
	return names
}
