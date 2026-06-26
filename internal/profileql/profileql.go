// Package profileql contains a parser and AST definitions for the Pyroscope
// profile query language.
//
// A ProfileQL query is a profile-type selector optionally followed by a brace
// enclosed list of label matchers, mirroring Pyroscope's query syntax (a
// Prometheus metric selector whose __name__ is the profile type):
//
//	process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="frontend",env=~"prod|staging"}
package profileql
