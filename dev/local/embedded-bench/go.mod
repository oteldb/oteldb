// The embedded-bench orchestrator is a standalone, stdlib-only Go program so it has no dependency
// on the oteldb module's replace directives. It shells out to `go` (build oteldb + otelbench from
// the oteldb repo, where the go.mod replaces resolve), `docker compose`, and the built otelbench —
// all with the right working directory wired through.
module embedded-bench

go 1.24
