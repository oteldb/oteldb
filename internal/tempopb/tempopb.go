// Package tempopb contains generated code for Tempo's protobuf definitions.
package tempopb

//go:generate protoc --proto_path=. --go_out=. --go_opt=paths=source_relative --go_opt=Mcommon/v1/common.proto=go.opentelemetry.io/proto/otlp/common/v1 --go_opt=Mresource/v1/resource.proto=go.opentelemetry.io/proto/otlp/resource/v1 --go_opt=Mtrace/v1/trace.proto=go.opentelemetry.io/proto/otlp/trace/v1 tempo.proto
