// Package tempopb contains generated code for Tempo's protobuf definitions.
package tempopb

//go:generate protoc --proto_path=. --go_out=. --go_opt=paths=source_relative common/v1/common.proto resource/v1/resource.proto trace/v1/trace.proto tempo.proto
