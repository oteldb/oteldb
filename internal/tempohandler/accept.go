package tempohandler

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"strings"

	"github.com/go-faster/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/oteldb/oteldb/internal/tempoapi"
)

type encoderFunc func(m proto.Message) ([]byte, error)

func protoEncoder(m proto.Message) ([]byte, error) {
	return proto.Marshal(m)
}

func jsonEncoder(m proto.Message) ([]byte, error) {
	return protojson.Marshal(m)
}

func llmEncoder(proto.Message) ([]byte, error) {
	return nil, errors.New("LLM encoder is not implemented yet")
}

const defaultTraceContentType = "application/json"

// negotiateTraceEncoding picks an encoder for the trace-by-ID response.
//
// An absent, empty or wildcard Accept selects [defaultTraceContentType], matching Tempo.
func negotiateTraceEncoding(ctx context.Context, accept string) (string, encoderFunc, error) {
	if strings.TrimSpace(accept) == "" {
		return defaultTraceContentType, jsonEncoder, nil
	}

	var lastErr error
	for entry := range strings.SplitSeq(accept, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		ct, params, err := mime.ParseMediaType(entry)
		if err != nil {
			lastErr = validationErr(ctx, err, fmt.Sprintf("invalid Accept header %q", accept))
			continue
		}

		switch ct {
		case "application/x-protobuf", "application/protobuf":
			return ct, protoEncoder, nil
		case "application/vnd.grafana.llm+json":
			return ct, llmEncoder, nil
		case "*/*", "application/*", "application/json", "text/json":
			// An absent charset means UTF-8, which is the only encoding we produce.
			if charset := params["charset"]; charset != "" && !strings.EqualFold(charset, "utf-8") {
				lastErr = acceptErr(ctx, fmt.Sprintf("unsupported charset %q in Accept header", charset))
				continue
			}
			if ct == "*/*" || ct == "application/*" {
				ct = defaultTraceContentType
			}
			return ct, jsonEncoder, nil
		default:
			lastErr = acceptErr(ctx, fmt.Sprintf("unknown or invalid Content-Type %q in Accept header", entry))
		}
	}

	if lastErr == nil {
		lastErr = acceptErr(ctx, fmt.Sprintf("unknown or invalid Content-Type %q in Accept header", accept))
	}
	return "", nil, lastErr
}

func acceptErr(ctx context.Context, msg string) error {
	return &tempoapi.ErrorStatusCode{
		StatusCode: http.StatusBadRequest,
		Response:   tempoapi.Error(appendTrace(ctx, msg)),
	}
}
