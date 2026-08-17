package promrw

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/go-faster/errors"
)

// Remote write picks its protobuf schema with the Content-Type parameter `proto=`, not with the
// X-Prometheus-Remote-Write-Version header — Prometheus sets that header when sending but ignores
// it when receiving, so keying off it would misread any sender that disagrees. A body with no
// `proto=` parameter is 1.0, which is what the 1.0 spec predates the parameter by.

// Message is the protobuf schema a request body carries.
type Message uint8

const (
	// MessageV1 is `prometheus.WriteRequest`, the remote write 1.0 schema.
	MessageV1 Message = iota
	// MessageV2 is `io.prometheus.write.v2.Request`, the remote write 2.0 schema.
	MessageV2
)

// Fully-qualified names of the two schemas, as they appear in a Content-Type's `proto=` parameter.
const (
	MessageV1Name = "prometheus.WriteRequest"
	MessageV2Name = "io.prometheus.write.v2.Request"
)

// contentTypeProtobuf is the only media type remote write uses. It must match exactly: a body of a
// different type is not a remote write request whatever its parameters say.
const contentTypeProtobuf = "application/x-protobuf"

// encodingSnappy is the only Content-Encoding remote write defines, in its block (not framed) form.
const encodingSnappy = "snappy"

func (m Message) String() string {
	if m == MessageV2 {
		return MessageV2Name
	}

	return MessageV1Name
}

// ContentType is the Content-Type a sender uses for this message.
func (m Message) ContentType() string {
	if m == MessageV2 {
		return contentTypeProtobuf + ";proto=" + MessageV2Name
	}

	return contentTypeProtobuf
}

// parseMessage resolves the schema a request body carries from its Content-Type. An empty type is
// taken as the bare protobuf media type, i.e. 1.0.
func parseMessage(contentType string) (Message, error) {
	if strings.TrimSpace(contentType) == "" {
		return MessageV1, nil
	}

	parts := strings.Split(contentType, ";")
	if strings.TrimSpace(parts[0]) != contentTypeProtobuf {
		return 0, errors.Errorf("expected media type %s, got %q", contentTypeProtobuf, contentType)
	}

	for _, p := range parts[1:] {
		key, value, ok := strings.Cut(p, "=")
		if !ok {
			return 0, errors.Errorf("malformed content type parameter %q", p)
		}
		if strings.TrimSpace(key) != "proto" {
			continue
		}

		switch strings.TrimSpace(value) {
		case MessageV1Name:
			return MessageV1, nil
		case MessageV2Name:
			return MessageV2, nil
		default:
			return 0, errors.Errorf("unknown remote write protobuf message %q", value)
		}
	}

	// No proto= parameter: 1.0, which predates it.
	return MessageV1, nil
}

// validEncoding reports whether a Content-Encoding is one the body can be read with. Empty is
// allowed because the 1.0 spec's senders predate the header, and the body is snappy either way.
func validEncoding(encoding string) bool {
	encoding = strings.TrimSpace(encoding)

	return encoding == "" || encoding == encodingSnappy
}

// A 2.0 receiver reports what it wrote in these headers, and a sender reads them to decide whether
// a partial write needs resending. They are set on the failure path too, for exactly that reason:
// a request that stored most of its series and then answered 400 has to say so, or the sender
// resends what already landed.
//
// A 1.0 response must not carry them. Their absence is how a sender tells a 1.0 receiver from a 2.0
// one, so setting them on a 1.0 response would claim a guarantee 1.0 does not make.
const (
	writtenSamplesHeader    = "X-Prometheus-Remote-Write-Samples-Written"
	writtenHistogramsHeader = "X-Prometheus-Remote-Write-Histograms-Written"
	writtenExemplarsHeader  = "X-Prometheus-Remote-Write-Exemplars-Written"
)

// setWrittenHeaders reports the counts on w. It must be called before the status is written.
func (c Counts) setWrittenHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set(writtenSamplesHeader, strconv.Itoa(c.Samples))
	h.Set(writtenHistogramsHeader, strconv.Itoa(c.Histograms))
	h.Set(writtenExemplarsHeader, strconv.Itoa(c.Exemplars))
}
