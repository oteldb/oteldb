# Thin runtime image for the embedded-bench. The oteldb binary is built ON THE HOST (by the
# orchestrator, main.go) and copied in here — so `docker compose up --build` is one fast ADD layer,
# not a Go toolchain build. Both instances (the `file` and `s3` services) share this one image and
# differ only in their mounted config, so a single host build backs both.
#
# Match the host build flags: CGO_ENABLED=0 GOOS=linux GOARCH=$GOARCH (default amd64). A static
# binary runs unchanged under alpine.

ARG BASE_IMAGE=alpine:latest
FROM ${BASE_IMAGE}
RUN apk --no-cache add ca-certificates wget

WORKDIR /app
COPY oteldb /usr/local/bin/oteldb

ENTRYPOINT ["oteldb"]
