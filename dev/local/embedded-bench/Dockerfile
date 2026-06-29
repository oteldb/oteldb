# Thin runtime image for the embedded-bench. The oteldb binary is built ON THE HOST (see run.sh)
# and copied in here — so `docker compose up --build` is one fast ADD layer, not a Go toolchain
# build. That is the fast iteration loop: edit source → run.sh rebuilds the binary → `up --build
# --force-recreate` swaps oteldb in seconds while the data volume and vmagent ingest keep running
# (CLEANUP=0), then re-run the bench against the fresh binary over the same warm data.
#
# Match the host build flags: CGO_ENABLED=0 GOOS=linux GOARCH=$GOARCH (default amd64). A static
# binary runs unchanged under alpine.

ARG BASE_IMAGE=alpine:latest
FROM ${BASE_IMAGE}
RUN apk --no-cache add ca-certificates wget

WORKDIR /app
COPY oteldb /usr/local/bin/oteldb

ENTRYPOINT ["oteldb"]
