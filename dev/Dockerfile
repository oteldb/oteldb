ARG GO_VERSION=latest
ARG BASE_IMAGE=alpine:latest

FROM golang:${GO_VERSION} AS builder
ARG GOPROXY=https://proxy.golang.org,direct
ARG TARGETS="oteldb odbbackup odbrestore odbmigrate"

WORKDIR /app

COPY go.mod go.sum ../
ENV GOPROXY=${GOPROXY}
RUN go mod download

COPY .. ./
RUN set -e; mkdir /out && \
    for target in $TARGETS; do \
      CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /out/$target ./cmd/$target; \
    done

FROM ${BASE_IMAGE}
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /out/ /usr/local/bin/

ARG ENTRYPOINT_BIN=oteldb
ENV ENTRYPOINT_BIN=${ENTRYPOINT_BIN}
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/$ENTRYPOINT_BIN \"$@\"", "--"]
