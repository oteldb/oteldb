ARG GO_VERSION=latest
ARG BASE_IMAGE=alpine:latest

FROM golang:${GO_VERSION} AS builder
ARG GOPROXY=https://proxy.golang.org,direct

WORKDIR /app

COPY go.mod go.sum ../
ENV GOPROXY=${GOPROXY}
RUN go mod download

COPY .. ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/oteldb     ./cmd/oteldb
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/odbbackup  ./cmd/odbbackup
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/odbrestore ./cmd/odbrestore

FROM ${BASE_IMAGE}

WORKDIR /app
COPY --from=builder /app/oteldb     /usr/local/bin/oteldb
COPY --from=builder /app/odbbackup  /usr/local/bin/odbbackup
COPY --from=builder /app/odbrestore /usr/local/bin/odbrestore

ENTRYPOINT ["/usr/local/bin/oteldb"]
