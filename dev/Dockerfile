ARG BASE_IMAGE=alpine:latest

FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ../
RUN go mod download

COPY .. ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/oteldb     ./cmd/oteldb
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/odbbackup  ./cmd/odbbackup
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/odbrestore ./cmd/odbrestore

FROM ${BASE_IMAGE}

WORKDIR /app
COPY --from=builder /app/oteldb /oteldb
COPY --from=builder /app/odbbackup /oteldb
COPY --from=builder /app/odbrestore /oteldb

ENTRYPOINT ["/oteldb"]
