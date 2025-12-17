ARG BASE_IMAGE=alpine:latest

FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/oteldb ./cmd/oteldb

FROM ${BASE_IMAGE}

WORKDIR /app
COPY --from=builder /app/oteldb /oteldb

ENTRYPOINT ["/oteldb"]
