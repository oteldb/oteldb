ARG GO_VERSION=latest
ARG BASE_IMAGE=alpine:latest

FROM golang:${GO_VERSION} AS builder
ARG GOPROXY=https://proxy.golang.org,direct

WORKDIR /app

COPY go.mod go.sum ../
ENV GOPROXY=${GOPROXY}
RUN go mod download

COPY .. ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/chotel ./cmd/chotel

FROM ${BASE_IMAGE}
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /app/chotel /chotel

ENTRYPOINT ["/chotel"]
