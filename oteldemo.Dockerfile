ARG BASE_IMAGE=alpine:latest

FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/oteldemo ./cmd/oteldemo

FROM ${BASE_IMAGE}
RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /app/oteldemo /oteldemo

ENTRYPOINT ["/oteldemo"]
