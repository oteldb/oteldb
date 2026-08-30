ARG GO_VERSION=latest
ARG BASE_IMAGE=alpine:latest
ARG BUN_IMAGE=oven/bun:1-alpine

# Selects which of the frontend-* stages provides internal/adminhandler/frontend/dist:
# `build` runs the real bun build, `skip` supplies the tracked .gitkeep placeholder so the
# go:embed directive still compiles without a bun toolchain or a network fetch of npm deps.
ARG FRONTEND=build

# Builds the admin SPA the same way .github/workflows/frontend.yml does, so a clean checkout
# embeds a real UI instead of the "not built" placeholder page.
FROM ${BUN_IMAGE} AS frontend-build
WORKDIR /frontend
COPY internal/adminhandler/frontend/package.json internal/adminhandler/frontend/bun.lock ./
RUN bun install --frozen-lockfile
# orval generates the typed client from the same spec the ogen server is generated from.
COPY _oas/admin.yml /_oas/admin.yml
COPY internal/adminhandler/frontend/ ./
RUN bun run build

FROM scratch AS frontend-skip
COPY internal/adminhandler/frontend/dist/.gitkeep /frontend/dist/.gitkeep

FROM frontend-${FRONTEND} AS frontend

FROM golang:${GO_VERSION} AS builder
ARG GOPROXY=https://proxy.golang.org,direct
ARG TARGETS="oteldb odbbackup odbrestore odbmigrate odbingest odbselect"

WORKDIR /app

COPY go.mod go.sum ../
ENV GOPROXY=${GOPROXY}
RUN go mod download

COPY .. ./
# After the context copy, so the built SPA wins over any dist/ that leaked in from the host.
COPY --from=frontend /frontend/dist/ ./internal/adminhandler/frontend/dist/
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
