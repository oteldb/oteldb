ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}
ARG BINARY
ADD ${BINARY} /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
