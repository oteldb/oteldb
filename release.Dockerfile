ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/oteldb /usr/bin/local/oteldb

ENTRYPOINT ["/usr/bin/local/oteldb"]
