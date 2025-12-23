ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/oteldb     /usr/local/bin/oteldb
COPY $TARGETPLATFORM/odbbackup  /usr/local/bin/odbbackup
COPY $TARGETPLATFORM/odbrestore /usr/local/bin/odbrestore

ENTRYPOINT ["/usr/local/bin/oteldb"]
