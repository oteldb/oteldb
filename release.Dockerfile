ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/oteldb     /usr/local/bin/oteldb
COPY $TARGETPLATFORM/odbbackup  /usr/local/bin/odbbackup
COPY $TARGETPLATFORM/odbrestore /usr/local/bin/odbrestore
COPY $TARGETPLATFORM/odbmigrate /usr/local/bin/odbmigrate
COPY $TARGETPLATFORM/odbingest  /usr/local/bin/odbingest

ENTRYPOINT ["/usr/local/bin/oteldb"]
