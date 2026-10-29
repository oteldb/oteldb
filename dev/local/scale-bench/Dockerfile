FROM alpine:3.22
RUN apk --no-cache add ca-certificates
ARG REVISION
ARG STORAGE_REVISION
ARG STORAGE_DIFF
LABEL org.opencontainers.image.revision=$REVISION
LABEL dev.oteldb.storage.revision=$STORAGE_REVISION
LABEL dev.oteldb.storage.diff=$STORAGE_DIFF
COPY oteldb /usr/local/bin/oteldb
ENTRYPOINT ["oteldb"]
