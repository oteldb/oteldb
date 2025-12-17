ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ADD otelproxy /usr/local/bin/otelproxy

ENTRYPOINT ["otelproxy"]
