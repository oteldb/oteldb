ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ADD otelbot /usr/local/bin/otelbot

ENTRYPOINT ["otelbot"]
