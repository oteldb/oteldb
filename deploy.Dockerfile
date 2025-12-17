ARG BASE_IMAGE=ubuntu:25.10
FROM ${BASE_IMAGE}

ADD oteldb /usr/local/bin/oteldb

ENTRYPOINT ["oteldb"]
