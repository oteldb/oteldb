ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ADD oteldemo /usr/local/bin/oteldemo

ENTRYPOINT ["oteldemo"]
