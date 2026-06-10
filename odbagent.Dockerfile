ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/odbagent /usr/local/bin/odbagent

ENTRYPOINT ["/usr/local/bin/odbagent"]
