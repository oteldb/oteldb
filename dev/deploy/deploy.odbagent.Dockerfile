ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ADD odbagent /usr/local/bin/odbagent

ENTRYPOINT ["odbagent"]
