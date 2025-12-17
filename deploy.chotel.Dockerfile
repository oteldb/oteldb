ARG BASE_IMAGE=gcr.io/distroless/static
FROM ${BASE_IMAGE}

ADD chotel /usr/local/bin/chotel

ENTRYPOINT ["chotel"]
