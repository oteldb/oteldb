ARG BASE_IMAGE=gcr.io/distroless/static
ARG BUILDER_IMAGE=alpine:3.20

FROM ${BUILDER_IMAGE} AS builder
RUN ln -s /usr/local/bin/odbagent /otelcol-contrib && \
    ln -s /usr/local/bin/odbagent /otelcol

FROM ${BASE_IMAGE}

ADD odbagent /usr/local/bin/odbagent
COPY --from=builder /otelcol-contrib /otelcol-contrib
COPY --from=builder /otelcol /otelcol

ENTRYPOINT ["odbagent"]
