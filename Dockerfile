FROM golang:1.17.6 as builder

WORKDIR /tmp/ingest

COPY . .

ARG BUILDER
ARG VERSION

ENV INGEST_BUILDER=${BUILDER}
ENV INGEST_VERSION=${VERSION}

RUN apt-get update && apt-get install make git gcc -y && \
    make build_deps && \
    make

FROM alfg/ffmpeg:latest

WORKDIR /app

COPY --from=builder /tmp/ingest/bin/ingest .

CMD ["/app/ingest"]
