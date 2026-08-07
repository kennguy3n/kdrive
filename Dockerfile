# KChat Drive — shared gateway/worker image.
# Multi-stage build: build the Go binaries, then copy into a minimal
# Alpine runtime image that includes /bin/sh and envsubst (via
# gettext) so the compose entrypoint can render config templates.
FROM golang:1.25-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/drive-gateway ./cmd/drive-gateway
RUN CGO_ENABLED=0 go build -o /out/drive-worker ./cmd/drive-worker

FROM alpine:3.20
RUN apk add --no-cache gettext ca-certificates && \
    adduser -D -u 1000 kdrive
COPY --from=builder /out/drive-gateway /usr/local/bin/drive-gateway
COPY --from=builder /out/drive-worker /usr/local/bin/drive-worker
# Non-root; the compose file mounts the NVMe cache volume owned by
# this UID.
USER kdrive
EXPOSE 8080
