# syntax=docker/dockerfile:1
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
# Glama build context: repository root; Dockerfile: deploy/glama/Dockerfile.
# Inspection only, not a production deployment. No real provider or credentials.
FROM debian:bookworm-slim AS download
ARG TARGETARCH
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# Track Debian security updates; release payloads below are checksum-pinned.
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /download
# Pin the released checksum manifest itself, not merely hashes fetched beside assets.
RUN set -eu; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in amd64|arm64) ;; *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; esac; \
    base=https://github.com/KarakoramDev/Karakoram/releases/download/v0.4.7; \
    curl -fsSLO "$base/checksums.txt"; \
    echo 'f45120082779d05d20ea21333cc7083f9df3eabc42bd0c65329d253c6431300b  checksums.txt' | sha256sum -c -; \
    server="karakoram-v0.4.7-linux-$arch.tar.gz"; cli="karactl-linux-$arch"; \
    curl -fsSLO "$base/$server"; curl -fsSLO "$base/$cli"; \
    grep -E "  ($server|$cli)\$" checksums.txt | sha256sum --strict -c -; \
    tar -xzf "$server"; mv "$cli" karactl; chmod 755 karakoram karactl

FROM python:3.13-slim-bookworm
COPY --from=download /download/karakoram /download/karactl /usr/local/bin/
COPY --from=download /download/LICENSE /download/NOTICE /usr/local/share/karakoram/
COPY deploy/glama/entrypoint.py /usr/local/bin/glama-entrypoint.py
RUN mkdir /work && chown 65532:65532 /work
WORKDIR /work
USER 65532:65532
# No exposed ports: the control plane listens only on loopback; MCP uses stdio.
ENTRYPOINT ["python3", "/usr/local/bin/glama-entrypoint.py"]
