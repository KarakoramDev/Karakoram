# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
#
# Karakoram control plane as a Dokploy application (experimental Dokploy
# backend). Build context: the repository root; Dockerfile path:
# deploy/dokploy/Dockerfile.
#
#   KARAKORAM_SOURCE=release (default) downloads the signed release pinned in
#   deploy/zerops/RELEASE and verifies it like the Zerops setup.
#   KARAKORAM_SOURCE=local copies a prebuilt linux binary named `karakoram`
#   from the build context root (unreleased builds, development).
#
# Docker is used only because Dokploy runs Swarm services; the image runs the
# same signed `karakoram` binary as every other installation.
ARG BASE_IMAGE=ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55
ARG KARAKORAM_SOURCE=release

FROM ${BASE_IMAGE} AS release
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openssh-client \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY deploy/allowed_signers deploy/allowed_signers
COPY deploy/zerops/RELEASE deploy/zerops/fetch-release.sh deploy/zerops/
RUN sh deploy/zerops/fetch-release.sh

FROM ${BASE_IMAGE} AS local
WORKDIR /src
COPY karakoram ./karakoram
RUN chmod 0755 karakoram && ./karakoram version

FROM ${KARAKORAM_SOURCE} AS binary

FROM ${BASE_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 karakoram \
    && useradd --system --uid 10001 --gid karakoram --home-dir /var/lib/karakoram --no-create-home karakoram \
    && install -d -o karakoram -g karakoram -m 0700 /var/lib/karakoram
COPY --from=binary /src/karakoram /usr/local/bin/karakoram
USER karakoram
WORKDIR /var/lib/karakoram
ENV KARAKORAM_DATA_DIR=/var/lib/karakoram \
    KARAKORAM_BACKEND=dokploy \
    hostname=karakoram
EXPOSE 8080 9443
# SQLite allows one writer: the Dokploy application must use stop-first
# updates (see docs/dokploy.md).
HEALTHCHECK --interval=10s --timeout=5s --start-period=120s --retries=3 \
    CMD ["curl", "-fsS", "-o", "/dev/null", "http://127.0.0.1:8080/healthz"]
STOPSIGNAL SIGTERM
CMD ["karakoram", "server"]
