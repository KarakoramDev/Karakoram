#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
set -eu

usage() {
    printf '%s\n' 'Usage: install.sh [--source] PROJECT_ID [IMPORT_YAML]' >&2
    exit 2
}

setup=karakoram
if [ "${1:-}" = --source ]; then
    setup=karakoram-source
    shift
fi
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || [ -z "$1" ]; then
    usage
fi

command -v zcli >/dev/null 2>&1 || {
    printf '%s\n' 'zcli is required; install it and log in before running this script.' >&2
    exit 1
}

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
project_id=$1
import_yaml=${2:-"$root/deploy/zerops/import-local.yaml"}

# The prebuilt setup downloads the pinned release during the Zerops build, so
# upload only the files it reads instead of the whole checkout.
upload=$root
if [ "$setup" = karakoram ]; then
    [ -f "$root/deploy/zerops/RELEASE" ] || {
        printf '%s\n' 'deploy/zerops/RELEASE is missing: this checkout pins no release; use --source.' >&2
        exit 1
    }
    upload=$(mktemp -d)
    trap 'rm -rf "$upload"' EXIT
    mkdir -p "$upload/deploy/zerops"
    cp "$root/zerops.yaml" "$upload/"
    cp "$root/deploy/allowed_signers" "$upload/deploy/"
    cp "$root/deploy/zerops/RELEASE" "$root/deploy/zerops/fetch-release.sh" "$upload/deploy/zerops/"
fi

# Use the caller's zcli login and environment. Never load or store credentials.
zcli project service-import "$import_yaml" --project-id "$project_id"
zcli service push karakoram --project-id "$project_id" --setup "$setup" \
    --working-dir "$upload" --zerops-yaml-path zerops.yaml --no-git --disable-logs
# A code-free import cannot activate HTTP routing until a pipeline declares ports.
zcli service enable-subdomain karakoram --project-id "$project_id"
