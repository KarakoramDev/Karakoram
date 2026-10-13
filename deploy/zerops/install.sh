#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || [ -z "$1" ]; then
    printf '%s\n' 'Usage: install.sh PROJECT_ID [IMPORT_YAML]' >&2
    exit 2
fi

command -v zcli >/dev/null 2>&1 || {
    printf '%s\n' 'zcli is required; install it and log in before running this script.' >&2
    exit 1
}

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
project_id=$1
import_yaml=${2:-"$root/deploy/zerops/import-local.yaml"}

# Use the caller's zcli login and environment. Never load or store credentials.
zcli project service-import "$import_yaml" --project-id "$project_id"
zcli service push karakoram --project-id "$project_id" --setup karakoram \
    --working-dir "$root" --zerops-yaml-path zerops.yaml --no-git --disable-logs
# A code-free import cannot activate HTTP routing until a pipeline declares ports.
zcli service enable-subdomain karakoram --project-id "$project_id"
