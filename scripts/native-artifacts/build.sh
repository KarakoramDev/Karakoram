#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
set -euo pipefail
exec python3 "$(dirname "$0")/build.py" "${@:?usage: build.sh <all|artifact [artifact ...]>}"
