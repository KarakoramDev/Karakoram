# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
#
# Create once with an operator credential:
#   nomad volume create deploy/nomad/karakoram-volume.hcl
# Wait for the volume to be ready before registering the control-plane job.
# This is a volume specification, not an HCL2 jobspec: edit literal namespace,
# node_pool and name here to match the job's variables when changing defaults.
# Set node_id to a chosen eligible node if deterministic placement is required.
# Never recreate or delete this volume as part of a binary update: it contains
# SQLite and the sealed internal PKI. The mkdir plugin does not enforce quotas.

namespace = "karakoram"
name      = "karakoram-data"
type      = "host"
plugin_id = "mkdir"
node_pool = "default"

capability {
  access_mode     = "single-node-single-writer"
  attachment_mode = "file-system"
}

parameters = {
  mode = "0700"
}
