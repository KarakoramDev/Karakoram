# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
#
# Apply with an operator credential, then issue a client token carrying this
# policy; store its SecretID only in nomad/jobs/karakoram as
# KARAKORAM_NOMAD_TOKEN. The namespace must already exist and be dedicated to
# Karakoram. Register the control-plane job and its volume with an operator
# token: this client token deliberately cannot mount karakoram-data or read
# nomad/jobs/karakoram (the job's own workload identity renders its secrets).
# host-volume-delete still permits deleting ANY unclaimed dynamic volume here,
# including karakoram-data. For stronger separation, use operator-provisioned
# static client host storage for the node-pinned control plane; see docs/nomad.md.
#   nomad acl policy apply karakoram deploy/nomad/karakoram-policy.hcl
#   nomad acl token create -name=karakoram-control-plane -policy=karakoram
#
# ACL specs do not evaluate jobspec variables. When changing the job namespace,
# edit the namespace below and the host_volume prefix together. The prefix is
# kk-<first 8 hexadecimal characters of SHA-256(namespace)>-*. For karakoram,
# that digest prefix is 5932d69c.
# Node read access exposes cluster-wide node/host-volume metadata; this backend
# does not advertise fully scoped tokens. Do not distribute this token to users.

namespace "karakoram" {
  capabilities = [
    "list-jobs",
    "read-job",
    "submit-job",
    "read-logs",
    "alloc-lifecycle",
    "host-volume-create",
    "host-volume-read",
    "host-volume-delete",
  ]

  variables {
    path "karakoram/*" {
      capabilities = ["read", "write", "list", "destroy"]
    }
    path "nomad/jobs/*" {
      capabilities = ["read", "write", "list", "destroy"]
    }
    path "nomad/jobs/karakoram" {
      capabilities = ["deny"]
    }
  }
}

node {
  policy = "read"
}

host_volume "kk-5932d69c-*" {
  policy = "write"
}
