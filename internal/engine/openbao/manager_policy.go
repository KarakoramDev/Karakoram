// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

// managerPolicy deliberately excludes sys/raw and secret-value reads. Database
// integration may extend mount-specific paths without granting root privileges.
const managerPolicy = `
path "sys/storage/raft/configuration" { capabilities = ["read"] }
path "sys/storage/raft/remove-peer" { capabilities = ["update", "sudo"] }
path "sys/storage/raft/autopilot/*" { capabilities = ["read", "update"] }
path "sys/storage/raft/snapshot" { capabilities = ["read", "create", "update", "sudo"] }
path "sys/storage/raft/snapshot-force" { capabilities = ["create", "update", "sudo"] }
path "sys/step-down" { capabilities = ["update", "sudo"] }
path "sys/mounts" { capabilities = ["read"] }
path "sys/mounts/*" { capabilities = ["create", "read", "update", "delete", "sudo"] }
path "sys/policies/acl" { capabilities = ["list"] }
path "sys/policies/acl/*" { capabilities = ["create", "read", "update", "delete"] }
path "sys/auth" { capabilities = ["read"] }
path "sys/auth/*" { capabilities = ["create", "read", "update", "delete", "sudo"] }
path "auth/token/lookup-self" { capabilities = ["read"] }
path "auth/token/renew-self" { capabilities = ["update"] }
path "database/*" { capabilities = ["create", "read", "update", "delete", "list"] }
path "rabbitmq-*/*" { capabilities = ["create", "read", "update", "delete", "list"] }
path "sys/leases/revoke-prefix/*" { capabilities = ["update", "sudo"] }
`
