// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package store provides pure-Go SQLite persistence for Karakoram resources.
// Writes use a single connection; readers use a WAL-enabled connection pool.
// Migrations are embedded and ordered. Methods accept contexts and return
// ErrNotFound or ErrConflict for resource-level failures.
//
// Cluster identities use domain.ClusterRef, with names unique per namespace.
// Empty collection namespace filters include all namespaces; omitted namespaces
// on individual identities resolve to default. Default records preserve legacy
// scopes and encryption identities, so namespace migration never re-seals secrets.
//
// Operation checkpoints and provider-owned cluster state are committed together.
// Running work is resumed before Pending work; only the reconciler owns claims.
// Deletion intent and hard deletion retire unfinished operations transactionally,
// preserving failed history while preventing name reuse from resuming old work.
//
// Credential-bearing structs never serialize hidden fields into public JSON.
// Backup storage credentials, cluster KV values, and agent tokens are encrypted
// with identity-bound AES-GCM. Password and refresh/API token hashes are stored
// separately from public resource metadata. List methods omit authentication
// secrets; callers needing hashes use the identity-specific Get methods.
package store
