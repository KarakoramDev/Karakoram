// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package store provides pure-Go SQLite persistence for Karakoram resources.
// Writes use a single connection; readers use a WAL-enabled connection pool.
// The SQLite driver must close first-row statements on context cancellation
// (modernc.org/sqlite v1.40.1 or later); otherwise pooled readers can retain
// obsolete WAL snapshots even after later writes have committed.
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
// Namespace creation is insert-only. Updates, credentials and deletion are fenced
// by the registration's immutable creation time; obsolete snapshots return
// ErrStale instead of resurrecting a deleted record or overwriting a reused name.
// Namespace read-modify-write paths read registration and enrollment state through
// the writer connection before side effects, rather than a pooled reader snapshot.
//
// Backup drill admission is idempotent by ID and source. Phase updates use a
// compare-and-swap; cleanup remains active until a terminal report is committed.
// Completion retains the newest 50 terminal reports per source, without pruning
// queued or running work. Source deletion removes its reports and drill schedule
// cursor atomically; deleting a throwaway target preserves the source's reports.
//
// WithActor tags new desired generations and audit events with their requester.
// Request attribution survives restart and observed-state updates; unchanged
// desired writes retain the original requester. Automatic new events default to
// reconciler, while historical events with unknown actors remain unknown.
// Events include both cluster lifecycle and noncluster audit records, indexed
// by actor and RBAC resource. Cluster deletion does not remove audit history.
//
// Credential-bearing structs never serialize hidden fields into public JSON.
// Backup storage credentials, cluster KV values, and agent tokens are encrypted
// with identity-bound AES-GCM. Password and refresh/API token hashes are stored
// separately from public resource metadata. List methods omit authentication
// secrets; callers needing hashes use the identity-specific Get methods.
package store
