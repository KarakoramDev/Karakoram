# 0011 — PBM backups to S3, control-plane scheduling and PITR

Status: Accepted

## Context

Local Storage platform backups are live tarballs, not consistent MongoDB backups. Required recovery includes logical/physical snapshots, PITR, in-place restore and clone for replica sets and sharded clusters. Zerops Object Storage is S3-compatible; users may also supply another S3 endpoint.

## Decision

Use PBM as backup/inventory/restore authority, with `pbm-agent` on data-bearing mongod members. Define `BackupStorage` with endpoint, region, bucket, prefix and encrypted write-only credentials. Default `forcePathStyle: true` and `verifyTLS: true`; forward both explicitly to PBM rather than treating omitted verification as permission to skip TLS checks. Use cluster-isolated storage prefixes and retain the storage/PBM identity of each backup.

Execute manual and scheduled logical/physical backups as durable operations. Karakoram owns cron schedules and count-based retention, creating persisted requests and deleting eligible completed backups through PBM after the new backup succeeds. Count retention is not PBM's GFS policy; do not delete a base backup/oplog chain still needed by the advertised PITR window. Do not replace engine backup with platform volume archives.

PITR requires a successful base snapshot plus continuous oplog slices. Configure the span and report PBM-observed contiguous restorable windows, not merely the elapsed time since a backup. In-place restore is disruptive and serialized: quiesce writes, routers/balancer as required, use agent maintenance for physical restore, observe completion and validate health before reopening access. Reset the post-restore PITR timeline with a new base as required by PBM.

Clone creates a new cluster via `spec.dataSource` (backup or PITR time), validates topology/version compatibility, and preserves/reconciles required source authentication material. Source data and its usable recovery chain remain intact.

## Consequences

Recovery is database-aware and independent of a member volume. Credentials, object-store quota, PBM metadata and base/oplog dependencies are part of correctness. Control-plane downtime interrupts new scheduled requests/retention but already-running PBM work and configured agent PITR are not inherently dependent on it. Restores cannot promise uninterrupted writes.

## Alternatives considered

- **Platform volume backups:** lack database consistency and PITR.
- **mongodump scripts only:** useful logical baseline, but lack required physical/PITR workflows.
- **Node cron/PBM GFS retention:** splits scheduling ownership and changes requested count semantics.
