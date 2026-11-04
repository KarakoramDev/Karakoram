# 0011 — PBM backups to S3, control-plane scheduling and PITR

Status: Accepted

## Context

Local Storage platform backups are live tarballs, not consistent [MongoDB](https://www.mongodb.com) backups. Required recovery includes logical/physical snapshots, PITR, in-place restore and clone for replica sets and sharded clusters. [Zerops](https://zerops.io) Object Storage is [S3](https://aws.amazon.com/s3/)-compatible; users may also supply another S3 endpoint.

## Decision

Use [PBM](https://docs.percona.com/percona-backup-mongodb/) as backup/inventory/restore authority, with `pbm-agent` on data-bearing mongod members. Define namespace-local `BackupStorage` with endpoint, region, bucket, prefix and encrypted write-only credentials. A cluster and its schedules reference storages in their own namespace. Default `forcePathStyle: true` and `verifyTLS: true`; forward both explicitly to PBM rather than treating omitted verification as permission to skip TLS checks. Use cluster-isolated storage prefixes and retain the storage/PBM identity of each backup. Members transfer backup data directly to S3, never through the relay (ADR 0018).

Execute manual and scheduled logical/physical backups as durable operations. Karakoram owns cron schedules and count-based retention, creating persisted requests and deleting eligible completed backups through PBM after the new backup succeeds. Count retention is not PBM's GFS policy; do not delete a base backup/oplog chain still needed by the advertised PITR window. Do not replace engine backup with platform volume archives.

PITR requires a successful base snapshot plus continuous oplog slices. Configure the span and report PBM-observed contiguous restorable windows, not merely the elapsed time since a backup. In-place restore is disruptive and serialized: quiesce writes, routers/balancer as required, use agent maintenance for physical restore, observe completion and validate health before reopening access. Reset the post-restore PITR timeline with a new base as required by PBM.

Clone creates a new cluster via `spec.dataSource` (backup or PITR time), validates topology/version compatibility, and preserves/reconciles required source authentication material. Source data and its usable recovery chain remain intact. Cross-namespace clone/restore uses the source backup's recorded namespace-local storage and requires source `backups/export` in addition to source read and target mutation permissions (ADR 0013).

## Consequences

Recovery is database-aware and independent of a member volume. Credentials, object-store quota, PBM metadata and base/oplog dependencies are part of correctness. Control-plane downtime interrupts new scheduled requests/retention but already-running PBM work and configured agent PITR are not inherently dependent on it. Restores cannot promise uninterrupted writes.

## Alternatives considered

- **Platform volume backups:** lack database consistency and PITR.
- **mongodump scripts only:** useful logical baseline, but lack required physical/PITR workflows.
- **Node cron/PBM GFS retention:** splits scheduling ownership and changes requested count semantics.
