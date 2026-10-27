# 0003 — SQLite state on Local Storage, with explicit recovery

Status: Accepted

## Context

Desired state, operation checkpoints, events, accounts and encrypted secrets must survive runtime redeployment. [Zerops](https://zerops.io) runtime disks are ephemeral; Local Storage provides a real local filesystem but is not HA. Managed [PostgreSQL](https://www.postgresql.org) is available, but adds an independently operated service to every installation.

## Decision

Use [SQLite](https://sqlite.org) through the pure-[Go](https://go.dev) `modernc.org/sqlite` driver, WAL mode and schema migrations. Run exactly one control-plane container (`maxContainers: 1`), with `temporaryShutdown`, and store `karakoram.db` on `karakoramdata` mounted at `/mnt/karakoramdata`. Transactions persist related state/checkpoint/event changes together. Do not share the database between control-plane replicas.

Back up state using a transactionally consistent SQLite backup/export, not a copy of the live `.db` alone or an uncoordinated Local Storage tarball. Keep recoverable copies outside that volume, record the application/schema version, and preserve the master key separately under operator control. State backups contain sensitive metadata and encrypted secrets; access must remain restricted. These backups are independent of [PBM](https://docs.percona.com/percona-backup-mongodb/) database backups.

Recovery procedure: fence/stop any old control plane; restore a consistent snapshot to a replacement volume; supply the original master key and a valid project-scoped token; start one compatible Karakoram instance; reconcile existing service identities, persisted platform process IDs and engine/PBM observations before resuming disruptive work. Do not import duplicate members or assume a checkpoint proves external completion.

## Consequences

Installation is inexpensive and self-contained, but there is **no control-plane HA**. Host/volume loss requires operator restoration; state RPO is the snapshot interval and recovery has downtime. Existing agents/database replication continue, while management and control-plane backup scheduling stop. Loss of the master key makes encrypted state unusable. A stale snapshot requires observation-led reconciliation, not blind replay.

## Alternatives considered

- **Zerops managed PostgreSQL:** better basis for multiple control-plane replicas, but incurs another service and still needs leader/lock coordination; reconsider if CP HA becomes required.
- **Ephemeral SQLite/shared [SeaweedFS](https://seaweedfs.com):** loses durable state or introduces unsuitable filesystem semantics.
