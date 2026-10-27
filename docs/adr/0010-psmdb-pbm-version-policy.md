# 0010 — PSMDB/PBM, a curated catalog and explicit FCV promotion

Status: Accepted

## Context

Karakoram uses [Percona Server for MongoDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) (PSMDB) and [Percona Backup for MongoDB](https://docs.percona.com/percona-backup-mongodb/) (PBM), including physical backups. [MongoDB](https://www.mongodb.com) release availability does not establish PSMDB package availability or PBM compatibility on [Ubuntu](https://ubuntu.com) 24.04. The supported catalog must reflect the actual engine packages and their tested compatibility.

## Decision

Implement the `psmdb` engine with PSMDB and PBM packages. Maintain a engine driver-owned catalog of exact supported package versions, default/deprecated status, compatible PBM versions and allowed upgrade paths, backed by package and integration evidence. Resolve/persist the chosen version at creation; changing the catalog default must not silently upgrade existing clusters. Do not offer unavailable majors or infer support from a package name. Advertise logical/physical backup support only for compatible catalog combinations.

Treat desired engine-version changes as reconciliation. Validate an allowed path, health/quorum and backup readiness before disruptive work. Upgrade one member at a time: healthy secondaries first, wait for version/readiness/replication convergence, then step down the primary and upgrade the former primary. For sharding, apply the target version's supported config-server/shard/router order as well as per-set sequencing. Major transitions are one supported major at a time; do not auto-upgrade or promise arbitrary downgrade.

Leave FCV at its previous value while binary upgrades are validated. Raising FCV is a distinct, explicit user-triggered durable `raise-fcv` operation after all required components converge. Surface that promotion can close the normal major-version rollback window. Karakoram binary upgrades are separate from database upgrades.

## Consequences

Users receive predictable versions and a deliberate rollback boundary. Slow initial sync, lag, unhealthy members or failed readiness pause the rollout instead of advancing multiple members. Catalog curation and compatibility testing are ongoing maintenance duties. Small/single-member clusters cannot obtain zero-downtime guarantees from sequencing.

## Alternatives considered

- **Upstream MongoDB instead of PSMDB:** violates the engine decision and weakens PBM physical-backup compatibility.
- **Latest package/automatic FCV:** less operator effort, but silently changes compatibility and rollback behavior.
- **Simultaneous service upgrades:** faster, but risks quorum and availability.
