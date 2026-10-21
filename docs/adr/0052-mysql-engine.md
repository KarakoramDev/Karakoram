# 0052 — Native Percona Server for MySQL with Group Replication and a client proxy

Status: Accepted

## Context

Applications that require MySQL cannot assume MariaDB compatibility. A MySQL engine needs a native runtime, a single-writer authority that keeps working while the control plane is unavailable, physical backups with binary-log point-in-time recovery, and MySQL-shaped account administration. A member count alone is not a failure-domain guarantee, and a singleton is not highly available.

## Decision

### Artifacts and versions

The `mysql` provider runs official Percona Server minimal binary tarballs with compatible Percona XtraBackup as checksum-locked [native artifacts](0036-native-artifacts.md), not distribution-managed services. Offer 8.4 LTS by default and 8.0 for existing workloads. Each server catalog entry pins compatible backup tooling without disabling XtraBackup's server-version check. [Native artifacts](../native-artifacts.md) owns the current inventory and runtime dependencies.

### Topologies and writer authority

`single` has one member with GTIDs and row-based binary logs and is not highly available. `group` has three or five members in Group Replication **single-primary** mode. Native Group Replication elects the writer; Karakoram is not the established group's failover authority. Use authenticated TLS on the `MYSQL` communication stack, default `BEFORE_ON_PRIMARY_FAILOVER` consistency and explicit read-only minority exit. Quorum loss can block writes before they fail; applications must handle uncertain outcomes.

Formation and whole-group re-formation are journaled, evidence-based control-plane actions, not a repeated startup bootstrap. Full-outage recovery requires an offline GTID observation from every member and selects a member whose set includes all others; missing or incomparable sets fail closed. New members clone only from compatible exact-version donors. Configuration stays outside copied data, and `SET PERSIST` is not used. [Configuration](../mysql/configuration.md) and [operations](../mysql/operations.md) describe rejoin, maintenance and recovery behavior.

### Client proxy and access

ProxySQL supplies the application endpoint with one routed writer. Groups require a proxy; only single-member deployments permit direct proxy-free access. Optional reader routing is a convenience, not a read-after-write guarantee. Clients reconnect after failover.

Frontend proxy authentication requires recoverable credentials. Native MySQL access manages TLS-only accounts, databases and database-level grants, synchronizing sealed managed passwords to proxy configuration. SQL-created identities are not automatically routable. System identities remain separate and protected. [Access and TLS](../mysql/access-and-tls.md) owns the user-facing routes, permissions, password rotation and connection guidance.

### Backups and recovery

Use XtraBackup physical full/incremental streams to registered S3 storage and GTID-keyed binary-log archiving from the current writer. PITR coverage is conservative and must be continuous; later successful uploads cannot bridge a gap. Restore is destructive and offline, preserves recovered GTIDs, regenerates server UUIDs and isolates restored groups with a new group identity. Source/target provider and server major must match. Retention preserves chain dependencies; managed account recovery additionally requires sealed backup-time credential provenance. [Backups and PITR](../mysql/backups.md) owns repository structure, preparation, transaction boundaries and account caveats.

### Maintenance

Use desired-state configuration with live application of supported dynamic values and safe rolling maintenance for restart-required changes. Groups maintain secondaries before moving the primary. Major upgrades are backup-gated and forward-only; single→group and group 3↔5 are supported, group→single is not. Retained-volume resume uses the full-outage evidence policy. [Operations](../mysql/operations.md) owns procedures and [verification](../mysql/verification.md) owns portable acceptance instructions.

## Consequences

Group Replication gives automatic failover without an external coordination service, but quorum loss is visible to clients as blocked writes, and full outages need control-plane-driven, evidence-based re-formation. The proxy adds one hop and a recoverable-credential obligation. Archiving from the writer makes PITR depend on bounded rotation and member-local binary-log retention. Percona Server and XtraBackup are GPLv2 programs run as separate processes, distributed from upstream bytes.

## Alternatives considered

- **Asynchronous or semi-synchronous replication with Karakoram-driven failover:** makes the control plane the writer authority and needs its own fencing; Group Replication already provides quorum-based single-writer election.
- **Group Replication multi-primary:** write conflicts surface as transaction rollbacks and DDL restrictions; single-primary is the safe default.
- **XCom communication stack:** needs a second routable port and IP allowlists per member; the MySQL stack reuses authenticated TLS on the client port.
- **MySQL Router:** requires InnoDB Cluster metadata created by MySQL Shell; ProxySQL is already locked and operated for PostgreSQL.
- **Distribution packages:** Ubuntu ships Oracle MySQL, not Percona Server; Percona repositories install services and do not pin XtraBackup to the server.
- **Logical dumps as the primary backup:** slow to restore for larger databases and no consistent binary-log starting point without locking.
- **Percona XtraDB Cluster in the same provider:** different binaries, consensus and recovery; see [0053](0053-pxc-engine.md).

## References

[Group Replication](https://dev.mysql.com/doc/refman/8.4/en/group-replication.html), [communication stack](https://dev.mysql.com/doc/refman/8.4/en/group-replication-connection-security.html), [clone plugin](https://dev.mysql.com/doc/refman/8.4/en/clone-plugin.html), [Percona XtraBackup](https://docs.percona.com/percona-xtrabackup/8.4/), [xbcloud](https://docs.percona.com/percona-xtrabackup/8.4/xbcloud-binary-overview.html), [ProxySQL Group Replication](https://proxysql.com/documentation/main-runtime/#mysql_group_replication_hostgroups), [native artifacts](0036-native-artifacts.md).
