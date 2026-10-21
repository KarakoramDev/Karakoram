# 0053 — Percona XtraDB Cluster as a separate provider over the MySQL core

Status: Accepted

## Context

Some MySQL users want synchronous multi-node replication with Galera semantics (virtually synchronous commit, certification, every node a candidate writer) rather than Group Replication. Percona XtraDB Cluster (PXC) ships its own server binaries with the Galera library, its own state transfer (SST/IST) and its own full-outage recovery rules. Its backups are XtraBackup backups, but a PXC data directory carries Galera state that a Percona Server cluster does not.

## Decision

`pxc` is a separate provider that composes the [MySQL core](0052-mysql-engine.md): it reuses the member, proxy, TLS, account, access API (`/clusters/{cluster}/mysql/...`), backup storage layout and maintenance machinery, and replaces the replication layer. It is not a `mysql` topology, because its version catalog, artifacts, consensus, recovery procedure and backup provenance differ; backups of `mysql` and `pxc` clusters are not interchangeable providers for restore.

### Artifacts and topology

The provider runs the official PXC `glibc2.35` minimal tarball (majors 8.4 default and 8.0) as a locked native artifact. The tarball bundles the matching XtraBackup builds used for SST; bundled tools take precedence for upstream SST scripts. One topology, `cluster`, has three or five data members; one- and two-member clusters are not offered because they cannot tolerate a failure. `pxc_strict_mode=ENFORCING` stays on: applications must use compatible InnoDB tables with primary keys and avoid `LOCK TABLES`. Incompatible non-transactional DDL can succeed while subsequent writes fail; creation alone is not a compatibility test.

Every member requires encrypted client SQL, Galera, IST and SST traffic, explicit channel ports, and `wsrep_sst_method=xtrabackup-v2`. State transfer prefers IST from the Galera cache and falls back to SST. Replication encryption uses explicit `wsrep_provider_options` (`socket.ssl=yes` and mandatory traffic certificate, key and CA paths) and `[sst] encrypt=4` with the same traffic credentials. The `pxc_encrypt_cluster_traffic` **automatic configuration switch is OFF**, not the encryption: pinned upstream code appends its automatic SSL options after manually supplied options and obtains them from the MySQL client-facing leaf. Automatic configuration therefore cannot preserve distinct member SQL identities while supplying the identical replication credentials required by PXC. Manual channel configuration keeps SQL and agent leaves member-specific and fails closed when any replication trust material is absent.

Galera/SST use a dedicated traffic CA and one cluster-shared traffic leaf; SQL and agent leaves do not chain to that trust root. Adding or replacing a peer receives the existing traffic credentials and does not trigger a TLS outage. Traffic certificate renewal or explicit TLS rotation replaces both the traffic CA and shared leaf under a durable, **planned all-data-member outage**: stop every data process, publish/acknowledge the new credentials while stopped, recover every position, then bootstrap the highest-position seed and rejoin the others. The same-state-UUID/highest-sequence and missing-observation fail-closed rules apply. Client-facing leaves reload independently; this traffic-key rollover is not advertised as online rolling rotation.

Scheduled traffic renewal waits for active recovery, restore, backup or other maintenance ownership to finish; it must not prevent that owner's reconciliation from progressing. Inventory and certificate errors remain visible rather than being treated as a scheduling fence.

The dedicated traffic root has subject `<cluster>-galera-ca`, distinct from the shared leaf subject `<cluster>-galera`, so native OpenSSL consumers do not misclassify a self-issued-looking leaf as self-signed.

### Writer and routing

Although Galera accepts writes on every node, clients get a single writer: ProxySQL `mysql_galera_hostgroups` with writer/backup-writer/reader/offline hostgroups 10/11/20/30, `max_writers=1` and `writer_is_also_reader=2`. Traffic eligibility is `wsrep_cluster_status=Primary`, `wsrep_local_state=Synced` and `wsrep_ready=ON`, not TCP reachability. A node in a non-Primary component returns error 1047 to reads and writes and is removed from routing. A query that times out during writer failover has an unknown commit outcome; applications need idempotent retries.

### Formation and recovery

The control plane bootstraps the first member with `--wsrep-new-cluster` exactly once, recorded in the formation journal; the others join by state transfer. A single-member loss needs no control-plane action: the supervisor restarts the process, which rejoins by IST or SST.

The durable Galera cluster name is a random 32-character hexadecimal identity, generated anew for initial formation and restore. It is used literally by configuration, bootstrap, fencing and SST receipts, without a logical-name/protocol-name alias. The pinned Galera handshake stores its group name in [`String<32>`](https://github.com/percona/galera/blob/13ff9ed6577de1aa01e6e296f87ba439cde70b98/gcomm/src/gmcast_message.hpp#L61-L63), whose [constructor rejects longer names](https://github.com/percona/galera/blob/13ff9ed6577de1aa01e6e296f87ba439cde70b98/gcomm/src/gcomm/types.hpp#L24-L30); a hyphenated 36-character UUID therefore cannot be used as this name. Recovered wsrep state UUIDs remain separate, full UUID observations used for recovery comparison. MySQL Group Replication retains its required UUID-form group identity.

Quorum loss (a minority survivor) is not resolved automatically. A forced `pc.bootstrap=YES` is offered only as an explicit operator action after the missing members are fenced, on the survivor with the highest committed position.

After a full-cluster stop or crash, stored sequence numbers alone are not sufficient evidence (crashes commonly leave `grastate.dat` sequence `-1`). Recovery requires every stopped member's recovered redo position, one common cluster state UUID and the highest sequence number. Only the selected seed is marked safe to bootstrap and started at its recovered position; the others rejoin normally. Missing or disagreeing observations fail closed with an operator-visible condition. Graceful suspension uses the same evidence-based procedure on resume.

### Backups and restore

Backups and binary-log archiving follow ADR 0052 (XtraBackup through `xbcloud`, GTID-keyed binary logs from the routed writer). Restore always creates a new Galera history: the seed restores the prepared backup, starts as a new one-member cluster without the predecessor's peer list, and the remaining members join by SST. A backup's Galera coordinates are not trusted for recovery positions (prepared backups can carry a zero coordinate).

User-facing configuration, access, backup provenance and maintenance procedures are in the [MySQL and PXC guide](../mysql/README.md), especially [operations and recovery](../mysql/operations.md) and [portable native verification](../mysql/verification.md). PXC repositories retain the shared subtree shape under a separate `pxc` provider prefix.

## Consequences

PXC gives every committed transaction a certified copy on all Primary members at the cost of higher commit latency, flow control under slow members and strict schema rules. Full-outage recovery is deterministic only when every member's recovered position can be observed. Two engine providers share most code but keep separate catalogs, backup provenance and replication logic.

Compromise of any member's shared traffic private key compromises cluster-wide replication/SST authentication and confidentiality: that credential is not a per-member authorization boundary. Replacing only its leaf under the same CA does not revoke an exfiltrated credential. The coordinated rotation therefore replaces the dedicated traffic trust root as well, so reformed members reject the old credential. The traffic CA signing key remains sealed in the control plane and is not distributed to members; compromise of it has the same cluster-wide revocation scope. Trust rollover does not repair data already exposed or modified before fencing. SQL/agent trust and client identities remain separate from this traffic revocation domain.


## Alternatives considered

- **PXC as a topology of `mysql`:** would mix two binaries, catalogs and backup identities behind one provider name and complicate version and restore validation.
- **Galera arbitrator (`garbd`) for two-member clusters:** adds a third process without data and a separate failure domain obligation; three data members are simpler and safer.
- **Multi-writer routing:** certification conflicts surface as deadlock errors on commit; a single routed writer avoids them.
- **Automatic `pc.bootstrap` on quorum loss:** can create a second Primary component during a network partition.
- **Automatic traffic TLS with separate explicit replication leaves:** rejected because the pinned PXC startup implementation overwrites the affected Galera options with client SQL certificate paths. Fully encrypted manual configuration avoids that override without weakening any channel.

## References

[PXC 8.4](https://docs.percona.com/percona-xtradb-cluster/8.4/), [crash recovery](https://docs.percona.com/percona-xtradb-cluster/8.4/crash-recovery.html), [encrypted traffic](https://docs.percona.com/percona-xtradb-cluster/8.4/encrypt-traffic.html), [strict mode](https://docs.percona.com/percona-xtradb-cluster/8.4/strict-mode.html), [ProxySQL Galera](https://proxysql.com/documentation/galera-configuration/).

Pinned implementation evidence: [`wsrep_mysqld.cc`, auto-generated SSL options appended after provider options](https://github.com/percona/percona-xtradb-cluster/blob/33f7510189c5719c852837d7ad9ba5e7d6431f4d/sql/wsrep_mysqld.cc#L1208-L1236) and [`ssl_init_callback.cc`, options copied from MySQL SSL certificate paths](https://github.com/percona/percona-xtradb-cluster/blob/33f7510189c5719c852837d7ad9ba5e7d6431f4d/sql/ssl_init_callback.cc#L559-L564). The [upstream encryption guide](https://docs.percona.com/percona-xtradb-cluster/8.4/encrypt-traffic.html#ssl-manual-configuration) documents explicit Galera/IST and SST channel encryption and identical inter-node credentials.
