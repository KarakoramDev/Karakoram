<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# MySQL and Percona XtraDB Cluster

Karakoram offers two native MySQL-compatible providers. Choose the replication model before creating a cluster; switching providers is not a topology change.

| | Percona Server for MySQL | Percona XtraDB Cluster (PXC) |
| --- | --- | --- |
| Provider | `mysql` | `pxc` |
| Server majors | 8.4 LTS default; 8.0 for existing workloads | 8.4 LTS default; 8.0 for existing workloads |
| Topology | `single`: 1 member; `group`: 3 or 5 | `cluster`: 3 or 5 members only |
| Replication | Group Replication, single-primary, `MYSQL` communication stack | Galera certification, encrypted IST/SST |
| Application writer | Group-elected primary | One ProxySQL-routed writer, despite Galera's multi-writer capability |
| ProxySQL | 1–5 replicas; zero permitted only for `single` | 1–5 replicas required |
| Backup types | XtraBackup `full`, `incremental`; optional binary-log PITR | Same lifecycle, separate provider provenance and repository |
| Native access | MySQL users, databases and database-level grants | Same MySQL access interface |

Use `karactl providers get mysql -o json` or `karactl providers get pxc -o json` for exact available versions, presets, resources and upgrade paths. Version strings are provider-specific; do not substitute a Percona Server version for a PXC version. Catalog defaults currently select Percona Server **8.4.11-11** and PXC **8.4.11-11.1**. A compatible, checksum-locked XtraBackup release is selected with the server; its own compatibility check is not bypassed. Native artifacts target Linux amd64 with the declared Ubuntu 24.04 runtime dependencies; see [native artifacts](../native-artifacts.md).

## Create

The UI's **New cluster** wizard lists both providers. Choose a catalog preset, inspect topology and resources, then review TLS, exposure, backups and engine options. **MySQL access** is available on both providers when authorized. A successful create response accepts asynchronous work; wait for the observed generation, member health and application connectivity.

A minimal API-shaped creation file for MySQL:

```yaml
name: appdb
spec:
  provider: mysql
  topology:
    type: single
  components:
    engine:
      replicas: 1
    extra:
      proxy:
        replicas: 1
  tls:
    mode: requireTLS
    ca: selfSigned
```

```sh
karactl -n NS clusters create -f mysql.yaml --wait
karactl -n NS clusters status appdb -o json
karactl -n NS clusters members appdb -o json
```

For Group Replication, change topology to `group` and data replicas to 3 or 5. For PXC, set provider `pxc`, topology `cluster` and replicas 3 or 5. Omitting `version` selects that provider's default. The provider supplies resource defaults; inspect the catalog before production sizing. A singleton is **not highly available**. More members do not guarantee independent failure domains; see [placement limitations](../limitations.md).

## Choose with the failure model in mind

- **MySQL single:** no automatic failover; maintenance interrupts access.
- **MySQL group:** native single-primary election survives loss of the control plane while a majority remains available. Writes can block during quorum loss, then fail after minority fencing. A whole-group outage requires evidence-based re-formation.
- **PXC:** route applications through ProxySQL, not independently to every candidate writer. Only a `Primary`, `Synced`, `wsrep_ready=ON` member is eligible. A non-Primary minority rejects reads and writes. Certification does not mean every reader has already applied a commit; flow control can slow the whole cluster. Use InnoDB tables with primary keys and keep strict mode `ENFORCING`.
- **Both:** failover interrupts connections. A timeout can leave the transaction's commit outcome unknown. Reconnect and use application-level idempotency rather than blindly replaying writes. Replication is not a backup, nor a zero-loss or failover-time guarantee.

Physical incremental chains are pinned to a source agent and data incarnation, not a hostname or replication group: replicated InnoDB LSNs are not interchangeable. If that source is replaced, cloned, restored, or unavailable, take a new full backup. Legacy full backups remain restorable, but backups without physical provenance cannot parent or form incremental chains. Backup inventory records the selected member's running version even when a newer desired version is pending, so a fresh old-version backup can satisfy the major-upgrade prerequisite.

The backup API permits that prerequisite while a major upgrade is durably waiting at its backup gate, before rolling side effects, only when the desired version is the sole unpublished configuration change. Combining the version update with scaling, parameters, TLS, or other changes does not bypass normal backup readiness admission.

PXC shared replication-certificate rotation fences backup, restore, and backup-deletion operations until its full-stop recovery journal completes.

## Guides

- [Configuration and topology](configuration.md): supported options, proxy behavior and parameter ownership.
- [Access and TLS](access-and-tls.md): application identities, password rotation and connection examples.
- [Backups and PITR](backups.md): backup chains, GTID coverage, UUID isolation and account recovery.
- [Operations and recovery](operations.md): status, maintenance, scaling, upgrades and safe bootstrap boundaries.
- [Verification](verification.md): environment-selected native suites and recovery acceptance checklist.

Architecture decisions remain in [ADR 0052](../adr/0052-mysql-engine.md) and [ADR 0053](../adr/0053-pxc-engine.md). See [release status and backend availability](../backends.md#engine-availability).
