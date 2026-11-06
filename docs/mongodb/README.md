<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# MongoDB

Manage [Percona Server for MongoDB](https://docs.percona.com/percona-server-for-mongodb/) (PSMDB) and [Percona Backup for MongoDB](https://docs.percona.com/percona-backup-mongodb/) (PBM) on [Zerops](https://zerops.io/).

Opt-in [cluster data MCP](../data-mcp.md) uses a private `mongodb-mcp-server` companion and separate scoped read/write users. Configure `scope.databases`; system databases cannot be scoped. It is disabled by default and independent of ordinary credential reveal.

## At a glance

| Setting | Supported value / default |
| --- | --- |
| Engine driver | `psmdb` |
| PSMDB catalog | `6.0.29-23` (deprecated); `7.0.40-22`, `7.0.43-23`, **`7.0.45-24` (recommended 7.0)**, `8.0.29-13`, `8.0.32-14`; **`8.0.34-15` (default)** |
| PBM | `2.16.0-1`; Debian package pins append `.noble` |
| Shell | Official MongoDB `mongosh` 2.14.1 on 7.0/8.0; legacy Percona 2.8.3 on 6.0 |
| Topologies | `replicaSet`: 1/3/5/7 data-bearing members; `sharded`: config set, shard sets and mongos |
| Resource floors | Whole CPU cores ≥1; RAM ≥0.5 GiB; disk ≥5 GiB; maxima ≥minima; start CPU within bounds |
| Engine driver resource defaults | SHARED CPU, min/max 1 core, 1 GiB RAM, 5 GiB disk; omitted maxima follow effective minima |
| Database TLS | `requireTLS`, generated per-cluster CA |
| Recovery | PBM `logical` / `physical` snapshots and PITR |
| Public TCP | Opt-in, CIDR allowlist; project IPv6 or dedicated IPv4 |

Query your installation with `karactl providers get psmdb -o yaml`. Catalog defaults are resolved at creation, not automatic upgrades. Existing patches remain offered for existing clusters and forward upgrades; use the latest patch in each line for new clusters. Package availability is not an upstream lifecycle guarantee; PSMDB 8.3 GA is excluded pending upgrade/FCV qualification. [Operations](operations.md) describes supported upgrade paths.

PSMDB 6.0 [reached end of life on 2025-07-31](https://www.percona.com/release-lifecycle-percona-software-for-mongodb/). Fresh 6.0 creates are rejected with `spec.version` / `unsupported` and disabled in the create wizard. Existing 6.0 clusters keep running and remain editable; upgrade through 7.0 to 8.0, raising FCV at each adjacent major. Recovery of existing 6.0 data remains possible through the API/CLI with a validated `spec.dataSource` (succeeded same-major/topology backup or observed PITR window), including retained backups whose source cluster is gone. This is a recovery-only exception, not renewed vendor support. The current PBM compatibility matrix excludes 6.0; retain and qualify existing recovery tooling/backups before migration.

## How it runs

Each mongod has a singleton Zerops runtime and persistent Local Storage volume. Karakoram deploys its agent and pinned Percona packages, bootstraps the first member through MongoDB's localhost exception, and establishes reserved management accounts. Sharded creation initializes config servers, shard sets and routers, then registers shards.

Runtime names are `{cluster}m{index}`, `{cluster}s{shard}m{index}`, `{cluster}c{index}` and `{cluster}r`; volumes append `v`. Replacement indexes are monotonic. Routers are stateless. Agents supervise engine processes and cache protected configuration; see [runtime operations](operations.md).

## Create

Select the target [namespace](../namespaces.md), then **Clusters → Create cluster → MongoDB**. The wizard exposes:

| Step | MongoDB choices |
| --- | --- |
| Basics | Name, exact version, replica set or sharded topology, optional preset |
| Topology / Resources | Member/shard/config-server/router counts; component CPU/RAM/disk bounds; validated YAML overrides; routers have no durable data-disk editor |
| Backups | Registered storage or None, logical/physical schedules, PITR interval, optional backup/time seed and source namespace |
| Access & TLS | TLS mode/CA and optional certificate pairs, exposure/IP family/CIDRs, monitoring |
| Review | All settings, aggregate runtime CPU/RAM, data volumes/capacity, separate runtime disks and placement warning |

**Custom** uses no preset; **dev** selects one member without redundancy; **prod** selects three voters; **sharded** selects two three-member shards, three config servers and one router. Presets use the resource defaults above and remain editable; none certifies capacity or host separation. Check aggregate footprint against project capacity; object storage is additional, and totals are not a price quote or initial allocation.

CLI example (after [CLI authentication](../cli.md)):

```sh
karactl -n default clusters create demo --provider psmdb --topology replicaSet --members 3 --wait
```

This uses CLI convenience resource bounds; use the explicit bounds in the replica-set guide when sizing a fixed footprint.

Use the [replica-set checklist and CLI alternative](replica-sets.md) or [sharded creation example](sharding.md). Connect using [scoped application users and TLS](access-and-tls.md), not revealed administrative credentials.

## Limits

- Names and topology type are immutable; names are 2–12 lowercase alphanumeric characters starting with a letter.
- No arbiters/custom voting layout or per-shard independent component specs; [topology bounds and shard administration](sharding.md) apply.
- No shard removal or replica-set-to-sharded conversion; migrate into a compatible new deployment.
- Singletons incur maintenance downtime; multiple voters do not establish [independent failure domains](../limitations.md).
- The RAM floor is for disposable tiny workloads, not a load/recovery guarantee; reserve WiredTiger/PBM headroom.
- No arbitrary package versions, downgrades or skipped-major upgrades; [binary upgrades and explicit FCV](operations.md) are separate.
- Overrides cannot replace managed networking/authentication/TLS/replication/sharding/process/logging/dbpath/cache settings.
- Public seeds are not external replica-set HA; read [public exposure and discovery](access-and-tls.md#public-exposure-and-discovery).
- Shared [SeaweedFS](https://seaweedfs.com/) dbpaths and live volume archives are not supported MongoDB recovery substitutes; use [PBM](backups.md).
- Restore is disruptive, compatibility-bound and not necessarily a complete logical rollback; [restore and clone](backups.md) define coverage and retained-snapshot requirements.
- Count retention does not guarantee PITR duration; inspect observed continuous windows after backup/storage changes.
- [Monitoring and process tails](operations.md#monitoring-vocabulary) have bounded coverage; see [log retention](../operations.md#health-and-logs).
- Existing agents update on member deployment, not control-plane restart; lost control-plane state cannot safely adopt members. Preserve [state and original encryption key](../operations.md#control-plane-state-backup-and-recovery).
