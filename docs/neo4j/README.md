<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Neo4j Community

Karakoram operates a native [Neo4j](https://neo4j.com/) Community server with one persistent member volume. See [release status and provider availability](../providers.md#engine-availability) and [ADR 0055](../adr/0055-neo4j-community-engine.md).

## At a glance

| Setting | Value |
| --- | --- |
| Engine driver | `neo4j` (Neo4j Community, GPLv3) |
| Versions | 2026.09.0; **2026.10.0** (default) |
| Topology | `single`, exactly one engine replica; no additional components |
| Runtime | Official Neo4j Unix tarball and Eclipse Temurin 21 JRE 21.0.12.1+1 |
| Resources | Default shared 1 CPU, 2 GiB RAM, 10 GB disk; minimum 1 GiB RAM, 1 GB disk |
| TLS | Required; cluster CA, `selfSigned` by default or `userProvided` |
| Recovery | Offline `dump` of `system` and `neo4j`; every backup is an outage |

Inspect `karactl providers get neo4j -o json` for the running catalog. Native downloads have exact sizes, SHA-256 hashes and upstream provenance in the [artifact lock](../../internal/engine/nativeartifacts/lock.json). An operator mirror takes precedence but must serve identical bytes; see [native artifacts](../native-artifacts.md). The member supervises the server JVM directly rather than retaining the launcher's additional JVM.

## Create

In the web UI, select **Neo4j Community** in **Clusters → Create cluster**. Its engine section exposes optional heap/page-cache MiB overrides, the exact allowlisted settings, and **Accept scheduled backup downtime** consent. Empty sizing fields use the resource-derived defaults below; consent is required before enabling schedules.

Save `neo4j.yaml` and choose a [namespace](../namespaces.md):

```yaml
name: graphdb
spec:
  provider: neo4j
  version: 2026.10.0
  topology:
    type: single
  components:
    engine:
      replicas: 1
  tls:
    mode: requireTLS
    ca: selfSigned
  engineOptions:
    acceptBackupDowntime: false
    settings:
      db.transaction.timeout: "60s"
```

```sh
karactl -n NS clusters create -f neo4j.yaml --wait
karactl -n NS clusters status graphdb -o json
karactl -n NS clusters members graphdb -o json
```

Wait timeouts do not cancel server work. Observe the existing operation rather than submitting another create.

## Memory and options

Heap defaults to 40% and page cache to 25% of **minimum** engine RAM, each rounded down to 64 MiB. Heap is bounded to 256–31744 MiB (31 GiB); page cache is at least 64 MiB. At the default 2 GiB RAM these are 768 MiB and 512 MiB. Remaining memory is needed by the JVM's native allocations, Lucene, Netty, the operating system and member agent; these are not workload sizing recommendations.

`engineOptions` accepts only:

- `heapMB` and `pageCacheMB`: integer MiB overrides with the same bounds; their sum must not exceed 80% of minimum RAM.
- `acceptBackupDowntime`: defaults false; must be true for any enabled backup schedule.
- `settings`: string-valued map restricted to the following keys:
  - `db.transaction.timeout`
  - `db.transaction.concurrent.maximum`
  - `db.lock.acquisition.timeout`
  - `db.logs.query.enabled`
  - `db.logs.query.threshold`
  - `db.query.default_language`
  - `dbms.cypher.planner`

Unknown fields, `null` options, multiline values and generic `engineConfig` text are rejected. Paths, connectors, TLS, authentication and JVM flags are managed. Memory/settings changes restart the singleton and interrupt all service. Disk cannot shrink.

## Access and limits

Use [Bolt, native users and TLS](access-and-tls.md), [offline backups](backups.md) and [operations](operations.md).

- Community only: no Enterprise/BYOL mode, clustering, failover, online backup, RBAC or native [Prometheus](https://prometheus.io/) endpoint.
- Only one user database, `neo4j`, alongside `system`; no additional user databases.
- **Every native user has full administrative privileges.** Separate credentials do not provide least-privilege isolation.
- Backups, restore, restart, TLS rotation, memory/settings changes and upgrades are full outages.
- No PITR, incremental backups, downgrade or disk shrink. Retain independent backups and test recovery.
