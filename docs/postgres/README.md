<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL

Karakoram operates native [Percona PostgreSQL](https://www.percona.com/postgresql/software) on provider-managed runtimes and per-member persistent volumes. This brief guide describes the implemented interface, not deployment acceptance or an availability guarantee.

Opt-in [cluster data MCP](../data-mcp.md) uses a private [pgEdge](https://www.pgedge.com/) companion and dedicated `kk_mcp_ro`/`kk_mcp_rw` roles. Configure database/schema scope; the companion connects to the first selected database (default `postgres`). Review the guide's PUBLIC-grant caveat before treating scope as isolation.

## At a glance

| Setting | Value |
| --- | --- |
| Engine driver | `postgres` |
| Versions | 16.14.1, 16.15.1; 17.10.1, **17.11.1** (default); 18.4.1, 18.6.1 |
| Topology | `single`: one member; `ha`: three or five with Patroni; [`distributed`](distributed.md): Citus coordinator + worker groups, gated by artifact availability |
| Presets | `dev`: single; `prod`: three-member HA; both include one ProxySQL replica |
| Data defaults / minima | Shared 1 CPU, 2 GiB RAM, 5 GB data disk / 1 GiB RAM and 5 GB disk |
| Client service | ProxySQL 3.0.11; `components.extra.proxy`, default 1, enabled range 1–5; zero only with single-member topology/groups |
| TLS | `requireTLS`, `selfSigned` by default |
| Recovery | pgBackRest `full` / `diff` / `incr`, opt-in PITR; [backups](backups.md) |
| Platform features | [PostGIS, pgvector and TimescaleDB profiles](profiles.md); [consumer bindings](bindings.md); [distributed Citus](distributed.md); see [release status/provider availability](../providers.md#engine-availability) |

Inspect the running catalog with `karactl providers get postgres -o json`; versions are pinned to exact Percona packages. A Percona product version such as 17.11.1 differs from PostgreSQL's reported 17.11 patch version. Each HA data member has its own volume containing PostgreSQL, [etcd](https://etcd.io/) and runtime state. Member names use `<cluster>p<index>`; the proxy service is `<cluster>px`. Indexes are monotonic. Separate members do not guarantee separate hosts; see [failure domains](../limitations.md).

The exact server package remains the APT candidate throughout subsequent profile
and dependency installation, with a separate preference per PostgreSQL major.
Adding a profile must not silently upgrade the server. Member status reads the
running server's Percona product release through SQL, rather than echoing the
assigned version; an unavailable version observation is empty.

The member agent supervises PostgreSQL and [ProxySQL](https://proxysql.com/). Preparation suppresses distribution-managed daemons; on [systemd](https://systemd.io/) runtimes it also stops and masks the packaged ProxySQL unit so it cannot compete for the agent's listeners. Runtimes without systemd skip that unit-management step.
The pinned Percona repository-bootstrap package is downloaded, SHA-256 verified
and installed in one privileged shell inside a fresh root-owned private staging
directory. Cleanup removes that directory on exit; installation never consumes a
package from a fixed pathname writable by the runtime user.

## Create

Save `postgres.yaml`, choose a [namespace](../namespaces.md), then create:

```yaml
name: appdb
spec:
  provider: postgres
  version: 17.11.1
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
  engineOptions:
    synchronousMode: "off"
    parameters:
      max_connections: "100"
    proxy:
      readWriteSplit: false
      readerPort: false
```

```sh
karactl -n NS clusters create -f postgres.yaml --wait
karactl -n NS clusters status appdb -o json
karactl -n NS clusters members appdb -o json
```

Quote `"off"` in YAML if your YAML tooling interprets it as a boolean. For HA, set topology `ha` and engine replicas 3 or 5. Names are lowercase alphanumeric, start with a letter and follow the shared API's cluster-name bounds. Wait timeouts do not cancel work; observe the existing operation/generation rather than submitting another create.

## Options

`engineOptions` is a strict object: unknown fields and `null` are rejected. `engineConfig` is not accepted.

- `synchronousMode`: `off` (default), `on`, `strict`. Strict HA can block writes when a synchronous standby is unavailable; it is not protection against every client durability override.
- `profiles`: additive workload set such as `[postgis, pgvector]`; absent means none. See [profiles, availability and upgrade rules](profiles.md).
- `parameters`: string-valued allowlisted PostgreSQL settings, including connection/memory, planner, logging, timeout and selected WAL/checkpoint tuning. Engine-owned networking, TLS, archiving, replication and preload settings cannot be overridden. See [`options.go`](../../internal/engine/postgres/options.go) for the exact allowlist.
- `proxy.readWriteSplit`: default false; SELECT-pattern reader routing is opt-in, not a general SQL classifier or read-after-write guarantee.
- `proxy.readerPort`: default false; enables the private reader listener on 5433. The managed public endpoint is 5432.

The catalog also describes `distributed` admission for [Citus](https://www.citusdata.com/) formations (see
[distributed operations guide](distributed.md) and [ADR 0046](../adr/0046-citus-distributed-topology.md)). This topology requires a
locked, resolvable `citus-pg<major>` native artifact for PostgreSQL 16–18; builds
without that artifact reject it. `components.engine.replicas` is the per-group
count, either 1 or 3 (default 3), and three-member groups require a proxy.
`engineOptions.citus` is a strict object with `workerGroups` (1–8, default 2)
and `database` (nonempty, at most 63 bytes, default `app`). Citus options are not
accepted for single/HA clusters. Conversion to or from distributed topology
and distributed major upgrades are rejected.

## Access and ports

Use [native roles, databases, grants and TLS](access-and-tls.md), not [MongoDB](https://www.mongodb.com/)-shaped `db-users`/`db-roles`. Ordinary connection summaries contain no password; credential reveal is separately authorized. Managed login passwords are synchronized into ProxySQL; SQL-created roles are not automatically routable through it.

Managed services use [consumer bindings](bindings.md) for owned databases, sealed credentials, rotation and dependency guards.

| Listener | Port | Scope |
| --- | --- | --- |
| PostgreSQL client endpoint | 5432 | Proxy service; private or opt-in public TCP; direct single member when proxy disabled |
| Optional proxy reader | 5433 | Private |
| PostgreSQL on data members | 5432 | Private replication/internal access, except explicit proxy-free single exposure |
| Patroni REST | 8008 | Private, TLS/authenticated |
| etcd client / peer | 2379 / 2380 | Private, mutual TLS |
| ProxySQL admin | 6132 (PG), 6032 (MySQL) | Loopback only |
| Member control | 9443 | Private management only |

## Limits

- ProxySQL does not implement the entire PostgreSQL protocol; review [driver/protocol limits](access-and-tls.md#proxy-compatibility) before selecting it for an application.
- HA requires quorum and reconnect-capable clients; no failover-time SLA is implied.
- Only single→HA and HA 3↔5 transitions are supported; no HA→single or disk shrink.
- Adjacent major upgrades are offline, backup-gated and use `pg_upgrade --link`; no downgrade or skipped major.
- Restores are destructive/offline and engine driver/major matched. Replication is not a backup.
- Native proxy artifacts target [Linux](https://www.kernel.org/) amd64/[Ubuntu](https://ubuntu.com/) 24.04. See [native artifacts](../native-artifacts.md), [operations](operations.md) and [ADR 0034](../adr/0034-postgres-engine.md).
