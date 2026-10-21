<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL

Karakoram operates native Percona PostgreSQL on backend-managed runtimes and per-member persistent volumes. This brief guide describes the implemented interface, not deployment acceptance or an availability guarantee.

Opt-in [cluster data MCP](../data-mcp.md) uses a private pgEdge companion and dedicated `kk_mcp_ro`/`kk_mcp_rw` roles. Configure database/schema scope; the companion connects to the first selected database (default `postgres`). Review the guide's PUBLIC-grant caveat before treating scope as isolation.

## At a glance

| Setting | Value |
| --- | --- |
| Provider | `postgres` |
| Versions | 16.14.1, 16.15.1; 17.10.1, **17.11.1** (default); 18.4.1, 18.6.1 |
| Topology | `single`: one PostgreSQL member; `ha`: three or five members with Patroni and co-located etcd |
| Presets | `dev`: single; `prod`: three-member HA; both include one ProxySQL replica |
| Data defaults / minima | Shared 1 CPU, 2 GiB RAM, 5 GB data disk / 1 GiB RAM and 5 GB disk |
| Client service | ProxySQL 3.0.11; `components.extra.proxy`, default 1, enabled range 1–5; zero only with `single` |
| TLS | `requireTLS`, `selfSigned` by default |
| Recovery | pgBackRest `full` / `diff` / `incr`, opt-in PITR; [backups](backups.md) |

Inspect the running catalog with `karactl providers get postgres -o json`; versions are pinned to exact Percona packages. A Percona product version such as 17.11.1 differs from PostgreSQL's reported 17.11 patch version. Each HA data member has its own volume containing PostgreSQL, etcd and runtime state. Member names use `<cluster>p<index>`; the proxy service is `<cluster>px`. Indexes are monotonic. Separate members do not guarantee separate hosts; see [failure domains](../limitations.md).

The member agent supervises PostgreSQL and ProxySQL. Preparation suppresses distribution-managed daemons; on systemd runtimes it also stops and masks the packaged ProxySQL unit so it cannot compete for the agent's listeners. Runtimes without systemd skip that unit-management step.
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
- `parameters`: string-valued allowlisted PostgreSQL settings, including connection/memory, planner, logging, timeout and selected WAL/checkpoint tuning. Engine-owned networking, TLS, archiving, replication and preload settings cannot be overridden. See [`options.go`](../../internal/engine/postgres/options.go) for the exact allowlist.
- `proxy.readWriteSplit`: default false; SELECT-pattern reader routing is opt-in, not a general SQL classifier or read-after-write guarantee.
- `proxy.readerPort`: default false; enables the private reader listener on 5433. The managed public endpoint is 5432.

## Access and ports

Use [native roles, databases, grants and TLS](access-and-tls.md), not MongoDB-shaped `db-users`/`db-roles`. Ordinary connection summaries contain no password; credential reveal is separately authorized. Managed login passwords are synchronized into ProxySQL; SQL-created roles are not automatically routable through it.

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
- Restores are destructive/offline and provider/major matched. Replication is not a backup.
- Native proxy artifacts target Linux amd64/Ubuntu 24.04. See [native artifacts](../native-artifacts.md), [operations](operations.md) and [ADR 0034](../adr/0034-postgres-engine.md).
