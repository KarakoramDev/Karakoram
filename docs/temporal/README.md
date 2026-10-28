<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Temporal

The `temporal` engine driver runs native [Temporal](https://temporal.io/) workflow services with persistence and SQL visibility in a bound [Karakoram PostgreSQL cluster](../postgres/README.md). Temporal roles are stateless; database HA and durable recovery belong to the source PostgreSQL cluster.

## Prerequisites

- **Provider:** [Zerops](https://zerops.io/) is not supported yet; use [Dokploy](https://dokploy.com/) or [Nomad](https://developer.hashicorp.com/nomad). Temporal membership requires a private IPv4 route to the bound PostgreSQL writer, while Zerops uses private IPv6. Zerops support is pending IPv6 private networking; see [release status and provider availability](../providers.md#engine-availability).
- An existing Karakoram PostgreSQL source cluster, major **12 or later**, reachable from the Temporal members. The PostgreSQL engine driver's own version catalog still applies.
- The **PostgreSQL binding service** (ADR 0045), which the standard server constructs once and shares with Temporal, [OpenBao](https://openbao.org/), [Keycloak](https://www.keycloak.org/) and the bindings API. A composition without it rejects Temporal clusters with `PostgreSQL bindings are unavailable`.
- Locked native artifacts and private networking between roles and the source writer. Members advertise the private IPv4 address used to reach that writer.
- Provider horizontal scaling for component replicas above one; configured Dokploy HTTPS/subdomain routing for a public Web UI. Nomad supports private UI access only.

Create, replace and patch admission (including dry runs) reject providers whose
runtime layout does not declare private IPv4 networking, before persisting intent.

The `PostgreSQLBinding` status condition reports `Bound` only after the binding
has an identity and its source connection has resolved. An unreachable source
member leaves the condition `False/SourceUnavailable`, including when a binding
identity was saved during an earlier reconciliation.

## At a glance

| Setting | Value |
| --- | --- |
| Server catalog | **1.32.1** (default), **1.31.4**; `spec.version` selects the server |
| Web UI | Release-locked `ui-server` **2.56.0** |
| Artifacts | `temporal-server-1.32`, `temporal-server-1.31`, `temporal-ui-server`; checksum-pinned [native artifacts](../native-artifacts.md) |
| Topology | `combined` (default) or `split`; immutable after creation |
| Frontend | Private TLS gRPC 7233; client certificates required by default |
| Recovery | Source PostgreSQL `full` / `diff` / `incr` backup through the binding, plus a Temporal manifest |

## Components and presets

All roles use `spec.components.extra`, with independent `replicas` and `resources`. Omit the standard `components.engine` slot from create, replace and patch requests; it is unused for both combined and split topologies. Responses include that slot with zero replicas as an internal default, but the shared request schema does not accept an explicit zero there. Nonzero engine replicas are also rejected by the Temporal engine driver. Each extra component is one provider service; replicas share that service identity.

| Component | Combined | Split | Replica bounds |
| --- | --- | --- | --- |
| `server` | All four server roles, default 1 | Disabled | 1–5 when enabled |
| `history` | Disabled | Default 1 | 1–9 |
| `matching` | Disabled | Default 1 | 1–5 |
| `frontend` | Disabled | Default 1 | 1–5 |
| `worker` | Disabled | Default 1; Temporal's internal worker, not application workers | 1–3 |
| `ui` | Default 1 | Default 1 | 0–3; zero disables |

The `dev` preset uses combined `server: 1`, `ui: 1`. The `prod` preset uses split `history: 3`, `matching: 2`, `frontend: 2`, `worker: 1`, `ui: 1`. Presets are configurations, not availability guarantees. Inspect `karactl providers get temporal -o json` and copy the desired preset into a complete cluster request; there is no `--preset` flag.

## Create and engineOptions

A minimal API-shaped JSON request:

```json
{
  "name": "workflows",
  "spec": {
    "provider": "temporal",
    "version": "1.32.1",
    "topology": {"type": "combined"},
    "engineOptions": {
      "persistence": {"postgres": {"cluster": "appdb"}},
      "historyShards": 512,
      "frontend": {"requireClientAuth": true},
      "namespaces": [{"name": "orders", "retention": "168h", "description": "Order workflows"}],
      "ui": {"publicHttps": false}
    }
  }
}
```

```sh
karactl -n NS clusters create -f temporal.json --wait
karactl -n NS clusters status workflows -o json
```

`engineOptions` is a strict JSON object: unknown fields and inline secrets are rejected. `engineConfig` and generic public frontend exposure are not supported.

| Option | Default / rule |
| --- | --- |
| `persistence.postgres.cluster` | Required source PostgreSQL cluster name |
| `persistence.postgres.namespace` | Temporal cluster's Karakoram namespace |
| `persistence.postgres.database` | `temporal_<cluster>`; cluster-name hyphens replaced with underscores |
| `persistence.postgres.visibilityDatabase` | `temporal_<cluster>_visibility`, with the same substitution |
| `historyShards` | 512; integer 1–16384 |
| `frontend.requireClientAuth` | `true`; TLS remains enabled when false |
| `namespaces` | Additional declarations with `name`, Go-duration `retention` ≥24h, optional `description` ≤1024 bytes; `default` is always ensured at 72h |
| `ui.publicHttps` | `false`; public HTTPS requires enabled UI, OIDC and provider support |
| `ui.oidc.issuer` / `ui.oidc.clientId` | HTTPS issuer and nonempty client ID; OIDC requires `ui.publicHttps: true` |

Database names must be distinct lowercase SQL identifiers, at most 63 characters (`[a-z_][a-z0-9_]*`); PostgreSQL system databases and the `_karakoram` prefix are reserved.
Namespace declarations are limited to 99 entries, and the serialized ensure request including the implicit `default` namespace must fit within 64 KiB.

Both databases live on the same source and have one binding-managed owner/login role. Connections use `postgres12` (lib/pq), the stable writer endpoint and TLS hostname verification (`verify-full`). Passwords are resolved from the binding, never stored in options/status; credential-generation changes roll the components.

Server persistence and visibility connections and schema migrations set lib/pq
`disable_prepared_binary_result=yes`, forcing all-text results even for explicit
prepared statements. ProxySQL 3.0.11 cannot represent mixed text/binary result
formats. pgx's `default_query_exec_mode=exec` alone is insufficient: explicitly
prepared visibility queries bypass that default. The agent's schema checks do
not explicitly prepare and continue using pgx `exec`. These choices apply to
direct singleton sources too; bound parameters and TLS verification are retained.

Named prepared statements are supported by this proxy; they are not named
cursor portals. lib/pq executes them with an unnamed portal and no incremental
cursor fetch. The dedicated pgx schema-lock connection uses a session advisory
lock, which ProxySQL tracks to disable multiplexing. That lock is lost with its
backend session, not migrated across a database failover. Do not intentionally
fail over a source during schema migration.

After a primary failure the stable writer endpoint routes new database work to
the promoted primary; SQL pools can replace failed connections. In-flight
transactions may fail and Temporal must recover through its persistence/task
retry paths. Qualify history, binary payloads, timers and explicit-prepared
visibility lookups after failover; frontend health alone is insufficient. See
the [binding failover boundary](../postgres/bindings.md#protocol-compatibility-and-primary-failover).

This fix requires newly deployed consumer agents; a control-plane upgrade or
ordinary restart does not replace them. See the [v0.4.1 consumer rollout
procedure](../upgrade.md#keycloak-and-temporal-postgresql-proxy-compatibility)
before recreating any existing consumer with valuable state.

The persistence block, `historyShards` and topology are immutable after creation. The Temporal cluster name is the Karakoram cluster name. Choose database names and shard count before admitting production workflows.

## Status

Normal cluster status is password-free:

- `version`, `phase`, `message`, `observedGeneration` and `members` describe convergence. Members report component set/role, observed version and health; UI members report the UI artifact version.
- `connection.hosts`, `connection.uri` and the `frontend` endpoint identify the private gRPC service; public UI URLs appear as `ui` endpoints only when enabled.
- `PostgreSQLBinding` conditions describe source namespace/cluster, `shared` and `consumerCount`, or `SourceUnavailable` while waiting.
- `TemporalNamespaces` contains observed workflow namespaces; `TemporalSchema` conditions report persistence/visibility versions by component; `TemporalUpgrade` reports an active rolling upgrade.
- `TemporalRestoreRecovery` fences maintenance while stopped runtimes are restarted and readiness-checked after an interrupted or failed source restore.

A Ready component proves that every live instance passed authenticated generic
agent/configuration checks and native readiness. Status publishes that positive
agent evidence with a fresh control-plane timestamp so the shared availability
lease can accept it. Each live sweep replaces the prior observation identity;
failed or expired observations never renew readiness. Repairing missing agent
observation evidence requires only a control-plane upgrade, not agent replacement.

## Limits

No multi-cluster replication or global namespaces, no per-namespace authorization, and no individual client-certificate revocation. Namespace descriptions cannot be cleared through Temporal's API. Temporal restores require recorded backups, not a PITR timestamp or an arbitrary clone destination. Backup manifests live in Temporal cluster state, so preserve [control-plane state and its master key](../operations.md#control-plane-state-backup-and-recovery) as well as PostgreSQL backups.

See [access and TLS](access-and-tls.md), [operations and recovery](operations.md), and [ADR 0056](../adr/0056-temporal-service.md).
