<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Temporal

The `temporal` provider runs native Temporal workflow services with persistence and SQL visibility in a bound [Karakoram PostgreSQL cluster](../postgres/README.md). Temporal roles are stateless; database HA and durable recovery belong to the source PostgreSQL cluster.

## Prerequisites

- An existing Karakoram PostgreSQL source cluster, major **12 or later**, reachable from the Temporal members. The PostgreSQL provider's own version catalog still applies.
- The **PostgreSQL binding service** (ADR 0045), which the standard server constructs once and shares with Temporal, OpenBao, Keycloak and the bindings API. A composition without it rejects Temporal clusters with `PostgreSQL bindings are unavailable`.
- Locked native artifacts and private networking between roles and the source writer. Members advertise the private IPv4 address used to reach that writer.
- Backend horizontal scaling for component replicas above one; backend public HTTPS/subdomain support for a public Web UI.

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

All roles use `spec.components.extra`, with independent `replicas` and `resources`. Leave `components.engine` unused (zero replicas). Each component is one backend service; replicas share that service identity.

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
| `ui.publicHttps` | `false`; public HTTPS requires enabled UI, OIDC and backend support |
| `ui.oidc.issuer` / `ui.oidc.clientId` | HTTPS issuer and nonempty client ID; OIDC requires `ui.publicHttps: true` |

Database names must be distinct lowercase SQL identifiers, at most 63 characters (`[a-z_][a-z0-9_]*`); PostgreSQL system databases and the `_karakoram` prefix are reserved.
Namespace declarations are limited to 99 entries, and the serialized ensure request including the implicit `default` namespace must fit within 64 KiB.

Both databases live on the same source and have one binding-managed owner/login role. Connections use `postgres12_pgx`, the stable writer endpoint and TLS hostname verification (`verify-full`). Passwords are resolved from the binding, never stored in options/status; credential-generation changes roll the components.

The persistence block, `historyShards` and topology are immutable after creation. The Temporal cluster name is the Karakoram cluster name. Choose database names and shard count before admitting production workflows.

## Status

Normal cluster status is password-free:

- `version`, `phase`, `message`, `observedGeneration` and `members` describe convergence. Members report component set/role, observed version and health; UI members report the UI artifact version.
- `connection.hosts`, `connection.uri` and the `frontend` endpoint identify the private gRPC service; public UI URLs appear as `ui` endpoints only when enabled.
- `PostgreSQLBinding` conditions describe source namespace/cluster, `shared` and `consumerCount`, or `SourceUnavailable` while waiting.
- `TemporalNamespaces` contains observed workflow namespaces; `TemporalSchema` conditions report persistence/visibility versions by component; `TemporalUpgrade` reports an active rolling upgrade.
- `TemporalRestoreRecovery` fences maintenance while stopped runtimes are restarted and readiness-checked after an interrupted or failed source restore.

## Limits

No multi-cluster replication or global namespaces, no per-namespace authorization, and no individual client-certificate revocation. Namespace descriptions cannot be cleared through Temporal's API. Temporal restores require recorded backups, not a PITR timestamp or an arbitrary clone destination. Backup manifests live in Temporal cluster state, so preserve [control-plane state and its master key](../operations.md#control-plane-state-backup-and-recovery) as well as PostgreSQL backups.

See [access and TLS](access-and-tls.md), [operations and recovery](operations.md), and [ADR 0056](../adr/0056-temporal-service.md).
