<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Architecture

Karakoram is a self-hosted, single-project control plane for PSMDB/PBM on Zerops. It does not run Kubernetes or reuse OpenEverest's Kubernetes provider runtime. The [ADRs](adr/README.md) record the design rationale; current implementation details and explicit limitations are summarized here.

## Deployment and trust boundaries

```mermaid
flowchart LR
  Browser[Browser / karactl] -->|Public HTTPS| L7[Zerops HTTP routing]
  L7 -->|HTTP 8080| CP[Karakoram control plane]
  CP --> DB[(SQLite on karakoramdata)]
  CP -->|Project-scoped token| Z[Zerops REST API]
  CP -->|MongoDB wire protocol| M[PSMDB / mongos]
  CP -->|Pinned CA + member token, TLS 9443| A[Node agent]
  A -->|Pull config, TLS 9443| CP
  A -->|Supervise / render files| M
  A --> P[PBM agent / bounded PBM CLI]
  P --> S[S3-compatible backup storage]
```

- **Control plane:** one runtime service, normally `karakoram`, with one container and a `karakoramdata` Local Storage volume mounted at `/mnt/karakoramdata`. Public UI/API is HTTP 8080 behind Zerops HTTPS; the private agent listener is TLS 9443. The service holds the project's infrastructure token and the operator-held master key.
- **Data-bearing members:** one Ubuntu 24.04 runtime plus one Local Storage volume per mongod, including config servers. Singleton services provide stable replica-set identities; horizontal runtime scaling is not MongoDB replication. Persistent dbpath is under `KARAKORAM_DATA_DIR/db`.
- **Routers:** one stateless mongos service per sharded cluster, potentially multiple containers. It has no durable data volume. Multiple router containers share a service identity; they are not independent data-bearing voters.
- **Node agent:** the same executable as the control plane, started as `karakoram agent`, supervises mongod/mongos and PBM agents and exposes a bounded private API. It is not a remote shell.

Private networking is not a security boundary on its own: agent traffic has pinned trust roots and per-member bearer tokens. Database keyfile authentication, database TLS, internal agent TLS and public HTTPS protect different paths. See [security](security.md).

## Code boundaries

| Area | Responsibility |
| --- | --- |
| `internal/server`, `internal/api` | Runtime wiring, generated strict HTTP contract, handlers, session/RBAC enforcement |
| `internal/domain`, `internal/store` | Resource types, SQLite WAL/migrations, transactional intent, checkpoints, events and secrets |
| `internal/reconciler` | Per-cluster serialization, timed/triggered reconciliation, durable operation execution and cleanup |
| `internal/engine` | In-process Provider registry and narrow reconciliation/data-plane interfaces |
| `internal/engine/psmdb` | Catalog/defaults/validation, replica sets/sharding, topology observation, rolling changes, access/PKI |
| `internal/engine/psmdb/pbm` | Backup/PITR/inventory/restore/checkpoint orchestration |
| `internal/infra`, `internal/infra/zerops` | Backend intent and owned thin Zerops REST translation |
| `internal/agent`, `internal/agentapi` | Node supervision, protected file rendering, private command/config protocol |
| `internal/auth`, `internal/rbac` | Argon2id/OIDC identity, sessions/API tokens, persisted Casbin policy |
| `internal/scheduler`, `internal/events` | UTC backup schedules/count retention, durable events/SSE delivery |
| `internal/monitoring` | Agent scrapes, SQLite sample history, UI series and Prometheus exposition |
| `internal/cli`, `ui/`, `internal/ui` | Generated-client CLI, typed React SPA and embedded serving |

Only the `psmdb` provider and `zerops` backend are implemented. The seams isolate present responsibilities; they are not dynamically installable plugins or promises of compatibility with another project's providers.

## Desired state and durable operations

Clusters have desired `spec`, observed `status`, generation/observedGeneration and provider-owned internal state. Spec-driven work includes creation, membership/resource/disk/version changes, shard addition, TLS mode, exposure, backup configuration and PITR. Imperative work is a persisted Operation: backup, delete-backup, restore, raise-fcv, restart, replace-member, rotate-system-user or rotate-tls.

The provider exposes `Describe`, `ApplyDefaults`, `Validate`, `Sync`, `Status`, `RunOperation` and `Cleanup`. It receives store/secret/backend/agent capabilities rather than constructing generic API responses. A cluster worker serializes operations and Sync; unrelated clusters can progress separately. Steps persist checkpoints and platform identifiers and observe completion before advancing. Pending PSMDB work normally requeues after five seconds; converged clusters after thirty seconds. One Sync call performs at most one disruptive database/infrastructure step.

Neither an accepted HTTP mutation nor a successful Zerops configuration Process means a database is healthy. Platform Process success is `FINISHED`; `FAILED` and `CANCELED` are errors. Engine observation supplies health, elected role, lag, binary version and FCV. There is no distributed exactly-once transaction across SQLite, Zerops and PBM; durable IDs, receipts and observation reduce unsafe replay, and uncertain outcomes remain explicit errors.

Failed platform actions are retried with durable backoff. When an action is rate-limited, Karakoram persists a retry deadline rather than continuously resubmitting it; CPU-mode waits are surfaced as `PlatformCooldown`.

## Provisioning and database changes

A member progresses through planned/imported/deployed/healthy state. Karakoram imports runtimes/volumes **without inline pipeline YAML**, then creates an app version, uploads a gzip-tar containing its own executable and deploys the complete generated pipeline. Version-pinned apt preparation text determines the cached runtime image. `temporaryShutdown` prevents overlapping writers on a dbpath, and readiness uses `/var/www/karakoram agent readiness`.

The first member bootstraps a set and `userAdmin` through MongoDB's localhost exception. Authenticated administration establishes the five system accounts. Sharded plans initialize `cfg`, shard sets `rs0` onward and routers, then register shards. New members initially join non-voting/priority-zero, synchronize and are promoted after observed health/lag. Removal/rolling changes check quorum and transfer primary leadership as needed. Config servers precede shards and routers in binary upgrades; FCV promotion is separate.

Cluster names are 2–12 lowercase alphanumeric characters beginning with a letter. Runtime identities are `{cluster}m{index}`, `{cluster}s{shard}m{index}`, `{cluster}c{index}` and `{cluster}r`; volume names append `v`. Monotonic indexes are not reused after replacement. Compact generated hostnames leave room for component and member suffixes.

## Node-local durability

Agents pull assigned configuration from `/internal/agent/v1/config` and cache `DATA_DIR/karakoram/config.json` (0600). They re-pull every 60 seconds and on reload. Data-bearing members can start from the cache while the control plane is unavailable; first startup without a cache cannot. Stateless routers lose their cache across runtime replacement.

Rendered keyfiles are 0400; configs/certificates are owner-only. `controls.json` durably records explicit maintenance/process-stop holds independently of pulled configuration. Physical restore may intentionally stop mongod and let PBM exit successfully; neither periodic pulls nor agent restart may remove those holds. PBM initiation receipts under `pbm-receipts/` preserve completed responses and distinguish an interrupted, ambiguous initiation from a safe retry. See [operations](operations.md#node-agent-semantics).

## Recovery, monitoring and events

PBM agents run on every mongod, including config members, and never on mongos. Sharded commands run through the config-server replica set. S3 destinations and cluster-specific prefixes separate recovery histories. Restores checkpoint PITR disabling, balancer/router handling, maintenance, offline physical progress, process recovery, restored-user reconciliation, prefix resync and cleanup. A successful restore requiring PITR continuation queues a fresh base backup before re-enabling slicing.

The control plane owns UTC schedules and successful-backup count retention. Control-plane downtime pauses new scheduling/retention; existing PBM operations and already configured PITR run on members. MongoDB backup objects are independent of SQLite control-plane backups.

Agents export member metrics. The collector defaults to 30-second scrapes and 24-hour sample retention. JSON series derive reset-aware counter rates; `/metrics` exports authenticated Prometheus text with per-cluster authorization. PMM is not shipped. Durable events feed `/api/v1/events/stream`; SSE is an acceleration with replay IDs, not the persistence layer.

## Failure domains

There is **no control-plane HA**, guaranteed member anti-affinity, or rack/zone guarantee. Local Storage pins a member to a single machine, and separate services/volumes can be co-located. Best-effort placement fingerprints report uncertainty, not certification. Public TCP requires project IPv6 or dedicated IPv4 and does not implement automatic replica-set external split horizons. Review [limitations](limitations.md), [state recovery](operations.md#control-plane-state-backup-and-recovery) and [public access](user-guide/users-tls-access.md#public-exposure-and-discovery) before relying on availability or external failover.
