<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Clusters and operations

A cluster is a namespace-scoped desired configuration, observed engine topology and durable operation history.

## Cluster identity and capabilities

Each cluster selects an immutable engine driver/engine. Data-bearing members are singleton runtimes with independently identified persistent volumes supplied by the namespace's provider (Local Storage on [Zerops](https://zerops.io/)); auxiliary stateless components depend on the engine driver. Cluster names are 2–12 lowercase letters/digits, beginning with a letter, and unique within a [namespace](namespaces.md). Use `-n <namespace>` to select one; without a selected namespace, single-object requests default to `default`.

The engine driver catalog advertises versions, upgrade paths, topology/component limits, backup types and operations. Query `karactl providers list` or `karactl providers get <provider> -o yaml`. Supported does not mean disruption-free: follow the linked engine's prerequisites and limits.

| Capability | [MongoDB](mongodb/README.md) (`psmdb`) | [RabbitMQ](rabbitmq/README.md) | [Elasticsearch](elasticsearch/README.md) | [PostgreSQL](postgres/README.md) / [Supabase](supabase/README.md) | [Temporal](temporal/README.md) |  [Neo4j Community](neo4j/README.md) | [Weaviate](weaviate/README.md) |
| --- | --- | --- | --- | --- | --- |  --- | --- |
| Create | Yes | Yes | Yes | Yes; Supabase artifact availability gates components | Requires PostgreSQL source and binding service |  Yes; single member | Yes |
| Scale CPU/RAM/disk | Yes; disk only grows | Yes; disk only grows | Yes; disk only grows | Yes; disk only grows and needs provider capability | Stateless role resources; durable disk belongs to source |  Yes; disk only grows; memory changes interrupt service | Yes; disk only grows |
| Add/remove members | Yes | Yes | Yes | Single→HA; HA 3↔5; no HA→single | Per-role replicas within topology bounds |  No | Cluster 3–9; drain/remove non-voters only; no automatic rebalance |
| Replace member | Yes | Yes | Yes | Yes | No separate replacement operation |  No | Preserve identity; healthy/quorum or proven surviving replicas required |
| Rolling restart | Yes | Yes | Yes | HA; single interrupts service | Ordered by role; singleton roles interrupt service |  Restart interrupts service | Non-voters before voters; single interrupts service |
| Upgrade | Catalog paths | Catalog paths | Catalog paths | Rolling minor; offline adjacent major for postgres; PG17 minor only for Supabase | Catalog paths; no skipped minors/downgrades |  2026.09.0→2026.10.0; offline, fresh backup required | Cataloged patch-forward/one minor forward; no downgrade |
| Feature compatibility version (FCV) promotion | Yes | No | No | No | No |  No | No |
| TLS rotation | Yes | Yes | Yes | Yes; no CA replacement | Three-phase CA rotation |  Yes; full outage | Rolling restart |
| System-user rotation | Yes | Yes | Yes | Yes | Binding-managed database credentials |  Managed `neo4j` | Root key: two rolling passes; internal password: full restart |
| Public TCP exposure | Yes | Yes | Yes | Yes; selected client service | No; UI HTTPS only with OIDC |  Bolt only | REST/gRPC with TLS; internal ports private |
| Backup types | Percona Backup for MongoDB (PBM): logical, physical | Definitions only; no messages | Native snapshots | pgBackRest full/diff/incr; database data only | Source full/diff/incr plus state manifest |  Offline `dump`; system + graph; full outage | Native S3 full |
| Point-in-time recovery (PITR) | Yes | No | No | Yes, opt-in | No Temporal timestamp restore |  No | No |
| In-place restore | Yes | Merge definitions | Native restore | Destructive/offline | Destructive source restore; shared-source acknowledgement |  Destructive/offline | Destructive; same member count |
| Clone | Yes | Definitions only | Yes | Same engine driver and PG major only | No arbitrary destination; original source/name required |  Neo4j dump; target version not older than source | Same member count; fresh target users/roles |
| Suspend/resume | Yes | Yes | Yes | Yes | Retain bound databases and disable login role |  Yes | Yes |
| Cross-namespace clone | Yes | Definitions only | Yes | Yes; same engine driver and PG major | No arbitrary destination |  Yes; Neo4j backup only | Yes; storage/recovery admission applies |

See [Backups and storage](backups.md) for shared recovery mechanics and the engine guides for runtime, compatibility and checkpoint rules.

### Weaviate

[Weaviate](weaviate/README.md) supports immutable `single` or `cluster` topology, fixed voter identities and typed options instead of raw engine configuration. Its internal transports require isolated private networking even when client TLS is enabled. Read [native access/TLS](weaviate/access-and-tls.md), [draining, replacement and rotation](weaviate/operations.md), and [full-backup recovery constraints](weaviate/backups.md) before maintenance.

### PostgreSQL and Supabase

[PostgreSQL](postgres/README.md) adds `single` (one member) and `ha` (three/five [Patroni](https://patroni.readthedocs.io/) + [etcd](https://etcd.io/) members), a separate [ProxySQL](https://proxysql.com/) component, native roles/databases/grants and [pgBackRest](https://pgbackrest.org/) full/diff/incr/PITR recovery. It supports cataloged [Percona](https://www.percona.com/) majors 16/17/18, replica-first minor maintenance and offline backup-gated adjacent major upgrades. Single→HA and HA 3↔5 are supported; HA→single is rejected. See [access/TLS](postgres/access-and-tls.md), [operations](postgres/operations.md) and [backups](postgres/backups.md).

[Supabase](supabase/README.md) wraps that core, limits PostgreSQL to major 17 and adds independently sized stateless components. `spec.version` is the database version; application versions are release-locked. Internal database connections bypass ProxySQL through a leader-aware TCP forwarder. See [components/presets/dependencies and current artifact limits](supabase/components.md) and [credentials, SMTP and function operations](supabase/operations.md). Standard/full preset definitions do not imply their artifacts are currently available.

Both engine drivers support TLS/system-user rotation, public TCP subject to provider capability, restore/clone, replacement and retained-volume suspension. Backups match engine drivers exactly and protect database data, not separate Supabase object storage or all control-plane-held application state. [Shared recovery](backups.md) describes namespace and retention rules.

### Temporal

[Temporal](temporal/README.md) runs combined or split stateless workflow roles on an existing Karakoram PostgreSQL ≥12 source. It uses the control plane's shared PostgreSQL binding service. Server versions and UI artifacts are locked separately. Topology, persistence options and history shard count are immutable; scaling is per role. See [access/TLS and OIDC](temporal/access-and-tls.md) and [namespace, upgrade and recovery operations](temporal/operations.md). Temporal backup manifests live in cluster state, while the PostgreSQL source owns archives and retention.
### MySQL

The `mysql` engine driver runs Percona Server in `single` or three/five-member,
single-primary Group Replication topology. Group scaling clones new monotonic
member identities from the ONLINE writer before joining them. Removal drains
ProxySQL, hands the writer to an up-to-date ONLINE secondary when necessary,
leaves Group Replication, and confirms the stopped runtime before deleting the
ID-fenced runtime and volume. Rejected volume deletion is handed to orphan-volume
inventory. Replacement joins a new identity before retiring its predecessor;
cancellation before retirement rolls back the new identity, while cancellation
after retirement begins completes the cutover under reconciliation ownership.

Single→group retains the existing seed volume and uses a new group identity and
bootstrap epoch; group→single is rejected. Group topology requires ProxySQL.
Resource changes, startup-only parameter changes and minor upgrades roll
secondaries before the writer. The allowlisted `innodb_buffer_pool_instances`
and `table_open_cache_instances` settings require a restart and remain pinned per
member until its turn; the other allowlisted variables are applied live. The
8.0→8.4 major upgrade requires a successful current-version backup started since
the durable upgrade request, publishes and acknowledges `innodb_fast_shutdown=0`
before stopping each member, and upgrades the old writer last. Its departure lets
Group Replication elect an upgraded writer; lower-version join exceptions are
never enabled. A singleton upgrade is offline. Downgrades are rejected.

TLS rotation stages new member leaves under the existing CA, publishes them,
and waits for configuration acknowledgement. [MySQL](https://www.oracle.com/mysql/) reloads client TLS in place;
GR recovery uses the new leaves on its next connection. ProxySQL applies its
updated TLS configuration. Formation, restore and active physical backups fence
membership changes. Durable membership, retirement, rolling and TLS journals
resume after control-plane restart.

### Percona XtraDB Cluster

The separate `pxc` engine driver supports only [Galera](https://galeracluster.com/) `cluster` topology with three or five data members and at least one ProxySQL replica. It shares MySQL-native access and physical backup lifecycle, but keeps its own artifact/version catalog, `pxc` repository prefix and restore provenance. MySQL↔[PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster) conversion and cross-engine driver restore are rejected. Traffic requires Primary/Synced/readiness, with one proxy-routed writer; quorum loss rejects minority reads and writes. Full-outage recovery compares every stopped member's recovered same-UUID Galera position; explicit minority recovery additionally fences missing owned members before bootstrap. See [MySQL/PXC configuration](mysql/configuration.md), [operations and recovery](mysql/operations.md) and [backup/UUID caveats](mysql/backups.md).

## Create and change desired state

Use **Clusters → Create cluster** and the engine driver's wizard, or prepare a complete `{name, spec}` YAML/JSON request using its engine guide:

```sh
karactl -n <namespace> clusters create <cluster> -f cluster.yaml --wait
karactl -n <namespace> clusters list
karactl -n <namespace> clusters get <cluster> -o yaml
karactl -n <namespace> clusters patch <cluster> -f patch.yaml --wait
karactl -n <namespace> clusters edit <cluster> --wait
```

`spec` is desired state; `status` is observation. Accepted changes increment `generation`; `status.observedGeneration` records the generation reconciled. `Ready` alone can describe the previous generation: compare both numbers. `spec.version` is requested; `status.version` is observed, absent when unavailable and the oldest observed member version during a mixed-version rollout.

PATCH merges only `{spec: {...}}`; arrays replace the whole previous array. Edit replaces the complete specification. The write is conditional on the cluster incarnation and generation that admission inspected: a concurrent edit returns `409` rather than silently erasing another accepted change. Reload the cluster, merge your intended changes, and retry. Queued actions are likewise rejected if that inspected cluster was deleted, replaced, suspended, or changed before admission committed. Omitted create fields select engine driver defaults; explicit zero is not a request for defaults. See [REST API](api.md) for request validation and transport details.

Omitted PATCH collections retain their stored state, including absence; response-only empty arrays do not create a desired-state change. Supply `backup.schedules: []` or `exposure.allowlist: []` explicitly to clear the corresponding list.

### `spec.dataMcp`

Optional [cluster data MCP](data-mcp.md) exposes a separate token-authenticated data endpoint; it is disabled unless `enabled: true`. Fields are `enabled`, `access` (`read` by default or `readWrite`), engine-specific `scope` (`databases`, `schemas`, `indexPatterns`, `vhosts`), `tools.deny`, `tools.confirmWrites`, `auditQueryText` and companion `resources`. See the [field/default and scope reference](data-mcp.md#enable-and-scope-access) before enabling. The observed `status.dataMcp` is separate from database membership and reports provider/access/readiness, not a token's effective authority.

## Operations and status

| Layer | Meaning |
| --- | --- |
| HTTP acceptance | `202` accepts asynchronous work, not completion. Cluster creation returns `201`, also before convergence. Save the returned operation ID; see [API](api.md). |
| Operation phase | `Pending`, `Running`, then `Succeeded`, `Failed` or `Cancelled`. Inspect `target`, `targetGeneration`, `step`, `message` and timestamps. |
| Cluster phase/health | `Creating`, `Initializing`, `Ready`, `Updating`, `Scaling`, `Upgrading`, `Restoring`, `Degraded`, `Error`, `Deleting` or `Suspended`. Inspect conditions and individual members/agents, not just the phase. |

**Operation `Succeeded` is not a lasting health guarantee.** Creation requires its target generation to be observed `Ready`; a diagnostic `Degraded` result keeps create tracking Running until recovery or its normal deadline. Other desired-state tracking can succeed after its target generation is observed in `Degraded` with an explanation. A newer generation cancels superseded tracking; failed/cancelled tracking does not undo desired state or stop convergence.

Reconciler-generated `PhaseChanged` events retain the new phase's sanitized user-visible status message and a bounded summary of non-True conditions (`type / status / reason`), so the explanation remains available after recovery, including transitions without a generation change. They do not include cluster specs, internal state or condition message bodies. The admission-time transition to `Deleting` records only the phase arrow.

When an engine returns fresh health observations alongside a secondary error,
nonempty safety snapshots (for example, a failed quorum condition) are published
instead of leaving old `Ready` conditions visible. A safe diagnostic for the error
remains in the cluster message, a partial `Ready` result is reported as `Degraded`, and the
failed observation does not itself advance `observedGeneration`. An empty
error-only snapshot retains historical diagnostics, not positive availability:
`Ready`, `Healthy` and writable-primary claims become unknown/degraded.

Formed clusters request health observation at most five seconds after the
preceding reconciliation. Health is observed before recovery `Sync`; withdrawn
readiness is persisted by the existing serialized cluster worker before platform
repair can block. Read-only member probes allow four seconds for cold namespace/TLS
connection setup and the native response (separate from restart/restore/backup
action deadlines), and each complete health sweep has a
five-second deadline. One immediate fresh sweep retry is the entire transient
failure tolerance: a single failed probe can be absorbed by a successful retry
without publishing a degraded intermediate result. Failed/stale samples are never
reused as ready across cycles.

[MongoDB](https://www.mongodb.com/), [RabbitMQ](https://www.rabbitmq.com/) and [Elasticsearch](https://www.elastic.co/elasticsearch) collect project/service inventory once per
sweep and overlap per-member platform reads, with at most eight provider method
observations in flight per sweep. The same instance inventory is reused for placement and
instance readiness checks; native replica-set and independent Elasticsearch
reads run in parallel. Five-member regressions with 1.5-second platform calls
complete within the existing five-second sweep. The deadline is unchanged:
platform responses that cannot complete within it are unknown, not cached
successes, and additional worker waves still consume the same bounded budget.

Provider inventory also bounds its own read traversal rather than hiding a
serial per-runtime HTTP chain inside a single platform call. [Dokploy](https://dokploy.com/) overlaps
read-only runtime observations while retaining its target lock; pending
operation completion, legacy adoption and restoration decisions still execute
serially under that lock. A real-provider regression with three persistent
members and 600-millisecond platform HTTP responses completes a fresh sweep
within five seconds. Concurrent instance diagnostics reduce in inventory order,
and MongoDB FCV observations use bounded set probes with sorted successful
fallback instead of depending on map iteration.
[Nomad](https://developer.hashicorp.com/nomad) likewise bounds variable reads, service observations and missing-CNI
allocation hydration to eight concurrent reads per traversal; pending-operation
and missing-job mutation helpers retain their existing mutex/reload fences.
[Zerops](https://zerops.io/) uses bulk paginated service/container inventories, not a per-runtime
service-list traversal.

Dokploy and Nomad additionally enforce a shared ceiling of **eight HTTP
observation GETs per provider/platform origin**, across nested traversals,
provider instances and concurrently observed clusters in the same control-plane
process. Admission honors cancellation. Slots are released after each response,
before retry backoff; serialized lifecycle/mutation reads bypass the observation
queue rather than being starved by health sweeps. Concurrent read failures are
joined in stable input order, retaining cancellation and every observed
`Retry-After` constraint; the latest deadline controls retry scheduling. A failed
read-only prerequisite batch does not authorize mutation fallbacks.

A completed sweep that reports an unreachable or unhealthy member is a fresh
negative observation, not a failure to observe the entire cluster. After the
same-cycle retry, its `Degraded` status retains member-specific failure reasons
and fresh health for reachable siblings. Those positive member observations
still expire under the same sample-age lease. A failed/empty sweep or stale
positive member sample cannot renew availability. During creation, expected
not-yet-deployed agents remain unavailable without replacing deployment progress
diagnostics or gating recovery/provisioning `Sync`.

While an ordinary reconciliation is busy, an independent five-second availability
loop can refresh an already Ready/Degraded cluster from an isolated persisted
snapshot. A slow `Sync` cannot by itself starve publication. Publication compares
the complete inspected status/provider checkpoint, generation and cluster
incarnation; concurrent lifecycle changes discard the sample. It writes status
only, never provider internal state or applied generation. Running imperative
operations retain their serialized observation/checkpoint ownership; suspended,
restoring, deleting and not-yet-formed clusters are not promoted by this loop.
Ordinary and independent observation paths share a per-cluster publication gate
from snapshot acquisition through the completed status write. In particular,
finishing probes does not release the gate: an older delayed positive cannot
overwrite a newer failure, or replace a newer lease with an older sample.
The gate is released after pre-Sync publication and held again for the final
observation/publication, so it does not serialize renewal behind slow Sync.
Unavailable agents/platform reads still fail closed under the unchanged lease.
Reconciliation stages emit sanitized monotonic durations at DEBUG, or WARN when
a stage exceeds the 15-second lease; fields contain cluster identity, stage and
duration, not credentials, request bodies or private observation provenance.

Persisted health has a **15-second maximum sample age**: five seconds between
cycles plus at most two five-second sweeps. Cluster/status/member API projections
withdraw positive availability after that bound even if recovery, an operation,
or a provider call blocks further publication. Sample age uses the start of the
successful sweep, not condition transition times; missing/future timestamps are
unknown. Legacy snapshots therefore require one successful health observation
before they can report Ready. This is a bounded observation guarantee, not an
instantaneous failure detector or a writable-transaction guarantee.
After a CP restart, status reads Degraded/Unknown until the first post-start observation.
Successful sweeps carry private provenance of the current control-plane process,
independent of wall-clock timestamps. Old-process snapshots cannot replay Ready
after a restart or clock correction, even if their wall time exactly matches a
new sweep. Fresh current-process samples still require the normal timestamp lease;
future and expired samples remain unknown. API projection never rewrites persisted
observations or changes reconciliation's readiness decisions.
While one member or agent is hung, bounded reads can slow per-cluster observation publication enough that healthy members briefly report `ObservationStale`; this is fail-closed and clears once the sweep completes and a fresh observation is persisted.
This lease also expires positive member health during Creating, Initializing,
Scaling, Updating and Upgrading, even when overall readiness is already false.
Current component, DCS/failover, replication, WAL-capacity and archive health
conditions expire too; durable formation/certificate facts and definitive
negative diagnostics are preserved.

MongoDB additionally requires a current primary `hello` and fresh reachability
of its replica-set members. A surviving secondary's heartbeat-delayed PRIMARY
view alone cannot retain `WritablePrimary`. MongoDB, RabbitMQ and Elasticsearch
probe member agents concurrently. RabbitMQ and [PostgreSQL](https://www.postgresql.org/) also overlap generic
agent and native status requests because their HTTP transports establish separate
TLS connections. All use the same shared four-second read-probe limit; the
five-second sweep, 15-second readiness lease and same-cycle retry policy are unchanged.

MongoDB replication health uses the primary's `replSetGetStatus` view, rather
than a nearest secondary's possibly partitioned view; unhealthy-member conditions
name the affected member. [PBM](https://docs.percona.com/percona-backup-mongodb/latest/) inventory `list`/`status` reads race eligible members
with independent four-second limits (config-server members only for sharded
clusters). A blocked local PBM on one secondary cannot starve PITR configuration
inventory or the reachable members' health probes. Backup inventory runs alongside
member observation with its own four-second budget: failure reports
`Backup=False/InventoryUnavailable` and retains the last backup summary without
discarding fresh member health or reporting every agent disconnected. Backup and
restore initiation/cancellation receipts remain pinned; this parallelism applies
only to read-only inventory.

A MongoDB member is healthy only when its native state is PRIMARY or SECONDARY,
native health is 1, its agent and runtime are healthy, and its optime is no more
than **10 seconds** behind the newest replica-set optime. Reachability alone does
not establish replication health: a reachable secondary may temporarily fall
behind while MongoDB changes away from a partitioned sync source. Such observed
lag remains `health=false`; it is not an agent disconnection.
`Replication/<set>` enumerates affected members in member order with individual
reasons (observation unavailable or measured replication lag). Any unavailable
member observation takes precedence: status/reason is
`Unknown/MemberObservationUnavailable`, even when another member has observed
excessive lag. With only observed lag failures it is `False/ReplicationLag`.
Lag classification requires successful, non-empty, all-running runtime inventory.
The message is bounded to 2048 bytes, with an omitted-member count on overflow;
individual member health and lag remain available in the member list.

`operations wait` succeeds on Operation `Succeeded` without checking cluster health. Mutation `--wait` first waits for the operation, then checks cluster generation/phase; `clusters wait` observes the cluster directly. Cluster waiting fails on `Degraded`/`Error`; deletion waits for absence or the requested `Suspended` state. A client timeout/disconnect does not cancel server work.

```sh
karactl -n <namespace> clusters status <cluster>
karactl -n <namespace> clusters members <cluster>
karactl -n <namespace> operations list
karactl -n <namespace> operations cluster <cluster>
karactl operations wait <operation-id>
karactl operations cancel <operation-id>
```

Ordinary work defaults to 20 consecutive retryable failures or two hours of observed control-plane execution time; restores allow six hours. Successful steps reset consecutive failures; scheduled waits consume time, not failures. Elapsed budgets are persisted and measured with a monotonic clock: changing the host clock cannot expire active work, and control-plane downtime does not consume the budget. Engine driver-supported failure compensation has a separate six-hour observed-time window; expiration requires manual recovery. Tracking deadlines do not discard desired state or halt cleanup. If deletion later finishes, its failed tracking record is corrected to `Succeeded` atomically with cluster removal or suspension; startup also repairs historical failed deletion records whose original cluster is demonstrably absent.

Observed-time accounting includes waits between reconciliations and persists
each observation before reloading tracking state. Deletion completion uses an
explicit persisted cluster-incarnation equality token, not ordering of host-clock
timestamps. For legacy tracking without that token, startup proves completion
only when the same-named cluster is actually absent.

RabbitMQ rolling updates treat a refused agent reload connection and an
explicitly booting broker observation as scheduled startup waits, including
after the five-minute startup diagnostic threshold. These waits consume the
operation's observed-time budget, not its consecutive-failure budget. The
member's applied configuration and native cluster membership must still be
verified before the rollout advances or succeeds.

Cancellation requires `clusters/update` on the operation's recorded namespace/cluster. Pending work becomes `Cancelled` immediately, without reversing accepted spec changes. Running work is cancellable only at engine driver-supported safe checkpoints: acceptance sets `cancelRequested`, and cleanup may keep it Running. Terminal/unsafe work returns `409`. Cancellation is not rollback; wait for a terminal outcome before dependent actions. Checkpoint rules live in [MongoDB operations](mongodb/operations.md) and [backups](mongodb/backups.md), [RabbitMQ operations](rabbitmq/operations.md), and [Elasticsearch operations](elasticsearch/operations.md).

## Resources and autoscaling

| Setting/observation | Contract |
| --- | --- |
| `cpuMode` | `SHARED` or `DEDICATED` |
| CPU bounds | Whole cores, at least 1; optional `startCpu` is whole and within bounds |
| RAM/disk bounds | GiB in the API; each maximum must be at least its minimum; component floors are engine driver-specific |
| Persistent disk | Existing volume grows; decreasing either desired disk bound is rejected |
| Member `autoscaling` | Effective runtime service bounds read from Zerops, not desired spec |
| Member `resources` | CPU cores, memory limit and root disk the container currently has, read live by the member agent; falls back to effective autoscaling (start or minimum CPU, minimum RAM/disk) when the agent is unreachable or predates the reading |
| `diskTotalBytes` | Mounted data-volume capacity; runtime autoscaling disk fields describe disposable root disk |

PostgreSQL (including the shared Supabase database core) reports effective
autoscaling bounds for both data members and proxies when the backend supplies
them (Zerops). These are read on each observation, not from the last applied
member configuration. Nomad and Dokploy scheduler reservations/limits are not
autoscaling observations and are not used as allocation fallbacks. Live agent
allocation readings take precedence; an absent runtime has no effective bounds
to report.

Disk admission is provider/component-specific, not a universal 5-GiB API floor.
Explicit engine disk bounds accept values from 1 GiB at the HTTP schema layer;
provider validation then enforces these floors for create, replacement and patch:

| Component | Minimum desired disk (GiB) |
| --- | --- |
| [PostgreSQL](postgres/README.md) / Supabase data; [MySQL/PXC](mysql/README.md) data | 5 |
| [MongoDB](mongodb/README.md) data, config servers and routers; [RabbitMQ](rabbitmq/README.md); [OpenBao](openbao/README.md) | 5 |
| [Elasticsearch](elasticsearch/README.md) data/combined nodes and dedicated masters | 5 |
| [Neo4j](neo4j/README.md); [Weaviate](weaviate/README.md) | 1 persistent |
| [Keycloak](keycloak/README.md); enabled proxies, Supabase services, [Temporal](temporal/README.md) roles and Elastic optional components | 1 runtime |

Each maximum must remain at least its minimum. Omitted bounds use provider
defaults, which may be larger. Zerops separately rounds imported persistent
Local Storage volumes up to its 5-GB minimum; this does not raise the runtime
disk floor. Nomad host volumes and Dokploy named volumes do not enforce requested
capacity as a quota, and do not support disk growth. See [backends](backends.md).
These are validation floors, not workload sizing recommendations.

```sh
karactl -n <namespace> clusters scale <cluster> --min-cpu 1 --max-cpu 2 --wait
```

Use engine driver-supported `--component` and membership options from the engine guide, or a spec patch. In **Topology → Review & apply**, inspect **What will change** and **Merge patch to be sent**. Resource changes use per-member disruption precautions; even CPU-only changes can restart a runtime. Persistent capacity is configured on the volume, not the runtime root disk.

`Ready` plus matching generation means reconciliation completed, not that allocation changed instantaneously or provides a hard memory-allocation cap. CPU-mode rate limits surface `PlatformCooldown` with a persisted target and retry deadline; reconciliation resumes automatically, including after control-plane restart. The cluster remains `Updating` until the requested mode is applied. Scheduled cooldown waits do not emit repeated `ReconcileError` events; independent observation failures still log/emit them and use normal error backoff rather than waiting for the cooldown to expire.

Consecutive identical `ReconcileError` messages with the same actor and operation are recorded once per cluster worker. A changed message, actor or operation, or successful reconciliation resets suppression; a control-plane restart starts a new worker.

Rejected [Zerops](https://zerops.io/) resource updates and service imports remain authoritative. The `ReconcileError` event and cluster status message identify the service hostname, platform error code and safe validation details, followed by attempted CPU mode, CPU/start CPU, RAM, disk and container bounds. These values come from the actual platform request, including preserved settings, not only the cluster patch. Diagnostics never include credentials or environment values.

| Diagnostic | Limit / lifecycle |
| --- | --- |
| Cluster status message | 512 bytes; clears the reconciliation failure after successful observation and reconciliation |
| Error/event details | 1024 bytes; long validation details or multi-service imports may be truncated |
| Waiting update operation | Retains its last error while waiting for convergence |

When a waiting PATCH does not converge, inspect `karactl clusters get <cluster> -o yaml`, cluster events and the update operation's `message` and last error (`error`). Do not independently change Zerops autoscaling or restart all members during reconciliation.

## Rolling restart

```sh
karactl -n <namespace> clusters restart <cluster> --wait
```

Restart persists per-member progress and uses engine driver health/quorum gates before advancing. It is not a control-plane redeploy or agent executable upgrade; agents receive a newer executable on a engine driver-driven member deployment. A singleton is unavailable during its restart; multi-member rolling work is not a zero-disruption guarantee. For engine-specific order, drain and recovery rules, see [MongoDB](mongodb/operations.md), [RabbitMQ](rabbitmq/operations.md), [Elasticsearch](elasticsearch/operations.md) and [Supabase](supabase/operations.md#cluster-restart-and-member-software). Supabase includes every enabled application component after the [PostgreSQL](https://www.postgresql.org/) core, verifying each live instance's process restart and readiness.

## Retry exhausted platform deployments

`DeploymentRetriesExhausted` is terminal: normal reconciliation and control-plane restarts do not reset the exact-version deployment retry limit. Investigate and correct the platform failure before explicitly granting a fresh budget:

```sh
karactl clusters retry-deployments <cluster> --namespace <namespace>
karactl clusters retry-deployments <cluster> --namespace <namespace> -o json
```

The action requires `clusters/update`, atomically advances the server-managed `spec.retryEpoch` and desired generation, records the actor, old/new epochs and exhausted members/components in an event, and triggers reconciliation. It does not restart healthy members. If no deployment is exhausted in the current epoch, it returns `409 conflict` without changing the epoch. Reuse an `Idempotency-Key` when retrying the API request to replay its original result without granting another budget. Ordinary spec edits cannot reset or advance this epoch.

## Delete or suspend

**Warning:** default deletion removes runtimes, data volumes and owned recovery objects. Without retained volumes it cannot be resumed. Retention flags cannot recover already-deleted assets.

| `keepVolumes` | `keepBackups` | Result |
| --- | --- | --- |
| false (default) | false (default) | Full deletion, including owned backup cleanup |
| false | true | Cluster/volumes removed; recovery inventory retained |
| true | false | `Suspended`; volumes/identity/secrets retained, backups cleaned |
| true | true | `Suspended`; volumes and recovery objects retained |

```sh
# Suspend while retaining both assets; there is no separate suspend command.
karactl -n <namespace> clusters delete <cluster> --keep-volumes --keep-backups --wait --yes
karactl -n <namespace> clusters resume <cluster> --wait
# Destructive full deletion:
karactl -n <namespace> clusters delete <cluster> --wait --yes
```

Suspended member allocation is historical; agents are disconnected. Resume recreates runtimes on retained volumes. Normal scale-down/replacement deletes retired members' volumes; cluster retention flags are not per-member retention.

Deletion needs `clusters/delete`; resume, patch/edit/scale/restart need `clusters/update`. Read/create need `clusters/read` and `clusters/create`, respectively, on namespace-qualified cluster objects. See [Access control](access-control.md).

Running deletion cannot be cancelled. Repeating it with identical retention choices reuses the active operation. Before engine driver cleanup starts, changed retention choices supersede queued deletion tracking. A retain-volumes cleanup can be superseded by full deletion; stale suspension completion cannot clear that newer request. Once engine driver cleanup has started with removal of volumes or backups permitted, changing the corresponding flag to retain them returns `409`: already-issued deletions cannot safely promise retention. The cleanup-start fence is durable across control-plane restarts. Check cluster, backup and orphan-volume inventories separately. See [Backups and storage](backups.md) for retained access and cleanup.
