<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Clusters and operations

A cluster is a namespace-scoped desired configuration, observed engine topology and durable operation history.

## Cluster identity and capabilities

Each cluster selects an immutable provider/engine. Data-bearing members are Zerops singleton runtimes with individual Local Storage volumes; auxiliary stateless components depend on the provider. Cluster names are 2–12 lowercase letters/digits, beginning with a letter, and unique within a [namespace](namespaces.md). Use `-n <namespace>` to select one; without a selected namespace, single-object requests default to `default`.

The provider catalog advertises versions, upgrade paths, topology/component limits, backup types and operations. Query `karactl providers list` or `karactl providers get <provider> -o yaml`. Supported does not mean disruption-free: follow the linked engine's prerequisites and limits.

| Capability | [MongoDB](mongodb/README.md) (`psmdb`) | [RabbitMQ](rabbitmq/README.md) | [Elasticsearch](elasticsearch/README.md) |
| --- | --- | --- | --- |
| Create | Yes | Yes | Yes |
| Scale CPU/RAM/disk | Yes; disk only grows | Yes; disk only grows | Yes; disk only grows |
| Add/remove members | Yes | Yes | Yes |
| Replace member | Yes | Yes | Yes |
| Rolling restart | Yes | Yes | Yes |
| Rolling upgrade | Catalog paths | Catalog paths | Catalog paths |
| Feature compatibility version (FCV) promotion | Yes | No | No |
| TLS rotation | Yes | Yes | Yes |
| System-user rotation | Yes | Yes | Yes |
| Public TCP exposure | Yes | Yes | Yes |
| Backup types | Percona Backup for MongoDB (PBM): logical, physical | Definitions only; no messages | Native snapshots |
| Point-in-time recovery (PITR) | Yes | No | No |
| In-place restore | Yes | Merge definitions | Native restore |
| Clone | Yes | Definitions only | Yes |
| Suspend/resume | Yes | Yes | Yes |
| Cross-namespace clone | Yes | Definitions only | Yes |

See [Backups and storage](backups.md) for shared recovery mechanics and the engine guides for runtime, compatibility and checkpoint rules.

## Create and change desired state

Use **Clusters → Create cluster** and the provider's wizard, or prepare a complete `{name, spec}` YAML/JSON request using its engine guide:

```sh
karactl -n <namespace> clusters create <cluster> -f cluster.yaml --wait
karactl -n <namespace> clusters list
karactl -n <namespace> clusters get <cluster> -o yaml
karactl -n <namespace> clusters patch <cluster> -f patch.yaml --wait
karactl -n <namespace> clusters edit <cluster> --wait
```

`spec` is desired state; `status` is observation. Accepted changes increment `generation`; `status.observedGeneration` records the generation reconciled. `Ready` alone can describe the previous generation: compare both numbers. `spec.version` is requested; `status.version` is observed, absent when unavailable and the oldest observed member version during a mixed-version rollout.

PATCH merges only `{spec: {...}}`; arrays replace the whole previous array. Edit replaces the complete specification. Omitted create fields select provider defaults; explicit zero is not a request for defaults. See [REST API](api.md) for request validation and transport details.

## Operations and status

| Layer | Meaning |
| --- | --- |
| HTTP acceptance | `202` accepts asynchronous work, not completion. Cluster creation returns `201`, also before convergence. Save the returned operation ID; see [API](api.md). |
| Operation phase | `Pending`, `Running`, then `Succeeded`, `Failed` or `Cancelled`. Inspect `target`, `targetGeneration`, `step`, `message` and timestamps. |
| Cluster phase/health | `Creating`, `Initializing`, `Ready`, `Updating`, `Scaling`, `Upgrading`, `Restoring`, `Degraded`, `Error`, `Deleting` or `Suspended`. Inspect conditions and individual members/agents, not just the phase. |

**Operation `Succeeded` is not a health guarantee.** Desired-state tracking can succeed after its target generation is observed in `Degraded` with an explanation. A newer generation cancels superseded tracking; failed/cancelled tracking does not undo desired state or stop convergence.

`operations wait` succeeds on Operation `Succeeded` without checking cluster health. Mutation `--wait` first waits for the operation, then checks cluster generation/phase; `clusters wait` observes the cluster directly. Cluster waiting fails on `Degraded`/`Error`; deletion waits for absence or the requested `Suspended` state. A client timeout/disconnect does not cancel server work.

```sh
karactl -n <namespace> clusters status <cluster>
karactl -n <namespace> clusters members <cluster>
karactl -n <namespace> operations list
karactl -n <namespace> operations cluster <cluster>
karactl operations wait <operation-id>
karactl operations cancel <operation-id>
```

Ordinary work defaults to 20 consecutive retryable failures or two hours from acceptance; restores allow six hours. Successful steps reset consecutive failures; scheduled waits consume time, not failures. Provider-supported failure compensation has a separate six-hour window; expiration requires manual recovery. Tracking deadlines do not discard desired state or halt cleanup.

Cancellation requires `clusters/update` on the operation's recorded namespace/cluster. Pending work becomes `Cancelled` immediately, without reversing accepted spec changes. Running work is cancellable only at provider-supported safe checkpoints: acceptance sets `cancelRequested`, and cleanup may keep it Running. Terminal/unsafe work returns `409`. Cancellation is not rollback; wait for a terminal outcome before dependent actions. Checkpoint rules live in [MongoDB operations](mongodb/operations.md) and [backups](mongodb/backups.md), [RabbitMQ operations](rabbitmq/operations.md), and [Elasticsearch operations](elasticsearch/operations.md).

## Resources and autoscaling

| Setting/observation | Contract |
| --- | --- |
| `cpuMode` | `SHARED` or `DEDICATED` |
| CPU bounds | Whole cores, at least 1; optional `startCpu` is whole and within bounds |
| RAM/disk bounds | GiB in the API; each maximum must be at least its minimum; component floors are provider-specific |
| Persistent disk | Existing volume grows; decreasing either desired disk bound is rejected |
| Member `autoscaling` | Effective runtime service bounds read from Zerops, not desired spec |
| Member `resources` | Current container allocation, not configured autoscaling bounds |
| `diskTotalBytes` | Mounted data-volume capacity; runtime autoscaling disk fields describe disposable root disk |

Sizing floors/defaults: [MongoDB](mongodb/README.md), [RabbitMQ](rabbitmq/README.md), [Elasticsearch](elasticsearch/README.md). They are validation floors, not workload sizing recommendations.

```sh
karactl -n <namespace> clusters scale <cluster> --min-cpu 1 --max-cpu 2 --wait
```

Use provider-supported `--component` and membership options from the engine guide, or a spec patch. In **Topology → Review & apply**, inspect **What will change** and **Merge patch to be sent**. Resource changes use per-member disruption precautions; even CPU-only changes can restart a runtime. Persistent capacity is configured on the volume, not the runtime root disk.

`Ready` plus matching generation means reconciliation completed, not that allocation changed instantaneously or provides a hard memory-allocation cap. CPU-mode rate limits surface `PlatformCooldown` with a persisted target and retry deadline; reconciliation resumes automatically, including after control-plane restart. The cluster remains `Updating` until the requested mode is applied. Scheduled cooldown waits do not emit repeated `ReconcileError` events; independent observation failures still log/emit them and use normal error backoff rather than waiting for the cooldown to expire.

Consecutive identical `ReconcileError` messages with the same actor and operation are recorded once per cluster worker. A changed message, actor or operation, or successful reconciliation resets suppression; a control-plane restart starts a new worker.

Rejected Zerops resource updates and service imports remain authoritative. The `ReconcileError` event and cluster status message identify the service hostname, platform error code and safe validation details, followed by attempted CPU mode, CPU/start CPU, RAM, disk and container bounds. These values come from the actual platform request, including preserved settings, not only the cluster patch. Diagnostics never include credentials or environment values.

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

Restart persists per-member progress and uses provider health/quorum gates before advancing. It is not a control-plane redeploy or agent executable upgrade; agents receive a newer executable on a provider-driven member deployment. A singleton is unavailable during its restart; multi-member rolling work is not a zero-disruption guarantee. For engine-specific order, drain and recovery rules, see [MongoDB](mongodb/operations.md), [RabbitMQ](rabbitmq/operations.md) and [Elasticsearch](elasticsearch/operations.md).

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

Running deletion cannot be cancelled. Repeating it with identical retention choices reuses the active operation; changing choices supersedes tracking, not already-issued platform deletions. Check cluster, backup and orphan-volume inventories separately. See [Backups and storage](backups.md) for retained recovery objects and pending cleanup, and [orphaned volumes](operations.md#orphaned-platform-volumes).
