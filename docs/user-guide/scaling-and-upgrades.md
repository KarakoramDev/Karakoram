<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Scaling and database upgrades

These operations change managed databases. Updating the Karakoram control-plane executable is a separate procedure in [upgrade.md](../upgrade.md).

For a remote project, select its [namespace](namespaces.md) or add `-n NAME` to
the commands below. Keep its project token valid and relay connected throughout
observation and rolling work; a management outage is not evidence that member
replication stopped. Cluster identity and operation history include namespace.

## Resource and membership scaling

Cluster component resources express desired Zerops autoscaling bounds: `cpuMode` (`SHARED`/`DEDICATED`), min/max CPU cores, min/max RAM, min/max disk and optional start CPU. RAM/disk are GiB in the API. The provider requires RAM at least 0.5 GB, persistent disk at least 5 GB, positive CPU and valid min/max bounds. The normal provider default is 1 GB RAM, not a production sizing recommendation for every workload.

CPU bounds are whole numbers of cores, at least **1**; every maximum must be at least its corresponding minimum, and start CPU must lie within the CPU bounds. Explicit zero for replicas, shards, CPU, RAM, disk or backup schedule retain counts is rejected, never silently replaced with a default. Omission, not zero, selects documented provider defaults. PATCH validates only supplied fields before merging and validates the resulting desired configuration before accepting it. Unsupported versions identify the provider and offered versions; query `/api/v1/providers` for your installation's current catalog.


```sh
karactl clusters scale demo --min-cpu 1 --max-cpu 2 \
  --min-ram 2 --max-ram 2 --cpu-mode SHARED --wait
karactl clusters scale demo --min-disk 5 --max-disk 20 --wait
karactl clusters scale demo --members 5 --wait
```

Convenience scaling flags patch only settings explicitly supplied. `--component` selects `engine` (default), `configServer` or `router`; `--config-servers` and `--routers` change their replica counts. Engine resources/replicas apply to every shard set in a sharded cluster. [Sharding](sharding.md) explains shard count and router scaling.

In the Web UI's **Topology** tab, **Review & apply** shows **What will change**
for the actual merge patch, not a generic warning about all scaling:

- **Membership:** added data members complete initial sync and voting promotion;
  removals preserve a voting majority. Router-count changes update router capacity,
  not voting membership. Adding shards retains existing shard sets.
- **Resources:** changed bounds are applied one member at a time with rolling
  restart precautions; changed disk bounds update the existing data volume.
- **CPU-only:** changes limited to `cpuMode`, `minCpu`, `maxCpu` or `startCpu`
  do not move data. Platform updates still use per-member disruption safety
  checks, and a platform restart may occur; this is not a no-downtime promise.

Expand **Merge patch to be sent** to inspect the exact `{spec: {...}}` body.
Unchanged component replicas/settings are not independent scaling requests.

Persistent capacity is configured on the **volume service**; volume `maxDisk` determines mounted capacity. Runtime root disk is disposable and does not resize the database volume. **Disk shrinking is rejected**, including reducing either desired min/max disk bound. Plan headroom and inspect used/total disk in member status. A resource configuration Process finishing does not guarantee observed container allocation changed instantaneously.

Member status distinguishes **`autoscaling`** (the effective runtime bounds read back from Zerops service configuration, including CPU mode/start CPU) from **`resources`** (the container's current hardware allocation). Neither is a copy of the desired cluster spec. `Ready` plus the matching observed generation means Karakoram completed its resource/configuration work; it is not an allocation-convergence SLA. Do not infer a hard allocated-memory cap from desired bounds or force simultaneous infrastructure restarts. The UI's member “Platform resources” detail shows both observations; `autoscaling` disk bounds describe the disposable runtime root disk, while `diskTotalBytes` describes the mounted database volume.

RAM changes can change the provider-managed WiredTiger cache and require controlled member reload. Karakoram sends complete Zerops vertical/horizontal settings to avoid platform resets of omitted fields. Do not independently edit platform autoscaling or restart all database runtimes in parallel while Karakoram is reconciling.

CPU-mode changes may be rate-limited by the platform. Karakoram persists the target and retry deadline, reports a `PlatformCooldown` condition and resumes automatically when the wait expires. A control-plane restart does not discard the wait. The cluster remains Updating until the requested mode is applied. See [Zerops scaling semantics](https://docs.zerops.io/features/scaling).

Scheduled waits do not repeat error events. An independent observation failure during that wait (for example a rejected Zerops service-list request) still logs/emits `ReconcileError` and uses normal error backoff, rather than hiding the failure until the cooldown expires.

Membership addition/removal/replacement uses initial-sync and quorum checks. See [replica sets](replica-sets.md). A 0.5-GB minimum is a validation floor for disposable tiny workloads, not a guarantee of physical recovery or realistic load capacity.

## Version catalog and paths

```sh
karactl providers get psmdb -o yaml
```

Use the returned exact versions and `upgradePaths`. The current source catalog includes 6.0.29-23 (deprecated), 7.0.40-22, 7.0.43-23, 8.0.29-13 and default 8.0.32-14, with PBM 2.16.0-1. Debian package pins append `.noble`. Catalog availability is not a blanket upstream lifecycle guarantee. PSMDB 8.3 is excluded because Percona labels it **Technical Preview**.

Allowed binary changes move forward within a major or to the next catalog major. Downgrades and skipping a major are rejected. A next-major upgrade requires the current observed FCV to equal the old binary major. Confirm the advertised path on your deployed version; do not use arbitrary apt versions or infer PSMDB support from upstream MongoDB version availability.

### Required pre-upgrade backups

An upgrade path with **`requiresBackup: true`** is a server-enforced prerequisite,
not advisory UI copy. Both PUT and PATCH changes to `spec.version` require a
backup of **the same cluster in the same namespace**, in phase **`Succeeded`**
with `completedAt` within the **last 24 hours**. Pending, Running, Failed and
Cancelled backups, or records without a completion timestamp, do not qualify.
The backup must not predate cluster creation. When the observed version matches
the current desired version and successful create/upgrade operation history is
available, it must also complete no earlier than that version's latest known
successful convergence. Unrelated resource updates do not reset this cutoff.

If no qualifying backup exists, the API rejects the version change with **409
`conflict`**, and `details` identifies `resource: cluster`, its `name`, and
`reason: invalid_state`. The message names the **from → to** path and the remedy:

1. Create a backup with **`POST /api/v1/clusters/{name}/backups`**, selecting the
   cluster's namespace, a configured storage and a provider-supported backup type.
   See [backups and restore](backups-and-restore.md).
2. Observe the returned backup/operation until the backup is **`Succeeded`** with
   a completion timestamp. An accepted backup request alone is insufficient.
3. Retry the version change within 24 hours of backup completion.

There is **no bypass flag**. Paths without `requiresBackup` do not enforce this
gate, but a tested recovery point is still recommended before every upgrade.

The UI names the last recorded successful backup but does not itself establish
that it meets the full age/version cutoff. A visible or enabled **Upgrade…**
button is not proof that the gate will pass; the server checks on submission.

## Rolling binary upgrade

1. Take a successful backup and check storage access. Test recovery and application/driver compatibility with the target version.
2. Check all relevant replica sets are healthy, secondaries caught up, agents connected and FCV correct for the source major. Clear competing restore/replacement work.
3. Review provider-advertised upgrade paths and submit the new binary version, for example:

   ```sh
   # Example for a healthy 7.0 cluster, with FCV already 7.0:
   karactl clusters upgrade demo --version 8.0.32-14 --wait
   ```

4. Observe members, events and status until deployed/observed binaries match and replication catches up:

   ```sh
   karactl clusters members demo
   karactl clusters status demo
   karactl events list --cluster demo
   ```

Secondaries are deployed before primary stepdown/upgrading the former primary. The provider waits for actual pinned binary version and replication convergence after each deployment. For sharding, config servers precede shard sets and then routers; the balancer's prior state is saved/stopped/restored around binary cutover. An unhealthy member or lost quorum pauses progression rather than making simultaneous changes.

A singleton has downtime. Rolling order reduces disruption but is not a universal zero-failed-write guarantee: applications still need appropriate driver retry/read/write-concern handling, and Zerops placement is not guaranteed.

The UI's **Running version** comes from observed non-router member versions and
can show multiple versions during a rollout; **Desired version** is the requested
specification. No observed member version means **not yet observed**, not that
the desired version is running. Upgrade/restart dialogs repeat the availability
warning, including explicit singleton downtime.

## Explicit FCV promotion

A binary major upgrade **does not automatically raise featureCompatibilityVersion**. Keep the old FCV during application compatibility observation, then deliberately promote:

```sh
karactl clusters raise-fcv demo --version 8.0 --wait
karactl clusters status demo
```

FCV uses the major form (`8.0`), not the package version (`8.0.32-14`). The API rejects package-form or non-deployed-major `version` values with HTTP **400** and a field-specific message naming the accepted form, before enqueueing an operation. Promotion during an unfinished binary rollout returns **409** with reason `invalid_state`; wait until every member reports the exact desired package version and convergence finishes. The operation rechecks deployment and uses confirmation for 7.0+; a genuine run-time validation failure ends the operation as **Failed** with a reason. Transient agent or relay errors retain their diagnostic cause and retry within the operation's bounded consecutive-failure budget instead of being reported as a binary-rollout mismatch. Retained FCV is for compatibility observation, not a promise of supported binary rollback. Karakoram rejects downgrades regardless of retained FCV; follow the target engine's supported recovery process and use tested backups when necessary.

### Cancel and recover a stuck FCV request

An FCV request must use major form such as `8.0`, never `8.0.32-14`, and match
the observed deployed engine major. A validation error is not an accepted
operation: fix the reported field before submitting again. For an already
accepted request that is stuck or was made with the wrong target:

1. Inspect the operation and deployed state, selecting the database namespace:

   ```sh
   karactl -n default operations cluster demo
   karactl operations get OPERATION_ID -o json
   karactl -n default clusters status demo
   karactl -n default clusters members demo
   karactl -n default events list --cluster demo
   ```

   Use actual observed member versions/FCV, not only the desired version. A
   binary upgrade in progress must finish before an FCV promotion can succeed.

2. Cancel the wrong request using **`POST /api/v1/operations/{id}/cancel`**.
   With the installation origin and bearer token loaded as described in the
   [curl bootstrap](api.md#copyable-curl-bootstrap):

   ```sh
   OPERATION_ID='the-operation-id'
   curl --fail-with-body --silent --show-error -X POST \
     -H "Authorization: Bearer $KARAKORAM_TOKEN" \
     "$SERVER/api/v1/operations/$OPERATION_ID/cancel"
   karactl operations get "$OPERATION_ID" -o json
   ```

   The response is a tracking Operation (202). Observe it until terminal
   `Cancelled`, `Failed` or `Succeeded`; cancellation is not rollback and cannot
   lower an FCV that MongoDB already applied. A 409 `invalid_state` means the
   operation cannot be cancelled in its current state: read its phase/message
   and resolve the reported prerequisite rather than submitting competing work.

3. Once the old operation is terminal, the cluster is healthy, and every relevant
   component is deployed on the intended major, re-read FCV. If it is already
   correct, no promotion is needed. Otherwise submit the **major-form** target:

   ```sh
   karactl -n default clusters raise-fcv demo --version 8.0 --wait
   karactl -n default clusters status demo
   ```

   Substitute the major actually deployed on your cluster. Do not patch a newer
   desired binary version merely to make an invalid FCV target look plausible,
   manually edit operation records, or assume retained old FCV makes binary
   downgrade supported. If native FCV change failed partway, preserve diagnostics
   and use MongoDB's supported recovery procedure before retrying.


## Rolling restart and overrides

```sh
karactl clusters restart demo --wait
```

Restarts checkpoint per-member progress, check health/quorum and process secondaries before a primary. They are not control-plane redeploys or agent binary upgrades. Existing agents receive a newer executable on their next provider-driven **member deployment**, not merely a control-plane restart.

**A one-member replica set is unavailable while its only member restarts or upgrades.** There is no secondary or primary failover to preserve availability; plan an application maintenance window for singleton binary upgrades, explicit restarts and resource changes that require member replacement/restart.

`engineConfig` accepts validated YAML overrides, but provider-owned networking/authentication/TLS/replication/sharding/process/logging/dbpath/cache settings cannot be replaced. Do not use overrides to bypass orchestration safety. Use API-shaped patch files for supported advanced settings and inspect validation errors; see [API](api.md) and [operations](../operations.md).

If a member replacement reaches its operation limit during old-member removal,
the operation fails but ordinary convergence resumes removal and promotes the
caught-up replacement. Inspect replica-set membership and voting state before
raising FCV; a failed replacement operation does not roll back its desired
member changes.
