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

```sh
karactl clusters scale demo --min-cpu 1 --max-cpu 2 \
  --min-ram 2 --max-ram 2 --cpu-mode SHARED --wait
karactl clusters scale demo --min-disk 5 --max-disk 20 --wait
karactl clusters scale demo --members 5 --wait
```

Convenience scaling flags patch only settings explicitly supplied. `--component` selects `engine` (default), `configServer` or `router`; `--config-servers` and `--routers` change their replica counts. Engine resources/replicas apply to every shard set in a sharded cluster. [Sharding](sharding.md) explains shard count and router scaling.

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

## Explicit FCV promotion

A binary major upgrade **does not automatically raise featureCompatibilityVersion**. Keep the old FCV during application compatibility observation, then deliberately promote:

```sh
karactl clusters raise-fcv demo --version 8.0 --wait
karactl clusters status demo
```

FCV uses the major form (`8.0`), not the package version (`8.0.32-14`). The operation checks observed/deployed component majors and uses confirmation for 7.0+. Raising FCV is a compatibility/rollback boundary: do not assume redeploying an old binary is a supported downgrade after it. Karakoram does not offer an arbitrary engine rollback command; follow the target engine's supported recovery process and use tested backups when necessary.

## Rolling restart and overrides

```sh
karactl clusters restart demo --wait
```

Restarts checkpoint per-member progress, check health/quorum and process secondaries before a primary. They are not control-plane redeploys or agent binary upgrades. Existing agents receive a newer executable on their next provider-driven **member deployment**, not merely a control-plane restart.

`engineConfig` accepts validated YAML overrides, but provider-owned networking/authentication/TLS/replication/sharding/process/logging/dbpath/cache settings cannot be replaced. Do not use overrides to bypass orchestration safety. Use API-shaped patch files for supported advanced settings and inspect validation errors; see [API](api.md) and [operations](../operations.md).
