<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# MongoDB operations

Scale, upgrade and diagnose managed PSMDB members and PBM processes.

## Resource scaling

Use [Clusters and operations](../clusters.md) for desired bounds versus observed allocation, disk growth, platform cooldowns and generic restart semantics; use [replica sets](replica-sets.md) for membership and [sharding](sharding.md) for component counts.

```sh
karactl -n default clusters scale demo --min-cpu 1 --max-cpu 2 \
  --min-ram 2 --max-ram 2 --cpu-mode SHARED --wait
karactl -n default clusters scale demo --min-disk 5 --max-disk 20 --wait
```

RAM changes alter managed WiredTiger cache (`max(0.25, 0.5 × (maxRam − 0.5))` GiB) and require controlled reload. Minimum-sized members still need PBM/container headroom. Sharded engine bounds apply to every shard; configServer/router resources are separately selectable. Do not independently edit platform autoscaling or restart database runtimes in parallel during reconciliation.

In **Topology → Review & apply**, inspect **What will change** and **Merge patch to be sent**: member sync/promotion/removal, router capacity, added shards and resource reloads differ. CPU-only changes do not move data but may restart a member; this is not a no-downtime promise.

## Version catalog and paths

[Overview](README.md) owns exact versions/defaults. Query `karactl providers get psmdb -o yaml` for deployed `upgradePaths`. Only forward same-major or adjacent-major changes are allowed; no downgrade/skipped major. Adjacent-major upgrades require observed FCV equal to the old major. Retaining FCV does not make binary downgrade supported.

### Required pre-upgrade backups

For any advertised path with `requiresBackup: true`, PUT/PATCH version changes require a **Succeeded**, timestamped backup from the same cluster/namespace, completed within **24 hours** and not before cluster creation. When observed/current desired version and successful convergence history are known, completion must also follow that version's latest successful create/upgrade convergence; unrelated resource edits do not reset the cutoff. Pending/Running/Failed/Cancelled backups do not qualify.

Missing prerequisites return **409 conflict**, `details.resource: cluster`, cluster name, reason `invalid_state`, naming path/remedy. Complete a [backup](backups.md), then retry within 24 hours; no bypass exists. UI backup age/button state is not proof of eligibility. The current catalog does not mark any path `requiresBackup`; nevertheless take and test a recovery point before every upgrade.

## Rolling binary upgrade

1. Complete a backup, verify storage/recovery and target application/driver compatibility.
2. Check healthy replica sets, caught-up secondaries, connected agents and source-major FCV; finish competing restore/replacement work.
3. Submit an offered path, for example from healthy 7.0 binaries/FCV:

   ```sh
   karactl -n default clusters upgrade demo --version 8.0.32-14 --wait
   karactl -n default clusters members demo
   karactl -n default clusters status demo
   ```

Secondaries upgrade first, then primary stepdown/former-primary deployment, waiting for exact pinned binary and replication convergence per member. Sharded order is config servers, shards, routers; prior balancer state is saved/stopped/restored. Lost quorum/unhealthy members pause progression.

**Warning:** Singletons are unavailable during restart/upgrade. Multi-member rolling order still permits brief write failures; configure appropriate driver retries/read/write concerns and plan maintenance.

**Upgrades** shows observed **Running version**, possibly mixed during rollout, separately from **Desired version**. Missing observations mean **not yet observed**, not deployment success.

Zerops exact-version deployment retries have a hard budget.
`DeploymentRetriesExhausted` records the blocked member and stops submissions for
that target while other members remain observable and can finish provisioning.
An unstarted member is not treated as bootstrapped or upgraded, and existing
replication/quorum fences remain in effect. Correct the platform/runtime failure,
then run `karactl --namespace NS clusters retry-deployments NAME` to re-arm.

## Explicit FCV promotion

Binary upgrades do not raise featureCompatibilityVersion automatically. Observe application compatibility with old FCV, then use **Upgrades → FCV** or:

```sh
karactl -n default clusters raise-fcv demo --version 8.0 --wait --yes
```

Use major form, not `8.0.32-14`. Wrong/non-deployed major returns **400** with field guidance before enqueueing; unfinished binary convergence returns **409** `invalid_state`. Every relevant member must report exact desired package version. Pre-acceptance agent/relay observation failure returns **503 unavailable** with retry hint and no operation. After acceptance, transient observation errors retry within bounded limits; genuine runtime validation fails with its reason. FCV uses confirmation for 7.0+ and enables on-disk features Karakoram cannot demote.

For stuck/wrong requests, inspect `operations cluster demo`, `operations get ID`, members/status/FCV and events in the database namespace. `karactl operations cancel ID` (API `POST /api/v1/operations/{id}/cancel`) is safe only **before `apply-fcv`**. Acceptance is 202, not completed cancellation; wait to terminal. A 409 `invalid_state` means inspect the new checkpoint, not submit competing work. Cancellation cannot undo applied FCV.

Once terminal and healthy, re-read observed FCV; promote only if still needed and all binaries match. Never edit operation records or change desired binaries to justify a wrong FCV target. Preserve diagnostics and use MongoDB's supported recovery if native FCV partially failed.

## Rolling restart and overrides

`karactl -n default clusters restart demo --wait` checkpoints members, checks quorum/health and processes secondaries before primary. It restarts database processes, not the agent executable.

Use `karactl -n default clusters upgrade-agents demo --wait` to deploy the current control-plane executable to the **same runtimes and volumes**, without changing MongoDB versions. Config servers precede shards and routers; each replica set rolls secondaries before its primary, with safe-restart, step-down and replication catch-up gates. Members already reporting the target build are skipped unless `--include-current` is set. Completed members are journaled and never repeated after a control-plane restart. Cancellation is safe between members, not during an active member change. If the control plane changes build during a rollout, the operation fails before another deployment; re-run it to freeze the new target.

`spec.maintenance` windows gate new engine upgrades, rolling resources/configuration changes and scale-in, plus automatic database leaf renewal while more than seven days remain. A member already restarting finishes rejoining even if the window closes. Creation/scale-out, healing, agent transport certificates and database leaves within seven days of expiry are exempt. Imperative restart, TLS rotation, FCV promotion, replacement and agent upgrades wait before claim; `--ignore-maintenance-window` explicitly overrides supported actions. See [maintenance policy](../operations.md).

Use validated `engineConfig` YAML only for non-managed settings; see the [overview limits](README.md). Restart/replacement/rotation and Running desired-state/deletion work have no safe general abort; agent rollout cancellation is available only between members. [Operation semantics](../clusters.md#operations-and-status) distinguish cancellation from rollback.

## Processes, files and readiness

`mongod` serves data/config members; `mongos` serves routers; PBM placement is described in [sharding](sharding.md). Platform `karakoram agent readiness` probes **TCP 127.0.0.1:27017 only**, without credentials/environment; it does not validate users, replication or PBM. Authenticated provider observations determine [cluster health](replica-sets.md).

Ready requires an observed, writable primary in every replica set (including config servers and each shard), with data members in PRIMARY/SECONDARY state and within the managed replication-lag gate. Reachable secondary processes alone are not healthy write service. `Replication/<set>` reports `NoWritablePrimary` when election/quorum cannot provide a verified primary. Restore retained voters, disk capacity, DNS and connectivity; Karakoram does not force replica-set reconfiguration or reinitialize a previously formed set. A missing last data copy requires recovery from a verified backup, not an empty replacement. A lost secondary may be safely reseeded only from a verified retained writable primary.

Every live mongos instance must acknowledge its configuration and perform an authenticated majority read of config-server metadata. One healthy logical DNS endpoint or a running mongos PID cannot hide a failed sibling or unavailable config-server quorum. `Router/<member>` identifies failed instance observations.

Normal steady-state reconciliation reapplies the five reserved system-user credentials/roles and `pbmAnyAction`; it does not rewrite customer-created users or roles. If the retained `userAdmin` credential or privileges no longer authorize repair, `ManagedSystemUsers` reports `CredentialRepairRequired`. Restore that account's managed credential and `userAdminAnyDatabase` access using a surviving authorized administrator; restarting MongoDB does not repair authentication drift.

Reconciliation bounds each replication observation (connect, writable-primary check and replication status together) to four seconds. Managed-user connection checks, native `rolesInfo`/`usersInfo` reads, and each router instance status read also have independent four-second observation budgets. An expired read fails closed and identifies the read and connectivity/responsiveness checks; user/role mutations retain their original administration context and are not replayed by the observation timeout.

Agents pin internal trust and pull assigned protected configuration. With usable cache they can run through control-plane outages; without it, first configuration retries every five seconds. Normal pull is every 60 seconds, with immediate reload available to the controller. Activated member snapshots prevent simultaneous disruptive adoption; stateless router replacement loses cache.

| Under `DATA_DIR` | Recovery significance |
| --- | --- |
| `karakoram/config.json` | Atomic 0600 config cache with locally required secrets |
| `karakoram/keyfile` | Internal authentication key |
| `karakoram/mongodb.pem`, `ca.pem`, `mongod.conf` / `mongos.conf` | Protected rendered files; private material owner-only |
| `karakoram/controls.json` | Atomic 0600 durable maintenance/stopped-process holds |
| `karakoram/pbm-receipts/` | Durable 0600 PBM initiation outcomes |
| `db/` | Persistent mongod database files |

Supervisor owns start/stop/backoff and clean SIGTERM; do not launch duplicate mongod/PBM supervisors. Explicit stops survive pulls/restarts; only explicit maintenance-off or process start/restart releases corresponding holds. Never edit controls to undo [physical restore](backups.md).

## Logs

```sh
karactl clusters logs demo demom0 --process mongod --lines 100
karactl clusters logs demo demom0 --process pbm-agent --lines 100
# Router: actual member name, --process mongos
```

API: `GET /api/v1/clusters/{cluster}/members/{member}/logs?process=mongod&lines=100`. Default is mongod. Tails hold at most **1000 in-memory lines/process**, lost on agent restart; cursor/since are not a durable archive. Zerops runtime/deployment/application logs cover early startup, forwarded child output and supervisor diagnostics. Signed log URLs/tokens are credentials; never paste them into tickets. No arbitrary-file/exec endpoint exists.

## Monitoring vocabulary

Enable `spec.monitoring.enabled` (CLI create default true), then **Monitoring** or `karactl clusters metrics demo --range 1h --step 30s -o json`. Agents authenticate with `clusterMonitor` and collect process/filesystem data. [Shared monitoring](../monitoring.md) owns scrape/history/API range semantics and authorization; MongoDB ships exporter/dashboard, not PMM/query analytics.

Every public metric has `namespace`, `cluster`, `member`; `set`/`role` are status properties, not guaranteed labels. Availability-dependent metrics may be absent, notably mongos oplog/WiredTiger values. Counter JSON series are reset-aware rates, not raw counters.

| Prometheus metric | Type/unit | JSON series / meaning |
| --- | --- | --- |
| `karakoram_mongodb_up` | Gauge 0/1 | `up`: authenticated serverStatus success |
| `karakoram_mongodb_uptime_seconds` | Gauge seconds | `uptime` |
| `karakoram_opcounters_total` | Counter operations | `opcounters.total`: operations/s |
| `karakoram_mongodb_opcounters_total` | Counter; `operation` | `opcounters.byOperation`: insert/query/update/delete/getmore/command |
| `karakoram_connections_current` | Gauge count | `connections.current` |
| `karakoram_connections_available` | Gauge count | `connections.available` |
| `karakoram_repl_lag_seconds` | Gauge seconds | `repl.lagSeconds`: latest visible optime difference |
| `karakoram_repl_member_state` | Gauge state code | `repl.memberState`: MongoDB myState |
| `karakoram_oplog_window_seconds` | Gauge seconds | `oplog.windowSeconds`: local first/last span, not PITR coverage |
| `karakoram_cache_used_bytes` | Gauge bytes | `cache.usedBytes` |
| `karakoram_cache_max_bytes` | Gauge bytes | `cache.maxBytes` |
| `karakoram_process_cpu_seconds_total` | Counter CPU seconds | `cpu.percent`: rate ×100, JSON clamped 0–100 |
| `karakoram_ram_used_bytes` | Gauge bytes | `ram.usedBytes`: mongod/mongos RSS, excludes PBM/container overhead |
| `karakoram_ram_total_bytes` | Gauge bytes | `ram.totalBytes`: proc/cgroup available bound |
| `karakoram_disk_used_bytes` | Gauge bytes | `disk.usedBytes`: DATA_DIR filesystem |
| `karakoram_disk_total_bytes` | Gauge bytes | `disk.totalBytes`: DATA_DIR capacity |
| `karakoram_pbm_agent_up` | Gauge 0/1 | `pbm.up`: process running, not registration/backup health |
| `karakoram_mongodb_documents_total` | Counter; `operation` | `documents.total`: inserted/updated/deleted/returned |
| `karakoram_mongodb_network_bytes_total` | Counter bytes; `direction` | `network.bytes`: bytesIn/bytesOut |

Prometheus throughput: `rate(karakoram_opcounters_total[5m])`; never rate gauges. Raw CPU `rate(karakoram_process_cpu_seconds_total[5m]) * 100` can exceed clamped UI values on multicore work.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| Deployment/config stuck | Read operation checkpoint/events, generation and agent config/connectivity; inspect early Zerops logs |
| Empty charts | Check collection, agents, credentials/CA, range and enough counter samples; missing data is not healthy zero |
| Scrape 403 | Check `monitoring/read` on namespace/cluster and token/account status |
| Relay/config pull unavailable | Check [relay health](../namespaces.md#relay-health), public origin and namespace routing; retain CA validation |
| PBM up, no backups | Inspect PBM registration and backup records, not process-up alone. The local PBM URI must not set `directConnection=true`: agents on secondaries must discover the primary. Run one `pbm-agent` per `mongod`, none on `mongos`. |
| Offline during restore | Follow [PBM recovery](backups.md); do not release holds/start competing processes |
| Memory near bound | Include PBM/other processes; inspect Zerops allocation and runtime logs |
| Application connection failure | Check project DNS/network, saved password/authSource/roles, then cluster CA; do not bypass verification |
