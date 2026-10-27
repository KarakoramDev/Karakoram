<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Operations

Operate [Elasticsearch](https://www.elastic.co/elasticsearch) nodes and Stack components with health-gated maintenance and shard-aware removal.

Native health, node-membership (`/_nodes`) and elected-master (`/_cat/master`)
observations are required for Ready. A failed observation withdraws member health,
preserves agent reachability separately, and reports actionable member/cluster
messages. `ClusterHealth=Unknown/ObservationUnavailable` means native evidence is
incomplete, not that a timeout proves unhealthy shards or a missing master.
Fresh shard evidence establishing failure remains False if another probe fails.
Previously acceptable green or yellow health becomes Unknown when required
observations fail; acceptable yellow is not positive failure evidence.
Inspect native API access, member processes and connectivity; restore fresh
observations before maintenance. Successful fresh observations clear this diagnosis.
The convergence membership fan-out also has a two-second read budget, so a
paused JVM does not hold the serialized status worker until the native client's
longer administration timeout. Expired status snapshots still withdraw positive
agent connectivity claims until a fresh sweep succeeds; inspect `ObservationStale`
and agent last-seen times to distinguish this from a fresh failed agent RPC.
Convergence-membership read failures identify the two-second budget and direct
operators to inspect member responsiveness/connectivity.

## Scale, upgrade, restart and replace

Change counts/resources in **Topology & scaling** or a spec merge patch. The UI reviews the outgoing patch, not unrelated form defaults. For example, `scale.yaml`:

```yaml
spec:
  components:
    engine:
      replicas: 5
```

```sh
karactl clusters patch search -f scale.yaml --wait
karactl clusters members search
karactl operations cluster search
```

New nodes use never-reused indices and hot-reloaded discovery seeds. Scale-down serially drains shards through allocation exclusions, then voting exclusions where needed. Overview distinguishes desired, observed and **draining** members until confirmed departure. Remaining data nodes must hold each index's primary plus maximum replicas, including auto-expanded replicas evaluated after removal.

`ShardDrainBlocked` reports insufficient capacity or pinned shards, with a Warning on transition rather than every observation. Even green health can hide pinned primaries: three idle observations retaining the same nonzero shard count trigger a blocked drain. Progress/relocation resets that window; repeated flaps do not repeat the same warning. Allocation explanations identify indices, eligibility and filters when available; missing diagnostics do not suppress the condition.

To recover:

- Restore unavailable primaries first. Red health blocks voting exclusion/runtime/volume deletion, including vanished nodes and retried deletion; never force removal.
- Relax restrictive `index.routing.allocation.require.*`, `include.*` or `exclude.*` filters through Elasticsearch. More capacity/fewer replicas cannot free a pinned primary. Lower replica policies only if reduced redundancy is acceptable.
- Cancel an unfinished scale-down by restoring `components.engine.replicas` to cover **all observed data nodes**. This clears owned exclusions/blocked state and emits `ShardDrainCancelled`; further growth adds capacity. Once voting-excluded/deleting, removal must finish. Replacement removals cannot be cancelled this way.

Only Karakoram-owned exclusions are removed, including after interrupted writes. Dedicated masters skip shard-inventory waiting and proceed to voting exclusion, but retain primary-safety gates.

RAM/heap and restart-relevant settings trigger rolling maintenance; CPU/disk-only changes use nondisruptive updates within [platform constraints](../clusters.md). Optional replica changes adjust containers; zero deletes the service and ephemeral contents.

Upgrades allow newer advertised versions within a major, or `8.19.x` → `9.1+` (never `9.0`); targets come from the [catalog](README.md). Cross-major changes require a fresh successful snapshot. Resolve Upgrade Assistant/deprecation/index compatibility issues, then apply a version-only patch: `karactl clusters patch search -f upgrade.json --wait`. An in-place downgrade is not rollback: rebuild an earlier-version cluster using a compatible pre-upgrade snapshot.

```sh
karactl clusters restart search --wait
karactl clusters replace-member search MEMBER_NAME --wait --yes
```

Restart/replacement and other maintenance (backups/restores/rotations) reject requests before initial formation; restart/replacement also require an active cluster. Select observed member names, not bootstrap recovery targets.

Rolling disruption requires all planned masters present and green health, or narrowly acceptable yellow: only unplaceable replicas, no initialization/relocation. Red/unexplained yellow blocks progress. Maintenance restricts allocation to primaries, flushes, changes one node, waits for rejoin, restores allocation and waits for stable health. Data-only nodes precede non-elected masters; elected master last. After Elasticsearch: [Kibana](https://www.elastic.co/kibana) (all instances stopped together), [Logstash](https://www.elastic.co/logstash), APM. Status/Operation identify current member/component/step. Mixed Kibana versions are unsupported.

A failed deployment retry can recover the same absent target only with every other planned master and committed quorum; it cannot advance to another node while that target remains absent. Replacement joins a new node before draining the old and needs temporary capacity. Single-node replacement is rejected: use snapshots. One data node interrupts service during maintenance even with dedicated-master quorum; two combined nodes cannot preserve quorum with one stopped. Kibana has a separate interruption. [Suspend/delete](../clusters.md#delete-or-suspend) observations are historical: members show suspended/agents offline, not cached live health. [Interrupted restore and snapshot cleanup](snapshots.md) have additional retention rules.

On [Zerops](https://zerops.io/), the exact-version deployment retry budget is finite.
`DeploymentRetriesExhausted` stops submissions for that node or stateless stack
component, without hiding other members' observations. The rolling journal and
quorum checks remain in force. Correct the platform/runtime failure, then run
`karactl --namespace NS clusters retry-deployments NAME` to re-arm deployment.

If a restarted Elasticsearch node cannot start because of a product-invalid setting, correct `engineOptions.elasticsearch.settings` and reapply it. The in-flight rolling pass publishes the correction to that same node while waiting for rejoin; it does not disrupt the next member. During upgrade, configuration repair keeps the journal's target binary version and never attempts an in-place downgrade.

## Agent rollout and maintenance windows

`karactl clusters upgrade-agents search --wait` refreshes Karakoram agents without changing Elasticsearch or Stack versions, runtime identities or volumes. The durable rollout freezes the control-plane build and skips healthy members already reporting it; `--include-current` explicitly refreshes those members too. Data-only nodes roll first, then non-elected master-eligible nodes, the elected master, Kibana, Logstash and APM. Each node retains the master/quorum, shard-health, primaries-only allocation, flush and recovery gates above, and must report the exact target agent build before advancing. Stack components must also report that build and pass component readiness. Completed members are journaled across control-plane restarts. Cancellation is allowed between members, never while allocation or a stopped member needs recovery. If the control-plane build changes again, re-run the operation for the new build.

`spec.maintenance` windows gate the start of each new disruptive member step: restart, TLS rotation, agent rollout, engine upgrade, restart-required configuration/resource changes and scale-in (including optional component removal). Automatic database leaf renewal waits while more than seven days remain. A member already allocation-restricted or stopped always completes rejoin, allocation restoration and shard recovery even after the window closes.

Healing, create/scale-out, delete/suspend/resume, backup/restore, credential rotation, agent transport-certificate renewal and database certificates with at most seven days remaining are exempt. No windows means always open. Use `--ignore-maintenance-window` on a supported operation or spec update for an explicit override; it does not bypass native safety or deployment admission. See [maintenance policy](../operations.md) for schedules and override scope.


## Readiness and monitoring

| Process | Local readiness |
|---|---|
| `elasticsearch` | TCP listener reachable; does not establish formation/cluster health |
| `kibana` | `/api/status` returns HTTP 200; 503 and redirects are not ready |
| `logstash` | Every configured pipeline loaded and running via local monitoring APIs |
| `apm-server` | Intake TCP listener reachable; does not establish indexed delivery |

Inspect conditions, observed version/membership, elected master, component readiness, heap/disk and allocation. Green assigns primaries/replicas; yellow lacks replicas (sometimes expected on one data node); red lacks a primary. An `up` sample is not delivery/durability proof. Collection/authentication/history conventions: [Monitoring](../monitoring.md).

`IndexWriteBlocked=True` degrades an otherwise green cluster and names the affected indices and native settings (`index.blocks.read_only_allow_delete`, `index.blocks.read_only`, or `index.blocks.write`), including hidden/system indices. Free disk space or add capacity when flood-stage protection fires. Every supported Elasticsearch version [automatically releases flood-stage blocks](https://www.elastic.co/docs/troubleshoot/elasticsearch/fix-watermark-errors) after usage falls below the high watermark; Karakoram does not clear disk protections or operator-set blocks. Review intentional read-only/write restrictions before removing them. If index settings cannot be inspected, the condition is `Unknown` and the cluster is not reported Ready.

| Metric | Meaning |
|---|---|
| `karakoram_elasticsearch_up` | Local availability |
| `karakoram_elasticsearch_cluster_status` | 0 green, 1 yellow, 2 red |
| `karakoram_elasticsearch_cluster_nodes` | Observed nodes |
| `karakoram_elasticsearch_active_shards`, `karakoram_elasticsearch_unassigned_shards`, `karakoram_elasticsearch_relocating_shards` | Allocation/recovery |
| `karakoram_elasticsearch_jvm_heap_used_bytes`, `karakoram_elasticsearch_jvm_heap_max_bytes` | Heap usage/capacity |
| `karakoram_elasticsearch_docs`, `karakoram_elasticsearch_store_bytes` | Documents/indexed storage |
| `karakoram_elasticsearch_indexing_total`, `karakoram_elasticsearch_search_query_total` | Workload counters; use rates |
| `karakoram_elasticsearch_gc_collection_seconds_total` | Accumulated GC time |
| `karakoram_elasticsearch_thread_pool_rejected_total` | Rejected-work counter |
| `karakoram_elasticsearch_fs_available_bytes` | Available data-filesystem space |
| `karakoram_kibana_up`, `karakoram_kibana_status_level` | Availability; status 0 available, 1 degraded, 2 unavailable |
| `karakoram_logstash_up`, `karakoram_logstash_events_in_total`, `karakoram_logstash_events_out_total`, `karakoram_logstash_jvm_heap_used_bytes` | Availability, event counters, heap |
| `karakoram_apm_up`, `karakoram_apm_requests_total`, `karakoram_apm_events_accepted_total` | Availability, request/event counters |

Generic process CPU/RSS/disk metrics remain available. Historical query steps can average categorical states: fractional cluster/Kibana status means mixed samples, not another native state. Use external alerting/history for independent availability monitoring.

## Troubleshooting

Check status, members, conditions and Operations first; retrieve protected component logs with `karactl clusters logs search MEMBER --process PROCESS`. For blocked drain, use the recovery actions above. For Logstash compile/start failures, [correct and reapply pipelines](stack.md). For Kibana initialization, distinguish allocated routes from ready applications; check proxy prefix handling. For HTTPS/network failures, check returned endpoint visibility, project reachability, CA trust and [public-route boundaries](access-and-security.md), never disable verification. For missing intake events, verify indexed data and producer replay, not just readiness. For repository/restore failures, preserve inventory and [snapshot recovery prerequisites](snapshots.md).

For Kibana startup under a tight memory limit, inspect native startup logs and the runtime's memory limit/current usage and cgroup `memory.events` where available. Repeated `max` events indicate limit pressure, not proof of an OOM kill or permanent failure; listener refusal alone does not identify the cause. Enabled Kibana 9.x requires at least 1.5 GiB minimum RAM for new or changed admissions; 8.x retains 1 GiB. These floors are not startup-time guarantees. Apply an intentional resource change through `spec.components.extra.kibana.resources`, increasing both `minRam` and `maxRam` together. Increasing only the maximum does not increase the Node.js old-space cap. A deployed but not-yet-ready component can converge changed runtime resources before publishing its new heap and rechecking native readiness; an active platform operation must still finish first. Scheduler capacity and native initialization remain prerequisites for Ready.

Existing undersized 9.x Kibana remains admitted for unchanged same-version updates and replica decreases/disable, without automatic resizing. Status reports its below-minimum allocation and the explicit min/max RAM correction. Enabling, adding replicas or editing RAM must meet the active floor. For a version change targeting 9.x with Kibana enabled, including 9.x patch upgrades or 8→9, **resize Kibana first** to the target floor at the current version if needed; a combined resize and upgrade of an already enabled undersized instance is refused. Before changing version, the current generation must be fully applied (`status.observedGeneration == generation`) and the cluster must report `Ready`, including Kibana readiness. An accepted resize PATCH alone is not sufficient: wait for the pending change to finish before submitting the version PATCH. This convergence gate does not apply when Kibana is disabled or the target retains the 8.x floor. Existing cross-major backup requirements still apply.

When a child exits with a recognized Node/V8 fatal heap-limit marker, Kibana reports heap exhaustion with the configured old-space cap in MiB and directs an increase of `spec.components.extra.kibana.resources.minRam` and `maxRam` together (to at least 1.5 GiB on 9.x). This diagnostic is qualified by the applied configuration and failed child launch, remains available through retries, and clears after a later healthy start. It is not evidence of a cgroup OOM kill and does not depend on cgroup visibility. Status never copies raw fatal log lines into the diagnosis, and observation does not change sizing or restart policy.

With updated member agents on [Linux](https://www.kernel.org/) cgroup v2, an unavailable Kibana reports
`MemoryPressure=True / CgroupMemoryPressure` when its own cgroup has a fresh
increase in `memory.events:max` between uninterrupted samples less than ten
seconds apart (shared only until ten seconds after the earlier sample), or
nonzero `memory.pressure` `some avg10`. If an unavailable component has a readable
counter but no fresh evidence, its readiness request takes a second sample one second later,
bounded by the request context. This establishes a fresh comparison even on the
first request or after a long gap between control-plane observations, without
interpreting the accumulated delta across that gap as current pressure. The condition and
instance-qualified readiness message explain how to increase RAM; the message
includes numeric current/limit bytes when available. Old lifetime counters,
historical PSI totals and high usage alone do not assert current pressure.
Observation gaps, failed membership/mount resolution and counter resets discard
the counter baseline. Parent-relative cgroup paths are rejected before
normalization; oversized procfs/cgroup reads are rejected rather than truncated.
Unsupported/unreadable cgroup files produce `MemoryPressure=Unknown` with reason
`CgroupMemoryEvidenceUnavailable`, instance identity and instructions to inspect
allocation-scoped host evidence or provide an operator-approved read-only scoped
cgroup view. Unknown is not a pressure diagnosis. [Nomad](https://developer.hashicorp.com/nomad) `exec` tasks can have a
read-only `/sys` sysfs mount without any cgroup2 submount: host cgroup files then
exist but are invisible to the member agent. The product does not bypass that
isolation, add mount privileges, or expose host-wide cgroups automatically.
[Dokploy](https://dokploy.com/) and Zerops likewise require scoped cgroup visibility for a positive native
diagnosis; no backend-specific host visibility is assumed. Older agents retain
ordinary native readiness messages. Pressure is diagnostic, not a
terminal configuration error or evidence of an OOM kill. Successful native
readiness takes precedence; the condition disappears on the next fresh healthy
observation or after pressure evidence expires. No restart or sizing mutation
is performed by observation.

Karakoram retains the explicit **60% of minimum RAM** Node old-space cap
(614 MiB at a legacy 1 GiB allocation, 921 MiB at 1.5 GiB). This caps V8 old space, **not total
RSS**: young-generation heap, native/C++ allocations, buffers, code and other
process/runtime overhead also consume memory. The allocation's cgroup includes
the member agent and file-backed memory as well. Startup/plugin initialization
can consume additional memory; the observed limit hits do not identify which
allocator or an optimizer as the cause. Raising the old-space fraction may
worsen allocation pressure; lowering it may trade pressure for GC or V8
exhaustion. Neither is a proven fix for a particular workload.

Elastic's [8.19 production guidance](https://www.elastic.co/guide/en/kibana/8.19/production.html#memory)
and [current memory guidance](https://www.elastic.co/docs/deploy-manage/production-guidance/kibana-configure-memory)
describe scalable heap sizing, not an unconditional startup RAM minimum.
The current guidance documents 50% historically and 75% (capped at 4096 MB)
for containers starting in 9.4; Karakoram deliberately supplies its own cap.
Elastic recommends at least **2 GB** for reporting/detection/alerting/SLO and
advanced licensed features, and [1 vCPU / 2 GB as a safe starting point for
common analytics traffic](https://www.elastic.co/docs/deploy-manage/production-guidance/kibana-traffic-scaling-considerations).
Use that workload baseline rather than treating the admission boundary as a
production sizing recommendation. The 9.x admission floor is a supported operating
boundary, not a claim that every smaller launch is impossible or every admitted workload fits.
See Node's [old-space flag](https://nodejs.org/docs/latest-v22.x/api/cli.html#--max-old-space-sizesize-in-mib)
and [RSS/heap distinction](https://nodejs.org/api/process.html#processmemoryusage).
