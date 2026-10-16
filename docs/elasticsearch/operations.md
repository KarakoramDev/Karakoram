<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Operations

Operate Elasticsearch nodes and Stack components with health-gated maintenance and shard-aware removal.

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

Rolling disruption requires all planned masters present and green health, or narrowly acceptable yellow: only unplaceable replicas, no initialization/relocation. Red/unexplained yellow blocks progress. Maintenance restricts allocation to primaries, flushes, changes one node, waits for rejoin, restores allocation and waits for stable health. Data-only nodes precede non-elected masters; elected master last. After Elasticsearch: Kibana (all instances stopped together), Logstash, APM. Status/Operation identify current member/component/step. Mixed Kibana versions are unsupported.

A failed deployment retry can recover the same absent target only with every other planned master and committed quorum; it cannot advance to another node while that target remains absent. Replacement joins a new node before draining the old and needs temporary capacity. Single-node replacement is rejected: use snapshots. One data node interrupts service during maintenance even with dedicated-master quorum; two combined nodes cannot preserve quorum with one stopped. Kibana has a separate interruption. [Suspend/delete](../clusters.md#delete-or-suspend) observations are historical: members show suspended/agents offline, not cached live health. [Interrupted restore and snapshot cleanup](snapshots.md) have additional retention rules.

## Readiness and monitoring

| Process | Local readiness |
|---|---|
| `elasticsearch` | TCP listener reachable; does not establish formation/cluster health |
| `kibana` | `/api/status` returns HTTP 200; 503 and redirects are not ready |
| `logstash` | Every configured pipeline loaded and running via local monitoring APIs |
| `apm-server` | Intake TCP listener reachable; does not establish indexed delivery |

Inspect conditions, observed version/membership, elected master, component readiness, heap/disk and allocation. Green assigns primaries/replicas; yellow lacks replicas (sometimes expected on one data node); red lacks a primary. An `up` sample is not delivery/durability proof. Collection/authentication/history conventions: [Monitoring](../monitoring.md).

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
