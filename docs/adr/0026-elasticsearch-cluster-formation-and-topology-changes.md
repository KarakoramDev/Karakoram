# 0026 — Elasticsearch cluster formation and topology changes

Status: Accepted

## Context

[Elasticsearch](https://www.elastic.co/elasticsearch) bootstrap is not ongoing discovery. Reapplying `cluster.initial_master_nodes` after formation can create a different cluster instead of recovering the existing one. Restarts, shard evacuation and master voting changes require different safety gates ([bootstrapping](https://www.elastic.co/docs/deploy-manage/distributed-architecture/discovery-cluster-formation/modules-discovery-bootstrap-cluster), [node removal](https://www.elastic.co/docs/deploy-manage/maintenance/add-and-remove-elasticsearch-nodes)).

## Decision

Support immutable topology types `cluster` (1–9 combined master/data nodes) and `dedicatedMasters` (3 or 5 dedicated masters, 1–20 data nodes). Combined nodes also provide ingest, remote-cluster-client and transform roles; data-only nodes provide the same roles without master eligibility. Do not enable `ml`. Prefer one node for development or 3/5 combined nodes for resilient voting. Even combined counts are allowed with warnings; two master-eligible nodes need both votes and cannot retain writes after either is lost ([small-cluster resilience](https://www.elastic.co/docs/deploy-manage/production-guidance/availability-and-resilience/resilience-in-small-clusters)).

Persist the initial master-eligible name set before deployment and reuse it across retries. Observe a nonempty cluster UUID other than `_na_` and all planned nodes, persist the UUID, then remove `cluster.initial_master_nodes` from rendered configuration without restarting. Never set it again once that UUID is known, including resume with retained volumes. Keep the UUID as the bootstrap guard and cluster identity.

Use `discovery.seed_providers: file`, with a generated `unicast_hosts.txt` containing master-eligible service hostnames and transport port 9300. Elasticsearch hot-reloads it; additions/removals update discovery without restarting existing nodes ([file seed provider](https://www.elastic.co/docs/deploy-manage/distributed-architecture/discovery-cluster-formation/discovery-hosts-providers)). An open HTTP port is only process readiness, not evidence of formation or health.

Reject restart and replace-member operations until the cluster has formed and is active, with its nodes at the joined checkpoint. These operations cannot repair incomplete initial bootstrap.

Serialize rolling restarts, version changes and restart-relevant settings/resources using the [documented self-managed procedure](https://www.elastic.co/docs/deploy-manage/upgrade/deployment-or-cluster/elasticsearch):

1. Require all planned master-eligible nodes present before disrupting a master-eligible member, and green health, or acceptable yellow: all primaries assigned, only replicas unplaceable in the current topology, and no initializing/relocating shards. Single-data-node yellow is tolerated; red and unexplained yellow block disruptive work.
2. Set persistent `cluster.routing.allocation.enable: primaries`, then request a best-effort flush.
3. Change and restart/redeploy one node; observe its rejoin and expected version.
4. Clear the allocation override and wait for green or acceptable yellow before proceeding.

Order data-only nodes first, then non-elected master-eligible nodes, with the elected master last. Persist per-member progress so reconciliation resumes the same step. After Elasticsearch completes, upgrade [Kibana](https://www.elastic.co/kibana) with all its instances stopped, then [Logstash](https://www.elastic.co/logstash), then [APM Server](https://www.elastic.co/observability/application-performance-monitoring). Never run mixed Kibana versions ([Kibana upgrade](https://www.elastic.co/docs/deploy-manage/upgrade/deployment-or-cluster/kibana)).

Scale-up provisions a fresh monotonic node identity, updates seed files and waits for join/rebalance. Scale-down drains one node at a time: ensure remaining data nodes can hold the maximum configured replica count plus a primary; merge the departing name into persistent `cluster.routing.allocation.exclude._name`; wait for zero shards on that node and no relocation. Report `ShardDrainBlocked` rather than deleting when placement cannot satisfy the drain. For a master-eligible node, add a voting-configuration exclusion and wait before stopping it; remove only one master at a time and never the last. Delete runtime before volume, remove only the owned allocation exclusion, clear voting exclusions after departure with `wait_for_removal=true`, and update seed files ([allocation settings](https://www.elastic.co/docs/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings), [voting exclusions](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-post-voting-config-exclusions)). Dedicated-master count changes are 3↔5, serialized through those steps.

Replacement adds a new node, waits for join and stable health (green or narrowly acceptable yellow), then drains/removes the old node. Reject replacement before formation or for a single-node cluster; use snapshot recovery rather than promising a replica source that does not exist.

## Consequences

Maintenance may wait for replica capacity, disk space, recovery or quorum. Yellow tolerance is deliberately narrow, not a general permission to restart degraded clusters. Single-node maintenance has downtime; two combined nodes are not a resilient production layout. Replacement needs temporary capacity. Safe node counts still do not guarantee physical failure-domain separation ([ADR 0016](0016-placement-anti-affinity.md)). Persisted bootstrap and maintenance checkpoints are recovery-critical control-plane state.

## Alternatives considered

- **Node Shutdown API as used by [ECK](https://www.elastic.co/elastic-cloud-kubernetes):** convenient restart/remove/replace intents, but Elastic explicitly documents direct use as unsupported; Karakoram uses supported self-managed operations ([API contract](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-shutdown-put-node)).
- **`discovery.type: single-node`:** convenient first startup, but prevents joining additional nodes while enabled and requires a configuration transition to expand.
- **Static seed lists:** sufficient for fixed deployments, but introduce avoidable restarts/stale discovery during topology changes.
- **Delete before drain or replace in place:** can lose shard copies or quorum and reuses identity; rejected in favor of add-then-drain.
