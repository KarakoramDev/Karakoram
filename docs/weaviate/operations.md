<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Weaviate operations

Observe durable operations, member acknowledgements and native health, not only accepted requests. Keep a recent successful [full backup](backups.md) before maintenance and stop incompatible concurrent application changes.

## Readiness, health and logs

```sh
karactl -n default clusters status vectors
karactl -n default clusters members vectors
karactl -n default clusters logs vectors MEMBER --process weaviate --lines 100
karactl -n default operations cluster vectors
```

The authenticated agent readiness action is `POST /v1/weaviate/readiness`; it returns `ready`, applied `configVersion`, observed `version` and a bounded redacted message. This is private agent API, not a public Karakoram cluster endpoint. Native probes use `/v1/.well-known/ready`, `/v1/nodes?output=verbose` and `/v1/cluster/statistics`.

A member must acknowledge its active configuration, report the desired native version and have the `weaviate` process running. Maintenance gates require every expected member ready/healthy, synchronized statistics, loaded/open databases, the expected fixed voter configuration and a leader from that voter set. Native `bootstrapped` is a process-local bootstrap diagnostic, not a current quorum requirement: after reopening retained Raft state it can be false or omitted while the node is ready with a valid leader. A native ready endpoint alone does not prove quorum or data redundancy. Empty databases legitimately have null shard lists and zero shard counts.

If an older control plane reports false Raft quorum after a restart despite those native checks passing, upgrade the control plane to a build with this readiness correction. Its next reconciliation resumes the persisted roll and observes the pending generation; no agent rollout, Raft bootstrap, identity change or volume reset is needed to correct this observation. The pending storage/configuration roll still completes its normal restart and acknowledgement steps (with singleton downtime).

Logs are bounded process tails, not durable archives. Use process `agent` for management diagnosis; never publish keys, signed URLs, object contents or raw secret-bearing native responses.

### Startup fails with an unspecified Raft advertise address

The v0.4.3 member runtime explicitly advertises the member's private IP for both
memberlist and Raft. It resolves the existing private member hostname from inside
the runtime, selecting IPv6 for an IPv6 bind and IPv4 for an IPv4 bind. Listener
wildcards remain `::` or `0.0.0.0`; they are not peer addresses. The
`CLUSTER_ADVERTISE_ADDR` environment value is a bare IP (no IPv6 brackets or port).
Upstream formats Raft endpoints with brackets around IPv6 as needed.
If private DNS is temporarily unavailable, the agent retains the accepted
configuration and retries its apply with cancellation-aware backoff (one second,
doubling to at most 30 seconds), independently of control-plane fetches. A newer
accepted configuration supersedes the retained retry. Explicit stop intent and
local maintenance/stop holds do not require DNS resolution.
After an agent restart under a durable operator stop hold, the process remains
registered but cannot start with unresolved runtime settings. Explicit start
resolves the current member address and prepares the protected files/process
configuration before admission releases the hold. DNS failure refuses that
start without dropping the hold; retry start once private DNS is available.
Maintenance-period applies preserve an already configured process specification
rather than replacing it with unresolved settings. If that native process exits
while maintenance suppresses restart, releasing maintenance can resume it from
the retained valid specification even while the control plane is unavailable.


On older runtimes, IPv6-only deployments can repeatedly exit with
`local bind address is not advertisable` and `tcpAddress=[::]:8300`. This also
affects single-node deployments: a singleton still opens a Raft transport.
IPv4 backends normally auto-select a private interface, but explicit member
advertisement removes that dependency.

**Upgrading only the control plane does not update an already deployed agent
binary.** Deliver the corrected v0.4.3 member runtime through the backend's
authorized deployment procedure, preserving each original member identity,
credentials, volume attachment and data. The corrected agent accepts retained
member configurations unchanged; its next apply resolves the address and starts
the native process with the corrected environment. For a never-ready cluster,
do not rely on ordinary restart/upgrade operations: their native-health/quorum
gates cannot pass before startup is repaired. Do not clear Raft data, delete
voter volumes, or force one-node recovery. An empty failed initial provisioning
may instead be deleted and recreated after installing the corrected artifacts,
but only when its data is confirmed disposable.

After recovery, each member's `memberlist_config` log must show its own nonempty
private `advertise_addr` and gossip port 7946. Raft's advertised TCP endpoint
must use that same IP with port 8300, not an unspecified address. Require stable
processes, all expected members, a leader from the fixed voter set, synchronized
Raft statistics, successful configuration acknowledgements and `Ready` status
with `ClusterHealth`/`Quorum` true before declaring recovery.


## Restart and upgrade

```sh
karactl -n default clusters restart vectors --wait
karactl -n default clusters upgrade vectors --version 1.40.1 --wait
```

Rolling maintenance restarts one member at a time, non-voters before voters, checking health before disruption and after acknowledgement. Configuration, resources, version, native TLS material and restart tokens participate in restart decisions. Singleton maintenance interrupts database access. Rolling order and quorum gates are not a zero-disruption or application-consistency guarantee; choose application read/write consistency appropriately.

Supported versions are `1.39.12` and `1.40.1` (default). Only same-minor patch-forward or **one minor forward at a time** is allowed, and the target must be cataloged. The current cross-minor path is `1.39.12 → 1.40.1`. Downgrades, major changes and skipped minors are rejected. Rolling members can temporarily run source/target versions; wait for actual native version acknowledgement and observed desired generation before declaring convergence. Control-plane rollback does not downgrade engine data.

### Agent upgrades

```sh
karactl -n default clusters upgrade-agents vectors --wait
```

The rollout redeploys agents without changing applied database versions,
configuration, resources, identities or volume attachments. Its existing rolling
journal freezes the target build and member order: non-voters first, then voters,
with the observed Raft leader last. Already-current healthy agents are skipped
unless `--include-current` is supplied.

Before each new deployment, the maintenance window must be open, all members
must be native-ready with synchronized cluster statistics, and removing the
selected member must retain a live voting majority. Multi-member clusters require
an applied minimum replication factor of at least two, collection replication
factors of at least two, and native shard evidence proving that no selected
member holds a sole replica. Unknown evidence fails closed. These gates do not
promise uninterrupted application reads/writes at every consistency level.
**Single-member agent upgrades cause database downtime** and do not require
redundancy that a singleton cannot provide.

The next member waits for native readiness, synchronized cluster statistics and
the exact frozen agent build. Other rolling, replacement, scaling, rotation or
pending structural work blocks a new rollout. Cancellation stops between members;
an already-submitted deployment is recovered even after cancellation or window
closure. Cancellation is not rollback. A changed control-plane build cannot
silently retarget an existing operation; follow its diagnostic before continuing.


## Scaling

Change `spec.components.engine.replicas` or use `clusters scale --replicas`. `single` stays at one; `cluster` allows 3–9 with exactly three fixed voters. CPU/RAM resource changes are supported; disk only grows and depends on provider capability. See [resources](README.md#resources-and-runtime).

Scale-out allocates new monotonically named non-voters; it does **not** redistribute existing shards. Collection creation/replication settings and deliberate native shard movement determine data distribution. Increasing member count is not proof of increased redundancy for existing collections.

Scale-down selects the highest-index non-voter. Before deletion it:

1. Checks collection replication factors and actual shard replica counts against the remaining member count.
2. Finds a healthy retained target not already hosting each departing replica, choosing lower shard load first.
3. Submits native `POST /v1/replication/replicate` with `type: MOVE`, persists the operation ID and polls for `READY`. Transient error entries during movement do not terminate polling; cancellation or a missing operation triggers bounded failure handling.
4. Rechecks native sharding state and verbose node inventory until no departing replicas remain.
5. Stops the departing member and waits until **every survivor's memberlist view omits it**; `UNAVAILABLE` or `TIMEOUT` alone does not fence placement.
6. Performs a fresh collection/shard scan, including physical tenant shards. If the member acquired shards before the fence, restarts it, waits for readiness and drains again.
7. Only after the fenced scan is empty, removes persisted Raft membership through authenticated internal data-port 7947 `/v1/cluster/remove`, then deletes runtime and volume.

These are native internal operations, not Karakoram public routes. No eligible target, insufficient healthy capacity or RF greater than the remaining count blocks removal; do not stop/delete the member to bypass draining. No voter is automatically demoted or removed. If RF equals the original member count, reduce collection replication deliberately before reducing count; every retained node already hosting a shard leaves no MOVE destination. The configured minimum RF must also fit the requested member count.

## Replace a member

```sh
karactl -n default clusters replace-member vectors MEMBER --wait
```

Ordinary replacement requires healthy synchronized peers and Raft quorum, retains the original persistent volume and reuses member identity. Voters keep their original names; replacement is not voter retirement. Lost initialized volumes require explicit collection/shard replica evidence and a surviving voter majority before fresh-volume recovery. Replacement is refused if the lost member held a sole replica, sharding/replica evidence is missing, or voter quorum is insufficient. Recover the original volume or a full backup instead; increasing non-voter count cannot establish quorum.

Do not destroy a voter volume to repair missing runtime placement. Losing Raft majority cannot be fixed by adding non-voters or clearing bootstrap state. Repair original members/volumes and private connectivity, or follow a deliberate backup recovery procedure.

## Rotation

```sh
karactl -n default system-users list vectors
karactl -n default system-users rotate vectors karakoram-admin --yes
karactl -n default system-users rotate vectors karakoram-cluster --yes
karactl -n default clusters rotate-tls vectors --wait
```

Track the returned durable operation with `operations wait OPERATION_ID` where the command does not wait itself. Rotation requires synchronized healthy members and quorum and is serialized with other maintenance.

- **Root API key (`karakoram-admin`):** first rolling pass accepts old and new keys; Karakoram switches its sealed credential and verifies the new key on every member; a second rolling pass removes the old key. Do not assume the old key has expired until the second pass completes.
- **Shared internal password (`karakoram-cluster`):** upstream accepts one basic-auth password, not overlapping passwords. Rotation therefore stops all members, delivers the new shared configuration to all, then deploys all members and waits for health. This is a **coordinated full restart with brief unavailability**, not rolling maintenance.
- **Native TLS leaves:** certificate files load only at listener startup, so rotation issues certificates and rolls members, non-voters before voters. It is not a hot reload. A singleton is unavailable during restart; client trust changes require coordination.

Application database users use `weaviate users rotate-key`, not system-user rotation. See [access and secret handling](access-and-tls.md).

## Exhausted platform deployments

Platform retry exhaustion is durable and blocks further deployment submissions without advancing the active provisioning, rolling or rotation step. Fix the provider failure, then run `karactl -n default clusters retry-deployments vectors` (requires `clusters/update`). This increments `spec.retryEpoch` to re-arm the pending deployment; it does not bypass readiness/quorum gates. See [shared retry semantics](../clusters.md#retry-exhausted-platform-deployments).

Public REST/gRPC port allocation reconciles provider routes and durable member assignments, not stale in-memory reservations. Retired members' released ports can be reused; live routes and retained assignments remain reserved.

## Metrics

Enable `spec.monitoring.enabled`. Native port 2112 is scraped locally with bounded responses; the agent relays only the fixed vocabulary below, without arbitrary native metric labels. Public [Prometheus](https://prometheus.io/) collection uses Karakoram's authenticated `/metrics`, not a public native 2112 listener. See [monitoring](../monitoring.md) for authorization, retention and freshness.

| Exposition name | Catalog name | Unit (scale 1) |
| --- | --- | --- |
| `karakoram_weaviate_up` | `weaviate.up` | boolean |
| `karakoram_weaviate_objects` | `weaviate.objects` | count |
| `karakoram_weaviate_shards` | `weaviate.shards` | count |
| `karakoram_weaviate_collections` | `weaviate.collections` | count |
| `karakoram_weaviate_queries_total` | `weaviate.queries.total` | count |
| `karakoram_weaviate_requests_total` | `weaviate.requests.total` | count |
| `karakoram_weaviate_batch_objects_total` | `weaviate.batch.objects.total` | count |
| `karakoram_weaviate_vector_index_capacity` | `weaviate.vectorIndex.capacity` | count |
| `karakoram_weaviate_vector_index_tombstones` | `weaviate.vectorIndex.tombstones` | count |
| `karakoram_weaviate_lsm_bytes` | `weaviate.lsm.bytes` | bytes |
| `karakoram_weaviate_async_queue_length` | `weaviate.asyncQueue.length` | count |

Use `rate()` for `_total` counters; other entries are gauges. Native version is observed status, not a fabricated metric. No disk-readonly metric is advertised. Availability does not establish Raft quorum, collection replication coverage, successful backups or application query correctness.

## Troubleshooting

| Symptom | Check / recovery boundary |
| --- | --- |
| Collections become read-only | Default disk warning is 80%, readonly 90%. Check native disk/shard state and actual free space, grow disk or remove unneeded data safely, then follow native shard-state recovery. Do not raise thresholds past the validated maximum (95%) or treat `up` as write health. |
| Cluster fails to form | All initial three voters must be reachable; verify stable names, DNS, gossip **UDP and TCP** 7946, data 7947, Raft 8300/8301 and isolated-network policy. Bootstrap expect is not a quorum override. |
| Quorum loss / no leader | Three fixed voters need two surviving voters. Extra non-voters do not replace lost votes. Restore original voter runtimes, volumes and network; do not edit voter names, wipe Raft state or force bootstrap. |
| Voter runtime/volume loss | Preserve original identity and volume for runtime replacement. Lost initialized storage needs proven surviving replicas; otherwise restore original storage or recover a full backup into a suitable target. |
| Scale-down blocked | Compare minimum/collection RF and actual replicas with target count, native MOVE status, healthy target capacity and disk headroom. Keep the departing runtime and volume until draining and membership removal finish. |
| TLS rotation appears ineffective | Both REST and gRPC load PEM at startup. Follow the rolling operation and config/version acknowledgements; verify client CA, SAN and clock without disabling verification. |
| Backup/restore cannot reach S3 | Check bound storage, member-side egress, endpoint TLS, bucket and credentials; control-plane verification alone is insufficient. Restore endpoint/bucket/member count must match recorded metadata. |

For deletion/retained-volume suspension use [shared cluster procedures](../clusters.md#delete-or-suspend). Keep storage and namespace credentials available through backup cleanup; inspect orphaned volume inventory independently.
