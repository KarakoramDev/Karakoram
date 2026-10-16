<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# RabbitMQ operations

Use cluster specs and durable operations for broker lifecycle changes, not parallel manual platform deployments.

## Lifecycle and readiness

Members report role `broker`; the supervised process/log name is **`rabbitmq-server`**:

```sh
karactl -n NS clusters logs brokers <member> --process rabbitmq-server
karactl -n NS clusters status brokers -o json
karactl -n NS clusters members brokers -o json
karactl -n NS events list --cluster brokers --order desc
```

Ready requires every expected broker running, each seeing the same planned running membership, no alarms/partitions/maintenance, and active platform containers. Healthy survivors are not individually marked unhealthy/partitioned merely because another broker stopped; aggregate health still degrades. Local health does not prove queue quorum/delivery. See [operation and generation status](../clusters.md#operations-and-status).

Platform readiness probes localhost's configured AMQPS port (AMQP if TLS disabled). An intentional maintenance/stopped hold passes platform readiness: deployment success is not messaging readiness. Do not remove holds to bypass stalled work. During creation, member stages distinguish import/deployment/agent readiness/join; `RUNNING` still needs aggregate membership checks. Departing members stay visible as `LEAVING`/`LEAVING_BLOCKED` until removed, not counted healthy.

## Rolling maintenance

Multi-node changes await online quorum plus one and reject quorum-critical shutdowns. Karakoram drains traffic, persists a maintenance hold and stops the broker **before disruptive platform changes**. Each changed node must acknowledge configuration, run/rejoin and revive before the next changes. Drain suspends listeners, closes connections and transfers leadership/responsibilities; it does not consume/purge/back up messages. Ensure survivor capacity, client reconnection and no alarms/catch-up; singletons incur downtime.

The platform may force-stop a container about five seconds after SIGTERM. Always use the gated drain/stop workflow rather than relying on platform shutdown.

After all binary upgrades finish, feature flags are enabled and queue/stream leaders rebalanced. Use only the [catalog upgrade path](README.md); [TLS transport/CA changes](access-and-tls.md) are not ordinary upgrades.

Live platform RAM changes do not update broker limits until the gated rolling restart. Managed total memory follows desired minimum RAM, watermark **60%**, disk-free limit **max(0.5 GB, minimum RAM)**. Compare desired/platform/current metric limits. Disk growth works; shrinking is rejected. For CPU-mode cooldowns inspect `PlatformCooldown`/events and the existing retry deadline; do not resubmit changes.

## Quorum-blocked restarts and replica growth

Scale-up does **not** enlarge existing queue groups: a queue declared on one broker may still have one replica afterward. `QuorumCriticalQueues`/`RollingRestartBlocked` identify queue/vhost/member blockers. Recover stream/metadata majorities separately; queue additions do not repair them.

1. Open **cluster detail → Queue recovery** for commands containing actual broker, vhost, queue and candidate names. This is native Management API administration using revealed `admin`, outside Karakoram RBAC—not a `grow-replicas` operation. No broker shell, cookie, queue recreation or purge is needed.
2. Run inside the cluster's project, where private DNS/management is reachable. Export the CA; reveal the admin password in Connection details. `--user admin` prompts without putting it in shell history.
3. Inspect queue type, members and online replicas; require an existing live majority and a healthy joined target not already in the group.

```sh
karactl -n NS clusters ca brokers > ca.pem
curl --fail-with-body --user admin --cacert ca.pem \
  'https://brokersm0:15671/api/queues/%2F/orders'
curl --fail-with-body --user admin --cacert ca.pem \
  -X POST -H 'Content-Type: application/json' \
  --data '{"node":"rabbit@brokersm1"}' \
  'https://brokersm0:15671/api/queues/quorum/%2F/orders/replicas/add'
curl --fail-with-body --user admin --cacert ca.pem \
  'https://brokersm0:15671/api/queues/quorum/%2F/orders/status'
```

Use Queue recovery's values, not illustrative hostnames. Encode each vhost/queue segment once (`/` → `%2F`). With disabled TLS use private `http://BROKER:15672` without `--cacert`; never disable HTTPS verification or publish management for recovery. These are the pinned [RabbitMQ 4.3.6 routes](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.6/deps/rabbitmq_management/src/rabbit_mgmt_dispatcher.erl).

4. Add **one** missing replica at a time, normally growing to three. Inspect queue/Raft status after each: the new replica must be online, voting and caught up to the leader's committed log, allowing for active writes. HTTP success/member-list changes alone do not prove synchronization.
5. Re-read membership before retrying a timeout; the replica may already exist. If all brokers are members, recover offline replicas. Never force membership, add without a majority or restart several brokers. Once safe, observe the existing workflow resume rather than submit duplicate edits/restarts.

## Scaling, replacement and deletion

Scale-up joins new brokers then rebalances leaders. Scale-down removes highest indexes first, requiring healthy survivors: grow quorum replicas on survivors, migrate streams independently of the optional Stream plugin, then shrink the departing node. Its application stops while its Erlang VM remains a Khepri voter; a survivor forgets it before full shutdown/deletion.

Durable classic queues hosted there block **scale-down and replacement** with `MemberRemovalBlocked` and queue names. Explicitly migrate/delete them; no automatic classic-store migration. Resolve the reported cause rather than forcibly hide/delete a live member.

For lost nodes/volumes, preserve state/original key, surviving volumes and redacted events. Recover private DNS/network, alarms and surviving Khepri **and queue/stream majorities**, then request replacement using the recorded member name:

```sh
karactl -n NS clusters replace-member brokers <member> --yes --wait
```

Replacement joins a fresh monotonic identity and grows/migrates replicas before old-member removal. An unreachable old broker skips local shutdown and is forgotten only with a surviving Khepri voter majority. Never recreate a blank volume under the old identity, reset survivors or forget several nodes. Lost last copies/classic stores cannot be recovered; unavailable majorities need explicit recovery assessment, possibly a new cluster plus [definitions import and independent message replay](backups.md).

[Deletion/suspension](../clusters.md#delete-or-suspend) removes public routes/subdomains before runtimes; full deletion removes owned volumes/secrets and tracks ownership-checked refused volume deletions for [orphan cleanup](../operations.md#orphaned-platform-volumes). Owned runtimes observed `STOPPED`/`FAILED`/`READY_TO_DEPLOY` can be deleted without agent RPC; unknown/transitional states cannot bypass an unreachable agent. Retained-volume resume starts **all** brokers before individual readiness/revive waits so the original Khepri majority can recover.

## Alarms and diagnostics

| Observation | Inspect / response |
| --- | --- |
| Publishing blocked | `Alarms`, resident memory/limit, free disk/limit, backlog/unacked messages; reduce load/backlog or grow resources and finish rolling limit updates; never disable managed alarms |
| Missing peer/partition | `Healthy`/`Partition`, each member's view, DNS/network, identity/cookie/certificates/runtime; restore connectivity/quorum, preserve data |
| Rolling change stalled | Operation step, config acknowledgement, maintenance, platform action, quorum blockers; recover replicas/alarms, observe terminal platform status |
| Management unavailable | Private management ports, CA/hostname, `karakoram` credentials, namespace relay/session/policy; repair private administration, not TLS bypass |
| Restore has no messages | Expected [definitions-only behavior](backups.md); inspect imported objects/target credentials and arrange replay |

## Metrics

The agent scrapes loopback Prometheus and exports these exact names; all are **gauges** except the two marked counters:

| Metric | Meaning / type |
| --- | --- |
| `karakoram_rabbitmq_up` | Scrape succeeded, not messaging/quorum health |
| `karakoram_rabbitmq_uptime_seconds` | Broker uptime |
| `karakoram_rabbitmq_connections` | Connections |
| `karakoram_rabbitmq_channels` | Channels |
| `karakoram_rabbitmq_consumers` | Consumers |
| `karakoram_rabbitmq_queues` | Queues |
| `karakoram_rabbitmq_messages_ready` | Ready messages |
| `karakoram_rabbitmq_messages_unacked` | Unacknowledged messages |
| `karakoram_rabbitmq_published_total` | Published messages; **counter** |
| `karakoram_rabbitmq_delivered_total` | Deliveries; **counter** |
| `karakoram_rabbitmq_memory_alarm` | Memory alarm |
| `karakoram_rabbitmq_disk_alarm` | Disk alarm |
| `karakoram_rabbitmq_resident_memory_bytes` | Resident memory |
| `karakoram_rabbitmq_memory_limit_bytes` | Memory watermark |
| `karakoram_rabbitmq_disk_free_bytes` | Free disk |
| `karakoram_rabbitmq_disk_free_limit_bytes` | Disk alarm limit |
| `karakoram_rabbitmq_cluster_members_running` | Local node plus established configured-peer distribution links, not independent failure domains |

The UI adds host CPU/RAM/disk panels; process CPU/RAM usage covers the supervised `rabbitmq-server` wrapper and live descendants, including `beam.smp`. CPU also includes the wrapper's reaped-child CPU time. JSON rates derive from counters. Inspect `Healthy`, `Alarms`, `Partition`, `Placement` and external messaging probes; bounded aggregate monitoring is not independent alerting. See [shared monitoring](../monitoring.md) for collection/history defaults and [verification](../verification.md).
