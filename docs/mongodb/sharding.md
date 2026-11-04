<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Sharding

Provision config servers, shard replica sets and mongos routing for [PSMDB](https://docs.percona.com/percona-server-for-mongodb/).

## At a glance

| Component | Shape / omitted-field default |
| --- | --- |
| Config servers | Replica set `cfg`; 1/3/5/7 members, default 3 |
| Shards | `rs0`, `rs1`, …; 1–128 shards, default 1; each set uses the engine component's 1/3/5/7 members, default 3 |
| Routers | Stateless mongos service; 1–32 containers, default 1 |

These are validation bounds, not load-tested maxima or a project-capacity guarantee. Each mongod has its own runtime/volume; mongos has no durable database volume. All services remain in the selected namespace's project. The [management relay](../namespaces.md) is not cross-project replication or application networking.

## Create

In **Create cluster → [MongoDB](https://www.mongodb.com/)**, choose **Sharded**, configure each component, and review the complete runtime/storage footprint. The **sharded** preset is described in the [overview](README.md).

CLI alternative:

```sh
karactl -n default clusters create analytics --provider psmdb \
  --topology sharded --shards 2 --members 3 --config-servers 3 --routers 2 \
  --version 8.0.32-14 --min-ram 1 --max-ram 1 \
  --min-disk 5 --max-disk 5 --tls-mode requireTLS --wait
karactl -n default clusters members analytics
```

Use `-f` with a complete API-shaped create request for distinct `engine`, `configServer` and `router` resource blocks; see [OpenAPI Components](../../api/openapi.yaml). Explicit zero, including `--shards 0`, is invalid. Engine driver defaults above differ from CLI convenience defaults, notably its two routers.

## Connect and shard collections

Applications connect to **mongos only**, not config/shard members. The password-free connection summary contains router hosts and omits `replicaSet`; follow the [application-user TLS recipe](access-and-tls.md). Private connections need access to the database project.

Karakoram registers shard infrastructure; it does not select shard keys, shard collections or distribute application data automatically. Use an authorized MongoDB client through mongos to administer database/collection sharding. Adding a shard does not prove existing collections are sharded or balanced.

## Add shards and scale components

```sh
karactl -n default clusters scale analytics --shards 3 --wait
karactl -n default clusters scale analytics --routers 3 --wait
karactl -n default clusters scale analytics --component configServer \
  --min-ram 2 --max-ram 2 --wait
karactl -n default clusters scale analytics --members 5 --wait
```

`--component` selects `engine` (default), `configServer` or `router`; `--config-servers` and `--routers` select their counts. Engine replicas/resources apply uniformly to every shard. New shard sets bootstrap and register before balancing. Existing membership changes use [initial-sync/quorum safeguards](replica-sets.md). Shard count cannot decrease; reducing members within a shard is different from removing it.

## Balancer

```sh
karactl -n default clusters balancer get analytics
karactl -n default clusters balancer set analytics --enabled=false
karactl -n default clusters balancer set analytics --enabled=true
```

Inspect balancer status separately from member health; manual disablement is not automatically an error. Binary upgrades save, stop and restore its prior state. Restores coordinate balancer/router holds; do not override those holds during an operation.

While restore holds routers stopped or they are restarting, balancer GET returns HTTP 503 `unavailable`; retry after router connectivity recovers. Database-user and database-role reads use the same availability contract. Authentication and non-transient command failures are not reported as temporary unavailability.

## Router readiness

Router readiness uses an authenticated `clusterMonitor` read of `config.shards`
through each mongos, with primary read preference and majority read concern.
The metadata command has a two-second deadline within the three-second probe
budget; the MongoDB driver derives the server-side time limit from that deadline.
One config-server member is supported but has no config-server redundancy;
majority is one in that topology. A running mongos or an open TCP port alone
does not satisfy readiness. `Router/<member>` / `ConfigMetadataUnavailable`
reports a failed probe. Native command failures include the bounded MongoDB
numeric code and code name in the condition and agent log. Client-side timeout,
cancellation, network, TLS certificate/handshake and authentication failures use
fixed diagnostic categories. Server-selection failures identify typed handshake
causes from the observed servers when available, rather than hiding them behind
the selection deadline. Raw native messages, connection URIs and credentials
are not exposed by this diagnostic.

## Backups, access and failure domains

[PBM](backups.md) runs on every config/shard mongod, never mongos; sharded PBM commands use `cfg`. Recovery is whole-cluster and disruptive, not a per-shard restore or topology conversion.

[Public exposure](access-and-tls.md#public-exposure-and-discovery) publishes mongos only. Multiple router containers share one service hostname, not independent per-container public discovery. Extra routers add process-level redundancy, not [host/rack separation](../limitations.md); their caches disappear on replacement. See [operations](operations.md) for component upgrades and resource changes.
