<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Sharding

A sharded Karakoram cluster contains:

- A config-server replica set, `cfg`, holding sharding metadata.
- One or more shard replica sets, `rs0`, `rs1`, and so on; each shard has the same engine component replica/resource specification.
- A stateless mongos router service, through which applications connect. Config servers and shard members are not application seed hosts.

Engine and config-server sets accept 1/3/5/7 members. The provider accepts 1–128 shards and 1–32 router containers. These are validation bounds, not a claim that a maximum-sized deployment fits your project or has been load-tested. Each mongod gets its own runtime/volume; the mongos service is horizontally scalable and has no durable database volume.

Select the project's [namespace](namespaces.md) in the UI or add `-n NAME` to
the commands below. Every config/shard/router service stays in that project;
cross-project management uses its relay, not cross-project database replication.
Private mongos hostnames in connection details require application access to
that namespace's project.

## Create

In the creation wizard choose **Sharded**, configure each component's size, and review the complete service/storage cost. For a small multi-member topology:

```sh
karactl clusters create analytics --topology sharded --shards 2 \
  --members 3 --config-servers 3 --routers 2 \
  --version 8.0.32-14 --min-ram 1 --max-ram 1 \
  --min-disk 5 --max-disk 5 --tls-mode requireTLS --wait
karactl clusters members analytics
```

CLI flags are convenient for common settings; `-f` accepts an API-shaped create request with distinct engine/configServer/router resource blocks. Consult [api/openapi.yaml](../../api/openapi.yaml) for `ClusterSpec` and `Components`. Config servers are initialized before shards/routers, and Karakoram registers each shard with mongos.

Topology type is immutable. You cannot turn an existing replica set into a sharded cluster by patching `topology.type`; create a new compatible topology and migrate application data using a suitable migration procedure. Clone restores also require matching source/target topology, not topology conversion.

## Connect and shard application collections

Read the cluster's password-free connection summary or explicitly [reveal connection details](users-tls-access.md#connection-details). For sharded clusters the returned application hosts are **mongos only**, and no replicaSet URI option is needed for routers. Follow the [complete application TLS recipe](users-tls-access.md#connect-an-application-user-with-tls): create the scoped user, export `ca.pem`, build the URI from `.connection.hosts` with `authSource=appdb`, and connect with mongosh or PyMongo while trusting the cluster CA. Set `CLUSTER=analytics` for the example above. Run private connections from an application service inside that namespace's Zerops project; the management relay and public control-plane origin do not provide private database DNS or application network access.

Karakoram provisions shard infrastructure and membership. Application database/collection sharding, shard-key selection and application data distribution are MongoDB administration responsibilities: use an authorized MongoDB client through mongos. Adding a shard is not proof that a collection is already sharded or balanced.

## Add shards and scale components

```sh
karactl clusters scale analytics --shards 3 --wait
karactl clusters scale analytics --routers 3 --wait
karactl clusters scale analytics --component configServer \
  --min-ram 2 --max-ram 2 --wait
karactl clusters scale analytics --members 5 --wait
```

`--members` changes the selected component's replica count (engine by default); engine replica settings apply to each shard. New shard sets follow bootstrap and registration before becoming available for balancing. Existing set membership changes retain the initial-sync/quorum safeguards described in [replica sets](replica-sets.md).

**Shard removal is not supported.** Decreasing shard count is rejected; scale-down of members inside a shard is different from removing that shard.

## Balancer

```sh
karactl clusters balancer get analytics
karactl clusters balancer set analytics --enabled=false
karactl clusters balancer set analytics --enabled=true
```

Inspect the balancer status separately from member health. Rolling binary upgrades preserve and temporarily stop the balancer's prior state; restores coordinate balancer and router shutdown to protect metadata consistency. If an operation has stopped it, do not override that hold midway through the operation. A manually disabled balancer is not automatically an error.

## Backups, access and failure domains

PBM agents run on config-server and shard mongod members, **not** routers. PBM control commands use the config-server replica set. [Restore](backups-and-restore.md) is a whole-cluster disruptive procedure: stop application writers; Karakoram coordinates routers/balancer, offline physical progress and post-restore user recovery. Compatible sharded snapshots restore into sharded targets.

Public exposure publishes client-facing mongos routes only. It does not publish every shard/config member. Multiple routers improve process-level access redundancy but do not establish host/rack separation; Zerops has no anti-affinity guarantee. Router configuration cannot survive replacement from a persistent cache, because routers have no data volume. See [limitations](../limitations.md), [public access](users-tls-access.md#public-exposure-and-discovery), and [scaling/upgrades](scaling-and-upgrades.md).
