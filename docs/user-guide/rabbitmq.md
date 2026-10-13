<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# RabbitMQ clusters

Karakoram manages RabbitMQ on your own Zerops runtimes; Zerops does not provide a managed RabbitMQ service. Select **RabbitMQ** in the creation wizard or use `spec.provider: rabbitmq`. PSMDB remains the default when no provider is selected. Read the [RabbitMQ limitations](../limitations.md#rabbitmq) before production use.

## Versions, topology and cost

- RabbitMQ **4.3.5** and **4.3.6** are cataloged; 4.3.6 is the default. Both install exact Team RabbitMQ apt packages on Ubuntu 24.04 with **Erlang 27.3.4.17** (`1:27.3.4.17-1`). The only upgrade edge is **4.3.5 → 4.3.6**; downgrades and other series are rejected.
- Topology is `cluster`, with only the `engine` component and **1, 3, 5 or 7** nodes (default 3). There are no shards, routers or configurable arbiters. `dev` is one broker; `prod` is three, not a physical-placement guarantee.
- Each node costs **one singleton runtime plus one Local Storage volume, at least 5 GB**. Runtime disk is separate from broker storage. Defaults per node are shared CPU, 1 core, 1 GB RAM and a 5 GB volume; validation permits RAM from 0.5 GB. Replacement temporarily adds another runtime/volume. Retained and orphaned volumes continue consuming storage and may incur charges.
- Nodes keep stable `rabbit@<service hostname>` identities and persistent data across redeployment/restart. Member indexes are monotonic and not reused. Separate services/volumes do not guarantee separate hosts ([ADR 0016](../adr/0016-placement-anti-affinity.md)).

RabbitMQ 4.3 uses **Khepri-only metadata**. Metadata replication does not replicate classic-queue messages. Karakoram defaults new queues to `quorum`, with initial queue replicas `min(3, planned nodes)`; clients can explicitly select another queue type. Five/seven brokers do not automatically give each queue five/seven replicas. Use durable quorum queues, publisher confirms, acknowledgements and reconnect-capable clients for replicated messaging. One broker has no HA.

## Create and configure

A creation file accepted by `karactl clusters create -f FILE` can be as small as:

```yaml
name: brokers
spec:
  provider: rabbitmq
  version: 4.3.6
  topology:
    type: cluster
  components:
    engine:
      replicas: 3
  tls:
    mode: requireTLS
    ca: selfSigned
  engineOptions:
    plugins: []
    managementUI:
      public: false
  engineConfig: |
    consumer_timeout = 1800000
```

Provider defaults fill omitted resources. Use `-n NAME` for another namespace; the same project-scoped backend and authenticated relay model applies. Observe cluster generation, member health and events rather than treating request acceptance as completion.

RabbitMQ access request objects reject undeclared fields with an `invalid_request` error naming the field. Policy `definition` remains an open RabbitMQ-defined object. Collection responses always use arrays, including empty collections and user or virtual-host tags.

### Engine options and overrides

`engineOptions` must be one JSON object (or its YAML equivalent). Unknown fields, `null`, unsupported plugins and duplicate plugins are rejected. `rabbitmq_management` and `rabbitmq_prometheus` are always enabled; do not add them to the optional list. Allowed extras are:

- `rabbitmq_shovel`, `rabbitmq_shovel_management`
- `rabbitmq_federation`, `rabbitmq_federation_management`
- `rabbitmq_stream`, `rabbitmq_stream_management`

Each optional management extension requires its corresponding base plugin in the list: `rabbitmq_shovel_management` requires `rabbitmq_shovel`, `rabbitmq_federation_management` requires `rabbitmq_federation`, and `rabbitmq_stream_management` requires `rabbitmq_stream`.

`managementUI.public` enables a public HTTPS management subdomain for each broker, independently of public AMQP exposure. Enabling a plugin is not an application topology, message migration or native stream-protocol public routing workflow.

`engineConfig` is additional `rabbitmq.conf` **`key = value`** lines; blank lines and `#` comments are accepted. It is not YAML, `advanced.config` or arbitrary environment/command input. These keys/prefixes are managed and rejected:

| Managed area | Keys / prefixes |
| --- | --- |
| Listeners and TLS | `listeners.`, `ssl_options.`, `management.tcp.`, `management.ssl.`, `prometheus.`, `distribution.` |
| Discovery and queue defaults | `cluster_formation.`, `quorum_queue.initial_cluster_size`, `default_queue_type` |
| Resource limits and logs | `vm_memory_high_watermark*`, `total_memory_available_override_value*`, `disk_free_limit*`, `log.` |
| Credentials and authentication | `default_user*`, `default_pass*`, `loopback_users*`, `auth_backends*`, `ssl_cert_login*` |
| Definitions loading | `management.load_definitions*`, `load_definitions*`, `definitions.` |

The corresponding bare keys are also protected. Other syntactically valid settings are still subject to RabbitMQ's own validation; a setting accepted by Karakoram can fail broker startup. Configuration, plugin, certificate and relevant resource changes roll through nodes rather than restarting every broker together.

## TLS and connectivity

| Mode | AMQP clients | Private Management API | Erlang distribution |
| --- | --- | --- | --- |
| `disabled` | Plaintext 5672 | HTTP 15672 | IPv6 TCP |
| `preferTLS` | Plaintext 5672 and TLS 5671 | HTTPS 15671; HTTP 15672 also enabled | IPv6 TCP |
| `requireTLS` (default) | TLS 5671 only | HTTPS 15671 | IPv6 TLS with peer certificate verification |

The Erlang transport is **fixed at creation**: `disabled ↔ preferTLS` can roll in place, but crossing the `requireTLS` boundary is rejected. Create a new cluster and restore definitions instead; move messages separately. `requireTLS` does not require AMQP client certificates: applications authenticate with passwords and should verify the broker CA/hostname. CA source (`selfSigned` or `userProvided`) is immutable. Supplied PKI requires a signing CA certificate/key and valid leaf coverage; see [user-provided PKI](users-tls-access.md#user-provided-pki).

Connection status exposes password-free per-node `amqp`/`amqps` and `management` endpoints. Explicit credential reveal through the UI or `karactl clusters connection brokers --reveal` requires `cluster-credentials:read` and returns the customer `admin` password and CA when applicable. Use separate least-privilege application users, not `admin`, in workloads. Configure clients with multiple endpoints and connection recovery; Karakoram does not provide a shared AMQP load balancer.

### Public access

`spec.exposure.public` requires a nonempty CIDR allowlist and project IPv6 or dedicated IPv4. Each broker gets a distinct project TCP port targeting **5671** for TLS modes, **5672** when disabled. Use the returned endpoints, not an assumed external port. Verify allowed and denied source access and CA/SAN coverage from an external client.

`engineOptions.managementUI.public: true` exposes the native RabbitMQ UI **and HTTP API**, authenticated by RabbitMQ accounts, not Karakoram sessions/RBAC. Zerops terminates public HTTPS and forwards to private HTTP 15672. Even `requireTLS` enables that HTTP listener for this option. The AMQP CIDR allowlist does **not** protect the management subdomain. Keep management private unless this additional surface and project-private HTTP hop are acceptable; never publish cookie/distribution, Prometheus or node-control ports.

## Access API, CLI and UI

The cluster's **Access** tab manages virtual hosts, users, vhost permissions, topic permissions, policies and operator policies. Equivalent REST collections are under `/api/v1/clusters/{cluster}/rabbitmq/`, with `?namespace=NAME`; URL-encode names such as `/` in path segments. These are direct Management API changes, not durable declarative topology resources continuously reconciled by Karakoram.

```sh
karactl rabbitmq vhosts list brokers -o json
karactl rabbitmq vhosts create brokers -f vhost.yaml
karactl rabbitmq users create brokers -f user.yaml -o json
karactl rabbitmq permissions create brokers -f permission.yaml
karactl rabbitmq policies list brokers
```

All six command groups have `list/get/create/update/delete`. Reads and writes require the corresponding `database-users` permission on `<namespace>/<cluster>`. See the [CLI access reference](cli.md#rabbitmq-access) for payloads and argument order. Generic MongoDB database-users/roles endpoints are not RabbitMQ access endpoints.

User tags govern management access; vhost/topic permissions govern messaging access. Password and passwordHash are mutually exclusive. Creating a user without either generates a password returned **once**; subsequent reads expose neither passwords nor hashes. Updating without either preserves the password. `karakoram`, `admin`, `monitor` and `guest` are reserved: access mutations cannot change their users or grants. Bootstrap creates `karakoram` (control-plane administration) and `admin` (customer administrator), with administrator tags and all permissions on `/`, and removes `guest`; `monitor` is reserved but not created by bootstrap. System rotation supports `karakoram` and `admin`.

Deleting a vhost deletes its queues and messages. Topic-permission deletion removes **all exchanges' grants for the vhost/user pair**, not just one displayed row. Review policy effects before applying them; changes take effect directly in RabbitMQ.

## Lifecycle and maintenance

1. Import each runtime/volume, deploy pinned packages and configuration, and wait for the broker.
2. Bootstrap accounts on the first node. `classic_config` lists planned peers; if discovery has not already joined a node, Karakoram explicitly stops its RabbitMQ application, joins a seed and starts it. Ready requires every node to see the planned members running, without alarms, partitions or maintenance.
3. Scale up by provisioning/joining new nodes and rebalancing queue/stream leaders. Adding nodes alone does not grow all existing queue replica groups.
4. Scale down highest indexes first: require healthy survivors, grow quorum replicas on every survivor, migrate stream queues independently of the optional stream-protocol plugin, and shrink the leaving node. Drain and stop its RabbitMQ application while keeping its Erlang VM voting in Khepri; forget it through a survivor before full process shutdown and runtime/volume deletion.

Durable classic queues hosted on the leaving member block automatic scale-down **and replacement** with `MemberRemovalBlocked` and the affected queue names. Explicitly migrate or delete those queues before retrying; Karakoram does not migrate classic message stores automatically.

Rolling changes check online quorum and whether the node is quorum-critical (except a singleton), drain client traffic, persist a maintenance hold and stop the broker **before** platform deployment/restart. Zerops force-stops containers roughly five seconds after SIGTERM; platform shutdown alone is not the broker shutdown procedure. A changed node must acknowledge configuration, run, observe its peers and revive before the next node changes. After all upgraded nodes return, Karakoram enables feature flags and rebalances leaders. Drain closes connections and transfers responsibilities; it does not consume, purge or back up messages.

RAM updates can be live at the platform, but the broker memory limit/watermark applies only after its rolling restart. Managed total memory is desired minimum RAM; the watermark is **60%** of it. Disk-free alarm limit is **max(0.5 GB, minimum RAM in GB)**. Disk growth is supported, shrinking is rejected. CPU-mode failures retain their exact action and retry deadline instead of hot-looping.

Replacement provisions and joins a fresh identity before growing/migrating replicas and removing the old member. A reachable broker follows the drain/application-stop/forget/full-stop sequence; an unreachable lost broker skips local drain/stop and is forgotten through a survivor only with a surviving Khepri voter majority. Surviving queue/stream majorities are also needed: replacement cannot recover classic messages or messages whose last surviving copy is lost. Preserve data/state and consult the [operations notes](../operations.md#rabbitmq-operations) after permanent node/volume loss or majority loss; never reset survivors to make a join succeed.

## Definitions backups and cleanup

RabbitMQ supports only the **`definitions`** backup type. Manual and UTC scheduled backups export definitions through the Management API, gzip them and upload to registered S3-compatible storage. Successful scheduled-backup count retention and explicit backup deletion remove recorded objects. No PBM is deployed for RabbitMQ.

**Definitions contain no messages and provide no PITR.** They contain topology, application users/password hashes, grants, policies and runtime parameters; treat the object storage and exports as sensitive. Before compression/upload, Karakoram excludes `karakoram`, `admin`, `monitor` and `guest` users and their vhost/topic grants. Restore imports **merge** objects into the target, not rewind it or remove unrelated existing objects. The same reserved-user/grant filter applies on import, including older backups, so target credentials remain authoritative. Imports are bounded to 64 MiB of uncompressed definitions. A creation-time `dataSource` clone needs a successful RabbitMQ definitions backup and imports after the target is ready; it does not replicate source messages continuously.

Deletion drains/stops brokers and removes public routes/subdomains before runtime removal. `keepVolumes` suspends the cluster, retaining broker volumes and secrets for resume. `keepBackups` independently retains definitions records/objects for later recovery/deletion. Full deletion removes owned volumes/secrets; volumes the platform refuses to delete are ownership-checked and tracked for orphan cleanup. Retained backups still require the control-plane state, original master key and storage credentials. Preserve these separately from message durability.

Resume starts every retained broker before waiting for individual readiness/revive, so the retained Khepri majority can recover together; it is not a one-node-at-a-time fresh bootstrap.

## Monitoring

The node scrapes RabbitMQ's loopback-only Prometheus endpoint on 15692 and exports a fixed `karakoram_rabbitmq_*` vocabulary: availability/uptime, connections/channels/consumers/queues, ready/unacknowledged messages, publish/delivery counters, resident memory/limit, disk free/limit, memory/disk alarms and running distribution peers. The UI presents these plus host CPU/RAM/disk panels; JSON rates derive from counters. Default collection is every 30 seconds with 24 hours of history.

`up` means the local Prometheus scrape succeeded, not that messaging or replication is healthy. Running-member metrics count established configured-peer links, not independent failure domains. Inspect `Healthy`, `Alarms`, `Partition` and `Placement` conditions and external messaging checks. See [verification](../verification.md#rabbitmq-checks), [security](../security.md#rabbitmq-security) and [ADRs 0020–0022](../adr/README.md).
