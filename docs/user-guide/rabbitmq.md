<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# RabbitMQ clusters

Karakoram manages RabbitMQ on your own Zerops runtimes; Zerops does not provide a managed RabbitMQ service. Select **RabbitMQ** in the creation wizard or use `spec.provider: rabbitmq`. PSMDB remains the default when no provider is selected. Read the [RabbitMQ limitations](../limitations.md#rabbitmq) before production use.

## Versions, topology and cost

- RabbitMQ **4.3.6** is the default and recommended new-cluster version. Deprecated **4.3.5** exists only as an upgrade source and is not selectable in the creation wizard; explicit API provisioning remains accepted with a deprecation warning in the response and event, naming stream authorization disclosures [GHSA-6jvc-jx8w-m757](https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-6jvc-jx8w-m757) and [GHSA-342v-cj47-c9g8](https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-342v-cj47-c9g8), fixed in 4.3.6. Both install exact Team RabbitMQ apt packages on Ubuntu 24.04 with **Erlang 27.3.4.17** (`1:27.3.4.17-1`), pinned once in the compatible broker catalog. The only upgrade edge is **4.3.5 → 4.3.6**; downgrades and other series are rejected.
- Topology is `cluster`, with only the `engine` component and **1, 3, 5 or 7** nodes (default 3). There are no shards, routers or configurable arbiters. `dev` is one broker; `prod` is three, not a physical-placement guarantee.
- Each node costs **one singleton runtime plus one Local Storage volume, at least 5 GB**. Runtime disk is separate from broker storage. Defaults per node are shared CPU, 1 core, 1 GB RAM and a 5 GB volume; validation permits RAM from 0.5 GB. Replacement temporarily adds another runtime/volume. Retained and orphaned volumes continue consuming storage and may incur charges.
- Nodes keep stable `rabbit@<service hostname>` identities and persistent data across redeployment/restart. Member indexes are monotonic and not reused. Separate services/volumes do not guarantee separate hosts ([ADR 0016](../adr/0016-placement-anti-affinity.md)).

RabbitMQ 4.3 uses **Khepri-only metadata**. Metadata replication does not replicate classic-queue messages. Karakoram defaults new queues to `quorum`, with a stable initial replica target of **3**; RabbitMQ caps it to the available cluster size when each queue is declared. This default does not depend on the seed broker's initial discovery list. Clients can explicitly select another queue type. Five/seven brokers do not automatically give each queue five/seven replicas. Use durable quorum queues, publisher confirms, acknowledgements and reconnect-capable clients for replicated messaging. One broker has no HA.

## Create and configure

Save this creation file as `rabbit.yaml`:

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

Provider defaults fill omitted resources. Replace `NS` below with the selected namespace; its existing Zerops project, project-scoped backend and authenticated relay provide the broker network.

```sh
karactl -n NS clusters create -f rabbit.yaml --wait --timeout 30m
karactl -n NS clusters get brokers -o json
karactl -n NS clusters status brokers -o json
karactl -n NS clusters members brokers -o json
karactl -n NS events list --cluster brokers --order desc
```

Request acceptance is not completion. Success means the cluster is **Ready**, `status.observedGeneration` is at least `generation`, all three expected brokers are present, running and healthy, and no broker reports alarms, partitions or maintenance. Each broker must see the same expected running membership. Inspect status conditions, member details and events together rather than relying on a successful create response.

A timeout or interruption does not cancel provisioning. Resume observation in the same context and namespace; **do not re-create the cluster**:

```sh
karactl -n NS clusters wait brokers --timeout 30m
karactl -n NS events list --cluster brokers --order desc
```

RabbitMQ access request objects reject undeclared fields with an `invalid_request` error naming the field. Policy `definition` remains an open RabbitMQ-defined object. Collection responses always use arrays, including empty collections and user or virtual-host tags.

### Engine options and overrides

Discover the running server's provider catalog with `karactl providers get rabbitmq -o json` (`GET /api/v1/providers/rabbitmq`). Its `engineOptions` metadata describes optional plugin groups/dependencies, always-enabled plugins and public-management TLS restrictions; `engineConfig` metadata describes the `rabbitmq.conf` format, syntax and managed-key matching. Use it alongside the detailed constraints below.

`engineOptions` must be one JSON object (or its YAML equivalent). Unknown fields, `null`, unsupported plugins and duplicate plugins are rejected. `rabbitmq_management` and `rabbitmq_prometheus` are always enabled; do not add them to the optional list. Allowed extras are:

- `rabbitmq_shovel`, `rabbitmq_shovel_management`
- `rabbitmq_federation`, `rabbitmq_federation_management`
- `rabbitmq_stream`, `rabbitmq_stream_management`

Each optional management extension requires its corresponding base plugin in the list: `rabbitmq_shovel_management` requires `rabbitmq_shovel`, `rabbitmq_federation_management` requires `rabbitmq_federation`, and `rabbitmq_stream_management` requires `rabbitmq_stream`.

`managementUI.public` enables a public HTTPS management subdomain for each broker only in `preferTLS`/`disabled`, independently of public AMQP exposure; `requireTLS` rejects it because the infrastructure forwards plaintext HTTP to the broker. Enabling a plugin is not an application topology, message migration or native stream-protocol public routing workflow.

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

Until patched OTP 27.3.4.18 is packaged in the configured Team RabbitMQ noble repositories, generated Erlang distribution and CLI TLS clients plus Osiris stream replication client/server are restricted to **TLS 1.2** to mitigate [CVE-2026-89422 / GHSA-rgxr-4g4w-j875](https://github.com/erlang/otp/security/advisories/GHSA-rgxr-4g4w-j875) in OTP 27.3.4.17. Peer verification remains enabled; public AMQPS, stream and management TLS server versions are unchanged. User-supplied Shovel/Federation runtime parameters need their own safe outbound TLS settings.

Connection status exposes password-free per-broker `amqp`/`amqps` and `management` endpoints. Explicit credential reveal through the UI or `karactl -n NS clusters connection brokers --reveal -o json` requires `cluster-credentials:read`. Each `credentials` entry contains `username: admin`, its `password`, `database: "/"` (**virtual host**, not a MongoDB database), and a credential-bearing `uri`. There is one URI per broker endpoint, not a shared cluster URI: the URI's hostname identifies that broker; `preferTLS` can return both plaintext and TLS entries for a broker. The response's `caCertificate` is the PEM CA for TLS; [export it separately](cli.md#requests-output-and-waits) with `karactl -n NS clusters ca brokers > ca.pem`. Use separate least-privilege application users, not `admin`, in workloads. Karakoram does not provide a shared AMQP load balancer.

### Connect an application over TLS

Install a [RabbitMQ-capable CLI](cli.md#build-and-install) and log in first. The following smoke test uses Python 3, `jq` and [Pika](https://pika.readthedocs.io/). Run it **inside the selected namespace's Zerops project**: private broker service hostnames are not reachable from an unrelated project or your laptop. Use a TLS-enabled cluster (`requireTLS` or `preferTLS`).

From a private directory, export the CA and select **one broker's AMQPS credential**, not a comma-joined list of URIs:

```sh
umask 077
karactl -n NS clusters ca brokers > ca.pem
karactl -n NS clusters connection brokers --reveal -o json |
  jq -e '[.credentials[] | select(.uri | startswith("amqps://"))][0] // error("no AMQPS broker")' \
  > connection.json
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install pika
```

`connection.json` contains the administrator password: keep it out of logs, source control and shared directories. Selection `[0]` chooses the first broker; choose another array index to test another broker. The private URI uses **5671** and includes the encoded `/` vhost. No client certificate or private key is needed: the CA authenticates the server, and the username/password authenticate the application.

Save this as `tls_smoke.py`. It publishes and acknowledges one persistent message on a dedicated durable quorum queue; use an otherwise unused smoke-test queue, not a live application queue:

```python
import json
import ssl
from urllib.parse import urlsplit

import pika

with open("connection.json", encoding="utf-8") as source:
    credential = json.load(source)
uri = credential["uri"]
endpoint = urlsplit(uri)
if endpoint.scheme != "amqps" or not endpoint.hostname:
    raise ValueError("Select one broker's amqps URI")

context = ssl.create_default_context(cafile="ca.pem")
context.check_hostname = True
context.verify_mode = ssl.CERT_REQUIRED
parameters = pika.URLParameters(uri)
parameters.credentials = pika.PlainCredentials(
    credential["username"], credential["password"]
)
parameters.virtual_host = credential["database"]
parameters.ssl_options = pika.SSLOptions(context, endpoint.hostname)
parameters.heartbeat = 30
parameters.blocked_connection_timeout = 30
parameters.socket_timeout = 10

connection = pika.BlockingConnection(parameters)
try:
    channel = connection.channel()
    queue = "tls-smoke"
    channel.queue_declare(
        queue=queue, durable=True, arguments={"x-queue-type": "quorum"}
    )
    channel.confirm_delivery()
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=b"hello over AMQPS",
        properties=pika.BasicProperties(delivery_mode=2),
        mandatory=True,
    )
    method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
    if method is None:
        raise RuntimeError("No message received from the smoke-test queue")
    channel.basic_ack(delivery_tag=method.delivery_tag)
    print(body.decode("utf-8"))
finally:
    connection.close()
```

Run `python3 tls_smoke.py`; successful output is `hello over AMQPS`. This leaves the empty queue in place. Remove the private credential file after the test with `rm connection.json`. For an application, obtain its scoped username/password and virtual host from your secret manager instead of revealing/distributing `admin` credentials, and use its permitted queue; see the [least-privilege example](#least-privilege-application-access).

This is a single-connection smoke test, not a reconnect loop. Production clients should retain the returned per-broker endpoints and reconnect to another broker with bounded backoff after a connection failure or rolling drain. Pika's blocking connection does not automatically recover an established connection: recreate connections/channels, declarations and consumers, and enable confirms again. Retry unconfirmed publishes with application-level deduplication because their delivery may be uncertain; acknowledge deliveries only after processing. Keep CA and hostname verification enabled on every connection. Public clients must use the returned public host/port (which can differ from 5671), its certificate-covered hostname and an allowed source address; private service URIs are not public endpoints.

### Public access

`spec.exposure.public` requires a nonempty CIDR allowlist and project IPv6 or dedicated IPv4. Each broker gets a distinct project TCP port targeting **5671** for TLS modes, **5672** when disabled. Use the returned endpoints, not an assumed external port. Verify allowed and denied source access and CA/SAN coverage from an external client.

`engineOptions.managementUI.public: true` exposes the native RabbitMQ UI **and HTTP API**, authenticated by RabbitMQ accounts, not Karakoram sessions/RBAC. It is accepted only with `preferTLS`/`disabled`: Zerops terminates public HTTPS and forwards to private HTTP 15672. `requireTLS` rejects the option and keeps management HTTPS 15671 private, because certificate-verified HTTPS backend routing is unavailable; a future TCP passthrough route would need separate routing/state and certificate/trust support. The AMQP CIDR allowlist does **not** protect the management subdomain. Keep management private unless this additional surface and project-private HTTP hop are acceptable; never publish cookie/distribution, Prometheus or node-control ports.

## Access API, CLI and UI

The cluster's **Access** tab manages virtual hosts, users, vhost permissions, topic permissions, policies and operator policies. Equivalent REST collections are under `/api/v1/clusters/{cluster}/rabbitmq/`, with `?namespace=NAME`. In raw HTTP API paths, encode each name **once**: vhost `/` is `%2F`, for example `/api/v1/clusters/brokers/rabbitmq/vhosts/%2F?namespace=NS`. Do not double-encode it as `%252F`. CLI arguments and UI inputs take the literal `/`; typed clients likewise take the raw name and perform transport encoding. These are direct Management API changes, not durable declarative topology resources continuously reconciled by Karakoram.

```sh
karactl rabbitmq vhosts list brokers -o json
karactl rabbitmq vhosts create brokers -f vhost.yaml
karactl rabbitmq users create brokers -f user.yaml -o json
karactl rabbitmq permissions create brokers -f permission.yaml
karactl rabbitmq policies list brokers
```

All six command groups have `list/get/create/update/delete`. Reads and writes require the corresponding `database-users` permission on `<namespace>/<cluster>`. See the [CLI build/install instructions](cli.md#build-and-install) and [access reference](cli.md#rabbitmq-access) for argument order. The `rabbitmq` group is available in binaries built from a release containing RabbitMQ support; an older CLI will not gain commands by connecting to a newer server. Generic MongoDB database-users/roles endpoints are not RabbitMQ access endpoints.

User tags govern management access; they do **not** grant messaging permission. Vhost/topic permission grants govern messaging access. Password and passwordHash are mutually exclusive. Creating a user without either generates a password returned **once**; subsequent reads expose neither passwords nor hashes. Updating without either preserves the password. `karakoram`, `admin`, `monitor` and `guest` are reserved: access routes cannot create, modify or delete these identities or mutate their permission/topic-permission grants. Bootstrap creates `karakoram` (control-plane administration) and `admin` (customer administrator), with administrator tags and all permissions on `/`, and removes `guest`; `monitor` is reserved but not created by bootstrap. Use `karactl -n NS system-users rotate brokers admin` (or `karakoram`) for supported system-user rotation, not an access-user update.

Malformed permission regexes and rejected policy-definition keys return field errors. Check the named field (`configure`, `write`, `read` or `definition`) and RabbitMQ's accepted PCRE/policy definition rules; a syntactically valid regex can still grant more access than intended. A permission or topic-permission write referencing a missing user or vhost instead names `user,vhost` with reason `not_found`; create or correct that identity before retrying the grant.

Vhost create/update/delete audit events use resource `rabbitmq-vhosts` and name the affected vhost in the target (`<cluster>/<vhost>`), including `/`. Filter them with:

```sh
karactl -n NS events list --cluster brokers --resource rabbitmq-vhosts
```

Deleting a vhost deletes its queues and messages. Topic-permission deletion removes **all exchanges' grants for the vhost/user pair**, not just one displayed row. Review policy effects before applying them; changes take effect directly in RabbitMQ.

### Least-privilege application access

Save these three complete YAML payloads separately. This creates a dedicated `orders` vhost and a user that can configure/read/write only the `orders` queue and publish through the default exchange. The user has **no management tags**.

`vhost.yaml`:

```yaml
name: orders
tags: []
defaultQueueType: quorum
description: Orders application
limits:
  maxConnections: 20
  maxQueues: 1
```

`user.yaml` (omit password fields to generate a password once):

```yaml
name: orders-app
tags: []
```

`permission.yaml`:

```yaml
vhost: orders
user: orders-app
configure: '^orders$'
write: '^(orders|amq\.default)$'
read: '^orders$'
```

Apply the vhost, user and grant in that order:

```sh
umask 077
karactl -n NS rabbitmq vhosts create brokers -f vhost.yaml
karactl -n NS rabbitmq users create brokers -f user.yaml -o json > created-user.json
karactl -n NS rabbitmq permissions create brokers -f permission.yaml
jq -er '.generatedPassword' created-user.json
karactl -n NS rabbitmq permissions get brokers orders orders-app
```

The `jq` command deliberately reveals the new password once; run it only in a private terminal, store the result in your application's secret manager and remove `created-user.json` afterward. Use virtual host `orders` and username `orders-app` in the application. Setting `tags: []` removes management UI/API access, but neither empty nor administrator tags substitute for a messaging permission grant.

The anchored expressions match exact names, not every queue beginning with `orders`. For `basic_publish(exchange="", routing_key="orders", ...)`, RabbitMQ checks write access to the default exchange as **`amq.default`**; `write: '^orders$'` alone is insufficient. The example explicitly allows both `orders` and `amq.default`, while configure/read remain limited to `orders`. It does not grant access to arbitrary named exchanges, other queues or vhosts. With the TLS recipe, use these scoped credentials, set the virtual host to `orders` and change the queue name from `tls-smoke` to `orders`.

## Lifecycle and maintenance

1. Import each runtime/volume, deploy pinned packages and configuration, and wait for the broker.
2. Bootstrap accounts on the first node. `classic_config` lists planned peers; if discovery has not already joined a node, Karakoram explicitly stops its RabbitMQ application, joins a seed and starts it. Ready requires every node to see the planned members running, without alarms, partitions or maintenance.
3. Scale up by provisioning/joining new nodes and rebalancing queue/stream leaders. Adding nodes alone does not grow all existing queue replica groups.
4. Scale down highest indexes first: require healthy survivors, grow quorum replicas on every survivor, migrate stream queues independently of the optional stream-protocol plugin, and shrink the leaving node. Drain and stop its RabbitMQ application while keeping its Erlang VM voting in Khepri; forget it through a survivor before full process shutdown and runtime/volume deletion.

Durable classic queues hosted on the leaving member block automatic scale-down **and replacement** with `MemberRemovalBlocked` and the affected queue names. Explicitly migrate or delete those queues before retrying; Karakoram does not migrate classic message stores automatically.

Rolling changes check online quorum and whether the node is quorum-critical (except a singleton), drain client traffic, persist a maintenance hold and stop the broker **before** platform deployment/restart. Zerops force-stops containers roughly five seconds after SIGTERM; platform shutdown alone is not the broker shutdown procedure. A changed node must acknowledge configuration, run, observe its peers and revive before the next node changes. After all upgraded nodes return, Karakoram enables feature flags and rebalances leaders. Drain closes connections and transfers responsibilities; it does not consume, purge or back up messages.

RAM updates can be live at the platform, but the broker memory limit/watermark applies only after its rolling restart. Managed total memory is desired minimum RAM; the watermark is **60%** of it. Disk-free alarm limit is **max(0.5 GB, minimum RAM in GB)**. Disk growth is supported, shrinking is rejected. CPU-mode failures retain their exact action and retry deadline instead of hot-looping.

Replacement provisions and joins a fresh identity before growing/migrating replicas and removing the old member. A reachable broker follows the drain/application-stop/forget/full-stop sequence; an unreachable lost broker skips local drain/stop and is forgotten through a survivor only with a surviving Khepri voter majority. Surviving queue/stream majorities are also needed: replacement cannot recover classic messages or messages whose last surviving copy is lost. Preserve data/state and consult the [operations notes](../operations.md#rabbitmq-operations) after permanent node/volume loss or majority loss; never reset survivors to make a join succeed.

### Quorum-blocked restarts and replica growth

Adding brokers does **not** enlarge existing quorum queues. A queue originally
created on one broker can still have just one replica after scale-up. Restarting
that broker would make the queue unavailable, even when the other brokers are
healthy. Karakoram first waits for online quorum so recovering replicas can catch
up, then checks quorum-critical resources before draining the broker. A failed
wait also diagnoses the remaining blockers. `QuorumCriticalQueues` names up to ten
affected queues/vhosts and their known members (plus the remaining count); the
warning event is `RollingRestartBlocked`. Membership-inventory changes alone do
not repeat that warning. The operation and cluster message name the same blockers.
When quorum recovers, the condition clears and a Normal `RollingRestartRecovered`
event records that the rolling change can continue.
Blocker identities are validated before storage; the cluster retains at most fifty
blocker details while messages preserve the full remaining count.
Stream or metadata-component blockers require their own majority recovery, not
quorum queue additions.
If the native quorum check cannot run, the error reports that failure and its
command exit status rather than inventing queue blockers or exposing CLI output.

Open **cluster detail → Queue recovery** from the blocked-queue callout. It shows
targeted, copyable Management API commands with the actual broker, vhost, queue and
candidate node names. This is an explicit native administrative procedure, **not**
a Karakoram `grow-replicas` operation or CLI command: it requires the revealed
RabbitMQ `admin` credential and bypasses Karakoram RBAC. It does not require a
broker shell, Erlang cookie, deleting/recreating a queue or purging messages.

Run from an application/administration environment **inside the cluster's project**
so private broker DNS and management ports are reachable. Export the CA with
`karactl -n NS clusters ca CLUSTER > ca.pem` and reveal the admin password from
Connection details. `--user admin` prompts for the password; do not put it in
shell history. For example, for queue `orders` in `/`, adding `rabbit@ordersm1`:

```sh
# Inspect type, members and online replicas first.
curl --fail-with-body --user admin --cacert ca.pem \
  'https://ordersm0:15671/api/queues/%2F/orders'
# Only when an existing majority is online and the target is a healthy joined broker:
curl --fail-with-body --user admin --cacert ca.pem \
  -X POST -H 'Content-Type: application/json' \
  --data '{"node":"rabbit@ordersm1"}' \
  'https://ordersm0:15671/api/queues/quorum/%2F/orders/replicas/add'
# Observe Raft state before adding the next missing replica.
curl --fail-with-body --user admin --cacert ca.pem \
  'https://ordersm0:15671/api/queues/quorum/%2F/orders/status'
```

Use **your** values from Queue recovery, not these example hostnames. Encode each
vhost/queue path segment once (`/` becomes `%2F`). With explicitly disabled TLS,
use private `http://BROKER:15672` without `--cacert`; never disable TLS verification
for an HTTPS listener. Do not expose management publicly just for this recovery.
The endpoint/body and status endpoint are supported by the pinned RabbitMQ
[4.3.6 Management API](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.6/deps/rabbitmq_management/src/rabbit_mgmt_dispatcher.erl).

Add **one** missing member at a time, normally growing a one-replica group to three
across healthy brokers. After each addition, inspect the queue and Raft status:
the new member must be online, promoted to voting membership and caught up to the
leader's committed log (allowing for active writes). An HTTP success or a changed
`members` list alone is not proof of synchronization. Re-read membership before
retrying a timed-out request; it may already have added the replica. If the queue
already uses all brokers, recover its offline members instead of adding duplicates.
Do not add replicas without a live majority, force membership or restart several
brokers. Once safe quorum is restored, the existing rolling workflow retries its
checkpoint; observe it rather than submitting duplicate resource edits/restarts.

During creation, member state identifies `PLANNED` (platform import pending),
`IMPORTED` (package deployment pending), `DEPLOYED` (agent/broker readiness pending)
and `AGENT_READY` (bootstrap or join pending). `RUNNING` means the joined broker
has been observed; cluster readiness still requires every expected broker and
healthy membership. A blocked departing broker remains in member inventory as
`LEAVING_BLOCKED`, is not counted healthy, and emits one `MemberRemovalBlocked`
warning when its blocking reason changes. Resolve the reported cause; the broker
remains visible as `LEAVING` until its removal completes.


## Definitions backups and cleanup

RabbitMQ supports only the **`definitions`** backup type. Manual and UTC scheduled backups export definitions through the Management API, gzip them and upload to registered S3-compatible storage. Successful scheduled-backup count retention and explicit backup deletion remove recorded objects. No PBM is deployed for RabbitMQ.

**Definitions contain no messages and provide no PITR.** They contain topology, application users/password hashes, grants, policies and runtime parameters; treat the object storage and exports as sensitive. Before compression/upload, Karakoram excludes `karakoram`, `admin`, `monitor` and `guest` users and their vhost/topic grants. Restore imports **merge** objects into the target, not rewind it or remove unrelated existing objects. The same reserved-user/grant filter applies on import, including older backups, so target credentials remain authoritative. Imports are bounded to 64 MiB of uncompressed definitions. A creation-time `dataSource` clone needs a successful RabbitMQ definitions backup and imports after the target is ready; it does not replicate source messages continuously.
The UI repeats the password-hash sensitivity warning when choosing storage for a
schedule or a manual backup. Restrict bucket access and protect stored exports
even though they do not contain message payloads.

Deletion drains/stops brokers and removes public routes/subdomains before runtime removal. `keepVolumes` suspends the cluster, retaining broker volumes and secrets for resume. `keepBackups` independently retains definitions records/objects for later recovery/deletion. Full deletion removes owned volumes/secrets; volumes the platform refuses to delete are ownership-checked and tracked for orphan cleanup. Retained backups still require the control-plane state, original master key and storage credentials. Preserve these separately from message durability.

Resume starts every retained broker before waiting for individual readiness/revive, so the retained Khepri majority can recover together; it is not a one-node-at-a-time fresh bootstrap.

## Monitoring

The node scrapes RabbitMQ's loopback-only Prometheus endpoint on 15692 and exports a fixed `karakoram_rabbitmq_*` vocabulary: availability/uptime, connections/channels/consumers/queues, ready/unacknowledged messages, publish/delivery counters, resident memory/limit, disk free/limit, memory/disk alarms and running distribution peers. The UI presents these plus host CPU/RAM/disk panels; JSON rates derive from counters. Default collection is every 30 seconds with 24 hours of history.

`up` means the local Prometheus scrape succeeded, not that messaging or replication is healthy. Running-member metrics count established configured-peer links, not independent failure domains. Inspect `Healthy`, `Alarms`, `Partition` and `Placement` conditions and external messaging checks. See [verification](../verification.md#rabbitmq-checks), [security](../security.md#rabbitmq-security) and [ADRs 0020–0022](../adr/README.md).
