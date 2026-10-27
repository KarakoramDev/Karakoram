<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# RabbitMQ

Karakoram operates [RabbitMQ](https://www.rabbitmq.com/) on your [Zerops](https://zerops.io/) runtimes and Local Storage, not a native Zerops managed service.

Opt-in [cluster data MCP](../data-mcp.md) provides native Management API tools with `scope.vhosts` and separate data identities, without a companion. Every tool needs a permitted vhost. Message peeking consumes/requeues and is a write; queue purge is destructive and always requires confirmation.

## At a glance

| Setting | Value |
| --- | --- |
| Broker / Erlang | RabbitMQ **4.3.6** default; Erlang **27.3.4.17**, package `1:27.3.4.17-1` |
| Topology | `cluster`, component `engine`, **1/3/5/7** brokers; default 3; `dev` = 1, `prod` = 3 |
| New queues | `quorum`; initial replica target 3, capped by available membership at declaration |
| Per-broker defaults / minima | Shared CPU, 1 core, 1 GB RAM, 5 GB data volume / 1 whole core, 0.5 GB RAM, 5 GB data volume |
| TLS | `requireTLS`, `selfSigned` CA; [modes and supplied PKI](access-and-tls.md) |
| Recovery | [Definitions only](backups.md): no messages, physical backups or PITR |
| Public access | Opt-in AMQPS with CIDR allowlist; separate opt-in management UI/API without that restriction; [exposure rules](access-and-tls.md#public-access) |

Deprecated **4.3.5** is hidden in the creation wizard but accepted explicitly through the API as an upgrade source. Creation returns `DeprecatedVersion` and a Warning event naming stream authorization disclosures [GHSA-6jvc-jx8w-m757](https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-6jvc-jx8w-m757) and [GHSA-342v-cj47-c9g8](https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-342v-cj47-c9g8), fixed in 4.3.6. Upgrade before enabling stream access. Only **4.3.5 → 4.3.6** is supported; no downgrades, uncataloged series or arbitrary [Erlang](https://www.erlang.org/) packages.

## Topology, cost and runtime

Each broker uses one singleton [Ubuntu](https://ubuntu.com/) 24.04 runtime and one single-host Local Storage volume; runtime disk is separate. Replacement temporarily adds a runtime/volume. Retained/orphaned volumes continue consuming storage and may incur charges. Separate services/volumes and the `prod` preset do not guarantee separate hosts; see [placement limitations](../limitations.md).

Broker identity is `rabbit@<service hostname>` plus its persistent data directory. Node indexes increase monotonically and are never reused. Exact Team RabbitMQ apt packages are installed with package-triggered startup disabled; an `erlang*` apt preference at priority 1001 pins transitive dependencies too. The node agent supervises the broker on its volume. `classic_config` discovery is supplemented by explicit observed joins; discovery changes alone do not restart running brokers.

RabbitMQ 4.3 uses **Khepri-only metadata**. Metadata replication does not replicate classic-queue messages. Clients can explicitly choose other queue types; five/seven brokers do not give every queue five/seven replicas. Use durable quorum queues, publisher confirms, acknowledgements and reconnect-capable clients. A singleton has no HA. See [operations](operations.md) for membership changes and recovery, and [ADRs 0020–0022](../adr/README.md) for rationale.

## Create and configure

In **Clusters → Create cluster**, select the namespace and **RabbitMQ**, then version, broker count/preset, per-broker resources and plugins/configuration. Review aggregate footprint and project capacity. The backup step offers definitions only; Access & TLS selects CA, certificates, exposure and monitoring. Keep public management disabled unless its [separate security boundary](access-and-tls.md#public-access) is acceptable.

CLI alternative: [install and authenticate karactl](../cli.md), then save `rabbit.yaml`:

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

Replace `NS` with your [namespace](../namespaces.md):

```sh
karactl -n NS clusters create -f rabbit.yaml --wait --timeout 30m
karactl -n NS clusters status brokers -o json
karactl -n NS clusters members brokers -o json
```

A timeout does not cancel provisioning: resume with `karactl -n NS clusters wait brokers --timeout 30m`, not another create. Follow [operation/generation status](../clusters.md#operations-and-status) and [broker readiness](operations.md).

## Engine options and overrides

Inspect `karactl providers get rabbitmq -o json` (`GET /api/v1/providers/rabbitmq`) for current versions, plugin dependencies, public-management restrictions and configuration metadata. `engineOptions` must be one JSON/YAML object; unknown fields, `null`, unsupported/duplicate plugins are rejected. Management and [Prometheus](https://prometheus.io/) are always enabled, not optional-list entries.

| Plugin group | What it enables |
| --- | --- |
| Always: `rabbitmq_management`, `rabbitmq_prometheus` | Native management UI/API; local metrics |
| `rabbitmq_shovel`, `rabbitmq_shovel_management` | Configured message forwarding; management extension |
| `rabbitmq_federation`, `rabbitmq_federation_management` | Federation links; management extension |
| `rabbitmq_stream`, `rabbitmq_stream_management` | Native Stream protocol listeners; management extension |

Each optional `_management` plugin requires its corresponding base plugin. Enabling plugins does not configure application topology or migrate messages.

`engineConfig` appends `rabbitmq.conf` **`key = value`** lines, with blank lines and `#` comments allowed; not YAML, `advanced.config`, environment or command input. Protected keys/prefixes:

| Managed area | Keys / prefixes |
| --- | --- |
| Listeners and TLS | `listeners.`, `ssl_options.`, `management.listener.`, `management.tcp.`, `management.ssl.`, `prometheus.`, `distribution.`, `stream.listeners.`, `stream.replication.`, `stream.ssl.`, `osiris.` |
| Discovery and queue defaults | `cluster_formation.`, `quorum_queue.initial_cluster_size`, `default_queue_type` |
| Resource limits and logs | `vm_memory_high_watermark`, `total_memory_available_override_value`, `disk_free_limit`, `log.` |
| Credentials and authentication | `default_user`, `default_pass`, `loopback_users`, `auth_backends`, `ssl_cert_login` |
| Definitions loading | `management.load_definitions`, `load_definitions`, `definitions.` |

Matching rejects any key starting with a listed prefix, plus the bare key when a prefix ends in `.`. Other settings still undergo RabbitMQ validation and can fail startup. Configuration, plugins, certificates and relevant resource changes use [rolling maintenance](operations.md).

## Ports

Relay means authenticated namespace control-plane dialing, not a public application proxy. Listeners depend on [TLS mode](access-and-tls.md).

| Listener | Port | Exposure |
| --- | --- | --- |
| AMQP / AMQPS | 5672 / 5671 | Private, relay, public-capable TCP |
| Management HTTP / HTTPS | 15672 / 15671 | Private, relay; public-capable HTTPS subdomain forwards to 15672 only |
| Optional Stream plaintext / TLS | 5552 / 5551 | Private only; no relay or managed public route |
| Prometheus | 15692 | Loopback only |
| Node control | 9443 | Private, relay; never publish |
| Erlang distribution / epmd | 25672 / 4369 | Private broker-internal; never publish |

## Limits

- No shards, routers or configurable arbiters.
- [Definitions recovery](backups.md) is configuration recovery, not message durability.
- [TLS transport and CA-source transitions](access-and-tls.md) are constrained at creation.
- [Member removal/replacement](operations.md) can block on classic stores or missing majorities; never force progress.
- [Drain and monitoring](operations.md) do not establish queue durability or end-to-end delivery.
- [Access administration](access-and-tls.md) is direct, not continuously reconciled; queue/exchange/binding CRUD, arbitrary eval and native Stream public routing are not supplied.
- Mnesia partition-handling overrides are not a recovery policy for Khepri.
- Review the [OTP TLS compatibility restriction](access-and-tls.md) before configuring outbound TLS plugins.
