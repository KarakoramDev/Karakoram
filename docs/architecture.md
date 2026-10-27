<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Architecture

Karakoram is a self-hosted, multi-platform control plane that reconciles engine clusters across infrastructure backends and namespace targets. [Zerops](https://zerops.io/) is the reference backend; [Dokploy](https://dokploy.com/) and [Nomad](https://developer.hashicorp.com/nomad) are also available. See [Backends](backends.md).

## Deployment and trust boundaries

The diagram and concrete paths below show the Zerops backend deployment; other backends provide their own runtime layout, storage and public ingress.

```mermaid
flowchart LR
  Client[Browser / karactl] -->|Public HTTPS| Routing[Zerops HTTP routing]
  Routing -->|HTTP 8080| CP[Karakoram control plane]
  CP --> State[(SQLite / karakoramdata)]
  CP -->|Project-scoped credentials| Platform[Zerops REST API]
  CP -->|Provider-authenticated administration| Engine[Engine runtime]
  CP -->|Pinned TLS + member token / 9443| Agent[Node agent]
  Agent -->|Protected configuration pull / TLS 9443| CP
  Agent -->|Render files / supervise processes| Engine
```

- **Control plane:** one runtime service, normally `karakoram`, with one container and a `karakoramdata` Local Storage volume mounted at `/mnt/karakoramdata`. UI/API traffic enters through public HTTPS; the private agent listener uses TLS 9443. [SQLite](https://sqlite.org/) stores desired state, observations, operations, events and encrypted secret values. The master key is a separately managed service secret.
- **Data-bearing members:** singleton [Ubuntu](https://ubuntu.com/) 24.04 runtimes with independently identified persistent volumes and stable identities (Local Storage on Zerops). Increasing a runtime's container count is not engine replication.
- **Stateless components:** provider-supported auxiliary runtimes do not have durable data volumes. Containers sharing a service identity are not independent data-bearing members.
- **Node agent:** `karakoram agent` supervises the selected engine and exposes a bounded private API, not a remote shell.

Private networking alone is not an authentication boundary. Agent traffic uses pinned trust roots and per-member bearer tokens. Public HTTPS, internal agent TLS and engine authentication/TLS protect different paths. See [Security](security.md) for credentials, project-token authority and secret-storage limits. Backup transfer paths are documented in [Backups and storage](backups.md).

## Namespaces and relay routing

A namespace selects an immutable backend kind and target with its infrastructure credentials. Backends are the [Zerops backend](zerops.md) (targets are Zerops projects), the [Dokploy backend](dokploy.md) (Dokploy environments on a [Docker Swarm](https://docs.docker.com/engine/swarm/)), and the [Nomad backend](nomad.md) (Nomad namespaces on an operator-managed cluster). `default` is the control plane's own target and uses direct private networking. Additional namespaces use an outbound relay only when their backend's `BackendInfo.Relay` requires one; all three current backends require it.

The relay opens an outbound WebSocket connection through public HTTPS and carries an inner mutually authenticated TLS tunnel. Provider-approved administration traffic and protected agent traffic use namespace-qualified streams; agent configuration pulls retain end-to-end TLS. A namespace token or relay outage interrupts management of that namespace, not every project. The relay is not application networking.

See [Namespaces and relays](namespaces.md), the [relay threat model](security.md#relay-threat-model) and [ADR 0018](adr/0018-multi-project-namespaces-relay.md).

## Infrastructure backends

The server composition root registers compiled-in factories in `infra.Registry`. Each factory constructs and validates a namespace's `Target`; engines use `infra.Backend`, not the platform client. Neutral `ServiceSpec` values distinguish runtimes from independently identified persistent volumes. `Deployment` pairs an executable artifact with a structured `RuntimeSpec`; each backend renders its own manifest and supplies its runtime `Layout`. Operations remain opaque handles polled through `OpStatus`, and errors use neutral classes.

Backend capabilities describe optional public routing, disk growth, quotas, placement, logs and other resource/runtime features. Engine capability gates reject unsupported requests. `GET /api/v1/backends` publishes registered kinds and capabilities; namespace responses identify their backend and enabled capabilities. All backends must pass the shared contract suite. See [ADR 0030](adr/0030-backend-neutral-infrastructure-contract.md); the registry does not imply support for an unregistered platform.

## Desired state and durable operations

Clusters separate desired `spec` from observed `status`, with generation tracking. Mutations persist intent before reconciliation. Per-cluster workers serialize synchronization and imperative operations; unrelated clusters can progress independently. Providers persist checkpoints and platform identifiers, then observe completion before advancing disruptive work.

Provider persistence replaces each owned state value, not just its nonempty fields. [PostgreSQL](https://www.postgresql.org/), [Elasticsearch](https://www.elastic.co/elasticsearch) and [Supabase](https://supabase.com/) replace their complete `Cluster.Internal` namespace objects. Flat [MongoDB](https://www.mongodb.com/) and [RabbitMQ](https://www.rabbitmq.com/) state uses `internal/statejson.ReplaceOwned`: explicit JSON tags define owned root keys, omitted empty fields remove prior values, and unrelated workflow keys survive. Nested values are replaced whole; adding an optional owned field must not require a second, hand-maintained deletion list.

Neither HTTP acceptance nor a successful infrastructure configuration process proves engine health. There is no distributed exactly-once transaction across SQLite, infrastructure backends and engine/storage APIs: durable identifiers and observation constrain replay, while ambiguous outcomes require investigation. [Clusters and operations](clusters.md#operations-and-status) defines generations, operation phases, health, cancellation and wait behavior.

## Node-local durability

Agents cache assigned configuration on the data volume with owner-only permissions. Existing data-bearing members can start from that cache during control-plane downtime; first startup without a cache cannot. Stateless runtimes lose cached configuration when replaced.

Rendered credentials, certificates and configuration are restricted to the runtime owner. Explicit maintenance and process-stop holds are persisted separately from pulled configuration, so a configuration refresh or agent restart does not silently release them. Restore/restart recovery remains provider-specific: use the engine operations and recovery guides rather than manually overriding holds.

When the member executable is PID 1, a minimal init parent re-executes the real
agent as its child. The parent forwards termination, interrupt and hangup signals,
reaps adopted daemon descendants, and exits with the real agent's status. This
keeps orphan reaping separate from the agent's `os/exec` command waits; daemonized
database processes cannot remain zombies that block their supervisor's lifecycle
checks. Ordinary non-PID-1 members do not re-execute.

The shared process supervisor reaps each direct child independently of stdout and
stderr EOF. After exit it allows up to 250 ms for final log draining, then closes
its pipe readers so descendants retaining inherited descriptors cannot block exit
status, restart scheduling or stop completion. Normal EOF finishes the drain
immediately. Exit observation does not signal descendants; explicit stop/restart
retains the configured stop signal to the child's process group.

## Recovery, monitoring and events

The control plane owns scheduling and count retention; its downtime pauses new scheduling and retention. Already-started engine-side work can continue without the control plane. Engine backups and control-plane state backups protect different assets: preserve both the SQLite state and its master key as described in [state backup and recovery](operations.md#control-plane-state-backup-and-recovery).

Agents expose member metrics for control-plane collection, history and authenticated [Prometheus](https://prometheus.io/) export. Persisted events feed live UI/API updates; the live event stream is an acceleration, not the persistence layer. See [Monitoring and events](monitoring.md) and [API pagination and events](api.md#pagination-and-events).

## Failure domains

The control plane is single-replica. Persistent volumes are node-local on current backends. Zerops does not guarantee host separation; Dokploy and Nomad report authoritative node placement but cannot establish independent racks or physical hosts beneath the scheduler. Engine replication cannot compensate for every shared-host failure. See [Limitations](limitations.md) before selecting availability and recovery objectives.

## Engine providers

Providers are compiled into Karakoram, not dynamically installable plugins. Each owns its version catalog, validation/defaults, topology, health observation, lifecycle checkpoints, access APIs and backup semantics. Shared interfaces handle infrastructure, namespaces, secrets, operations and monitoring; capability metadata gates supported UI/API actions. A cluster's provider is immutable.

Optional auxiliary services use ordered `ComponentDef`/`ComponentSet` declarations with per-component replicas and resources in `spec.components.extra`. Shared stateless reconciliation handles enable/disable stages through engine callbacks; credentials, application readiness, resizing and stateful safety remain engine-owned. Elasticsearch uses this for [Kibana](https://www.elastic.co/kibana), [Logstash](https://www.elastic.co/logstash) and [APM](https://www.elastic.co/observability/application-performance-monitoring); PostgreSQL for [ProxySQL](https://proxysql.com/); Supabase for its modular native services. See [ADR 0031](adr/0031-engine-component-model.md).

| Engine | Provider | Runtime and operational details |
| --- | --- | --- |
| MongoDB | `psmdb` | [Overview](mongodb/README.md) |
| RabbitMQ | `rabbitmq` | [Overview](rabbitmq/README.md) |
| Elasticsearch | `elasticsearch` | [Overview](elasticsearch/README.md) |
| PostgreSQL | `postgres` | [Percona, extension profiles, Patroni/etcd HA, Citus, ProxySQL and pgBackRest](postgres/README.md) |
| Supabase | `supabase` | [PostgreSQL core plus modular native services](supabase/README.md) |
| Keycloak | `keycloak` | [Native identity service with a managed PostgreSQL binding](keycloak/README.md) |
| OpenBao | `openbao` | [Native secrets clusters, Raft recovery and leased database credentials](openbao/README.md) |
| Temporal | `temporal` | [Stateless workflow roles on bound PostgreSQL](temporal/README.md); requires the PostgreSQL binding service |
| MySQL | `mysql` | [Percona Server, Group Replication, ProxySQL and XtraBackup](mysql/README.md) |
| Percona XtraDB Cluster | `pxc` | [Galera, quorum recovery and separate backup provenance](mysql/README.md) |
| Neo4j Community | `neo4j` | [TLS-only singleton, native administrative users and offline dumps](neo4j/README.md) |
| Weaviate | `weaviate` | [Native lifecycle, access management and recovery](weaviate/README.md) |

Supabase wraps the PostgreSQL core rather than duplicating database lifecycle. Internal services use a protocol-transparent leader forwarder rather than ProxySQL; the client proxy retains explicit protocol limits. Native service availability comes from an embedded, checksum-pinned lock with operator mirror precedence, not floating downloads. See [ADRs 0034–0036](adr/README.md) and [native artifacts](native-artifacts.md).

See [ADR 0005](adr/0005-engine-provider-seam.md) for the provider seam and [Contributing](../CONTRIBUTING.md) for code layout and development workflows.
