<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Providers

Karakoram is multi-platform. A provider supplies infrastructure; an engine driver owns database topology, health, credentials and recovery. You operate the installation and its infrastructure credentials. Provider support does not make different platforms equivalent availability or security boundaries.

The API and configuration call an infrastructure provider the **backend**: identifiers such as `GET /api/v1/backends`, the namespace field `backend`, `KARAKORAM_BACKEND` and `--backend` keep their existing names.

## Infrastructure contract

A provider implements the compiled-in infrastructure contract, not an engine-specific deployment recipe:

- **Members and lifecycle:** create, observe, deploy, start, stop and delete runtime services; return durable, opaque handles for asynchronous operations. Accepted work is not proof of engine readiness.
- **Storage:** give persistent volumes separate identities and lifecycles, mount each data member's volume, and fence deletion against identity reuse. Retaining a volume does not replicate it or move its data to another node.
- **Placement:** report member/container placement and, where supported, place volumes from a spread group on distinct eligible nodes while capacity permits. Engines still own quorum and replacement decisions.
- **Networking:** provide private member addresses, runtime ports and optional public TCP/HTTPS routes. Additional namespaces require an outbound management relay on all current providers; the default namespace uses direct connectivity. Relays do not carry application traffic or bulk backups.
- **Runtime layout and capabilities:** supply paths and an unprivileged runtime user; translate structured artifacts, preparation commands, resources, secrets and readiness into platform-specific deployments. Unsupported optional capabilities are rejected rather than silently emulated.

See [Architecture](architecture.md#infrastructure-providers) and [ADR 0030](adr/0030-backend-neutral-infrastructure-contract.md) for the contract and registry design.

## Available providers

| Provider | Role / runtime | Existing target and provisioned resources | Installation |
| --- | --- | --- | --- |
| Zerops | Reference provider | A Zerops project; Ubuntu runtimes, separate Local Storage volumes, routing and additional-namespace relays. | [Zerops guide](zerops.md#install-the-control-plane-on-zerops) |
| Dokploy | Docker Swarm | A Dokploy environment on your Docker Swarm; applications, node-pinned Docker named volumes, built images, public routes and relays. | [Dokploy guide](dokploy.md#install-the-control-plane-on-dokploy) |
| Nomad | Native jobs | A dedicated Nomad namespace; native jobs, Variables records, node-pinned dynamic host volumes and relays. | [Nomad guide](nomad.md#install-the-control-plane-on-nomad) |

Karakoram does not create the underlying projects, [Swarm](https://docs.docker.com/engine/swarm/) or [Nomad](https://developer.hashicorp.com/nomad) cluster. [Dokploy](https://dokploy.com) and Nomad are experimental for every engine, with no production track record. Their [PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) paths use the shared native runtime contract, but the provider guides explicitly do not claim engine acceptance for those combinations.

## Engine availability

The table describes implemented runtime/admission support, **not live-provider acceptance**. Each new engine/topology's release status is stated here once; engine guides describe its interface and limits. All combinations remain subject to artifact availability and the capability differences below.

| Engine / topology | Status | Zerops | Dokploy | Nomad | Guide |
| --- | --- | --- | --- | --- | --- |
| OpenBao | **EXPERIMENTAL** until tested against live providers | Native runtime | Native runtime | Native runtime | [OpenBao](openbao/README.md) |
| Temporal | **EXPERIMENTAL** until tested against live providers | Not supported yet: membership requires private IPv4; Zerops uses private IPv6 | Private services; optional public UI with configured HTTPS routing | Private services/UI only; no managed public UI HTTPS | [Temporal](temporal/README.md) |
| MySQL / Percona Server | **EXPERIMENTAL** until tested against live providers | Single / Group Replication | Single / Group Replication | Single / Group Replication | [MySQL](mysql/README.md) |
| Percona XtraDB Cluster (PXC) | **EXPERIMENTAL** until tested against live providers | Galera | Galera | Galera | [PXC](mysql/README.md) |
| Neo4j Community | **EXPERIMENTAL** until tested against live providers | Singleton | Singleton | Singleton | [Neo4j](neo4j/README.md) |
| Weaviate | **EXPERIMENTAL** until tested against live providers | Single / cluster | Single / cluster | Single / cluster | [Weaviate](weaviate/README.md) |
| Keycloak | **EXPERIMENTAL** until tested against live providers | Private; optional public HTTPS | Private; optional public HTTPS with configured routing | Private only; no managed public HTTPS | [Keycloak](keycloak/README.md) |
| PostgreSQL distributed / Citus | **EXPERIMENTAL** until tested against live providers | Coordinator / worker groups | Coordinator / worker groups | Coordinator / worker groups | [Citus](postgres/distributed.md) |

[PostgreSQL](https://www.postgresql.org/) [extension profiles](postgres/profiles.md) ([PostGIS](https://postgis.net/), [pgvector](https://github.com/pgvector/pgvector) and [TimescaleDB](https://www.timescale.com/)) and [service bindings](postgres/bindings.md) use the shared PostgreSQL engine driver on [Zerops](https://zerops.io/), Dokploy and Nomad. Profiles are version/artifact-gated; bindings require a compatible ready source and consumer-specific PostgreSQL version. [OpenBao](https://openbao.org/) PostgreSQL storage, [Temporal](https://temporal.io/) and [Keycloak](https://www.keycloak.org/) require the shared binding service present in the standard server.

Temporal is restricted to Dokploy and Nomad; Zerops support is pending IPv6 private networking. Common admission checks public TCP/allowlisting and dedicated CPU; PostgreSQL/[MySQL](https://www.mysql.com)/[PXC](https://docs.percona.com/percona-xtradb-cluster/8.4/) proxy replicas and Temporal role replicas above one require horizontal scaling. All three providers offer horizontal scaling. Only Zerops enforces public TCP CIDR filtering; Dokploy and Nomad require the explicit all-address acknowledgement and external filtering. Managed public HTTPS is available on Zerops and configured Dokploy, not Nomad. This does not imply safe private networking for untrusted tenants; review each engine's transport requirements.

Fleet operations, [notifications](notifications.md), [control-plane recovery](control-plane-backup.md) and [backup drills](backup-drills.md) are **verified offline** (tests without a live provider); see [operations](operations.md) and [verification](verification.md) for scope.


## Capability differences

These are provider capabilities, not guarantees of engine availability. Query `GET /api/v1/backends` with `namespaces/read` on global `*` for registered metadata; namespace responses report enabled capabilities. Engine catalogs and validation determine allowed topologies and components.

| Capability | Zerops — reference | Dokploy | Nomad |
| --- | --- | --- | --- |
| Engine model | Shared native contract for catalog engines/topologies above, except Temporal | Shared runtime contract; subject to the status and feature gates above | Same runtime contract; no managed public HTTPS |
| Placement | No anti-affinity guarantee; observed placement is not authoritative | Authoritative Swarm node; volumes spread while eligible nodes remain | Authoritative Nomad node; volumes spread while eligible nodes remain |
| Persistent storage | Node-local Local Storage; enforced volume sizing and supported growth, no shrink | Node-local Docker named volumes; no enforced quota or supported growth | Dynamic `mkdir` host volumes; no enforced quota or supported growth |
| In-place disk growth (`diskGrowth`) | Supported | Unsupported; updates rejected before reconciliation | Unsupported; updates rejected before reconciliation |
| Public TCP | Project public addresses and provider-enforced CIDR allowlists | Swarm routing mesh; no provider CIDR filtering | Static host ports; operator load balancer must forward to the correct node; no provider CIDR filtering |
| Managed public HTTPS | Supported | Requires configured `domainSuffix`, wildcard DNS and Traefik | Unsupported; use a separately operated external proxy without enabling provider-managed HTTPS options |
| Resource bounds and autoscaling | Platform autoscaling bounds; dedicated CPU supported | Scheduler reservations/limits, not Zerops-style resource autoscaling; no dedicated CPU capability | Scheduler reservations/limits, not Zerops-style resource autoscaling; dedicated CPU requires whole cores |
| Stateless replicas | Supported; not database replication | Supported; persistent runtimes remain singleton | Supported; persistent runtimes remain singleton |
| Credential scope | Project-scoped token | Admin API key; use a dedicated Dokploy installation | Namespace ACL writes, but not hostile-tenant metadata isolation; trust all job submitters reaching the clients |

For public TCP on Dokploy and Nomad, admission requires an allowlist covering the whole address family and explicit `allowAllAddresses: true`. Apply narrower access rules at your firewall/load balancer; this acknowledgement does not create filtering. Nomad rejects managed public HTTPS for Supabase gateway, [Kibana](https://www.elastic.co/kibana), APM and [RabbitMQ](https://www.rabbitmq.com/) management. Review the complete [Zerops capabilities](zerops.md#capabilities), [Dokploy capabilities](dokploy.md#capabilities), [Nomad capabilities](nomad.md#capabilities) and [Limitations](limitations.md).

Creating volumes at their initial requested size and adding new members are not
in-place growth. Increasing either disk bound of an existing enabled component
requires `diskGrowth`, including updates that also add replicas. PUT, PATCH and
their dry-run equivalents return HTTP 400 with
`not supported for this cluster: backend BACKEND does not provide diskGrowth`
when the namespace provider lacks it; rejected requests do not change the cluster
generation or enqueue an operation. Legacy desired state is checked again before
convergence starts disruptive resource work: unsupported growth fails its
operation terminally without stopping a serving member.

For a size increase accepted by an older control plane, revert both disk bounds
to the volume size recorded by the provider before retrying. On a provider without
growth, admission checks every existing volume's provider-reported bounds rather
than trusting unapplied desired sizes or old provider sizing caches. A correction
cannot grow a smaller peer or shrink a larger peer in a mixed-size component.
Resuming retained volumes with a larger, unsupported desired size is rejected
before a resume operation is queued. Actual volume shrinking remains unsupported.

## Choose and configure a provider

1. **Zerops:** review the [Zerops guide](zerops.md), then use the dashboard-only [Quick start](quickstart.md) or [Install](install.md). The default provider is `zerops`; supply `KARAKORAM_ZEROPS_TOKEN` and the project identity as documented there.
2. **Dokploy:** first prepare the Swarm, registry, overlay network and access policy in the [Dokploy guide](dokploy.md#requirements), then [install the control plane](dokploy.md#install-the-control-plane-on-dokploy). Set `KARAKORAM_BACKEND=dokploy` and the guide's `KARAKORAM_DOKPLOY_*` target settings.
3. **Nomad:** first prepare dedicated clients, the private-copy [Ubuntu](https://ubuntu.com/) rootfs, routed networking, ACLs and storage in the [Nomad guide](nomad.md#requirements), then [install the control plane](nomad.md#install-the-control-plane-on-nomad). Set `KARAKORAM_BACKEND=nomad` and the guide's `KARAKORAM_NOMAD_*` settings. The operator owns public HTTPS, load balancing and host firewalls.

For additional targets, [register a namespace](namespaces.md) using `--backend zerops`, `--backend dokploy` or `--backend nomad`, a write-only credential and the provider's non-secret `--config key=value` settings. The API keeps the field name `projectId` for the provider target ID. Provider kind and target identity are immutable; existing records and omitted kinds remain Zerops. Do not change an existing installation's default provider as an in-place cluster migration.

Compare [Operating costs](operating-costs.md) before choosing capacity, storage, public addresses and recovery objectives.
