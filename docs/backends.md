<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backends

Karakoram is multi-platform. A backend supplies infrastructure; an engine provider owns database topology, health, credentials and recovery. You operate the installation and its infrastructure credentials. Backend support does not make different platforms equivalent availability or security boundaries.

## Infrastructure contract

A backend implements the compiled-in infrastructure contract, not an engine-specific deployment recipe:

- **Members and lifecycle:** create, observe, deploy, start, stop and delete runtime services; return durable, opaque handles for asynchronous operations. Accepted work is not proof of engine readiness.
- **Storage:** give persistent volumes separate identities and lifecycles, mount each data member's volume, and fence deletion against identity reuse. Retaining a volume does not replicate it or move its data to another node.
- **Placement:** report member/container placement and, where supported, place volumes from a spread group on distinct eligible nodes while capacity permits. Engines still own quorum and replacement decisions.
- **Networking:** provide private member addresses, runtime ports and optional public TCP/HTTPS routes. Additional namespaces require an outbound management relay on all current backends; the default namespace uses direct connectivity. Relays do not carry application traffic or bulk backups.
- **Runtime layout and capabilities:** supply paths and an unprivileged runtime user; translate structured artifacts, preparation commands, resources, secrets and readiness into platform-specific deployments. Unsupported optional capabilities are rejected rather than silently emulated.

See [Architecture](architecture.md#infrastructure-backends) and [ADR 0030](adr/0030-backend-neutral-infrastructure-contract.md) for the contract and registry design.

## Available backends

| Backend | Role / runtime | Existing target and provisioned resources | Installation |
| --- | --- | --- | --- |
| Zerops | Reference backend | A Zerops project; Ubuntu runtimes, separate Local Storage volumes, routing and additional-namespace relays. | [Zerops guide](zerops.md#install-the-control-plane-on-zerops) |
| Dokploy | Docker Swarm | A Dokploy environment on your Docker Swarm; applications, node-pinned Docker named volumes, built images, public routes and relays. | [Dokploy guide](dokploy.md#install-the-control-plane-on-dokploy) |
| Nomad | Native jobs | A dedicated Nomad namespace; native jobs, Variables records, node-pinned dynamic host volumes and relays. | [Nomad guide](nomad.md#install-the-control-plane-on-nomad) |

Karakoram does not create the underlying projects, Swarm or Nomad cluster. Dokploy and Nomad are experimental for every engine, with no production track record. Their PostgreSQL and Supabase paths use the shared native runtime contract, but the backend guides explicitly do not claim engine acceptance for those combinations.

## Engine availability

The table describes implemented runtime/admission support, **not live-backend acceptance**. Each new engine/topology's release status is stated here once; engine guides describe its interface and limits. All combinations remain subject to artifact availability and the capability differences below.

| Engine / topology | Status | Zerops | Dokploy | Nomad | Guide |
| --- | --- | --- | --- | --- | --- |
| OpenBao | **EXPERIMENTAL** until tested against live backends | Native runtime | Native runtime | Native runtime | [OpenBao](openbao/README.md) |
| Temporal | **EXPERIMENTAL** until tested against live backends | Not supported in v0.4.0: membership requires private IPv4; Zerops uses private IPv6 | Private services; optional public UI with configured HTTPS routing | Private services/UI only; no managed public UI HTTPS | [Temporal](temporal/README.md) |
| MySQL / Percona Server | **EXPERIMENTAL** until tested against live backends | Single / Group Replication | Single / Group Replication | Single / Group Replication | [MySQL](mysql/README.md) |
| Percona XtraDB Cluster (PXC) | **EXPERIMENTAL** until tested against live backends | Galera | Galera | Galera | [PXC](mysql/README.md) |
| Neo4j Community | **EXPERIMENTAL** until tested against live backends | Singleton | Singleton | Singleton | [Neo4j](neo4j/README.md) |
| Weaviate | **EXPERIMENTAL** until tested against live backends | Single / cluster | Single / cluster | Single / cluster | [Weaviate](weaviate/README.md) |
| Keycloak | **EXPERIMENTAL** until tested against live backends | Private; optional public HTTPS | Private; optional public HTTPS with configured routing | Private only; no managed public HTTPS | [Keycloak](keycloak/README.md) |
| PostgreSQL distributed / Citus | **EXPERIMENTAL** until tested against live backends | Coordinator / worker groups | Coordinator / worker groups | Coordinator / worker groups | [Citus](postgres/distributed.md) |

PostgreSQL [extension profiles](postgres/profiles.md) (PostGIS, pgvector and TimescaleDB) and [service bindings](postgres/bindings.md) use the shared PostgreSQL provider on Zerops, Dokploy and Nomad. Profiles are version/artifact-gated; bindings require a compatible ready source and consumer-specific PostgreSQL version. OpenBao PostgreSQL storage, Temporal and Keycloak require the shared binding service present in the standard server.

Temporal is restricted to Dokploy and Nomad in v0.4.0; Zerops support is pending IPv6 private networking. Common admission checks public TCP/allowlisting and dedicated CPU; PostgreSQL/MySQL/PXC proxy replicas and Temporal role replicas above one require horizontal scaling. All three backends offer horizontal scaling. Only Zerops enforces public TCP CIDR filtering; Dokploy and Nomad require the explicit all-address acknowledgement and external filtering. Managed public HTTPS is available on Zerops and configured Dokploy, not Nomad. This does not imply safe private networking for untrusted tenants; review each engine's transport requirements.

Fleet operations, [notifications](notifications.md), [control-plane recovery](control-plane-backup.md) and [backup drills](backup-drills.md) are **verified offline** (tests without a live backend); see [operations](operations.md) and [verification](verification.md) for scope.


## Capability differences

These are backend capabilities, not guarantees of engine availability. Query `GET /api/v1/backends` with `namespaces/read` on global `*` for registered metadata; namespace responses report enabled capabilities. Engine catalogs and validation determine allowed topologies and components.

| Capability | Zerops — reference | Dokploy | Nomad |
| --- | --- | --- | --- |
| Engine model | Shared native contract for catalog engines/topologies above, except Temporal | Shared runtime contract; subject to the status and feature gates above | Same runtime contract; no managed public HTTPS |
| Placement | No anti-affinity guarantee; observed placement is not authoritative | Authoritative Swarm node; volumes spread while eligible nodes remain | Authoritative Nomad node; volumes spread while eligible nodes remain |
| Persistent storage | Node-local Local Storage; enforced volume sizing and supported growth, no shrink | Node-local Docker named volumes; no enforced quota or supported growth | Dynamic `mkdir` host volumes; no enforced quota or supported growth |
| Public TCP | Project public addresses and backend-enforced CIDR allowlists | Swarm routing mesh; no backend CIDR filtering | Static host ports; operator load balancer must forward to the correct node; no backend CIDR filtering |
| Managed public HTTPS | Supported | Requires configured `domainSuffix`, wildcard DNS and Traefik | Unsupported; use a separately operated external proxy without enabling backend-managed HTTPS options |
| Resource bounds and autoscaling | Platform autoscaling bounds; dedicated CPU supported | Scheduler reservations/limits, not Zerops-style resource autoscaling; no dedicated CPU capability | Scheduler reservations/limits, not Zerops-style resource autoscaling; dedicated CPU requires whole cores |
| Stateless replicas | Supported; not database replication | Supported; persistent runtimes remain singleton | Supported; persistent runtimes remain singleton |
| Credential scope | Project-scoped token | Admin API key; use a dedicated Dokploy installation | Namespace ACL writes, but not hostile-tenant metadata isolation; trust all job submitters reaching the clients |

For public TCP on Dokploy and Nomad, admission requires an allowlist covering the whole address family and explicit `allowAllAddresses: true`. Apply narrower access rules at your firewall/load balancer; this acknowledgement does not create filtering. Nomad rejects managed public HTTPS for Supabase gateway, Kibana, APM and RabbitMQ management. Review the complete [Zerops capabilities](zerops.md#capabilities), [Dokploy capabilities](dokploy.md#capabilities), [Nomad capabilities](nomad.md#capabilities) and [Limitations](limitations.md).

## Choose and configure a backend

1. **Zerops:** review the [Zerops guide](zerops.md), then use the dashboard-only [Quick start](quickstart.md) or [Install](install.md). The default backend is `zerops`; supply `KARAKORAM_ZEROPS_TOKEN` and the project identity as documented there.
2. **Dokploy:** first prepare the Swarm, registry, overlay network and access policy in the [Dokploy guide](dokploy.md#requirements), then [install the control plane](dokploy.md#install-the-control-plane-on-dokploy). Set `KARAKORAM_BACKEND=dokploy` and the guide's `KARAKORAM_DOKPLOY_*` target settings.
3. **Nomad:** first prepare dedicated clients, the private-copy Ubuntu rootfs, routed networking, ACLs and storage in the [Nomad guide](nomad.md#requirements), then [install the control plane](nomad.md#install-the-control-plane-on-nomad). Set `KARAKORAM_BACKEND=nomad` and the guide's `KARAKORAM_NOMAD_*` settings. The operator owns public HTTPS, load balancing and host firewalls.

For additional targets, [register a namespace](namespaces.md) using `--backend zerops`, `--backend dokploy` or `--backend nomad`, a write-only credential and the backend's non-secret `--config key=value` settings. The API keeps the field name `projectId` for the backend target ID. Backend kind and target identity are immutable; existing records and omitted kinds remain Zerops. Do not change an existing installation's default backend as an in-place cluster migration.

Compare [Operating costs](operating-costs.md) before choosing capacity, storage, public addresses and recovery objectives.
