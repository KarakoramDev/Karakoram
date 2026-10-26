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

| Backend | Status | Existing target and provisioned resources | Installation |
| --- | --- | --- | --- |
| Zerops | Reference backend | A Zerops project; Ubuntu runtimes, separate Local Storage volumes, routing and additional-namespace relays. | [Install](install.md), dashboard-only [Quick start](quickstart.md) |
| Dokploy | Experimental; no production track record | A Dokploy environment on your Docker Swarm; applications, node-pinned Docker named volumes, built images, public routes and relays. | [Dokploy guide](dokploy.md#install-the-control-plane-on-dokploy) |
| Nomad | Experimental; no production track record | A dedicated Nomad namespace; native jobs, Variables records, node-pinned dynamic host volumes and relays. | [Nomad guide](nomad.md#install-the-control-plane-on-nomad) |

Karakoram does not create the underlying projects, Swarm or Nomad cluster. Dokploy and Nomad are experimental for every engine. Their PostgreSQL and Supabase paths use the shared native runtime contract, but the backend guides explicitly do not claim engine acceptance for those combinations.

## Capability differences

These are backend capabilities, not guarantees of engine availability. Query `GET /api/v1/backends` with `namespaces/read` on global `*` for registered metadata; namespace responses report enabled capabilities. Engine catalogs and validation determine allowed topologies and components.

| Capability | Zerops — reference | Dokploy — experimental | Nomad — experimental |
| --- | --- | --- | --- |
| Engine model | MongoDB replica sets/sharding, RabbitMQ, Elasticsearch, PostgreSQL single/HA and modular Supabase | Same engine runtime contract; PostgreSQL/Supabase remain unaccepted experimental combinations | Same engine runtime contract; PostgreSQL/Supabase remain unaccepted experimental combinations |
| Placement | No anti-affinity guarantee; observed placement is not authoritative | Authoritative Swarm node; volumes spread while eligible nodes remain | Authoritative Nomad node; volumes spread while eligible nodes remain |
| Persistent storage | Node-local Local Storage; enforced volume sizing and supported growth, no shrink | Node-local Docker named volumes; no enforced quota or supported growth | Dynamic `mkdir` host volumes; no enforced quota or supported growth |
| Public TCP | Project public addresses and backend-enforced CIDR allowlists | Swarm routing mesh; no backend CIDR filtering | Static host ports; operator load balancer must forward to the correct node; no backend CIDR filtering |
| Managed public HTTPS | Supported | Requires configured `domainSuffix`, wildcard DNS and Traefik | Unsupported; use a separately operated external proxy without enabling backend-managed HTTPS options |
| Resource bounds and autoscaling | Platform autoscaling bounds; dedicated CPU supported | Scheduler reservations/limits, not Zerops-style resource autoscaling; no dedicated CPU capability | Scheduler reservations/limits, not Zerops-style resource autoscaling; dedicated CPU requires whole cores |
| Stateless replicas | Supported; not database replication | Supported; persistent runtimes remain singleton | Supported; persistent runtimes remain singleton |
| Credential scope | Project-scoped token | Admin API key; use a dedicated Dokploy installation | Namespace ACL writes, but not hostile-tenant metadata isolation; trust all job submitters reaching the clients |

For public TCP on experimental Dokploy and Nomad, admission requires an allowlist covering the whole address family and explicit `allowAllAddresses: true`. Apply narrower access rules at your firewall/load balancer; this acknowledgement does not create filtering. Nomad rejects managed public HTTPS for Supabase gateway, Kibana, APM and RabbitMQ management. Review the complete [Dokploy capabilities](dokploy.md#capabilities), [Nomad capabilities](nomad.md#capabilities) and [Limitations](limitations.md).

## Choose and configure a backend

1. **Zerops:** use the [Quick start](quickstart.md) or [Install](install.md). The default backend is `zerops`; supply `KARAKORAM_ZEROPS_TOKEN` and the project identity as documented there.
2. **Dokploy — experimental:** first prepare the Swarm, registry, overlay network and access policy in the [Dokploy guide](dokploy.md#requirements), then [install the control plane](dokploy.md#install-the-control-plane-on-dokploy). Set `KARAKORAM_BACKEND=dokploy` and the guide's `KARAKORAM_DOKPLOY_*` target settings.
3. **Nomad — experimental:** first prepare dedicated clients, the private-copy Ubuntu rootfs, routed networking, ACLs and storage in the [Nomad guide](nomad.md#requirements), then [install the control plane](nomad.md#install-the-control-plane-on-nomad). Set `KARAKORAM_BACKEND=nomad` and the guide's `KARAKORAM_NOMAD_*` settings. The operator owns public HTTPS, load balancing and host firewalls.

For additional targets, [register a namespace](namespaces.md) using `--backend zerops`, `--backend dokploy` or `--backend nomad`, a write-only credential and the backend's non-secret `--config key=value` settings. The API keeps the field name `projectId` for the backend target ID. Backend kind and target identity are immutable; existing records and omitted kinds remain Zerops. Do not change an existing installation's default backend as an in-place cluster migration.

Compare [Pricing and cost estimates](pricing.md) before choosing capacity, storage, public addresses and recovery objectives.
