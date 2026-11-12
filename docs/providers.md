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
| Temporal | **EXPERIMENTAL** until tested against live providers | Private IPv6 services; optional public UI with OIDC | Private services; optional public UI with configured HTTPS routing | Private services/UI only; no managed public UI HTTPS | [Temporal](temporal/README.md) |
| MySQL / Percona Server | **EXPERIMENTAL** until tested against live providers | Single / Group Replication | Single / Group Replication | Single / Group Replication | [MySQL](mysql/README.md) |
| Percona XtraDB Cluster (PXC) | **EXPERIMENTAL** until tested against live providers | Galera | Galera | Galera | [PXC](mysql/README.md) |
| Neo4j Community | **EXPERIMENTAL** until tested against live providers | Singleton | Singleton | Singleton | [Neo4j](neo4j/README.md) |
| Weaviate | **EXPERIMENTAL** until tested against live providers | Single / cluster | Single / cluster | Single / cluster | [Weaviate](weaviate/README.md) |
| Keycloak | **EXPERIMENTAL** until tested against live providers | Private; optional public HTTPS | Private; optional public HTTPS with configured routing | Private only; no managed public HTTPS | [Keycloak](keycloak/README.md) |
| PostgreSQL distributed / Citus | **EXPERIMENTAL** until tested against live providers | Coordinator / worker groups | Coordinator / worker groups | Coordinator / worker groups | [Citus](postgres/distributed.md) |

[PostgreSQL](https://www.postgresql.org/) [extension profiles](postgres/profiles.md) ([PostGIS](https://postgis.net/), [pgvector](https://github.com/pgvector/pgvector) and [TimescaleDB](https://www.timescale.com/)) and [service bindings](postgres/bindings.md) use the shared PostgreSQL engine driver on [Zerops](https://zerops.io/), Dokploy and Nomad. Profiles are version/artifact-gated; bindings require a compatible ready source and consumer-specific PostgreSQL version. [OpenBao](https://openbao.org/) PostgreSQL storage, [Temporal](https://temporal.io/) and [Keycloak](https://www.keycloak.org/) require the shared binding service present in the standard server.

Temporal uses the backend's private network family: IPv6 on Zerops, IPv4 on Dokploy and Nomad. Common admission checks public TCP/allowlisting and dedicated CPU; PostgreSQL/[MySQL](https://www.mysql.com)/[PXC](https://docs.percona.com/percona-xtradb-cluster/8.4/) proxy replicas and Temporal role replicas above one require horizontal scaling. All three providers offer horizontal scaling. Only Zerops enforces public TCP CIDR filtering; Dokploy and Nomad require the explicit all-address acknowledgement and external filtering. Managed public HTTPS is available on Zerops and configured Dokploy, not Nomad. This does not imply safe private networking for untrusted tenants; review each engine's transport requirements.

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

## Platform read budget

Steady clusters renew availability using agent heartbeats and a process-local
platform snapshot below the provider HTTP client. Successful GET responses are
keyed by their exact URL and shared by every client and cluster of the same
target: Dokploy API origin + environment, Zerops API base + project, or Nomad
API origin + namespace + region. See [observation and freshness](clusters.md#operations-and-status)
and [ADR 0070](adr/0070-agent-heartbeats-and-platform-read-budget.md).

Before serving snapshot entries, the target must have a successful drift listing
check less than 60 seconds old. Entries themselves are usable for at most 40
minutes. These platform facts have a separate freshness class from the
15-second health-evidence lease.

| Provider | Shared target drift listing |
| --- | --- |
| Dokploy | `environment.one`: one GET per environment |
| Zerops | `GET /project/{id}/service-stack`: paginated, one GET per 100 services |
| Nomad | `GET /v1/jobs?meta=true` and `GET /v1/allocations`: two GETs per namespace/region |

Listings fingerprint stable identity, ownership and runtime-state fields rather
than volatile telemetry. A changed fingerprint invalidates the target snapshot
and wakes reconciliation; a failed listing check prevents snapshot serving until
a check succeeds. Checks are shared across clusters, not repeated for each one.
Every non-GET advances the target's mutation epoch before sending and after
completion. Entries are invalidated at the first advance; GETs that overlap a
mutation cannot populate the snapshot, and a steady attempt cannot publish a
positive result if its target epoch changed.

Only independent steady availability renewal can serve these entries. Reconcile
observations before and after `Sync`, lifecycle reads, operation polling,
writer/retirement/identity gates, maintenance and formation decisions, replacement
admission, proxy serving admission and pinned dial authorization always stay
fresh. Platform misses and never-pushing members go to the network; a member
known to push but lacking a usable sample rejects the steady attempt immediately.
A rejected result is discarded before the ordinary two-attempt fresh observation.
Busy or degraded clusters therefore retain fresh-read bursts; a sibling's mutation
in the same target also invalidates the shared snapshot.

The offline request-budget harnesses
(`TestDokploySteadyRequestBudget`, `TestZeropsSteadyRequestBudget` and
`TestNomadSteadyRequestBudget` in `internal/infra/<provider>/steady_budget_test.go`)
fail when their budgets are exceeded. Each fixture uses one target containing
MySQL Group Replication with three members and three proxy instances (GR3+3px),
PostgreSQL single with two proxy instances (plus a Data MCP companion on Dokploy
and Zerops), and Neo4j single.

| Provider | Kind | Fresh status pass (requests) | Warm steady renewal with heartbeats (requests) | Deep reconcile (requests) | Idle req/s per cluster |
| --- | --- | ---: | ---: | ---: | ---: |
| Dokploy | MySQL GR3+3px | 28 | 0 | 94 | 0.058 |
| Dokploy | PostgreSQL single + 2px | 16 | 0 | 104 | 0.063 |
| Dokploy | Neo4j single | 8 | 0 | 54 | 0.036 |
| Zerops | MySQL GR3+3px | 30 | 0 | 60 + shared listing | 0.041 |
| Zerops | PostgreSQL single + 2px | 16 | 0 | 58 + shared listing | 0.040 |
| Zerops | Neo4j single | 4 | 0 | 29 + shared listing | 0.024 |
| Nomad | MySQL GR3+3px | 40 | 0 | 84 | 0.058 |
| Nomad | PostgreSQL single + 2px | 13 | 0 | 57 | 0.043 |
| Nomad | Neo4j single | 4 | 0 | 32 | 0.029 |

Deep reconcile means fresh observation + idle `Sync` + fresh observation, with
`Sync` modelled from measured per-host backend calls. Idle rates use a one-hour
replay: renewal every five seconds, listing every 60 seconds shared by the three
clusters, and deep reconciliation every 30 minutes. Listings cost one GET
`environment.one` per minute per Dokploy target, one GET service-stack page per
minute per Zerops target in this fixture, and two GETs (jobs + allocations) per
minute per Nomad target. The provider harnesses observe the Data MCP companion
through a test helper; the production renewal path (provider and companion in one
steady attempt, zero companion requests when warm) is covered by the reconciler's
companion push tests.

**v0.4.7 cadence:** every cluster ran a full reconcile (observation + `Sync` +
observation) about every five seconds. With the deep-pass costs above, that is
roughly deep/5 requests per second per cluster, in MySQL/PostgreSQL/Neo4j order:
Dokploy **18.8/20.8/10.8**, Zerops **12/11.6/5.8**, Nomad **16.8/11.4/6.4**.
v0.4.7's own Dokploy status pass was larger still: **66/33/12** requests.

With older (v0.4.7 or v0.4.8) agents, steady renewal still pulls agents while platform
reads use the snapshot. Legacy renewal platform requests in the same topology
order are Dokploy **10/8/0** (fresh pinned task authorizations, each with its own fresh service inspection), Zerops **12/8/0**,
and Nomad **7/5/0**. Additional namespaces also perform a separate settled relay
service lookup every minute. On Dokploy that lookup fans out to six network GETs;
it is not one HTTP request. The default namespace has no managed relay.

The idle-hour model includes 660 conditional-helper pulls across all example
instances. Separate production-path budget tests on every provider obtain real
full MySQL/PostgreSQL configurations through authenticated HTTP with a running
reconciler's real steady-baseline predicate, then issue 180/120 phase-offset
proxy-instance pulls per hour with zero config network requests. Storage edits
force full updated responses; invalidation produces a heartbeat reload notice
rather than waiting another minute. Matching certified payloads require zero
per-member platform reads. Expired proof triggers one shared listing check
(one Dokploy GET, two Nomad GETs, or the normal Zerops listing pages), not a
full proxy render. Proof time is the request start, freshness remains strictly
60 seconds, and the listing request has a five-second budget. A full PostgreSQL proxy
configuration still validates each joined data peer using fresh `GetService`
reads; old agents that send no revision retain that additional periodic cost.
Upgrade agents as well as the control plane to obtain the measured idle budget.
Monitoring and namespace credential revalidation remain separate: namespace
credentials are validated every five minutes (`namespace`), costing four GETs
on a settled Dokploy target with a configured registry. For the three example
clusters sharing one such default target, the projected total including listings
and credential validation is **0.170 GET/s**. Use the caller breakdown in
[monitoring](monitoring.md); operations, full config fallbacks, monitoring and
additional relays are not included in that projection.

Updated agents defer pure force/steady notices until the baseline settles when
tracked content is unchanged. In the single-peer reference profile, one full
pull for each of the two PostgreSQL proxies adds 18 Dokploy GETs per settled
pass, or **0.010 GET/s** for one pass every 30 minutes (**0.180 GET/s**
including the base projection). Changed content
still notifies immediately. Independent minute pulls while nonsteady, errors
and retries are separate work; this is not a universal aggregate cap.

An operation on another cluster no longer triggers PostgreSQL notices merely
by advancing the shared target mutation epoch. The epoch remains in config
certificates: periodic full pulls can still cost one render/minute/proxy.
Listing drift still fans out. For ten PostgreSQL clusters with two proxies
and three peers each, two listing changes cost **1,080 Dokploy GETs** with
otherwise eligible baselines (10 × 2 × 3 × 2 × 9). The measured fan-out is
**nine GETs per peer per full proxy render**: three peers cost 27 GETs.
A real Manager single-settle path typically needs at most two renders per
proxy per listing drift: approximately **2,160 GETs** for that operation,
but **not a hard cap**. Health/trigger cycles, explicit reloads and retries
can add work. Over a 120 s operation window, v0.4.7's unconditional minute
pulls cost 1,080 GETs for that configuration: the typical two-render case
can therefore cost about **2× v0.4.7's config-pull work during the operation**.
Idle-hour behavior remains approximately **100× lower** for the reference
PostgreSQL polling profile (about 6.6 GET/s before versus 0.063 GET/s steady);
that workload-dependent comparison is not a configuration-only guarantee.
For v0.4.10 the plan is targeted reads/shared listing to reduce per-peer
render cost, per-cluster mutation/listing scoping, and a hard one-render/drift
bound. Those further improvements are not present in v0.4.9.

Own generation/peer/config changes remain prompt. A platform-only change is
normally detected within the 60 s listing cadence plus up to 5 s listing I/O,
then the next heartbeat (normally 3 s, plus up to 4 s sample collection) and
fresh config rendering/application. If a v0.4.9 agent's heartbeat path fails,
listing plus the next conditional minute pull can take approximately
**125 s before fresh rendering**, plus config I/O/application. Push health
evidence is already stale after 7 s and observation falls back to fresh
probes. Released v0.4.7 agents never send revisions and always receive full
60 s pulls; the longer conditional fallback does not apply to them.

### Active PostgreSQL operation cost

The idle-hour figures and proxy-render budgets are **not operation-wide
limits**. A PostgreSQL proxy-count operation can repeat a fresh reconcile
pass at the **two-second pending-receipt cadence**. An isolated experiment
with the production Manager, native engine provider, real Dokploy client and
fake API averaged approximately **100 GETs per PG pass**: `Sync` cost roughly 12–120 GETs,
fresh observations before and after it cost about 20 each, and degraded or
uncertain observations could immediately repeat with a new cache.

Expect **tens of GETs/s during active work**, roughly 15–50 GET/s rather
than the steady idle rate. The isolated experiment recorded **3,721 GETs
over 67.2 s** (about 55 GET/s including siblings); a live operation window
recorded **2,497 GETs over 110.5 s** (about 23 GET/s). In the isolated
experiment, 3,412 GETs belonged to PG and shared-target/sibling traffic was
only **309 GETs, or 8.3%**: listing-triggered sibling deep reconciles plus
sibling availability and one listing check. Immediate second observation
attempts alone added **1,130 GETs**. The live window does not establish
the same per-cluster attribution.

These mechanisms are unchanged **in kind** from v0.4.7: two-second pending
requeues, fresh lifecycle observations and the two-attempt observation
policy. v0.4.9's reference PG status pass is cheaper (**33 → 16 GETs**),
but this is not a measured v0.4.7/v0.4.9 total-operation comparison.
The **81-request harness scenario (78 GETs + 3 POSTs)** is one MySQL backend
count change, completion wait and fresh observation—not a complete PG
operation budget. Operation duration, phase transitions and retries matter.

For v0.4.10, the operation-pass plan is to avoid the immediate second
observation while a receipt is pending, deduplicate reads across a pass's
pre-/post-observations, use a slower pending cadence where safe and enforce
end-to-end per-operation budget tests. This work is additional to the
per-cluster notification/listing scoping above and is **not implemented
in v0.4.9**.



For drift bounds and an immediate fresh pass, see
[out-of-band platform changes and forcing a reconcile](operations.md#out-of-band-platform-changes-and-forcing-a-reconcile).

Use [monitoring](monitoring.md) to distinguish listing checks, snapshot hits,
steady renewals and lifecycle traffic.

## Choose and configure a provider

1. **Zerops:** review the [Zerops guide](zerops.md), then use the dashboard-only [Quick start](quickstart.md) or [Install](install.md). The default provider is `zerops`; supply `KARAKORAM_ZEROPS_TOKEN` and the project identity as documented there.
2. **Dokploy:** first prepare the Swarm, registry, overlay network and access policy in the [Dokploy guide](dokploy.md#requirements), then [install the control plane](dokploy.md#install-the-control-plane-on-dokploy). Set `KARAKORAM_BACKEND=dokploy` and the guide's `KARAKORAM_DOKPLOY_*` target settings.
3. **Nomad:** first prepare dedicated clients, the private-copy [Ubuntu](https://ubuntu.com/) rootfs, routed networking, ACLs and storage in the [Nomad guide](nomad.md#requirements), then [install the control plane](nomad.md#install-the-control-plane-on-nomad). Set `KARAKORAM_BACKEND=nomad` and the guide's `KARAKORAM_NOMAD_*` settings. The operator owns public HTTPS, load balancing and host firewalls.

For additional targets, [register a namespace](namespaces.md) using `--backend zerops`, `--backend dokploy` or `--backend nomad`, a write-only credential and the provider's non-secret `--config key=value` settings. The API keeps the field name `projectId` for the provider target ID. Provider kind and target identity are immutable; existing records and omitted kinds remain Zerops. Do not change an existing installation's default provider as an in-place cluster migration.

Compare [Operating costs](operating-costs.md) before choosing capacity, storage, public addresses and recovery objectives.
