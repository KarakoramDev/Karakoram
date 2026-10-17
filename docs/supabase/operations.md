<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Supabase operations

Database maintenance follows [PostgreSQL operations](../postgres/operations.md), [access/TLS](../postgres/access-and-tls.md) and [recovery](../postgres/backups.md). PostgreSQL 17 is the only admitted major. Source/target backup providers must match exactly; postgres↔supabase recovery is rejected.

## Components and health

Change desired replicas/resources under `spec.components.extra` and follow operation/generation convergence. Explicit zero disables a service, subject to dependency validation. Component application readiness is separate from database readiness. In member status, application releases use `componentVersion`; their database `version` is intentionally empty. Aggregate resource requirements include every enabled service, proxy and data member.

Gateway bootstrap readiness means its API listener is accepting TCP connections,
not that its upstream DNS records exist yet. This allows the gateway to join
before the remaining services are provisioned sequentially. Final stack `Ready`
(and observed-generation convergence) additionally requires **every live gateway
instance** to have applied the desired configuration and report at least one
resolved host in **each enabled Envoy upstream cluster**, including Studio. The
control plane obtains fresh runtime inventory and probes each instance directly,
preserving the logical member's TLS and bearer identity rather than sampling
service DNS. The same gate applies when adding or re-enabling components;
disabled/non-routed components do not participate. Component health probes still
check application readiness.

The gateway agent reads Envoy's `/clusters?format=json` only on
`127.0.0.1:8001` and reports credential-free `gatewayUpstreamHosts` counts in its
authenticated component status. That admin listener is never an infrastructure
port or public route. Missing/failed inventory, no live instances, unusable
individual addresses, unreachable instances, older configurations and
missing/unavailable upstream observations keep the stack non-Ready without
blocking the gateway's bootstrap join. Stopped or authoritatively disappeared
instances no longer participate; acknowledgements are not cached across inventory
snapshots. Upgrading requires both the control plane and member binary; gateway
configuration is refreshed on upgrade to install the admin listener.

STRICT_DNS refreshes every second with DNS TTL overrides disabled; failed
resolution retries back off from 250 ms to at most one second. The fixed refresh
also bounds successful empty-answer retries, reducing the old five-second delay
while discovery records appear during component creation. This modest per-service
DNS polling trades a little local discovery traffic for faster convergence; the
host-count gate, not timing assumptions, provides final readiness.

Components use pinned artifacts and a configuration digest. Relevant configuration or release changes trigger service configuration/redeployment one component at a time; component versions are not chosen with `spec.version`. A Karakoram release update can therefore change service versions. Review [artifact availability](components.md) first: a catalog preset or saved function source does not bypass missing-artifact admission.

Component member-agent TLS certificates use the database core's thirty-day renewal window. Renewal republishes an incremented member configuration even when application settings have not changed.

Configurations accepted while a component is explicitly stopped or held do not count as prepared. Before a later start or restart, including an explicit process action, migrations and tenant seeding must succeed for the current configuration digest. JWT rotation while Realtime is stopped therefore refreshes its tenant before that component resumes.

Database bootstrap includes the current migration plan in its reconciliation digest. Additive migrations apply to existing databases through new ledger entries without rewriting earlier receipts. Upgrading these migrations requires the matching data-member software as well as the control plane: members execute their embedded bootstrap plan.

Failed component infrastructure operations clear their pending receipt and report
the failure without applying the requested state. Reconciliation can retry after
the underlying placement or runtime problem is corrected.

A confirmed missing stateless runtime is re-imported and redeployed using its
existing durable member identity and ownership token. Lookup errors do not count
as absence, and conflicting ownership markers prevent adoption or modification.
Disabling gateway public HTTPS (or disabling the gateway component) revokes its
existing public route before database readiness checks, including when a core
member is unavailable. Pending infrastructure operations still serialize optional
component changes, but never block PostgreSQL provisioning, replacement or recovery.
Component polling errors are reported while core reconciliation continues;
enabling public exposure remains gated on core readiness.

Internal database connections use a local raw TCP forwarder on loopback 5432
(15432 for Supavisor). HA probes authenticated TLS Patroni `/primary` every second.
A negative response/error from the known writer revokes it immediately, without
waiting for other peers' probes. Leadership loss/change closes existing sessions;
services must reconnect. New sessions wait up to ten seconds for exactly one
confirmed writer instead of receiving an immediate EOF during an election, then
close if no writer is available. Cancellation/shutdown also ends that wait. This
does not replay SQL, preserve transactions, or override a client's own connection
timeout. Between probes, or while Patroni still reports the shutting-down member
as primary, a new connection can still reach the old database: this raw forwarder
does not inspect PostgreSQL errors or independently elect/fence writers. Strict
synchronous mode, DCS quorum loss and maintenance can interrupt API availability
even while stateless services are running.

Realtime supports one application replica, independently of the database's
single/HA topology. Create/update admission rejects any enabled count above one
with `realtime replicas must be 0..1`; provider catalogs advertise the same bound.
All presets select zero or one. The CLI submits optional-component file bodies
to that same server admission path rather than maintaining separate replica rules.
Realtime always renders an explicit empty `CLUSTER_STRATEGIES`, removing the
pinned default `POSTGRES` discovery topology. Its linked Postgrex notifications
process could exhaust supervision on database loss and exit Realtime even though
a singleton has no peers to discover. Ordinary database pools and logical
replication still reconnect through the forwarder; the member supervisor still
restarts any actual application exit. This removes an unnecessary failure path,
not all possible upstream exits.

**Previously stored multi-replica Realtime specs remain unsupported independent
nodes, not a distributed Realtime cluster.** The native release defaults every
member to `realtime@127.0.0.1`; the old PostgreSQL discovery ignored notifications
with its own node name, so replicas could not discover one another as distinct
nodes. PostgreSQL was discovery, not broadcast/presence transport. These replicas
do not share cross-replica broadcast/presence fan-out or cluster coordination.
The runtime declares only HTTP/agent ports and has no cluster-specific
distribution cookie. Upgrading removes the redundant discovery connection from
those old members too; it does not establish inter-node transport.

Admission is not retroactively run inside reconciliation: an already-stored
count above one does not wedge database/component reconciliation, nor does an
upgrade silently downscale the user's intent. To correct it, submit an explicit
update setting `spec.components.extra.realtime.replicas` to `1` (or `0` to disable).
An otherwise unrelated spec update that leaves the old count above one is
rejected; include the singleton correction in that same update. Until corrected,
the persisted count continues to reconcile as independent nodes and must not be
treated as Realtime HA. This compatibility of stored intent is not admission of
new multi-replica deployments.

The pinned runtime also supports `CLUSTER_STRATEGIES=DNS` (`DNS_NODES` service
hostname, `APP_NAME` node basename, five-second polling) and `EPMD` (explicit
`CLUSTER_EPMD_HOSTS`). DNS could remove clustering's database dependency when the
platform returns all instance IPs, but changing those two variables alone is not
equivalent clustering. It additionally requires unique routable `<APP_NAME>@<IP>`
identities (`POD_IP` is honored by this pinned release), compatible private address
families, a sealed per-cluster cookie, declared/reachable EPMD and fixed Erlang
distribution ports, gen_rpc connectivity, and multi-node broadcast/presence tests.
Those networking changes are not part of the singleton failover correction.

The optional `pgsodium` extension is available for plain cryptographic functions
only. Karakoram does not manage a pgsodium server root key and never adds
`pgsodium` to `shared_preload_libraries`; root-key-dependent key management and
transparent column encryption are not enabled.

Realtime's logical decoding uses the packaged `wal2json` output plugin. Supabase
data members explicitly trust it through Percona's `output_plugin_libraries`,
alongside the default `pgoutput` and `test_decoding` plugins.

Native services and their migration commands use the member's protected data
directory as `HOME` and the portable `C.UTF-8` locale; they do not depend on a
login shell's environment or generated system locales.

The optional Supavisor pooler uses a loopback distribution address for its native
release and imports the cluster CA into its tenant's DER trust field. Dynamic
database-role authentication remains enabled through `pgbouncer.get_auth`.

## Credentials and rotation

API paths are relative to `/api/v1/clusters/{cluster}`:

| Route | Purpose / authorization |
| --- | --- |
| GET `/supabase/credentials` | API/Studio URLs, anon/service-role and publishable/secret keys, dashboard credentials, JWT secret; `cluster-credentials:read`, no-store |
| PUT `/supabase/smtp-password` | Write-only `{password: ...}`; `clusters:update` |
| POST `/supabase/rotate-jwt-secret` | Durable JWT/API-key rotation; `clusters:update` |

```sh
karactl -n NS supabase credentials appapi
karactl -n NS supabase credentials appapi --show-secrets
karactl -n NS supabase smtp-password set appapi
karactl -n NS supabase rotate-jwt-secret appapi --yes
```

The CLI masks credential fields by default in every output format. Protect revealed output, clipboard and redirected files. SMTP reads a hidden prompt or first stdin line, never a password argument. SMTP settings other than the password belong in `engineOptions.auth.smtp`.

JWT rotation replaces signing/API-key material and rolls dependent service configuration; old application JWTs/keys stop working as the new configuration takes effect. It is not a dual-key overlap rollout. Coordinate clients and follow the returned operation. Independent dashboard password, database-role passwords and encryption keys are retained. Dashboard username is `supabase`. Service-role/secret keys bypass ordinary application restrictions and must never be embedded in public clients.

## Security boundaries

Postgres-meta runs with the PostgreSQL superuser credential. Its API binds
`127.0.0.1:8081`; its adjacent admin listener also remains on loopback at
`127.0.0.1:8082`. Private port `8080` is an authenticated metadata proxy:
requests need the cluster service-role API key in `apikey`, or the internal
service JWT produced by the gateway's opaque secret-key translation. Anonymous,
missing and invalid keys are rejected before reaching postgres-meta, including
requests from other workloads on the shared private network. Gateway `/pg/`
routes retain their service-key-only policy.

Studio's raw application binds only `127.0.0.1:3000`. Private port `3002`
authenticates the cluster service-role key in `apikey` and strips it before
forwarding to Studio. The gateway injects this credential only after dashboard
Basic authentication succeeds, replacing any caller-supplied value and removing
dashboard credentials. Private unauthenticated SQL requests are rejected.
Discovery, runtime ports and readiness all use the authenticated ingress.
Existing Studio runtimes are redeployed during upgrade so the backend port
declaration changes as well as the application configuration; a reload alone
would leave the authenticated ingress unreachable.

Studio reaches metadata through a loopback-only bridge on `127.0.0.1:8083`.
That outgoing bridge supplies the service-role key to the private metadata proxy.
These private HTTP hops use bearer credentials, not transport encryption:
isolate the workload network from untrusted packet observers and never expose
component ports publicly. Local processes running as the member's runtime user
remain within the trusted member boundary.

Realtime's raw administration listener cannot select a loopback bind address
through the pinned release's runtime environment. Instead its admin API uses an
independent, random, sealed per-cluster signing secret projected only to Realtime.
Application anon/user JWTs and even ordinary service-role keys cannot authenticate
its tenant-admin routes. Tenant seeding preserves the application JWT secret and
JWKS; readiness signs an admin probe with the independent secret. Seeding starts
the application under the admin secret before changing the environment read by
the seed script, avoiding a temporary shared-secret listener. Tenant websocket
and broadcast authentication continue using the seeded tenant credentials.

Studio's encrypted per-request metadata connection URLs carry the database TLS
policy explicitly, including cluster-CA verification when TLS is enabled.
The pinned Studio constructs these URLs from `POSTGRES_DB`, so that managed
environment value includes an escaped database path and connection query options;
it is not a bare database name. Disabling database TLS explicitly selects plaintext.

Member configurations are projected before digesting, publishing and local
caching. Each component receives only its consumed database-role credentials
and service secrets. Auth alone receives SMTP credentials and private asymmetric
signing keys; Storage alone receives S3 backend/frontend credentials; imgproxy, Vector
and the gateway receive no database passwords. Metadata, Studio and Functions
retain their required privileged database connection; protect their code and
administrative access accordingly. HA database consumers retain the Patroni
probe credential; enabled log shipping receives its ingestion credential.

The projection changes configuration digests, so existing components redeploy
once when upgrading to this behavior. The resulting member configuration cache
contains only projected credentials; the sealed control-plane credential store
remains the source of the cluster's complete secret set.

## Functions

GET `/supabase/functions` lists names; GET/PUT/DELETE `/supabase/functions/{name}` reads/replaces/deletes source. Reads require `clusters:read`; mutations `clusters:update`. Source responses are no-store. Execution requires the qualified Functions artifact and an enabled `functions` component; saving source alone does not enable a runtime.

```sh
karactl -n NS supabase functions list appapi
karactl -n NS supabase functions deploy appapi hello ./hello
karactl -n NS supabase functions get appapi hello
karactl -n NS supabase functions delete appapi hello --yes
```

A directory recursively maps relative paths to source text; a single file becomes `index.ts`. A nonempty `index.ts` is required. Maximum 100 functions and 1 MiB source text per function. Names match `[a-z0-9][a-z0-9_-]{0,62}` and cannot be `main`; no absolute/dot/parent paths, symlinks or special files. Sources are sealed at rest but readable with cluster-read permission: do not embed application secrets. The pinned runtime supplies the main dispatcher; `functions.verifyJwt` defaults true.

The agent writes sources and an empty shared `deno.jsonc` import map under its
persistent functions directory, outside the runtime's worker-private `/tmp`.
Use explicit `npm:` or `jsr:` imports rather than relying on undeclared aliases.

## Recovery boundaries

Database backups restore database data/roles, not Storage S3 objects or the whole control-plane secret/function store. Protect S3 objects separately and keep consistent control-plane state plus its original master key. A database clone has its own service configuration/keys/object tenant; it is not a complete copy of an application's external objects or deployment. Plan coordinated object/database recovery and re-check application access after restore, especially credentials restored from an older snapshot.

Suspension/removal follows shared cluster retention choices and PostgreSQL volume rules; optional services are stateless runtimes, not independently retained application disks. Preserve external S3 data according to your own policy. See [shared recovery](../backups.md) and [control-plane recovery](../operations.md#control-plane-state-backup-and-recovery).
