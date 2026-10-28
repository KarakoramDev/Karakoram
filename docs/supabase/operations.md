<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Supabase operations

Database maintenance follows [PostgreSQL operations](../postgres/operations.md), [access/TLS](../postgres/access-and-tls.md) and [recovery](../postgres/backups.md). PostgreSQL 17 is the only admitted major. Source/target backup engine drivers must match exactly; postgres↔supabase recovery is rejected.

When bootstrap inputs change, PostgreSQL writer-discovery status reads each have
a four-second budget. A deadline fails convergence closed with the member name
and a request to inspect responsiveness/connectivity; no SQL bootstrap or digest
acknowledgement follows missing writer evidence. SQL bootstrap mutations retain
their existing, independent administration budget.

## Monitoring

Application agents emit `karakoram_supabase_up` (readiness gauge, 1/0) and
`karakoram_supabase_restarts_total` (supervised process restart counter), each
labelled by `component`. Use reset-aware `rate()`/`increase()` for restarts.
These are component observations, not whole-stack health or PostgreSQL quorum;
missing scrapes are not healthy zeroes. PostgreSQL core metrics are separate.
See the [metric vocabulary](../monitoring.md#supabase-application-components)
and the bundled Supabase Grafana dashboard.

## Cluster restart and member software

`karactl clusters restart NAME --wait` first performs the [PostgreSQL](https://www.postgresql.org/)/[ProxySQL](https://proxysql.com/)
rolling restart, then restarts every live instance of each enabled [Supabase](https://supabase.com/)
component, one instance at a time. Disabled components are skipped. Each component
instance must show a changed supervised child PID, launch identity, restart count
or start time, acknowledge its configuration, and regain native readiness before
the next instance is restarted. Final stack readiness is required for success.
Missing instances, unknown inventory and unavailable probes cannot certify a
restart; inspect the named component/instance when the operation fails or waits.
A singleton is unavailable during its restart; this is not a zero-disruption
operation. Progress survives control-plane restarts.

This restarts **application processes in place**, not the member agent or its
deployment archive. An unchanged allocation, job version or agent artifact is
therefore expected. Without opt-in automatic rollout, a control-plane upgrade alone does not roll member agents.
Release/configuration reconciliation only redeploys components when its desired
runtime/version requires it; a new control-plane executable is not itself such
an intent. Restart is not an artifact-upgrade command.

Use the agent rollout command below to update member artifacts without changing
application versions or recreating component identities. Realtime's internal RPC
listener remains bound to loopback and must never be reachable.

Explicitly disabling an existing component can proceed while database readiness
is failing (including WAL retention caused by that component), but starting
runtime removal still requires an open maintenance window or an explicit override.
Public exposure withdrawal also needs that permission: backend routing changes
can replace the serving task. Already-submitted operations finish even after
the window closes. Wait for service removal, not just operation success:
the generation remains unacknowledged
until the database and remaining components are Ready. Re-enabling waits for core
Ready; it cannot deploy a replacement or expose a new public route during a core
readiness failure.

Database member-agent upgrades require a engine driver-driven database member
deployment using the PostgreSQL maintenance/recovery procedures; do not disable
database members or delete their volumes to emulate a component roll.

## Agent rollout and maintenance windows

`karactl -n NS clusters upgrade-agents NAME` first performs the [PostgreSQL](https://www.postgresql.org/)
replica-first/primary-switchover rollout, then redeploys enabled optional
component runtimes one at a time. Each component retains its runtime identity,
applied application version and published configuration, and must pass native
readiness and report the frozen target agent build before advancing. A component
with several instances completes only when every instance agrees on the target.
Expect a brief interruption for single-instance components.

Already-current agents are skipped unless `--include-current` is supplied.
Completed members are journaled across control-plane restarts; cancellation
waits for an in-flight member before stopping. A changed control-plane build
cannot start another deployment under the previous frozen target. Normal provider
deploy admission caps and retry budgets apply.

The PostgreSQL maintenance-window rules also apply here. Additionally, component
version/configuration redeploys or restarts, resource changes, scale-in and
component removal wait before starting a new disruption. Existing deployments
continue through readiness after window closure. Component runtime healing,
initial provisioning/scale-out and agent transport-certificate renewal do not
wait. See [PostgreSQL operations](../postgres/operations.md#agent-rollout-and-maintenance-windows)
for database leaf renewal's seven-day emergency exception and policy overrides.

## Components and health

Change desired replicas/resources under `spec.components.extra` and follow operation/generation convergence. Explicit zero disables a service, subject to dependency validation. Component application readiness is separate from database readiness. In member status, application releases use `componentVersion`; their database `version` is intentionally empty. Aggregate resource requirements include every enabled service, proxy and data member.
Enabling or disabling a component changes the shared upstream map in application configuration hashes, so the remaining [Supabase](https://supabase.com/) applications can restart in place as their configurations converge sequentially; this can take multiple passes and temporarily outlive the [15-second availability observation lease](../clusters.md), without restarting the [PostgreSQL](https://www.postgresql.org/) core.

Changing only an application's replica count or resource bounds (for example,
`components.extra.rest.resources.maxCpu`) does not publish a new PostgreSQL or
[ProxySQL](https://proxysql.com/) configuration or redeploy their runtimes. The database core tracks only
the engine and proxy component bounds. Database/proxy settings, managed access
revisions, and Supabase engine options still participate in database configuration
convergence, including extension preload changes.

Database slot-retention blockers report `Ready=False/SlotWALRetention`, including
when all optional components are healthy. An optional-component failure does not
overwrite that independent database diagnosis. Disabling the offending optional
consumer can still remove its owned service while that condition persists; this
does not clear slot health or acknowledge the generation early. See
[PostgreSQL slot health](../postgres/operations.md).

A smaller positive count supersedes a still-pending larger scale without first
waiting for the obsolete count. Terminal scale/resize failures retain their exact
requested count/resource target across restarts and report
`ComponentRuntimeFailed`; unchanged targets are not repeatedly submitted.
When the provider can prove the complete desired runtime already exists as healthy
owned allocations, reconciliation may adopt or start its placement-free recovery
receipt. Otherwise convergence keeps returning the terminal error so tracking
operations exhaust their existing bounded failure budget truthfully. A genuinely
corrected count/resource target gets a new attempt; disabling/deleting the
component remains available.
Resource corrections after a failed count are dispatched before retrying that
count, including when the corrected desired target differs from the failed one.
The replacement receipt records the corrected resources and full desired target;
if that correction also fails, the unchanged corrected target remains bounded.
Terminal receipt handling and corrected resource requests run independently of
database readiness. A temporarily unhealthy database core cannot hide a terminal
component error from the tracking operation's failure budget or prevent submitting
corrected bounds after a failed/superseded rollout. This does not automatically
replay an unchanged failed target, and database recovery continues independently.
Once maintenance permits the route change, gateway public-exposure revocation
takes priority over this failure handling. An unrelated terminal component
failure cannot prevent revocation or hide its accepted receipt; the component
error remains visible to bounded tracking.
Every accepted corrective receipt records the exact failed operation it corrects
and its full desired target, whether it changes resources or recovers retained
allocations. While that receipt owns the same component, failure and target, its
nonterminal wait does not charge the old failure again. Evidence remains recorded
until completion; a failed correction reports its own terminal outcome, and
unrelated failures still report normally.
After an upgrade, older pending receipts without this linkage are still observed
without replay when they match the same component, failed/current desired target,
and recorded resource-after-scale or retained-recovery shape. This compatibility
wait neither submits a new operation nor treats unrelated pending work as recovery.

Readiness requires the desired number of live instances for **every enabled
component** and an application/configuration observation from each live instance,
including extra instances during a rollout. The control plane pins probes to
provider-provided instance addresses while retaining the logical member's TLS and
bearer identity. A healthy service-DNS response cannot mask an unhealthy sibling.
Missing instances, inaccessible inventory, unusable instance addresses, stale
configuration and native health failures keep `ComponentsReady=False`; the
component member's `agent.message` identifies the failed instance and native
reason where available. Database health conditions remain visible independently.
After the dependency or instance recovers, fresh observations clear the condition;
an HTTP health failure alone does not trigger a blanket process restart.

If newly restored REST instances serve authenticated requests but their native
database probe returns HTTP 401, check that the control plane includes the
service-role credential in Auth/REST member configurations. After upgrading a
control plane that omitted it, normal convergence republishes the corrected
configuration even for instances still waiting for initial readiness. Instances
already running the database-backed probes do not need another agent artifact
roll for this credential correction. Leave the existing Running update in place:
do not retry the patch, cancel the operation, or disable/re-enable the component.
Completion still requires fresh applied-configuration and native serving success
from every instance; a control-plane upgrade alone is not evidence of recovery.

The final gateway upstream sweep emits an actionable gateway member, condition
and cluster message when observations fail or enabled upstreams are missing.
It withholds final Ready without revoking otherwise healthy gateway bootstrap
health: later upstream services must still be able to join sequentially, including
component additions and re-enables. Listener/bootstrap health is not proof of
complete native upstream evidence. Inspect every gateway instance's native status
and upstream connectivity.
`ComponentsReady` is a readiness gate (False while evidence is incomplete), not
an assertion that a timeout proves a native outage. PostgreSQL-derived conditions
retain the [database observation semantics](../postgres/operations.md).

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
host-count gate provides a discovery readiness observation, **not** proof that a
gateway can reach every upstream. Retained DNS hosts can outlive connectivity.
To bridge rollout discovery races (a replacement registered before listening or a
retired allocation still cached in DNS), every upstream route retries up to twice
on `connect-failure,refused-stream,reset-before-request`, preferring a host not
already attempted (three host-selection attempts). These are failures before
upstream processing; HTTP 5xx and resets after request headers were sent are
**not retried**, protecting non-idempotent Storage/REST writes. Existing route
timeouts still include retries; Storage streaming and Realtime websocket/longpoll
retain unlimited route/idle duration. An established websocket is not replayed.
The pinned [Envoy 1.39.1 retry semantics](https://www.envoyproxy.io/docs/envoy/v1.39.1/configuration/http/http_filters/router_filter#config-http-filters-router-x-envoy-retry-on)
include `reset-before-request`.

All upstream clusters passively eject a host after one local-origin failure,
separately from application errors, with one-second analysis sweeps and a
five-second base ejection (repeat ejections back off up to 30 seconds). HTTP 5xx
and success-rate ejection are disabled. The ejection cap is **50%**, with
`always_eject_one_host=false`: a singleton is never ejected, and two hosts can
eject one. This retains at least 50% healthy hosts, avoiding Envoy's default
panic mode (which would otherwise balance across unhealthy hosts below the
50% healthy threshold). See [outlier detection](https://www.envoyproxy.io/docs/envoy/v1.39.1/api-v3/config/cluster/v3/outlier_detection.proto).
With a reachable healthy alternative, pre-connect rollout failures can therefore
be absorbed instead of repeatedly surfacing stale-host 503s. This is not
zero-downtime protection: retry exhaustion, connection/route timeouts, large
streaming bodies exceeding Envoy's retry buffer, loss of all reachable hosts,
and in-flight requests on a killed member can still fail. Clients must reconnect
interrupted websocket sessions and decide whether a failed write is safe to retry.

Direct component probes do not traverse the gateway's network path; use
authenticated end-to-end route/application checks. A REST or Auth instance
failing native/serving readiness withdraws `ComponentsReady` and stack readiness
with an instance-scoped message, but remains in the gateway's resolved pool.
There are no application-level active health checks: [Nomad](https://developer.hashicorp.com/nomad) service checks test
only the listener, and [Zerops](https://zerops.io/) deploy readiness checks do not feed gateway
membership. An instance that accepts connections but returns [PostgREST](https://postgrest.org/) HTTP 503
is neither retried nor ejected for that response, so extra REST/Auth replicas do
not mask one instance's database-path loss. Restore or replace the instance named
in status; see [cluster restart and member software](#cluster-restart-and-member-software).

Storage readiness combines the native `/status` endpoint with an authenticated,
non-mutating [S3](https://aws.amazon.com/s3/) bucket `HEAD` from the Storage member, using its already-projected
provider registration and optional bucket override. The probe has the shared
four-second observation timeout; concurrent status/metrics callers share a sample
for at most the five-second readiness interval. Both successful and failed samples
expire, and a changed endpoint, bucket or credential invalidates the cache.
The control plane accepts a bucket-probe timestamp up to one second ahead of its
own clock to tolerate modest cross-host skew; larger future timestamps, zero
timestamps, and samples older than the fifteen-second readiness limit remain
invalid. This allowance does not extend the cache or turn a failed `HEAD` healthy.
Authentication/access denial, a missing bucket and a bounded observation failure
produce `StorageS3Unavailable` on `Ready=False` and `ComponentsReady=False`, with
phase `Degraded` when the database core is otherwise Ready; an independent database
blocker retains its own phase and reason. Healthy unrelated components retain their
own health. A fresh
successful check restores readiness without a component restart. No objects are
created, listed, changed or deleted by this check.

The probe retains and signs the endpoint's base path for legacy configurations;
new [storage registrations](../backups.md#register-s3-storage) require an origin
without a mount path so every backup engine can use the same registration.
Addressing shares the registration client's force-path rules: IP endpoints and
HTTPS dotted buckets use path style; ordinary custom DNS endpoints use virtual-host
style unless path style is forced.
Member application observations run with bounded instance concurrency, so several
slow S3 probes overlap rather than consuming the whole five-second status sweep
one member at a time. All live instances must still pass, including rollout extras.

Bucket `HEAD` requires bucket-level access and does not certify per-object read,
write or delete permissions, content integrity or a particular object's existence.
Diagnose those separately with fresh authenticated object operations and compare
acknowledged bytes after recovery. Older member binaries omit the S3 observation
and retain their previous readiness semantics; roll Storage members to obtain
dependency coverage. Present observations with invalid or expired timestamps are
not accepted as successful dependency evidence.

Components use pinned artifacts and a configuration digest. Relevant configuration or release changes trigger service configuration/redeployment one component at a time; component versions are not chosen with `spec.version`. A Karakoram release update can therefore change service versions. Review [artifact availability](components.md) first: a catalog preset or saved function source does not bypass missing-artifact admission.

Component member-agent TLS certificates use the database core's thirty-day renewal window. Renewal republishes an incremented member configuration even when application settings have not changed.

Configurations accepted while a component is explicitly stopped or held do not count as prepared. Before a later start or restart, including an explicit process action, migrations and tenant seeding must succeed for the current configuration digest. JWT rotation while Realtime is stopped therefore refreshes its tenant before that component resumes.

Database bootstrap includes the current migration plan in its reconciliation digest. Additive migrations apply to existing databases through new ledger entries without rewriting earlier receipts. Upgrading these migrations requires the matching data-member software as well as the control plane: members execute their embedded bootstrap plan.

Failed component infrastructure operations clear their pending receipt and report
the failure without applying the requested state. Reconciliation can retry after
the underlying placement or runtime problem is corrected.

A confirmed terminal component deployment failure is retained across retries and
control-plane restarts. `Ready=False` and `ComponentsReady=False` report
`ComponentDeploymentFailed` with phase `Degraded`, even during initial creation.
The member message includes the immutable provider service and deployment handle,
the terminal platform status, and a request to inspect deployment/runtime
preparation diagnostics. On Zerops, `version:<id>` identifies the exact app
version; inspect its deployment process and prepare-runtime log, not merely the
service's imported/creating state. Successful preparation alone does not prove
that Zerops created a runtime instance.
Container-inventory errors are appended to this evidence, rather than replacing
the deployment handle or last observed platform status.

A submitted component deployment reports `ComponentDeploymentPending` rather
than an unexplained imported stage. Its member/condition message identifies the
immutable service/version handle, last observed platform phase (for example,
`PREPARING_RUNTIME`), time observed in that phase, and submission age. Phase age
is checkpointed and survives control-plane restarts; older in-flight receipts
start tracking when first observed after upgrade. Pending work does not imply a
terminal failure or justify replay. A previous confirmed failure remains the
higher-priority diagnosis while its retry is pending.

An ambiguous or still-running platform operation remains pending and is not
blindly replayed. A confirmed terminal outcome releases the pending deployment
for automatic retry. Zerops can also retire a definitively unsubmitted
infrastructure attempt as `RETRY_REQUIRED` after its request-settle evidence
gate; that is not a terminal platform deployment failure and does not create or
replace a `ComponentDeploymentFailed` diagnosis. The previous failure remains
visible while its retry is pending and clears when a deployment succeeds.
Zerops retries confirmed deployment/runtime-preparation failures using the same
uploaded version with at most three 15/30/60-second backoffs. Another confirmed
failure enters durable `DeploymentRetriesExhausted`; reconciliation continues
observation but stops deployment submissions until an explicit audited
`karactl clusters retry-deployments NAME` grants one fresh three-retry budget.
Restarts retain the attempt count, terminal state and retry epoch. Failure
conditions preserve sanitized platform diagnostics. RUNNING/unknown work remains
fenced regardless of age or elapsed deadlines. A corrected artifact/runtime
intent can supersede a conclusively terminal receipt under the same
terminal-process evidence gate.
The engine driver exposes a `Deployment/COMPONENT_HOSTNAME` condition with the precise
`DeploymentRetriesExhausted` reason and operator command. An exhausted component
is skipped while other components continue reconciliation; pending deploy
receipts are retired without marking the failed artifact applied. There is no
scheduled retry deadline after exhaustion: the retry epoch must be re-armed.
Application/configuration and instance health must still pass before Ready.
An enabled component's terminal deployment failure also keeps combined
`Ready=False` when the deployment guard has already set phase `Degraded`.
An independent database readiness failure retains its own reason; healthy
database members do not imply that the complete enabled stack is ready.
Disabled optional components do not participate in this readiness gate.
Database recovery continues while the optional deployment is pending or failed.
Correct a provider placement/runtime problem using the exact failed handles; do
not clear operation receipts, start unverified runtimes, or disable Storage
merely to make the cluster Ready.

Component imports first persist the original request without bearer credentials.
After an interrupted setup, an owned provider reservation is replayed with the
same resources and replica count before applying subsequent desired edits or
component removal. Its presence alone does not authorize deployment. Preserve the
original import intent and provider state if an incomplete reservation is reported
without a matching intent; do not adopt or recreate it as a different request.
Cluster deletion likewise finishes reserved setup before removing its services,
without deploying the component. Accepted import handles retain the same replay
ownership instead of waiting indefinitely for setup which still needs replay.

A confirmed missing stateless runtime is re-imported and redeployed using its
existing durable member identity and ownership token. Lookup errors do not count
as absence, and conflicting ownership markers prevent adoption or modification.
Disabling gateway public HTTPS (or disabling the gateway component) revokes its
existing public route before database readiness checks, including when a core
member is unavailable, once the maintenance window or generation override admits
the potentially disruptive route update. Pending infrastructure operations still serialize optional
component changes, but never block PostgreSQL provisioning, replacement or recovery.
Component polling errors are reported while core reconciliation continues;
enabling public exposure remains gated on core readiness.
Explicitly disabled existing components use the same ownership-checked deletion
path before core readiness, after pending receipts and frozen import replay have
finished. This cleanup does not deploy or positively scale optional components;
re-enable and new public exposure wait for core Ready. Dependency validation still
requires dependents to be disabled first.

Internal database connections use a local raw TCP forwarder on loopback 5432
(15432 for [Supavisor](https://github.com/supabase/supavisor)). HA probes authenticated TLS [Patroni](https://patroni.readthedocs.io/) `/primary` every second.
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
See the [frozen PostgreSQL primary boundary](../postgres/operations.md#failover-suspension-and-deletion): a suspended postmaster with healthy Patroni does not trigger automatic failover.

Realtime supports one application replica, independently of the database's
single/HA topology. Create/update admission rejects any enabled count above one
with `realtime replicas must be 0..1`; engine driver catalogs advertise the same bound.
All presets select zero or one. The CLI submits optional-component file bodies
to that same server admission path rather than maintaining separate replica rules.
Realtime always renders an explicit empty `CLUSTER_STRATEGIES`, removing the
pinned default `POSTGRES` discovery topology. Its linked [Postgrex](https://github.com/elixir-ecto/postgrex) notifications
process could exhaust supervision on database loss and exit Realtime even though
a singleton has no peers to discover. Ordinary database pools and logical
replication still reconnect through the forwarder; the member supervisor still
restarts any actual application exit. This removes an unnecessary failure path,
not all possible upstream exits.

A Realtime restart or replacement closes client channels; clients receive a channel
error and must resubscribe. Client SDKs may attempt to reconnect automatically,
but applications must monitor subscription state and handle recovery themselves;
Karakoram does not guarantee client-side recovery. Postgres Changes committed while
a channel is disconnected are not replayed after it resubscribes. Changes committed
around a PostgreSQL primary failover can also be missed: Realtime re-creates its
temporary replication slot on the new primary, even while the SDK channel remains
`SUBSCRIBED` without a `CHANNEL_ERROR` or resubscription. Treat Realtime as a
notification stream, not a durable change log. Consistency-critical consumers must
reconcile from the source table after reconnects or database failovers rather than
relying on channel state as proof of complete delivery.
During a Realtime outage the cluster reports `ComponentsReady=False`
with the component's current observation error and returns to Ready once the
replacement is serving and all other readiness conditions hold.

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
families, a sealed per-cluster cookie, declared/reachable EPMD and fixed [Erlang](https://www.erlang.org/)
distribution ports, gen_rpc connectivity, and multi-node broadcast/presence tests.
Those networking changes are not part of the singleton failover correction.

The optional `pgsodium` extension is available for plain cryptographic functions
only. Karakoram does not manage a pgsodium server root key and never adds
`pgsodium` to `shared_preload_libraries`; root-key-dependent key management and
transparent column encryption are not enabled.

Realtime's logical decoding uses the packaged `wal2json` output plugin. Supabase
data members explicitly trust it through [Percona](https://www.percona.com/)'s `output_plugin_libraries`,
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

[Postgres-meta](https://github.com/supabase/postgres-meta) runs with the [PostgreSQL](https://www.postgresql.org/) superuser credential. Its API binds
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
Existing Studio runtimes are redeployed during upgrade so the provider port
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
signing keys; Storage alone receives [S3](https://aws.amazon.com/s3/) provider/frontend credentials; [imgproxy](https://imgproxy.net/), [Vector](https://vector.dev/)
and the gateway receive no database passwords. Metadata, Studio and Functions
retain their required privileged database connection; protect their code and
administrative access accordingly. HA database consumers retain the [Patroni](https://patroni.readthedocs.io/)
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

Database backups restore database data/roles, not Storage [S3](https://aws.amazon.com/s3/) objects or the whole control-plane secret/function store. Protect S3 objects separately and keep consistent control-plane state plus its original master key. A database clone has its own service configuration/keys/object tenant; it is not a complete copy of an application's external objects or deployment. Plan coordinated object/database recovery and re-check application access after restore, especially credentials restored from an older snapshot.

Suspension/removal follows shared cluster retention choices and [PostgreSQL](https://www.postgresql.org/) volume rules; optional services are stateless runtimes, not independently retained application disks. Preserve external S3 data according to your own policy. See [shared recovery](../backups.md) and [control-plane recovery](../operations.md#control-plane-state-backup-and-recovery).

Cluster deletion supersedes a component's deployment/readiness wait. The owned
service's immutable-ID and ownership checks still apply; an in-flight deployment
receipt is not a prerequisite for submitting deletion. This does not override
[Zerops](https://zerops.io/)'s own per-service process queue: an accepted `stack.delete` may remain
`PENDING` behind that service's `RUNNING stack.deploy`, including runtime
preparation. Deleting reports the exact delete process/service, platform status
and age derived from platform timestamps; when available, it also names the
same-service running process and its age. PostgreSQL cleanup preserves this diagnostic
in cluster status and the tracked delete operation.

Only observation is retried while a deletion is accepted: each platform read has
the client's finite request timeout and bounded transient retries (three retries
by default). Queue inspection is additionally bounded by the provider wait timeout.
An unavailable queue inventory is reported, not treated as permission to replay
deletion. Inventory failures propagate to reconciliation's failure backoff;
`429`/`503` responses with `Retry-After` retain that reset and stop the observation
before further platform requests. Reconciliation keeps the accepted handle across restarts and does not
resubmit it on a timer, cancel a running deployment, or infer successful deletion
from age. A CLI wait timeout stops that client's wait, not accepted cleanup.
Inspect/escalate the named platform processes if they stop progressing; confirmed
terminal failure or expiry returns to the existing cleanup retry/error policy.
