# 0070 — Agent heartbeats and a platform read budget

Status: Accepted

## Context

Up to v0.4.7 every active cluster was observed by pulling: each reconcile pass (requeued at most five seconds after the previous one), each independent availability renewal and each backoff observation re-read the platform services and runtime tasks of every member and dialled every agent and every pinned proxy instance. Caches were pass-local by design, so nothing was shared between passes, loops or clusters. In relay-backed namespaces every pinned proxy connection additionally re-enumerated the member's tasks to authorize the dial.

On Dokploy a MySQL Group Replication cluster with three proxies cost 66 GETs per status pass in v0.4.7 (28 after the v0.4.9 pass-local snapshot fix), PostgreSQL with two proxies 33 (16), Neo4j 12 (8). A lab with three such clusters measured 29 GET/s. Dokploy authenticates every REST request twice and better-auth writes the API-key row twice per verification, so each GET costs four `apikey` UPDATEs; the lab's Dokploy PostgreSQL sustained 116 UPDATE/s, about 1 MB/s and 233 write IOPS with no user activity. Request count is the only lever on the control-plane side. Zerops and Nomad have the same structure (estimated 18–30 and 38–77 requests per GR pass); Nomad adds placement and stop diagnostics per member on every pass.

The goal for v0.4.9 is that an idle cluster costs less than 0.1–0.2 platform requests per second on every provider, with bursts only while operations run, without slowing failure detection beyond 15 seconds and without letting any safety decision consume cached state.

## Decision

### Health is pushed by agents

Member agents push heartbeats to the control plane over the existing authenticated agent→CP HTTPS channel (`POST /v1/agent/heartbeat`, the same bearer, CA and listener as configuration retrieval; relay-backed namespaces carry it through the relay's existing agent stream). A heartbeat carries:

- the agent session (random per process) and runtime identity: task/allocation ID where the platform exposes one (`KARAKORAM_TASK_ID`, `NOMAD_ALLOC_ID`), hostname and interface addresses;
- the generic `/v1/status` document;
- results of the read-only native observations the control plane asked for in its previous reply.

The control plane learns which native reads to request from the reads it actually performs: every allowlisted read action (`mysql/status`, `mysql/proxy-status`, `mysql/galera-status`, `postgres/status`, `neo4j/readiness`, `weaviate/readiness`, `elasticsearch/readiness`, `rabbitmq/observe`, `keycloak/status`, `supabase/status`, `temporal/status`, `openbao/status`, `datamcp/readiness`) issued against a member in an observation context becomes a subscription for 10 minutes, renewed whenever the read is performed or served from a pushed sample (so subscriptions survive a 30-minute steady period). The agent enforces the same allowlist independently: the control plane cannot make an agent execute any other action periodically. Each action is executed in-process through the agent's own handler with the 4-second observation timeout.

**Conservative sample time.** Every heartbeat reply carries a fresh opaque token. The next heartbeat echoes it and its samples are taken after the reply was received, so the control plane timestamps the samples with the *issue time of the echoed token* on its own clock. This is a lower bound of the real sample time that is immune to agent clock skew and to buffering in the relay or network; a sample can only look older than it is, never newer. Unknown or expired tokens are answered with a new token and their samples are discarded. After a control-plane restart the first heartbeat of every agent is discarded the same way.

**Numbers.**

| Parameter | Value | Why |
|---|---|---|
| Heartbeat interval | 3 s (sent in every reply; agent clamps 1–60 s) | Below the 5 s evaluation cadence, so every evaluation finds a sample younger than one interval plus transit. |
| Pushed sample usable age | 7 s | Two intervals plus 1 s: one lost heartbeat does not cause a fallback; two do. |
| Availability evaluation | every 5 s (`ReadinessInterval`, unchanged), plus immediately on a failed sample, a new agent session/identity or a member's samples becoming stale | Event-driven where it matters, cheap otherwise. |
| Health lease | 15 s (`ReadinessMaxAge`, unchanged), anchored at the **oldest** evidence used | Positive claims never outlive their evidence by more than 15 s. |

**Detection.** A database failure with a live agent is reported by the next heartbeat (≤ 3 s): a failed observation wakes evaluation at once, a successful observation reporting a degraded engine is evaluated at the next 5-second tick (≤ 8 s in total). A killed container, crashed agent or partitioned member stops heartbeating. Its newest samples carry the issue time of the token echoed in the last heartbeat, about one interval before that heartbeat arrived, so they become unusable at most about 4 s after the last heartbeat was received. That transition wakes evaluation within about 1 s; because the member is known to push, the steady attempt fails immediately instead of pulling, and the unchanged fresh two-attempt observation decides. A refused connection fails at once (≤ about 6 s after the last heartbeat); a black-holed member exhausts two 4-second probes (≤ about 14 s). Independently, the API stops projecting Ready no later than 15 s after the oldest evidence of the last positive sample. v0.4.7 detected the same failures at the next 5-second pass plus up to two 4-second probes (≤ 13–15 s), so detection is not slower and native failures are faster. Members that have never pushed (older agents) are pulled inside the steady attempt, exactly as before.

**Binding to the runtime.** A pushed sample is served only for the member whose bearer authenticated it. Pinned instance reads are served only if the pinned address matches the heartbeat's identity (an interface address, or a Dokploy instance hostname ending in the reported task ID). Logical reads (the member's service endpoint) are served only from the agent session that answered the most recent successful live logical read on a newly opened connection: every agent response carries its process session in `X-Karakoram-Heartbeat-Session`, the same session its heartbeats carry, and a live response received on a new connection binds that session; a live response without the header (an older agent behind the endpoint) clears the binding, with binds and clears ordered by request start rather than response arrival. A binding is a 10-second lease measured from that request's start: after it expires the steady attempt probes the logical endpoint live through a connection that is never reused, so the probe follows current routing; it re-binds the current responder, clears the binding for an older agent, or fails for a dead one. Within an unexpired lease, a bound session whose samples are stale fails the attempt at once. A predecessor that keeps pushing after its successor took over, or after the endpoint was re-routed without any live response reaching the new runtime, therefore cannot renew the member's health for more than one lease plus one evaluation interval. Live logical probes cost an agent request but no platform request. A new session or a changed identity invalidates the target's platform snapshot and triggers a full reconcile. All replicas of a logical member hold the same credential today, and any of them can already answer that member's probes; this decision does not widen that trust boundary.

**Bounded state.** The push store keeps at most 16 sessions per member, only subscribed samples plus the generic status, at most 15 native subscriptions and 64 KiB of subscription requests per member and 1 MiB of retained responses per session; sessions are pruned 5 minutes after their last heartbeat. Excess sessions are refused rather than evicting a known one, and refusal disables pushed serving for the member until it clears. Heartbeat requests are limited to 1 MiB and 16 samples of at most 256 KiB.

### Platform reads only for platform facts

Platform responses are shared per target (Dokploy environment, Zerops project, Nomad namespace and region) by a snapshot below each provider's HTTP client, keyed by the exact GET URL:

- Every successful GET populates the snapshot, unless a mutation of the same target was in flight or completed while it ran.
- Every non-GET request through any client of the target advances the target's mutation epoch before it is sent and again when it returns; all entries become unusable at the first advance.
- Snapshot entries are served **only** to reads carrying the steady-observation marker (below), never to lifecycle reads, mutations, operation polling, admission or dial authorization. `WithoutObservation` and `WithFreshTaskInventory` remove the marker.
- Entries are served for at most 40 minutes, which outlives one steady deep revalidation. Before serving, the target's batched listing must have been checked within the last 60 s: Dokploy `environment.one` (one GET), Zerops `GET /project/{id}/service-stack` (one GET per 100 services), Nomad `GET /v1/jobs?meta=true` and `GET /v1/allocations` (two GETs). The listing is reduced to a fingerprint of stable fields; a changed fingerprint drops the whole target snapshot and advances the target's drift generation, and every steady cluster of the target that sees the new generation at its next renewal (≤ 5 s) leaves steady state and is reconciled. A failed listing check means nothing is served until a check succeeds.
- The first successful listing of a target only establishes its baseline, and it also drops every entry populated before it and advances the epoch, so no fact read before the baseline (or by a read still in flight across it) can be certified by it. A cluster's drift reference is captured before the reconcile that would make it steady; if the target's drift generation moved during that reconcile, the cluster does not enter steady state and is reconciled again at the normal cadence, so no drift is absorbed into the reference.
- Memory is bounded per target: 4 MiB per retained body, 64 MiB of bodies (oldest evicted first) and 4096 entries; expired entries are pruned on access at most once a minute. The process-wide registry holds targets weakly, so a target no client uses any more is reclaimed with its bodies.

60 s is the drift bound for out-of-band platform changes that are invisible to agents and visible in the listing; everything else is revalidated by the 30-minute deep reconcile. Changes that affect serving — a container dying, restarting or being replaced — are seen by heartbeats within seconds, so a longer platform cadence does not delay failure detection. One listing per minute per target costs at most 0.017–0.033 requests per second for all clusters in it together.

| Out-of-band change by someone else | Noticed |
|---|---|
| Member container/task killed, restarted, replaced; agent or database down | Heartbeats: seconds (see Detection) |
| Dokploy application deleted, created, renamed, stopped/started (`applicationStatus`), moved to another server, ownership description edited | Listing: ≤ 60 s |
| Zerops service deleted, created, renamed, stopped/started, mode/version/base changed, ports or subdomain access edited, autoscaling (resources, container counts) edited | Listing: ≤ 60 s |
| Nomad job deleted, stopped, re-registered (new version/modify index) or its `karakoram.*` meta edited; allocation added, replaced, rescheduled to another node or status changed | Listing: ≤ 60 s |
| Dokploy environment variables, mounts, resources, replicas, domains, ports, image or deploy settings edited without a redeploy; Zerops environment variables, domains or settings outside the service listing; Nomad changes not reflected in job version/status or allocations | Deep reconcile: ≤ 30 min |

Operators can always force an immediate full, fresh reconcile with `POST /api/v1/clusters/{cluster}/reconcile` (`karactl clusters reconcile CLUSTER`). It clears steady state and triggers the worker; repeated calls are coalesced so that at most one forced reconcile per cluster runs every 30 s, which keeps the endpoint from recreating the old polling load. Any spec change or operation reconciles immediately as well.

The namespace registry treats the relay the same way: its liveness is pushed through its hub session, so while a relay is settled (connected, running the current executable, no pending operation, secret rotation or certificate renewal, last check successful) its platform service is re-read at most every 60 s instead of on every 15-second namespace tick; anything unsettled is checked on every tick as before.

Platform snapshots have their own freshness class. The 15-second health lease applies to health evidence (pushed or pulled agent samples), not to platform facts; platform facts used by a steady observation are bounded by the 60-second listing check and the 40-minute entry age.

### Steady observation and its fallback

A cluster is **steady** after a reconcile in which the provider's `Sync` (and the Data MCP companion's, when one was reconciled) reported `Steady` — fully converged, no journal, receipt, rolling step, acknowledgement or maintenance wait pending — and that finished without errors or convergence errors, with the generation acknowledged, phase Ready, no pending or running operation, no deployment failures, and not deleting, suspended or restoring. Engines previously returned a routine 10–15 s requeue from their converged branch; they now return `Steady` with no requeue, or with a genuine timed wake-up (for example a MySQL PITR re-anchor stability or cooldown deadline, or a Citus restore-point quarter hour). Any trigger, operation, non-Ready availability result or listing drift clears it; a rejected steady attempt whose fresh fallback is Ready keeps it.

For a steady cluster, availability renewal makes one **steady attempt** under the steady-observation marker, in a new observation pass: agent reads are served from pushed samples, platform reads from the snapshot, and misses go to the network — except that a member which pushes heartbeats but has no usable sample fails the attempt at once instead of being pulled. The attempt is accepted only if it is a complete positive result by the existing criteria (no error, phase Ready, no Unknown/Unreachable conditions, every agent healthy with a timestamp) **and** the target's mutation epoch did not move while it ran. Its published `ObservedAt` is the oldest pushed sample time used (or the attempt start, if older), and each member's `Agent.LastSeen` is that member's oldest pushed sample time. Anything else — a negative, partial or failed result, a stale pusher, an epoch change — discards the attempt and runs the unchanged v0.4.7 two-attempt fresh observation. A cache can therefore never produce a negative result, and a positive result never outlives its evidence.

Steady clusters are reconciled every 30 minutes instead of every 5 seconds (or at the provider's timed wake-up if earlier), and the 30-second discovery sweep does not wake a steady cluster whose generation is unchanged and which has no pending or running operation. Reconciles are still triggered immediately by spec changes, operations, a forced reconcile, a non-Ready availability result, a listing change and a new agent session or identity. Clusters that are not steady keep the v0.4.7 cadence and fresh reads; this is the operation burst. 30 minutes is a compromise between the idle budget and repairs that only a full `Sync` performs (drift outside the listing, slow certificate or managed-access repairs); at 10 minutes the MySQL and PostgreSQL topologies below would exceed 0.1 requests per second on every provider (see Consequences). Snapshot entries live 40 minutes so that a steady cluster does not refetch between two deep reconciles.

Only independent availability renewal uses the steady attempt. Its provider-owned side effects are already discarded and only a fenced availability sample can be published. A Data MCP companion is observed after an accepted provider attempt with its own steady evidence; a rejected companion sample is replaced by a fresh companion observation, and if the provider's snapshot proof became invalid meanwhile, provider and companion are both observed fresh. Accepted companion evidence can only make the published timestamps older. The observations inside a reconcile (before and after `Sync`) feed durable provider state, so they stay fresh; a steady reconcile is also the periodic deep revalidation of everything the listing cannot see.

The steady-entry drift baseline is captured from the successfully resolved backend before convergence evidence is collected; it must not introduce a second namespace lookup that bypasses dependency-failure backoff. API request-caller attribution preserves the router's matched pattern on the original request so access logs retain route templates, including escaped RabbitMQ vhost paths.

### Configuration pulls are conditional, not cached peer discovery

Every updated member agent still pulls configuration every 60 seconds. It sends
the opaque revision of the last configuration it **successfully applied**; a
failed apply cannot authorize an unchanged reply, and startup always performs a
full pull. The control plane answers HTTP 304 only when the actual applied
payload's certificate matches unchanged inputs and a current steady baseline.
This bypasses configuration rendering and per-member platform reads. If the
target listing proof has expired, the pull first performs **one shared
single-flight listing check** (one Dokploy GET without retries), attributed to
`listing`, rather than a nine-GET proxy render. All engines and Data MCP
companions use the same protocol. MySQL GR/PXC proxy and Neo4j config rendering already use persisted
membership and secrets, so their minute pulls had no platform cost; PostgreSQL
proxy rendering independently validates every joined data peer with `GetService`
(nine Dokploy GETs per peer per proxy instance). Two proxies and one data peer
therefore previously added 180 GETs in ten idle minutes outside the observation
budget. Relay runtimes use the hub protocol, not this member-config endpoint.

The revision binds the control-plane process epoch, member, capabilities, desired
generation/specification, persisted membership state, sealed configuration and
agent-token records, and target mutation/drift generations. The manager must
still certify a fresh Ready observation from this process, the same steady
generation, no deletion/deployment failure/active operation, no reconcile in
progress, and no target listing drift. The target must have a successful listing
strictly less than 60 s old and no mutation in flight. Proof time is the listing
request's **start**, not its completion; HTTP latency must not shift the normal
60 s check onto a later 65 s renewal tick. A stale config proof is refreshed
through the shared listing path with a five-second request budget; there is
**no extended freshness/grace interval** and no slower listing policy. This
preserves the documented peer-change bound instead of permitting older proof.

Every successful full response carries a nonempty token derived from its
actual rendered payload and input fingerprint. A bounded process-local cache
stores only the last certificate per authenticated cluster/member/capability
combination (at most 4096 entries), never the secret payload. Conditional use
requires unchanged inputs across rendering and a valid **render-end** steady
baseline. If eligibility becomes known during a full render, that response can
already establish the usable certificate; the next pull need not render again
to recover an empty token. A different fresh peer set cannot reuse an old
input-only token. Genuinely changing or uncertified inputs, independent live
dependencies and cache misses still require fresh rendering; the body token
alone never authorizes 304. No secret plaintext is exposed in a token.

Full rendering and certificate publication are serialized for each authenticated
cluster/member/capability key. The gate covers the actual provider peer reads
through final certification; admission numbers alone cannot order observations
inside concurrent renderers. Different keys remain parallel. Acquisition honors
request cancellation and has a 30 s maximum queue wait. The bounded gate registry
retains active holders/waiters, evicts only idle gates, and fails closed under
capacity pressure. Certificate eviction cannot create a second gate for an
in-flight key. No global cache mutex is held through provider I/O. Redundant
render sequences and eviction watermarks are not used.
The mutex, certificate map and render-gate registry are owned by one state
pointer allocated with the Server. Request-local Server copies used for public
logging/correlation therefore share the complete synchronization domain and
never copy a live mutex away from its protected maps.

The dependency digest also includes namespace backup-storage settings and sealed
credentials, including cross-namespace storage scopes used by Elasticsearch,
and the provider runtime layout. Replacing storage without changing the cluster
therefore invalidates both conditional pulls and notification stamps. Retained
local apply retries acquire the same ordering lock as network reloads before
selecting their pending payload. Every apply outcome publishes or clears the
advertised revision under that ordering; an older equal-version retry cannot
overwrite a newer successfully applied payload.

Unchanged responses acknowledge only the change stamp attached to the matching
payload certificate, via `X-Karakoram-Config-Change-Ack`; they never acknowledge
an unrelated latest state. Full responses use the final eligible proof's stamp
when certification succeeds. Agents advance their applied stamp on this
authenticated acknowledgement only while the same payload token remains applied.
Before starting a notified reload they also recheck the reply's desired stamp,
so an old in-flight heartbeat cannot reload an already acknowledged target.

Updated agents report a content-only stamp in a heartbeat HTTP header, not the
strict JSON request envelope (older control planes ignore the header). This
stamp binds canonical config-relevant content, not raw observation journals,
phase-only changes or unrelated secrets. PostgreSQL proxy notices include
their own cluster generation. On Dokploy, notices and certificates additionally
bind exact member-service listing dependencies rather than the shared target
mutation epoch. A renewed listing is required after any mutation before
certifying 304; unrelated clusters cannot invalidate eligible minute pulls.
Nomad/Zerops retain global platform proofs. PostgreSQL skips live reads only
for the notification hint; full pulls retain mandatory fresh peer resolution.
Independent live dependencies and unknown sources fail safe to conservative
notices. When tracked content is unchanged, pure force notices wait until a
steady baseline returns: one notice-driven full render per proxy per settled
forced pass, rather than renders at both steady transitions. Actual tracked
content changes still notify immediately, even while nonsteady. Independent
minute pulls, changed inputs, errors and retries are not covered by that bound.
Old agents omit the header and retain full-response/notice compatibility.
If two consecutive notified reloads for the same target return unchanged,
updated agents limit further notices for that target to the ordinary 60 s
cadence and log this condition once. A distinct nonempty target bypasses that
backoff immediately; its 304 counter is independent. Every successful full
apply, including an ordinary minute pull, resets obsolete backoff state.
Missing-target legacy replies remain bounded without assuming a new target.

Own generation, relevant persisted peer membership and credentials notify
promptly, including during an operation. Platform-only peer recovery/removal/
replacement instead uses listing drift or the ordinary minute pull. With
working v0.4.9 heartbeats, normal detection-to-render-start is approximately
60 s listing cadence + up to 5 s listing I/O + 3 s heartbeat wait; bounded
sample collection can add another 4 s. Config HTTP/render/apply time follows.
If the mutation epoch advanced after the last applied full payload, the next
minute pull independently resolves peers freshly.

A v0.4.9 agent whose heartbeat path is failing can retain conditional pulls:
its fallback is at most 60 s to listing + up to 5 s listing I/O + 60 s to the
next pull (approximately 125 s before that pull's rendering/application).
This is a degraded push path: pushed health evidence expires after 7 s and
observation falls back to fresh probes; failed probes cannot renew readiness.
Successful fresh probes can still establish health, so this does not imply
the database must be unhealthy. These cadence bounds assume working control
plane, provider and config transport; failures can delay completion further.
This external/no-new-epoch fallback is **not** a promise of the same maximum
delay as v0.4.7. Released v0.4.7 agents send no revision, never receive 304 and
continue unconditional full pulls every 60 s; they cannot enter this
conditional-without-notices mode.


Inputs requiring independent live evidence explicitly forbid a conditional
response: OpenBao external SQL bindings/connected-cluster CA resolution,
initializing MySQL GR clone-donor discovery, legacy PSMDB RAM discovery, missing
Elasticsearch storage-reference resolution, and legacy Neo4j maintenance-time
decisions without accepted config options. These retain their original full
rendering path instead of pretending the local revision certifies those facts.

**Fresh-peer safety gate:** conditional pulls do not serve cached provider
responses and never remove the PostgreSQL peer-status check from actual config
rendering. Any potentially changed config or uncertain baseline still resolves
peers freshly. During membership changes or operations the steady marker is
cleared and fresh rendering is mandatory; a former peer cannot be admitted from
a 40-minute platform snapshot. Updated agents also echo a separate successfully
applied change stamp in heartbeats. A changed own dependency or explicit force/
health input produces an authenticated reload notice; PostgreSQL listing-driven
steady transitions are not separate force inputs. Each instance coalesces notices into one bounded in-flight
reload without blocking heartbeat sampling. The resulting pull is full/fresh
while nonsteady; it does not wait for another minute ticker after detection.

Detection and application are distinct: no lease can prove instantaneous
equality to a fresh platform read before an external change is observed.
The corner is a **listing-visible platform-only peer change while the incumbent
database/agent still reports healthy**: for example a Dokploy application record's
desired withdrawal, status or ownership metadata changes before its running task
stops; a Zerops service-stack record changes independently of native health; or
Nomad job stop/metadata or allocation desired-state changes precede the old
allocation leaving service. These are not healthy-agent database failures.
Their bound is **≤60 s listing detection + ≤7 s heartbeat notice + fresh
render/apply time**. The notice bound is heartbeat scheduling delay only: the
3 s scheduled wait plus up to 4 s bounded sample collection, excluding
transport/server time and retries after a failed heartbeat. This corner is
approximately **7 s worse** than
v0.4.7's ≤60 s fresh configuration pull (plus the same render/apply work), not
an instantaneous-fresh-read guarantee. Ordinary stopped members still use the
shorter pushed-health detection path.

`karactl -n NS clusters reconcile CLUSTER` forces the fresh pass immediately
when admitted, rather than waiting for the listing or 30-minute resync.
Repeated requests retain the existing 30 s coalescing/minimum-spacing rule.
An admitted trigger advances a monotonic per-worker invalidation generation
bound into both the conditional validator and heartbeat change stamp. Thus even
a force pass that is already steady again before the next heartbeat still
causes a notice and full fresh peer resolution; the command does not wait for
agent application. All-provider authenticated force-endpoint tests exercise
this chain, including external Dokploy withdrawal before a scheduled 304.

The base idle projection excludes notice-driven full configuration responses.
For unchanged tracked content, one settled forced pass requires at most one
notice-driven full render per updated proxy. In the single-peer reference
profile the two PostgreSQL proxies together now cost 10 Dokploy GETs, or
0.0056 GET/s for one such pass every 30 minutes: approximately 0.176
GET/s including the 0.170 base projection. Actual tracked content changes,
independent minute pulls while nonsteady, failed applies and retries remain
separately attributed work, not a universal aggregate request cap.

### Cluster-scoped configuration dependencies

On Dokploy, PostgreSQL configuration notices and conditional certificates bind the exact
persisted member service names, including proxies, rather than the target-wide
mutation epoch or listing fingerprint. Unrelated cluster mutations and listing
drift therefore cause **zero additional PostgreSQL renders**, including periodic
conditional pulls with an otherwise eligible baseline. Target mutations still
invalidate listing eligibility: the next proof requires a successful independent
listing check before a 304 can be certified. Own membership, generation, secrets,
force intent, readiness and deployment failures retain their independent gates.

A listing-driven wake is classified as content, not force intent. Its reconcile
does not increment the force generation, and steady re-entry is excluded from
the scoped notice stamp. Thus a single own-cluster listing drift produces at
most one notice-driven full render per proxy; genuine additional force, health,
content changes, independent nonsteady pulls and retries remain separate inputs.

Dokploy full proxy rendering performs one fresh environment GET, one shared
fresh Swarm inventory GET, and at most three fresh reads per eligible peer:
application identity when AppName is omitted, service-ID-scoped ownership
inspection, and actual task state queried by the inspected full service ID.
The inspected service must satisfy the same `ownsService` predicate as the
generic GetService path; a foreign replacement at a known AppName cannot lend
its running tasks to the inspected service. Dokploy v0.30.8 omits AppName
([pinned source](https://github.com/Dokploy/dokploy/blob/v0.30.8/packages/server/src/services/environment.ts#L34-L65)),
so three peers cost **11 GETs (3N + 2)**, down from 27. Summaries containing
AppName cost **8 GETs (2N + 2)**. No prior observation cache authorizes identity,
ownership or liveness. Missing/foreign runtimes are rejected; genuine owned
replacements use current identity and logical hostname, bound to the engine's
persisted application ID. For already eligible, deployed peers with
desired-running intent, an active deploy, restart or scale receipt also keeps
the backend in discovery through its rolling task gap. This exception requires
the receipt's application ID, locator and hostname to match the freshly owned
service, a positive replicated desired count, and matching task identities.
The receipt must have a nonfuture issuance time within the existing 15-minute
task-startup window (plus the six-hour build window for deployments); routing
cannot extend admission indefinitely if lifecycle polling stops.
`Active` records that routing intent without claiming a task is Running.
Cleared, failed, uncertain or other pending receipts, stopped/deleting intent,
retired members and planned replacements cannot authorize the exception.
Routing does not poll, complete or clear lifecycle receipts.

Deleting a temporarily absent task from proxy discovery also deletes its local
postmaster-epoch observation. Even after PostgreSQL becomes ready, the proxy
then cannot recover until another configuration pull re-admits it. Keeping the
owned rolling member in discovery lets native monitoring and the agent's
one-second epoch loop recover independently of pull timing. The old sequential
GetService path also had this render-to-apply race; baseline rolls that happened
not to apply during the task gap did not prove otherwise. No cached
authorization window is introduced.
Nomad and Zerops retain their existing fresh GetService routing gates **and their
conservative target-wide listing/epoch fallback**: unrelated target drift still
notifies those proxies and invalidates their eligible certificates. Cluster
dependency scoping in this release is Dokploy-only.

The ten-cluster/two-proxy/three-peer fake-API budget covers eight foreign mutation
events over 120 seconds. Foreign mutations with two foreign listing drifts cost
zero config renders/GETs; two own-cluster drifts cost four renders and **44 GETs**
for only that cluster's two proxy instances. The shared listing cadence remains
separate background traffic. The same new budget rejects the unmodified
production base's 27-GET initial render. These are configuration-only budgets,
not end-to-end operation limits.


Rejected alternatives: using the 40-minute observation snapshot for config
rendering could publish stale proxy backends; relying only on desired generation
would miss operation/membership progress and secret rotation. A short TTL alone
does not prove those inputs stayed unchanged. Conditional revision validation
retains the fresh path instead of weakening it.

### Active PostgreSQL operation cost

The idle and configuration-pull budgets above are **not an end-to-end
operation budget**. Before v0.4.10, a PostgreSQL proxy-count operation ran
a fresh reconcile pass about every **two seconds** while a deployment receipt
was pending. An isolated experiment using the production Manager, native engine
provider and fake Dokploy API averaged approximately **100 GETs per PG
reconcile pass** over 34 attempts:
pending `Sync` calls cost 12 GETs, transition calls approximately 46–122,
and fresh pre-/post-`Sync` observations cost about 20 GETs each. Degraded or
uncertain results also invoke an immediate second observation with a new
cache. In that experiment these second attempts added **1,130 GETs**;
observations together accounted for about 72% of PG reconcile GETs.

Before this change, active PG operations could sustain roughly **15–50 GET/s**, rather
than the idle rate. The isolated experiment made **3,721 GETs in 67.2 s**
(about 55 GET/s including siblings), of which 3,412 belonged to PG.
A live operation window recorded **2,497 GETs in 110.5 s** (about 23 GET/s).
These are different workloads, not an exact reproduction or an operation SLO.
In the isolated experiment, cross-cluster/shared-target work was **309 GETs
(8.3%)**: 100 from listing-drift-triggered sibling deep reconciles, 208 from
independent sibling availability work and one shared listing GET. Thus sibling
fan-out was not the dominant source of this operation's cost. The live
window does not establish the same own/sibling split.

This burst shape already existed in v0.4.7: the same two-second pending
requeue, fresh lifecycle observations and two-attempt observation policy.
v0.4.9 reduces baseline work per status pass (the reference PG status
fixture falls from **33 to 16 GETs**), but no head-to-head v0.4.7 total for
this operation was measured. Cheaper passes and low idle traffic do not
establish a low operation-wide total.

The harness's **81-request scenario (78 GETs + 3 POSTs)** covers one MySQL
backend `SetContainerCount`, its completion wait and one fresh observation.
It is a **single backend-call scenario**, not a complete PG Manager
operation budget. It does not account for all repeated lifecycle passes or
their immediate second observation attempts.

v0.4.10 bounds this amplification in three ways:

- PostgreSQL, MySQL and Neo4j skip the immediate second status attempt while
  their durable infrastructure receipt remains checkpointed. PostgreSQL also
  recognizes an agent-upgrade deployment receipt. Intermediate degradation is
  still published; a failed read still withdraws readiness rather than renewing
  an old lease. A terminal receipt that stays checkpointed while native or
  configuration readiness completes still suppresses the status retry, but its
  `Sync` polling returns to the two-second cadence. Idle observations and
  observations after the receipt is consumed retain the fresh retry that
  protects against transient false negatives.
- Dokploy first-attempt platform facts are shared across pre-/post-`Sync`
  observations within **one reconcile pass**, not between passes. Keys include
  the shared target mutation epoch, which changes before and after every HTTP
  mutation, including failed mutations and mutations from another client.
  Retries discard the earlier platform snapshot. Native probes are not cached
  by this mechanism and timestamps retain the original pass boundary. Nomad
  and Zerops do not opt into this pre-/post-observation cache.
- Unchanged pending PostgreSQL receipts and serving MySQL count-only receipts
  poll after **2, 3, 4, then 5 seconds** (capped there). New or superseding
  receipts reset the counter. Poll errors and every terminal platform poll reset it
  immediately, even if native/configuration readiness still retains the receipt:
  that readiness is checked again at the ordinary two-second cadence. Scheduled
  receipt-detection delay is bounded by five seconds
  (the previous bound was two seconds), excluding platform-call execution and
  scheduler latency. Deployment itself is not slowed, normal
  transition passes remain fast, and the Manager's five-second readiness cap
  remains unchanged.

Fresh safety decision points remain mandatory. These changes reduce request
pressure; they do not guarantee availability observations cannot time out when
the platform is slow or overloaded.

The committed native-Manager fixture uses a **20-second task-progression hold**,
not the historical 60-second hold above. Matched fixture runs on the unmodified
v0.4.9 base (ordinary tests) and optimized implementation (race-instrumented
tests) measured these network GET totals:

| Operation | v0.4.9 GETs | Optimized GETs | Former ceiling (historical) | Optimized completion after enqueue |
|---|---:|---:|---:|---:|
| PostgreSQL proxy count 2→3 | 1,572 | 1,038 | 1,250 | 28.678 s |
| MySQL GR proxy count 3→4 | 1,430 | 1,197 | 1,400 | 38.078 s |
| PostgreSQL agent roll, including current agents | 704 | 511 | 580 | 25.765 s |

All operations succeeded with zero operation failures. These operation-total
ceilings were used during development; they are not enforced by the committed
test (see below). Completion
includes the artificial hold but excludes fixture warmup and any build/test
queue. These fixture measurements are not a live
platform completion SLO, and must not be directly divided into the historical
60-second experiment's totals.

The operation-wide totals above depended on how many timer-driven observation
sweeps fired during the hold, so the committed test does not enforce
operation totals. It asserts ordinary reads per observation sweep and bounds
availability's serialized receipt-recovery transactions separately, by exact
request context and endpoint. Each recovery requires a pending receipt observed
in that availability pass; a pass permits one recovery while pending and one
on completion, with a deterministic receipt count per operation. Unrelated
uncached availability reads have zero allowance. Other uncached reads are bounded
per reconcile or operation attempt and per configuration request. The test bounds held
proxy sweeps by the startup ramp and the five-second receipt backoff, and stops
at a steady fence. An extra provider GET per member per sweep, or a backoff
pinned to two seconds, fails it.

Live Dokploy measurements of the released change differ from these fixture
figures: proxy count changes used about 15–25 % fewer platform reads than
v0.4.9, while a PostgreSQL agent roll (`upgrade-agents --include-current`) used
about 35 % more and took about 20 seconds longer.


### Safety decision points stay fresh

Writer handoff, retirement and identity gates (MySQL GR UUID-set convergence,
PostgreSQL etcd identity, stop proofs), maintenance fences, formation,
scale/replace/lost-volume admission, proxy admission (`WithFreshTaskInventory`),
pinned dial authorization and every mutation keep their fresh reads. `Sync`
and operation contexts never receive the pre-/post-observation platform memo.
Each status attempt has its own fresh authorization memo. Descriptive cache hits
from the shared pass snapshot are never promoted into that memo. Pinned
authorization cannot consult the shared map; application lookups assembled from
inherited raw records are not memoized as fresh evidence. Fresh reads may still
be shared inside one attempt, exactly as before this pass-level optimization.
Safety reads do not carry the steady-observation marker and cannot use pushed
samples or persistent target snapshots. Pinned connections still enumerate
current tasks on every authorization; mutations strip observation state,
invalidate the target epoch, and revalidate ownership under exclusion.

### Request budget enforced by tests

Each provider package has a request-counting harness (real engine providers and real backend against the provider's fake API) that fails if, for GR3 + three proxies, PostgreSQL + two proxies, and Neo4j:

- a fresh status pass exceeds its per-pass maximum;
- a warm steady observation with heartbeats issues any platform request;
- the listing check exceeds its per-target count, or is not shared by clusters in the same target;
- a simulated idle hour exceeds the idle per-cluster rate;
- a single backend-call burst scenario exceeds its scenario count, or steady observation fails to resume afterward.

These component/scenario limits, including the 81-request backend-call example,
remain separate from the Manager operation limits. Dokploy's
`TestManagerOperationRequestBudget` runs the production Manager, queue, native
providers, configuration rendering and HTTP backend against fake Dokploy and
TLS agents. It holds platform task progression for 20 seconds and requires a
successful, zero-failure operation whose recorded completion is within 55 seconds
of enqueue, even if the polling goroutine discovers completion later. It covers PostgreSQL
proxy count 2→3, MySQL GR proxy count 3→4, and PostgreSQL `upgrade-agents`
including current agents. It bounds GETs per endpoint in each observation
sweep, uncached GETs per reconcile pass, operation attempt and configuration
request, and the number of sweeps during the 20-second hold, instead of
operation-wide totals. The native database processes are simulated; these are bounded
request-budget regressions, not live deployment-latency SLOs. Companion
reconciliation and pushed-heartbeat/config-route behavior are outside this
fixture; their separate budgets above still apply.

The reconciler has simulated-time tests for the steady schedule, discovery skipping and immediate triggers.

### Compatibility

- **v0.4.7 and older agents, plus v0.4.8 hotfix agents** never heartbeat. Their reads miss and fall back to live pulls on every evaluation (unchanged detection), while platform reads still come from the snapshot. Pinned proxy dials in relay-backed namespaces still authorize against fresh tasks, so such clusters land between the old and new rates until `clusters upgrade-agents` (or the opt-in automatic rollout) installs v0.4.9 agents.
- **v0.4.9 agents under an older control plane** receive 404 for the heartbeat endpoint, back off for 10 minutes and keep serving pulls exactly as before.
- Dokploy proxies expose their task ID only when they run with proxy serving admission (`KARAKORAM_TASK_ID`); unmarked proxies' pinned reads keep falling back to pulls.
- The control-plane upgrade does not automatically redeploy members. Conditional
  configuration pulls require updated agent binaries; released agents that send
  no revision continue receiving the complete, freshly resolved configuration.

## Consequences

- Idle request rates drop by more than two orders of magnitude. The idle-hour model includes 660 conditional-helper invocations across the three fixtures; these model calls alone are not end-to-end eligibility/full-render evidence. Separate production HTTP tests on every provider use a real running Manager's steady predicate and real MySQL/PostgreSQL renderers, obtain initial revisions from full authenticated responses, and perform 180/120 independently phased proxy-instance pulls per hour with zero configuration network starts. Storage-only changes and invalidation require full real rendering. The base observation/deep-reconcile budgets below remain model measurements per cluster (GR3 + three proxies / PostgreSQL + two proxies / Neo4j):

  | Provider | Fresh status pass (requests) | Deep reconcile (requests) | Idle, v0.4.7 cadence (req/s) | Idle, v0.4.9 (req/s) |
  |---|---|---|---|---|
  | Dokploy | 28 / 16 / 8 | 94 / 104 / 54 | 18.8 / 20.8 / 10.8 | 0.058 / 0.063 / 0.036 |
  | Zerops | 30 / 16 / 4 | 60 / 58 / 29 | 12.0 / 11.6 / 5.8 | 0.041 / 0.040 / 0.024 |
  | Nomad | 40 / 13 / 4 | 84 / 57 / 32 | 16.8 / 11.4 / 6.4 | 0.058 / 0.043 / 0.029 |

  The v0.4.7-cadence column applies one deep reconcile every 5 s with the same per-pass costs (v0.4.7's own Dokploy pass was larger: 66 / 33 / 12). `Sync` is modelled from its measured per-host backend calls, because the fixtures cannot pass lifecycle ownership proofs. A warm steady renewal with heartbeats issues no platform request; with v0.4.7 agents it issues only the pinned task authorizations (Dokploy 10 / 8 / 0: 6 / 4 / 0 task enumerations plus one environment, application, service inventory and ownership inspection per proxy service). Pass-local memos share fresh discovery with steady reads; pinned dial authorization never consumes data served under the steady marker, on every provider. A settled additional-namespace relay adds one backend service lookup per minute instead of one every 15 s; the HTTP fanout is provider-specific. At a 10-minute deep cadence the MySQL and PostgreSQL rows would be 0.11–0.18 req/s.
- Namespace credential validation remains every five minutes and costs four GETs on a settled Dokploy target with a configured registry. A settled additional-namespace relay service lookup is six Dokploy GETs each minute; the default namespace has no managed relay. Three example clusters sharing one default Dokploy target therefore project to `(94+104+54)/1800 + 1/60 + 4/300 = 0.170 GET/s` after member agents are upgraded. Monitoring, active work and full config fallbacks are separate.
- Operators who need an out-of-band change picked up before the next deep reconcile use `karactl clusters reconcile`.
- The control plane receives one heartbeat per member every 3 s (relay or direct) and agents run their native observation every 3 s instead of being polled several times per 5 s pass.
- Out-of-band platform drift that agents cannot see is detected within 60 s instead of 5 s; configuration drift not visible in the listing within 30 minutes.
- A sibling cluster's operation in the same target invalidates the shared snapshot, so steady clusters next to a busy one pay for fresh reads until the target settles.
- Degraded clusters are not steady and keep the v0.4.7 cost while degraded.
- `Agent.LastSeen` and `ObservedAt` can be up to one heartbeat interval older than the evaluation, by construction.

## Alternatives considered

- **Shared TTL cache over the existing pulls:** an age bound alone cannot prove that facts predate no mutation, and relabels old facts as new. Rejected in favour of mutation epochs, a fallback on any doubt and evidence-anchored leases.
- **Agent heartbeats without platform snapshots:** removes pinned authorization reads but leaves 20–50 platform requests per pass per cluster.
- **Platform watch/event streams:** Dokploy and Zerops expose none over their public APIs; Nomad event streams would cover one provider only.
- **Declaring a member dead on a missed heartbeat without a pull:** faster, but a control-plane pause or relay hiccup would produce false Degraded results; the pull fallback preserves v0.4.7 behaviour exactly when push evidence is missing.
- **Agent-chosen sample timestamps:** vulnerable to clock skew and to buffering; the echoed control-plane token gives a strict lower bound without clock synchronization.
