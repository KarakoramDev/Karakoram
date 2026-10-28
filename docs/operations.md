<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Operations and troubleshooting

Operate the control plane, managed engines and provider infrastructure as separate systems; each has its own health and recovery signals. Provider-specific procedures below are labelled [Zerops](https://zerops.io/); [Dokploy](dokploy.md#operations-and-limitations) and [Nomad](nomad.md#operations-and-limitations) have their own operational guides.

The fleet Ops features below are **verified offline** (tests without a live provider). See also [notifications](notifications.md), [control-plane backup/recovery](control-plane-backup.md) and [backup drills](backup-drills.md).

## Maintenance windows

Set `spec.maintenance` to permit new member disruptions only during explicit
windows. The time zone is an IANA name and defaults to `UTC`; at most 16 windows
are accepted. Each schedule is a five-field cron **start time**, paired with a
[Go](https://go.dev/) duration from `15m` through `168h`:

```yaml
spec:
  maintenance:
    timeZone: Europe/Prague
    windows:
      - schedule: "0 2 * * 0"
        duration: 2h
      - schedule: "0 23 * * 3"
        duration: 45m
```

Starts follow the selected zone's daylight-saving rules: nonexistent local
times do not fire, repeated times may fire twice. Durations measure elapsed time,
not local clock hours. A window includes its start and excludes its end.
Overlapping or touching windows merge. Omitting the policy or using `windows: []`
permits disruption at any time.

The disruption gate covers restart, TLS rotation, agent upgrades, FCV changes,
and user-requested member replacement or move. Engine drivers consult the same gate
before starting each new engine upgrade, rolling resource/configuration change
or scale-in disruption. Automatic database leaf-certificate renewal is gated only
while more than seven days remain. A window is permission to **start**, not a
deadline to finish: work already past a member's stop/drain boundary continues
through recovery and rejoin.

Public HTTPS enable/disable changes on existing Keycloak, Temporal UI, Supabase
gateway, Elasticsearch Kibana/APM and RabbitMQ management runtimes also wait for
the window (or a generation override). A backend may replace tasks to change
ingress networks or service labels, so routing is not a non-disruptive exemption.
This includes explicit route revocation; for urgent revocation use an override.
Initial provisioning and deletion remain exempt.

Healing and system lost-volume replacement, delete/suspend/resume, create and
scale-out, backup/restore/initial restore, credential rotation, agent transport
certificate renewal, and certificates with at most seven days remaining are
exempt. A queued disruptive operation does not block exempt queued work behind
it. Never-started closed-window operations remain Pending without attempts or
execution-budget consumption, with a durable `MaintenanceWindowWait` event and
next-start message. This includes desired-generation PUT/PATCH receipts when
convergence defers their disruptive change: a healthy engine's Ready message
does not replace the operation's `WaitingForWindow` diagnostic. An operation
already Running retains that phase while waiting for its next member disruption.
Provider execution is recorded durably before a convergence sweep begins. A
first sweep that discovers only a window deferral returns its newly evaluated
receipt to Pending; subsequent known closed-window waits stay Pending until
opening or override. It never rewinds an operation started by an earlier sweep.
Engine driver waits publish `MaintenanceWindow=False/WaitingForWindow`. A running
imperative operation pauses its execution budget only when that invocation
actually waits at its own next-disruption gate; a stale cluster-wide condition
cannot pause unrelated backup/restore work. Recovery after a member disruption,
cancellation and failure cleanup continue consuming their applicable budgets,
and an override does not create a maintenance pause.

Cluster responses calculate `status.maintenance.open`, `currentWindowEnd` and
`nextWindowStart` at response time. No status is emitted for an empty policy.
An absent current end while open means continuously overlapping weekly coverage.
The next start is the next cron occurrence, which may lie inside an already open
merged window.

Use `ignoreMaintenanceWindow=true` on a gated action or cluster PUT/PATCH for an
explicit override. Imperative overrides belong to that operation; desired-state
overrides apply only to the accepted generation, not future spec changes. They
never bypass engine driver quorum, health, placement or deployment-admission checks:

```sh
karactl clusters restart demo --ignore-maintenance-window
karactl clusters patch demo -f maintenance.yaml
karactl clusters edit demo --ignore-maintenance-window
karactl clusters create demo --maintenance-time-zone UTC \
  --maintenance-window '0 2 * * 0;2h'
```

The same desired-state gate applies to Keycloak, Neo4j, MySQL/PXC, Temporal,
OpenBao and Weaviate, including auxiliary-component scale-in and restart-required
configuration. PostgreSQL profile preparation/restarts and Citus worker-group
drain/retirement follow it too. A binding credential-generation roll in Keycloak
or Temporal is credential rotation and remains exempt, even when applying those
credentials restarts the consumer. An unrelated desired application change in
the same reconciliation still needs its own maintenance permission.
Deferring that change does not stop reconciliation of accepted running state:
lost runtimes are redeployed at their accepted version/resources, sealed peers
can recover, and rotated binding credentials or renewed transport certificates
can be published while pending application settings remain unchanged. A deferred
change in one application component must not hold another component's exempt
credential roll behind its maintenance gate.
Retiring members are not lost serving runtimes: their scale-in workflow retains
exclusive ownership of stopping, any drain-required restart, and runtime/volume
deletion. Ordinary recovery must not recreate a runtime during its retirement.
Urgent database leaf renewal bypasses the time window, not native quorum or
one-member-at-a-time readiness/acknowledgement gates. Accepted application
settings and resource bounds remain frozen during that renewal.
An already-admitted retirement finishes before a new native-leaf rolling journal
is installed; independent transport-certificate renewal can still proceed.

## Moving a member

```sh
karactl clusters placement demo
karactl clusters members move demo MEMBER --wait
# Only when an immediate disruption is intended:
karactl clusters members move demo MEMBER --ignore-maintenance-window --wait
```

Moving is a native **replacement**, not an in-place disk migration: it creates a
new runtime and volume, synchronizes data using the engine's replacement safety
gates, and retires the predecessor only after it is safe. Allow temporary capacity
for the extra member. Supported targets are [MongoDB](https://www.mongodb.com/) data/config members, [RabbitMQ](https://www.rabbitmq.com/)
members, [Elasticsearch](https://www.elastic.co/elasticsearch) data/master nodes, and [PostgreSQL](https://www.postgresql.org/) HA data members
(including [Supabase](https://supabase.com/)'s PostgreSQL core). Routers, proxies, standalone PostgreSQL,
and auxiliary Elasticsearch/Supabase components cannot be moved this way.

Move admission is limited to the `psmdb`, `rabbitmq`, `elasticsearch`, `postgres`
and `supabase` engines whose replacement workflows honor relocation constraints.
[MySQL](https://www.oracle.com/mysql/)/[PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster), [Neo4j](https://neo4j.com/), [Weaviate](https://weaviate.io/), [Keycloak](https://www.keycloak.org/), [OpenBao](https://openbao.org/) and [Temporal](https://temporal.io/) moves are rejected
before an operation is queued, even when an engine supports ordinary member
replacement. Placement reads remain advisory and available independently of
move support; reported groups do not establish that an engine can relocate.

The API is `POST /clusters/{cluster}/members/{member}/move`; it returns a durable
`replace-member` operation with `relocate:true` and the observed `avoidGroup`.
It shares replacement admission, authorization and maintenance-window rules.
Use the returned operation ID rather than repeating a request after a timeout.

| Provider | Placement and move guarantee |
| --- | --- |
| Zerops | Best-effort runtime/volume replacement only: there is **no host placement guarantee**. Re-observe fingerprints afterwards; do not repeatedly recreate members to chase a different fingerprint. |
| Nomad / Dokploy | Authoritative node placement. Move excludes the predecessor's node; it fails with “no other eligible node” before retiring the old member if capacity is unavailable. An unknown current group returns HTTP 409. |

`placement.source` is `backend` for authoritative node evidence, `best-effort`
for agent fingerprints only, or `unavailable` when no usable evidence exists.
`guaranteedAntiAffinity` is true only when the provider supports Placement,
**every** member has an authoritative group, and those groups are all distinct.
[Nomad](https://developer.hashicorp.com/nomad)/[Dokploy](https://dokploy.com/) can therefore guarantee distinct hosts when eligible capacity and
observed separation allow it; neither this flag nor fingerprints certify rack
or zone independence. Missing observations never establish separation.

Durable `Warning MembersCoLocated` events report a newly observed co-located
group set; unchanged sets are deduplicated across reconciliations and restarts.
`Normal MembersSeparated` records when co-location is no longer observed.
Unknown groups (including unknown fingerprints) are not co-location evidence,
and a cleared warning alone is not proof of host separation.

## Health and logs

```sh
curl --fail --show-error https://karakoram.example.com/healthz
karactl health
karactl clusters status demo
karactl clusters members demo
karactl operations cluster demo
karactl events list --cluster demo
```

`/healthz` checks the control-plane store, not engine health. Inspect cluster conditions, observed generation, member health, agent connectivity, allocations, disk headroom and backup status. An active infrastructure runtime (for example [Zerops](https://zerops.io/) ACTIVE) alone is insufficient. Operation success is not a health guarantee; see [operations and status](clusters.md#operations-and-status).

[PostgreSQL](https://www.postgresql.org/) health and desired-generation application are separate signals. A
healthy cluster can still be applying an exposure or configuration change.
While its observed generation lags, the cluster and tracking-operation message
reports `waiting for generation N: <barrier>`, naming the current deployment,
recovery, member/configuration or post-exposure readiness barrier. This message
is replaced on each reconciliation rather than emitting repeated warning events;
inspect it before treating `Ready=True` as proof that an update completed.
Native-running generation waits include bounded, redacted observation failures.
Proxy readiness requires both healthy individual instances and a running native
observation through the advertised private service's logical agent endpoint.
When that endpoint is unavailable, readiness is false even if pinned tasks are
healthy; inspect the private DNS alias/VIP, overlay network and member agent.

Cluster restart is a engine driver-owned application-process operation, not a member
agent/artifact upgrade. Upgrading only the control plane leaves existing member
executables in place. [Supabase](https://supabase.com/) restart includes PostgreSQL/[ProxySQL](https://proxysql.com/) and every
enabled application component, with per-instance restart evidence and readiness;
allocation IDs need not change. For component artifact rolls after an upgrade,
see [Supabase restart and member software](supabase/operations.md#cluster-restart-and-member-software).

Agent connectivity records a successful agent response, not native database
readiness. [Elasticsearch](https://www.elastic.co/elasticsearch) observes each native endpoint independently with a
two-second budget and uses `/_nodes/_local` for that member's identity, so a
stopped JVM cannot consume every peer's observation budget. A missing response
produces `NativeObservation/<member>` = `Unknown` with JVM/port guidance; missing
node evidence is `unknown`, not proof that the member is `absent`. Healthy peers
retain their own native evidence and agent connectivity. Cluster-wide conditions
retain successful evidence, but any unobserved member prevents **Ready**.
A partially failed sweep can retain agent responses sampled in that sweep while
withdrawing availability claims; these partial responses still expire under the
normal observation lease. Historical agent responses never renew connectivity.

Running imperative operations continue bounded availability observations
between checkpoints, without invoking unrelated convergence or reconciling
operation-owned native backup inventory. Unavailable namespace dependencies
withdraw freshness without falling back to the default project's resources.
Their polling delay
is capped at the normal readiness interval; restore and other lifecycle phases
remain authoritative even when a health probe succeeds. A blocking engine driver call
can still expire the readiness lease: an in-flight mutation is not evidence of
health. [MongoDB](https://www.mongodb.com/) backup polling uses [PBM](https://docs.percona.com/percona-backup-mongodb/latest/)'s heartbeat-aware snapshot status as well
as `describe-backup`, so an owner's death cannot leave raw `running` metadata
hiding PBM's terminal stale-heartbeat failure.
During error backoff, the same serialized cluster worker also takes read-only,
availability-only samples at the normal readiness interval. These samples do not
retry the failed operation, run ordinary convergence, consume native receipts,
or shorten platform retry deadlines. Thus a fenced backup can remain Running
without making reachable member agents appear disconnected solely because its
mutation retry delay exceeds the readiness lease. In-flight engine driver calls can
still expire that lease; stale observations never certify availability.
An accepted cancellation remains cooperative: active PBM backups receive the
durable native cancel request and remain nonterminal until PBM confirms a terminal
outcome. Already failed/stuck snapshots finalize without a redundant native
cancel, while completed native backups retain their successful inventory record.
A transient heartbeat-status read failure neither invents termination nor
prevents the first requested cancel after describe confirms nonterminal metadata.

Use two log sources:

- **Backend deployment/application logs:** preparation, readiness, startup, supervisor diagnostics and forwarded child output, including failures before the agent is reachable. `karactl clusters logs demo <member> --process agent --lines 100` reads the member runtime's application log tail through the namespace backend (Zerops, Dokploy or Nomad). Zerops selects the newest requested records, then returns them oldest-first before redaction; equal-timestamp records retain their platform source order. Runtime messages pass through the shared credential redactor, including multiline values, before the API returns them. Redaction can replace suppressed continuations with omission markers. It is not filtered to supervisor-only messages; retention and timestamps are platform-dependent. A backend without log support rejects the request; an unavailable backend returns 503 and a missing runtime returns 404. Signed project-log URLs/tokens are credentials.
- **Karakoram child-process logs:** authenticated, bounded process tails, not a historical archive:

  ```sh
  karactl clusters logs demo <member> --process <process> --lines 100
  ```

  `GET /api/v1/clusters/{cluster}/members/{member}/logs` accepts `process` and `lines` (default 100; accepted range 1–10000). The default process is the engine driver's first declared process. Each process retains at most **1000 in-memory lines and 1 MiB of message bytes**, lost on restart; individual lines are truncated to 16 KiB while the stream continues draining. External collector output is best effort through a bounded queue: a stalled collector can drop output, but cannot block process control. **`cursor` and `since` are currently ignored; no `nextCursor` is returned.** Requesting more lines cannot recover older history.

The agent drains engine stdout and stderr through agent-owned pipes. If the whole
agent process is frozen (for example, by SIGSTOP), an engine's logging writes
block once the kernel pipe buffer fills and can stall the engine. On [Linux](https://www.kernel.org/) the
default capacity is typically **64 KiB per stream**, but can be smaller when the
shared UID exceeds `pipe-user-pages-soft`. In one observed mongos incident, the
engine stalled about **95 seconds** after the agent was frozen; the delay depends
on output volume and available pipe capacity, not a guaranteed grace period.
The member is reported unhealthy while the agent is unresponsive. Recover by
resuming the agent or restarting the allocation.

Process names, readiness checks and engine diagnostics: [MongoDB](mongodb/operations.md), [RabbitMQ](rabbitmq/operations.md), [Elasticsearch](elasticsearch/operations.md), [Neo4j Community](neo4j/operations.md).

Platform failures expose a Zerops error code and sanitized field/reason summary in events and conditions. Inspect the named resource bound or missing service before changing the spec; rejected resource updates/imports have [resource diagnostics](clusters.md#resources-and-autoscaling). For `serviceStackNotFound`, check whether the recorded service was removed externally; do not delete unrelated services. Use the Zerops dashboard when details are missing/truncated.

Redact passwords, credential-bearing URIs, environments, tokens, signed URLs and private keys before sharing logs. Preserve identifiers and timestamps instead.

Persisted diagnostic messages are sanitized again when read and served, including
operation progress, backup/restore errors, events and cluster conditions. Startup
migration `012_sanitize_diagnostics.sql` also rewrites legacy diagnostic fields in
bounded, restart-safe batches before recording completion. Resource identities,
phases and timestamps are unchanged. This does not securely erase old [SQLite](https://sqlite.org/)
pages/WAL, external logs or archived state snapshots: restrict their access and
coordinate credential rotation and retention remediation separately.
Event detail documents additionally redact credential-bearing keys, including
every nested audit before/after value, even beneath keys named payload,
checkpoint or internal. Unlabeled standard-base64 diagnostic tokens of at least
28 characters with mixed letter/digit classes or base64 punctuation are omitted;
hyphens, underscores, dots and colons separate tokens. Generated initial-restore
IDs, 24-character operation IDs and hexadecimal product digests/checksums remain
intact. Real execution checkpoints, payloads and cluster internal state remain
excluded from resource-record sanitization; public event copies are not exempt.

### Platform request recovery

Zerops honors future `Retry-After` timestamps for reconciliation retries, including resets longer than the ordinary backoff ceiling. Discovery and repeated user triggers do not bypass that reset. Check the sanitized reset timestamp and the platform API budget before replacing credentials; a rate limit is not an authentication rejection.

Zerops records non-secret operation receipts in the service variable `KARAKORAM_PLATFORM_OPERATION` before creating an app version or submitting a start, stop or restart. An accepted upload/deployment with a lost reply resumes using the original app-version identity; an already completed start/stop is recovered from the service state. Retrying after control-plane restart does not allocate another artifact or blindly restart the runtime. Deployment submission returns the durable app-version handle without waiting for a post-deployment environment update: Zerops queues those updates behind deployment, and waiting would prevent quorum peers from being deployed. Completion is observed separately, not inferred from submission.

Receipt consumption and new submissions share **service-scoped, context-cancelable exclusion** across provider instances within the single control-plane process. A slow upload or receipt operation on one member does not block another member's deployment or lifecycle observation; canceled callers stop waiting without interrupting the active mutation. Completion clears only the exact observed receipt; a delayed terminal observer cannot consume a successor's uncertain deployment/restart receipt. Receipt-write process polling does not consume lifecycle receipts.

After a lost app-version allocation response, Zerops recovery compares complete, service-scoped app-version and process listings against the exact immutable version IDs saved **before** allocation. With exclusive mutation ownership of the managed service, one new `UPLOADING` version is adopted and uploaded/deployed using that same ID; pre-existing versions are never selected by name, recency or status. The allocation API accepts a display name, but its documented version-list/detail response does not return that name as a request identity.

If no new version exists, recovery requires all service processes terminal and two successful complete observations separated by at least the provider polling interval. Before each allocation, receipt persistence and the allocation request share one HTTP-client timeout budget; the durable settle deadline adds one more timeout budget for delivery/visibility (four minutes total with the default two-minute client timeout). The wait diagnostic includes its UTC deadline and carries a retry timestamp. A control-plane restart retains the inventory, deadline and first negative observation. Legacy uncertain allocation receipts without an inventory can start this observation window only with an empty version list; existing unattributed versions remain fenced.

The same pre-send crash boundary exists for artifact upload and deployment submission. Each phase stores its own request/settle bound and first negative observation while retaining the selected version ID. An unchanged `UPLOADING` version with terminal service processes and no attributable version process can retry the same upload after two observations and the settle margin. A matching deployment process is observed instead of resubmitted, including when version status temporarily lags its process.

An unchanged `WAITING_TO_DEPLOY` version with no deployment process can similarly become retryable after the two-observation settle gate. A direct deployment retry submits that same uploaded version. If the control plane already holds a version operation handle, observation completes **that infrastructure attempt** as `RETRY_REQUIRED` (failed), retaining a `deploy-ready` receipt; ordinary engine retry re-enters deployment with its original artifact/runtime configuration and reuses the uploaded version. No runtime payload or credentials are copied into the readable receipt, and the cluster operation is not falsely reported successful before submission.

Confirmed `DEPLOY_FAILED` or `PREPARING_RUNTIME_FAILED` versions retain their
uploaded artifact and receive **at most three automatic exact-version retries**
after durable backoffs of 15, 30 and 60 seconds. After the initial attempt and
three failed retries, the durable `DeploymentRetriesExhausted` state stops further
deployment, allocation and upload mutations for that intent. The diagnosis includes
the service, app version, attempt count and sanitized last platform error.

Recovery requires a complete service process inventory, an attributable failed
deployment for the selected version, and no non-terminal service work. Completed
deployment process IDs fence every new submission; prior failures cannot be mistaken
for the current retry's outcome after a control-plane restart. The request/settle
evidence gate still applies to a crash before sending or a lost response. Neither
failure status nor an elapsed timer authorizes replaying RUNNING/unknown work.

After correcting the underlying problem, explicitly re-arm with
`karactl clusters retry-deployments NAME` (or `POST /clusters/{cluster}/retry-deployments`).
This audited action requires `clusters:update` and grants exactly one fresh budget
of three exact-version retries. Repeating observation or restarting the control
plane never grants a budget. A new artifact/runtime intent may supersede a
conclusively terminal receipt only after the same terminal-process proof.
Do not clear receipts or delete managed versions to bypass this boundary.
See [retrying exhausted deployments](clusters.md#retry-exhausted-platform-deployments)
for namespace selection, authorization and idempotency behavior.

The create/scale/update convergence-tracking operation fails immediately on
`DeploymentRetriesExhausted`, without spending its generic failure-retry budget.
Desired state and normal observation remain active; reconciliation does not
resubmit exhausted members. Re-arming advances the retry epoch and starts a new
update-tracking operation. Reported attempt counts include the original deployment
and all exact-version retries, including retries from earlier explicitly re-armed
budgets.

Deadline and retry-budget failures report the latest observed blocker and its UTC
observation time. A successful convergence pass replaces an older error with the
current placement/readiness diagnostic, without treating a placement wait as a
failed retry. The timestamp is an observation, not a condition-transition time or
a guarantee the blocker is still present. Legacy records without an observation
timestamp explicitly say the time is unknown. Create-from-backup failures also
include the initial restore inventory outcome (or pending/unknown); a succeeded
restore does not imply that proxy placement or cluster readiness succeeded.
Diagnostics are sanitized and bounded to 1024 bytes.

Platform admission and previous-deployment waits are backpressure, not failed
deployment attempts. `PlatformDeploymentWaiting` retains the current diagnostic
(including process ages), while events repeat only when the stable blocking
state changes. Provider retry deadlines survive reconciliation and concurrent
member failures; the latest applicable deadline wins, and discovery or user
triggers do not shorten it. These waits do not consume operation failure budgets.

#### Deployment admission and member startup

Zerops admits at most **two** owned deploy/build processes per project by default,
shared across namespaces in one control-plane process. Set
`KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS` to an integer **>= 1** to change the cap.
[Dokploy](https://dokploy.com/) applies the corresponding [server-wide cap](dokploy.md#platform-deployment-admission).
Bootstrap never raises or bypasses either cap, including for five-member clusters.

Initial member deployments and stateful quorum members use `karakoram agent readiness --startup --data-dir=<absolute path>`.
This local probe means **the agent supervisor is running and initialising**. It
can succeed while the agent awaits its first configuration or its database awaits
peer discovery, DCS quorum, replica-set initialization, or cluster formation.
It does not mean that SQL, MongoDB, AMQP, Elasticsearch, or Supabase is available.
The probe uses a container-local Linux abstract Unix socket, not a persistent
readiness file; an exited agent cannot leave a successful probe behind.
Only agent-level initialization failures fail startup readiness: invalid member
identity/agent TLS, unavailable agent storage/safety controls, and supervisor
executable/ownership start failures. Engine configuration/migration errors and
child exits or crash loops are not platform-readiness signals: dependency-induced
and deterministic engine failures cannot reliably be distinguished, and an
exact-version platform retry does not repair either. They remain visible in
member process/Apply diagnostics, make control-plane member/component health
unhealthy, and are retried or repaired by engine configuration/supervision logic.
A superseded control-plane configuration reply is refused and logged separately;
it neither invalidates a successful newer apply nor overwrites an active current
apply failure. A successful current apply clears that active failure.

This separation lets each deployment release its admission slot before peers
queued behind that slot are started. It applies to PostgreSQL HA (including the
Supabase HA core), MongoDB replica sets, RabbitMQ clusters, Elasticsearch, and
their auxiliary member services. Zerops executes the selected probe as deployment
readiness. Dokploy deliberately does not install a [Docker](https://www.docker.com/) `HEALTHCHECK`: Swarm
must publish service discovery while tasks bootstrap.

For stateless replacements (mongos, ProxySQL, Supabase services and Elastic stack
components), the engine selects the native application probe when an already
converged cluster has a fresh observation of healthy dependencies. It excludes
the replacement itself and conservatively requires every other observed member
to be healthy. Otherwise it selects startup readiness. The engine persists this
choice with the member until native convergence, so retries and control-plane
restarts use the same deployment specification even if dependency health changes.
With healthy dependencies, Zerops therefore retains the old stateless task until
the replacement passes native readiness; bootstrap does not wait on undeployed
dependencies.

Karakoram's engine-level observations still gate cluster **Ready**, native health,
configuration acknowledgement, quorum and replication catch-up. Day-2 rolling
operations still wait for the current replacement's native convergence before
disrupting another member; platform deployment completion alone cannot advance
that safety gate. `karakoram agent readiness` without `--startup` retains its
native application probe for local diagnostics. Startup failures retain the
normal bounded exact-version retry policy. With startup readiness selected,
waiting for application/dependency convergence does not consume platform retries;
a selected native day-2 probe can still fail and use that bounded retry budget.


The engine's exhausted-deployment fence includes both runtime configuration and
release identity. A corrected control-plane executable can therefore make the
same runtime eligible again without an explicit re-arm; an unchanged executable
or a restart alone cannot. Executable identity uses the artifact name, size and
[Go](https://go.dev/) build ID (file metadata when unavailable), cached without generating or
hashing an archive on blocked observations. Platform terminal-process and
ownership checks still apply before any new submission.

Elasticsearch retains an interrupted rolling deployment's recovery action for
the same disruption owner across admission waits and exhaustion. After re-arm
or a corrected release, recovery may redeploy that absent target, but still
requires the other planned masters and the committed voting quorum. It does
not advance to another rolling target while recovery is outstanding.

The retry cadence uses persisted wall-clock UTC deadlines, not the monotonic
CP-observed elapsed budgets used to expire operations. A forward clock
step can make a retry due early; this never bypasses exact-version/process proof.
On provider re-entry, a deadline more than sixty seconds plus a five-second margin
in the future is treated as due, bounding a single backward-step delay at that
gate. Ordinary deadlines within that bound remain unchanged across restart.
Engine-owned platform retries, CPU-mode cooldowns, import batches, runtime
recovery and MongoDB restore-router retries also persist the honored duration
and the clock observation that recorded it. After restart, a deadline more than
its own recorded duration plus five seconds ahead of now is treated as due.
This preserves legitimate long platform resets (including multi-day waits)
without mistaking the local retry backoff ceiling for a platform limit.
All durable engine schedules honor at most 30 days from observation; older
records without provenance retain their existing deadlines unless they are
more than that global maximum plus five seconds ahead, in which case they are
due. Small backward steps within the margin can extend the wait; repeated clock
steps have no global monotonic elapsed guarantee. These cadence rules never
expire an operation or bypass ownership, quorum or exact-process replay fences.

This policy applies to every engine's Zerops deployments, including persistent
MongoDB, RabbitMQ, Elasticsearch and PostgreSQL members, not just Supabase
components. Retries preserve the original runtime configuration and volume
identity, including stop-before-start fencing for persistent-volume writers.
Stop-before-start prevents overlapping persistent-volume writers; it is not an
availability guarantee. A broken version receives at most three exact-version
retries after its initial attempt, then remains durably exhausted until an
operator grants a fresh budget or changes the desired deployment. Each retry
can stop a serving predecessor again. Recovery does not detach/delete the
volume or change cluster membership. Engine recovery and rolling-maintenance
gates still apply; correct the broken desired artifact/runtime rather than
treating repeated retries as a safe rolling upgrade.

**Running preparation is a separate boundary.** A deployment can remain
`RUNNING` / `PREPARING_RUNTIME` without exposing a prepare container or live
runtime. The process-cancel endpoint rejects already-running work; the separate
app-version `cancel-build` endpoint is not a generic runtime-prepare/deploy
cancellation contract. Karakoram therefore does not force a replay or recreate
the managed service after an elapsed-time threshold, including a twenty-minute
stall. Supabase reports `ComponentDeploymentPending` with exact handles,
platform phase and persisted observation/submission ages. Preserve those
identities and obtain a definitive terminal outcome from the platform operator;
then normal evidence-fenced recovery can proceed. A timer or a successful cancel request
without positive terminal cancellation evidence is not a fencing proof.

These upload/deploy retries depend on the documented bounded normal-delivery assumption, not cancellation proof: an intermediary that deliberately forwards upload/deploy requests arbitrarily later can defeat any finite negative-evidence window. Restore reliable bounded platform routing before relying on automatic request recovery. Missing version/process identities, unknown or non-terminal service work, or evidence-read failures keep the request fenced. Diagnostic inventory reads also propagate rate limits/cooldowns before any receipt read/write that would otherwise follow them.

**Timing and ownership boundary:** an HTTP deadline does not cancel work already queued by an upstream proxy/server, and Zerops documents no hard maximum late-delivery or listing-staleness bound. The settle margin is a conservative operational assumption, not proof of exactly-once allocation. Only the durably selected version is ever uploaded/deployed. A late original allocation therefore stays non-serving; an additional unselected version is reported during operation observation without replacing or blocking the selected deployment. No unattributed version is automatically deleted. Multiple new candidates with no acknowledged selected ID, missing baseline entries, non-terminal/unknown processes, inconsistent exact-ID reads, or failed evidence listings keep **automatic replay refused**, with the evidence needed for the next attempt; HTTP 429/5xx evidence failures retain their original retry information. Do not mutate managed app versions concurrently with the control plane.

Other `OUTCOME_UNKNOWN` or **automatic replay refused** outcomes mean the provider cannot independently establish the original outcome. A restart without a process ID, for example, is still not safely attributable from an `ACTIVE` status. Inspect that service's processes/app versions in the Zerops dashboard, preserve identifiers and timestamps, and wait for all pending requests/processes to settle. Only after confirming the outcome, an operator with platform permissions can clear `KARAKORAM_PLATFORM_OPERATION` to permit another submission. Do not clear receipts just to dismiss an error, delete data volumes, or repeatedly resubmit deployments while the outcome is unknown.

Owned service imports carry a readable `KARAKORAM_IMPORT_REQUEST` receipt alongside their ownership variables. A lost import response can recover the original service IDs; a same-name foreign, masked or FAILED volume is never adopted automatically.

Full deployments of volume-less runtimes use Zerops rolling deployment (`temporaryShutdown: false`), preserving the selected startup/native readiness check described above. This applies to stateless frontends/components such as mongos, ProxySQL, Supabase services and [Kibana](https://www.elastic.co/kibana); persistent-volume writers retain stop-before-start fencing. Namespace relays are an exception: they retain their relay-specific readiness probe and remain stop-first because overlapping sessions evict one another and a reconnecting predecessor can displace the replacement. Rolling deployment needs platform capacity for overlapping instances and does not guarantee that existing client sessions survive retirement. Resource/count/routing updates and explicit Start/Stop/Restart retain their existing semantics.

## Reviewing audit activity

Events and Operations identify initiating actors, including persisted account names, [OIDC](https://openid.net/developers/how-connect-works/) issuer suffixes and API-token owner/name/ID. Use the complete actor value when filtering.

```sh
karactl -n dev events list --cluster demo
karactl -n dev operations cluster demo
# Installation audit access requires audit:read on '*'.
karactl events list --actor admin --resource accounts
karactl events list --resource auth -o json
karactl -n dev events list --actor alice --resource namespaces
```

Actor/resource filters are exact, case-sensitive AND matches intersected with the namespace. Preserve filters with `--cursor`; `events watch` supports them too. Audit scope, coverage gaps, indefinite retention and durability limits are defined in [Security](security.md#audit-attribution-and-durability). Check resource/operation state before retrying an action with missing audit evidence. Transport/replay: [API events](api.md#pagination-and-events).

For `reason: ClusterSpecChangeRequested`, the message summarizes up to the first three requested changes, for example `Cluster spec changed for demo (generation 3): components.engine.resources.maxCpu: 1 → 2`, followed by `and N more` for additional changes. A no-op reports `Cluster spec change requested for demo: no changes`.

1. With [structured audit access](access-control.md#policy-format), expand **Changed fields** in either the global or cluster Events view to compare **Before** and **After**. Missing fields normally display `(unset)`, distinct from JSON `null`. **Truncated** rows display omitted values as `(omitted)`; oversized values may display `(N bytes)`. A comparison-level warning means values or additional paths may be missing. **Sensitive** rows show only [redaction markers](security.md#audit-attribution-and-durability).
2. Inspect the structured comparison with `karactl -n <ns> events list --cluster <c> -o json`. See [API events](api.md#pagination-and-events) for paths, schema and limits.
3. Check cluster status and **Operations** for convergence; a requested spec change is not completion.

## Node-agent semantics

Agents pin the internal CA, authenticate with a per-member token and pull only their assigned configuration. They apply usable cached configuration at startup, retry first configuration every **five seconds**, and normally refresh every **60 seconds**. Cached members can keep running during control-plane unavailability; uncached startup cannot. Stateless caches do not survive runtime replacement.

Accepted configuration and maintenance/stopped-process holds are atomically persisted as owner-only **0600** files. Configuration contains locally required secrets. The supervisor owns child processes, restart backoff and SIGTERM forwarding; do not start competing supervisors. Maintenance holds suppress automatic restarts. Explicit stop holds survive config pulls and agent restarts until explicitly released by the controller. Do not edit safety files to force recovery. Engine-specific files and recovery holds belong in the engine operations guides above; private agent commands are not arbitrary remote exec.

Graceful agent shutdown stops children in reverse of their first launch-intent order, retaining that order across crashes and restarts. [PostgreSQL](https://www.postgresql.org/) HA launches [etcd](https://etcd.io/) before [Patroni](https://patroni.readthedocs.io/), so Patroni is stopped first while etcd can still accept its leader-key release. Maintenance resume uses the forward dependency order. Each child gets at most ten seconds and a share of the remaining shutdown deadline; a hung child produces a shutdown error but cannot prevent later children from receiving their graceful stop signals. No timeout triggers an automatic force-kill, and an exhausted deadline does not imply a clean engine shutdown.

Before child signals or HTTP draining, engines can perform a bounded
four-second graceful handoff. PostgreSQL HA uses Patroni's validated switchover
to an eligible standby while etcd remains alive; unsuccessful handoff still
runs the normal shutdown above. See [PostgreSQL operations](postgres/operations.md)
for synchronous-safety rules and platform stop-grace limitations.

The authenticated member status includes an `error` diagnosis when local storage, safety-state recovery or configuration application is blocked. Transport certificates can refresh independently of engine application; a reachable member is not proof that its desired configuration is applied. Mongo configuration versions advance only after required process actions succeed. Older configuration generations are rejected; unchanged desired Mongo files are compared with the actual files and repaired, including restrictive permissions.

- **Lost/corrupt local controls:** an established new-format member does not treat missing `karakoram/controls.json` as permission to run. Recover that file from trusted state for the same member, preserving stop/maintenance holds. Do not replace it with an empty object or remove `controls.initialized` to bypass fencing. Ordinary older members legitimately had only `config.json`: without new-format provenance, a valid legacy cache migrates automatically, retaining cached maintenance and desired-process intent; existing legacy controls remain authoritative. Interrupted first publication carries the exact initial controls in a recoverable initialization record, rather than requiring restoration of a file that never existed. Valid TLS configuration from cache or the control plane still enables authenticated diagnosis while starts remain blocked.
- **Damaged configuration cache or full/read-only storage:** malformed cached configuration is retried from the control plane rather than aborting before a pull. Identical files do not require publication. When writes are necessary but fail, status remains reachable once valid member TLS material is available; repair storage capacity/writability and let configuration refresh retry. This is not permission to initialize replacement database storage.
- **Surviving engine after supervisor death:** persisted `karakoram/processes/<process>.json` records bind ownership to PID, kernel start time and boot identity. A matching survivor blocks a competing start with an unmanaged-process conflict. Inspect the named process and use the engine's supported safe shutdown/fencing procedure; after that owner exits, the supervisor retries automatically. New interrupted-launch receipts also record the kernel boot identity: after a proven host reboot, no previous child can survive, so startup retries automatically without clearing safety files. Same-boot ambiguity, older receipts without a boot identity, and corrupt ownership records still require operator reconciliation of surviving processes. A task or agent restart alone is not reboot evidence. Never clear a record merely because the control plane cannot reach the engine.
- **Paused process:** a SIGSTOP/ptrace-stopped child is reported as `paused`, not `running`, with guidance to investigate and resume with SIGCONT. For [MongoDB](https://www.mongodb.com/), `status.members[].state` is `paused` and `status.conditions[]` includes `Process/<member>/<process>` (or `Process/<member>/<process>/<instance>` for pinned instance observations), with reason `ProcessPaused` and explicit recovery guidance. Every observed router instance contributes diagnostics: a healthy instance cannot hide a paused peer. Process errors are unhealthy with reason `ProcessError` and investigation guidance; consult the authenticated agent process status and logs for details. Independently observed database roles are preserved when publishing a process diagnostic. A reachable agent remains connected independently of child health or configuration-apply errors; these errors still prevent database health/readiness. A paused router's status skips the native metadata probe so the stopped child does not delay process diagnostics. The supervisor does not blindly kill or resume an administratively paused database. A timed-out restart retains desired-running intent and retries after the old child actually exits; descriptor exhaustion also retains bounded-backoff retries.
- **Uncertain restart response:** updated controllers and member binaries use a stable member-action request identity and durable restart receipt, so a lost response does not restart the recovered child again. Preparation/cancellation/control-write refusals before process mutation remain retryable under the same identity. An accepted restart that times out can acknowledge the supervisor's eventual replacement generation without stopping it again. A supervisor interruption across an uncertain execution boundary, or corrupt receipt, remains explicitly fenced for reconciliation. Older callers without identities and older member binaries retain legacy, non-idempotent semantics; a control-plane upgrade alone does not upgrade member binaries. Do not blindly replay uncertain legacy restart requests. Receipts and safety state must survive member restart.
- **Restart receipt retention:** each member retains at most 128 active receipts; unresolved outcomes are never evicted. Completed receipts are retired automatically before admitting a new identity at that bound. A durable, fixed-size 64 KiB conservative retirement filter refuses stale retired identities rather than re-executing them; a filter match may also conservatively refuse a new identity. Retirement is limited to 32,768 identities per member incarnation, with an explicit actionable refusal before filter saturation. Exhausting that allowance or filling the active quota with unresolved outcomes requires reconciliation or supported member replacement, not deleting replay history. Preserve `karakoram/restarts`, `restarts.retired` and `restarts.initialized`; missing established retirement metadata also fences new restarts. Invalid/unconfigured actions do not create execution receipts.

## Routine operating checks

- Review health, current allocations/disk headroom, failed Operations/events and generation convergence.
- Verify successful backups and storage capacity/access; rehearse data recovery, not just backup creation. See [backups](backups.md) and engine recovery guides.
- Preserve consistent state backups and the original master key; monitor credential/certificate expiry.
- Drain disruptive work before [upgrading Karakoram](upgrade.md). Existing member executables roll out on later engine driver-driven deployments, not control-plane restart alone.
- For project-token, relay or cross-namespace incidents, use [namespace operations](namespaces.md) and [relay health](namespaces.md#relay-health) before restarting members.

## Orphaned platform volumes

After runtime deletion and ownership verification, repeated mount-related volume-deletion refusals move the volume to orphan inventory. `VolumeOrphaned` allows member/cluster removal to finish, **not** storage deletion: disk usage and charges can continue.

```sh
karactl orphaned-volumes list
karactl orphaned-volumes list -o json
```

`GET /api/v1/orphaned-volumes` returns `{items: [...]}`. **Settings → Orphaned volumes** shows the same inventory: hostname, service ID, original cluster/member, failures, attempts, latest delete Process ID and retry timestamps. Cluster-read permission controls visibility; administrators also see records for deleted clusters.

The control-plane janitor retries after **two minutes**, doubling delays to **one hour**. `OrphanedVolumeDeleteFailed` warns on failure; accepted deletion removes the record and emits `OrphanedVolumeDeleted`. Events retain the original cluster. Before deletion, it checks the recorded service ID; absent/reassigned hostnames drop the record without deleting another service.

An in-flight delete retains its service-ID and process-ID fence. After 15 minutes
without completion, `lastError` and `OrphanedVolumeDeleteStuck` identify the
process to inspect or cancel in the namespace provider. A polling failure also
retains that fence. Neither elapsed time nor a disconnected control plane
authorizes another destructive request; confirmed service absence/replacement or
a terminal provider outcome permits normal reconciliation to continue.

The inventory has **no force-delete action**. With separate [Zerops](https://zerops.io/) project permissions, delete in the original namespace's Zerops GUI or use authenticated `zcli service delete <hostname>`. Persistent refusals need Zerops support: provide `serviceId` and failing `lastProcessId`, not secrets. The janitor drops manually removed volumes on its next attempt; do not edit state.

Recreated clusters/members skip orphaned and already occupied runtime/volume hostnames, allocating a new index. See [resource naming](adr/0009-resource-naming.md).

## Control-plane state backup and recovery

[SQLite](https://sqlite.org/) WAL state lives at `KARAKORAM_DATA_DIR/karakoram.db` (normally `/mnt/karakoramdata/karakoram.db`). It includes desired/observed state, ownership, checkpoints, events, identities/sessions/token hashes, policy/settings, metric history and encrypted credentials/PKI.

**Warning:** a state backup without its original `KARAKORAM_MASTER_KEY` cannot recover encrypted secrets. Preserve the key separately in a restricted secret manager, plus project identity and a replaceable integration token. Never replace the key on existing state. There is no `admin backup-db`, managed state-backup schedule or master-key re-encryption CLI.

### Storage faults and unreadable cluster state

At startup, unavailable or read-only state keeps the control plane unready and
logs a precise preservation/recovery message. Opening is retried every five
seconds without replacing the database; restoring free space and write access
allows startup to continue. Restore access for the database **and existing
WAL/SHM sidecars**. A live database lock/full condition rejects transactions
rather than accepting an unpersisted operation; existing committed intent is
retained and controllers retry after storage recovers. Metric history may have
gaps during the fault. Keep external alerts on control-plane logs and filesystem
capacity: a successful read-only health query alone does not prove writable
headroom.

A malformed logical cluster row is isolated from reconciliation and scheduling;
independently decoded clusters continue. Inspection errors identify its namespace,
name and damaged field with a restoration prerequisite. Karakoram does not invent
a replacement spec/checkpoint, adopt services, delete the damaged row, or
reinitialize an unreadable SQLite file. Preserve the entire state directory and
master key, investigate the storage failure and restore a verified consistent
snapshot before managing the damaged cluster. Physical database corruption can
make all state unavailable and requires operator recovery, not automatic repair.
An existing zero-length database is treated as potentially truncated
authoritative state and is **not** initialized again. Automatic first-install
initialization is reserved for an absent database path. An empty existing file
requires investigation and verified state recovery rather than silently
regenerating control-plane identities.

First installation migrates a uniquely named sibling `.karakoram.db.init-*`
staging file, checkpoints it independently of WAL sidecars, and publishes the
complete database atomically without replacing an existing path. An interruption
before publication leaves no authoritative database and startup can retry after
storage is repaired. Abandoned initialization stages are never adopted as state;
preserve them with the directory when investigating a storage incident. This
does not weaken the refusal to initialize over an existing empty database.


### Consistent backup

1. Let disruptive work settle. Stop the single control-plane runtime and wait for STOPPED.
2. In the Local Storage maintenance container, the volume is mounted at `/data`. Archive the **whole state directory**, including remaining WAL/SHM sidecars and persisted identity material, to a restrictively permissioned file **outside** that directory while no writer runs.
3. Transfer to encrypted off-platform storage. Record snapshot time, Karakoram version/revision and master-key identity; remove disposable copies after transfer.
4. Restart one control plane or proceed with the [upgrade](upgrade.md).

Never copy only a live `.db`, assume a live platform volume archive is consistent, or run two servers against one volume. A correctly used native SQLite backup tool can take a consistent live snapshot, but `sqlite3` is not guaranteed in the runtime. State archives exclude engine data; use engine-native backups separately.

### Restore a state snapshot

Fence every old control-plane process and preserve the failed/current state securely. Restore the complete archive into an emptied state directory on retained/replacement storage, without merging stale WAL/SHM files. Restore `zerops` ownership and restrictive permissions. Supply the original key, correct project identity/origins and valid integration token. Deploy one schema-compatible binary with singleton/temporaryShutdown settings.

Migrations are forward-only; an older binary does not roll back schema. State rollback rewinds specs, checkpoints, accounts/tokens and inventory, **not** external services or engine data. Inspect services, persisted Process IDs, members and engine recovery state before resuming disruptive checkpoints. Never replay an unknown restore initiation just because state is stale.

### Lost control plane

| Loss | Recovery |
| --- | --- |
| Runtime; state volume intact | Fence the old runtime; redeploy one runtime on retained state with original secrets, hostname and origins. Do not rerun fresh installation with replacement identities. |
| State volume; snapshot/key available | Restore as above; reconcile external changes since the snapshot. Preserve the private advertised endpoint and agent trust identity/internal CA. |
| State/key/snapshot unavailable | No automatic adoption/import-existing-cluster command exists. Recover original state/key or design engine-native data recovery into a new installation. Name collisions do not adopt services. |

RPO is the consistent snapshot interval. RTO includes replacement, secrets and external-state review. Control-plane downtime pauses scheduling/aggregation; it does not inherently stop existing engine processes or replication.

## Recovering local admin access

### Startup recovery

Keep the state volume, original master key and singleton (`maxContainers: 1`).

1. In **karakoram → Environment variables → Secret variables**, set `KARAKORAM_RECOVERY_PASSWORD` to a strong temporary value: **12 Unicode characters minimum, 1024 bytes maximum**. Optional `KARAKORAM_RECOVERY_ACCOUNT` defaults to `admin`. Use service secrets, not shared project/pipeline variables or CLI password arguments. Authorized platform automation can use `PUT /service-stack/{id}/user-data`: preserve unrelated entries, follow the current secret-variable schema and never log bodies/tokens.
2. Restart. Before listeners start, recovery resets/re-enables the local account, requires password change and revokes its sessions, refresh families and API tokens. A missing account becomes a local administrator. Recovery restores unrestricted `role:admin`, repairs this account's binding, clears conflicting denies/outgoing inheritance and replaces its previous roles; unrelated policy remains. It does not convert [OIDC](https://openid.net/developers/how-connect-works/) accounts: choose a local name. Review customized admin restrictions afterward.
3. Check the secret-free **“Local administrator access recovered; password change required”** log and `AdminAccessRecovered` event. Sign in over HTTPS, change the password, then sign in again. Reissue automation tokens deliberately.
4. Remove recovery variables and restart again to clear the process environment. Store only the final password.

A non-reversible receipt makes the same account/password apply only once, even if removed/re-added. A different password applies again. Interrupted recovery retries before serving; invalid inputs abort startup without account changes. Snapshot rollback rewinds receipts: remove stale recovery variables first unless reapplication is intended.

### Offline alternative

Stop every state writer. In a trusted maintenance environment, mount existing state and securely supply the original master key. Upload the matching executable privately; the maintenance container does not include it:

```sh
KARAKORAM_DATA_DIR=/data /tmp/karakoram admin reset-password admin
```

Use its terminal prompt or protected `KARAKORAM_RESET_PASSWORD` environment, never password arguments/history. Restart one server and complete mandatory password change. Sessions/tokens are revoked. Unlike startup recovery, this command does not recreate identities, repair RBAC or change OIDC passwords.

## Troubleshooting by symptom

| Symptom | Likely cause | Action |
| --- | --- | --- |
| READY_TO_DEPLOY | No executable deployed | Push/deploy the artifact; import acceptance is not readiness. Preserve retained data. |
| Startup/readiness fails | Token/project access, invalid key, origin or missing mount | Inspect deployment/application logs; check project access, master key ≥32 characters, URL and state mount without printing secrets. |
| `userDataZeropsPrefixForbidden` | Reserved secret prefix | Use documented `KARAKORAM_ZEROPS_*` names. |
| Edited secret not visible | Old process environment | Restart or redeploy; check YAML environment overrides. |
| Subdomain missing after failed deploy | Public access not enabled | After successful HTTP-port deployment, enable Subdomain access in Zerops. |
| Zerops token unauthorized | Wrong project/scope or expired token | Check integration token and [security](security.md); replacing a Karakoram API token cannot repair it. |
| Agent cannot pull config | Routing, pinned CA, identity or permissions | Restore original endpoint/state/CA/token environment; restart after secret changes. Never expose 9443 or disable verification. See [relay health](namespaces.md#relay-health). |
| Ownership conflict or expired platform Process | Incomplete legacy provisioning | Follow [upgrade recovery](upgrade.md); do not delete unrelated services. |
| API 403 or missing UI controls | Account/RBAC, required password change or cookie CSRF | Reauthenticate and check effective permissions; credential reveal has a separate grant. |
| Metrics denied/stale or charts empty | Authorization, disabled collection or failed scrape | Use [monitoring troubleshooting](monitoring.md). |
| Live events arrive in batches | Proxy buffering | Preserve cursors; read durable status/events before retrying work. See [event transport](api.md#pagination-and-events). |
| Engine health/recovery/placement warning | Engine driver-specific condition or platform constraint | Use the engine operations guides and [limitations](limitations.md); preserve evidence rather than clearing safety holds. |

## Externally stopped or deleted member services

Karakoram reconciles deployed member services against the cluster's running intent. An externally stopped runtime is started again. A confirmed-deleted logical runtime is imported with the same hostname and member identity, then deployed through the normal engine workflow, **without creating a new data volume**. Its original retained volume must still have the recorded identity. This applies to [MongoDB](https://www.mongodb.com/), [RabbitMQ](https://www.rabbitmq.com/), [Elasticsearch](https://www.elastic.co/elasticsearch), [PostgreSQL](https://www.postgresql.org/) and the stateless Elastic Stack/[Supabase](https://supabase.com/) components; Supabase's database storage follows PostgreSQL rules.

Recovery records an infrastructure operation and a `RuntimeRecovery` event. Failed or ambiguous submissions have a one-minute per-member cooldown, extended by any later provider `Retry-After` deadline and retained across control-plane restarts. Accepted recovery work must drain before MongoDB, RabbitMQ or Elasticsearch cleanup/suspension or a queued imperative operation can take ownership. Absent or stale inventory does not release an accepted import handle. Runtime imports first persist their exact secret-free batch; incomplete provider reservations resume that original batch, not a new subset or changed resource request. Deliberate cluster cleanup/suspension, removal and restore/maintenance workflows retain ownership of their stops; a stopped service is not a supported way to suspend an otherwise desired-running cluster.

[Nomad](https://developer.hashicorp.com/nomad) and [Dokploy](https://dokploy.com/) may restore an absent derived job/[Docker](https://www.docker.com/) service from their durable logical record before the engine acts. The engine helper leaves those transitional observations alone. [Zerops](https://zerops.io/) runtime recreation imports the runtime service only, then deploys it with the existing volume-service mount. An API error or unavailable storage/node is not treated as absence.

MongoDB router readiness requires authenticated majority metadata access from every live router instance. An upgraded control plane probes older agents without the additive `mongoReady` observation using the same native read over that instance's pinned namespace transport; a newer agent's explicit failure remains authoritative. No member rollout is required solely to obtain this observation. Existing active MongoDB member state is also migrated to an initialized-data receipt before physical-loss ownership checks, preserving its recorded volume incarnation.

### `VolumeLost` / `VOLUME_LOST`

An initialized member's data volume carries `karakoram/volume-identity.json`, bound to the cluster UID, member, volume hostname and logical volume incarnation. Updated member agents check it before applying configuration and before every child process start/restart. An empty replacement mount cannot silently initialize a fresh database under the old member identity. Existing installations without a sentinel are adopted only when recognizable retained native metadata is present. `VolumeLost` requires positively empty native storage on a member already recorded as initialized. Nonempty unrecognized layouts, partial/corrupt native metadata, retained upgrade/restore journals or retired data directories, and corrupt/mismatched sentinels instead report `VolumeIdentityUnavailable` with `lost: false`: startup remains blocked for original-volume repair/adoption, but these observations never authorize destructive member replacement.

Native emptiness excludes exact infrastructure-owned paths: Nomad/Dokploy's regular `.karakoram-writer.lock` file, known agent configuration, and the root ext4 `lost+found` directory (which may be unreadable by the member UID). These entries alone permit first initialization on a fresh authorized volume; they do not count as retained engine data on an initialized volume. The sentinel is validated before this classification, so corrupt or foreign sentinels remain fenced. Same-name symlinks and all other unknown native files/journals remain unknown, not permission to initialize or replace. All durable engine families share this gate; Supabase database members use the PostgreSQL core, while stateless Supabase components have no volume identity to initialize.

When storage is confirmed lost, status includes a `VolumeLost` condition and the member is `VOLUME_LOST`, not healthy merely because its platform service exists. For replicated engines, automatic recovery queues the existing safe `replace-member` workflow only when native surviving-data/quorum checks permit it. The replacement gets a new member/storage identity and must join/catch up before the old identity is retired. A singleton, lost primary-shard copy, missing writable source, or unverified surviving quorum refuses fresh initialization.

1. Preserve the affected storage and capture the named member, volume identity, condition and replacement-operation details. Do not delete or copy sentinels to bypass a refusal.
2. If the original volume/node is recoverable, restore that original storage and platform access. Do not substitute an empty same-name volume.
3. If healthy replicas retain the data, inspect the automatic replacement operation and its native safety blockers. A failed replacement is not endlessly re-enqueued; after resolving its blocker, use the supported engine `replace-member` operation if required.
4. If no healthy copy exists, recover the original data or restore an available backup into a new cluster. RabbitMQ definitions backups do **not** recover queued messages; preserve/recover the original message data where required.

Physical storage inventory can be unavailable or platform-local. Such observations remain an explicit storage/platform error rather than authorizing empty-volume creation. Upgrade both the control plane and deployed member agents before relying on the pre-start sentinel gate. See [the runtime/data-identity decision](adr/0037-runtime-data-identity.md) for placement, compatibility and safety boundaries.
