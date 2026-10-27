# 0057 — Rolling member upgrades, maintenance windows and explicit relocation

Status: Accepted; extends [ADR 0016](0016-placement-anti-affinity.md) using the [provider contract](0030-backend-neutral-infrastructure-contract.md) and [component model](0031-engine-component-model.md)

## Context

Upgrading the control plane does not replace executables already deployed to members. Database versions, configuration revisions and deployment retry budgets do not identify the running Karakoram build. Operators need a separate, observable rollout that preserves each engine's disruption safeguards.

Scheduled maintenance must cover both imperative operations and disruptive desired-state convergence without delaying emergency recovery. Placement guidance must distinguish authoritative provider placement from heuristic observations, and relocation must not promise an infrastructure guarantee that the provider cannot enforce.

## Decision

### Agent identity and rollout

Members report `agent.version`, `agent.commit` and `agent.build`; the build uses the same identity as `ExecutableArtifact.ArtifactIdentity()`. Persist these observations per member. Missing reports from older members mean unknown, not current. Shared observation computes `status.agents` with target build/version and outdated/unknown member lists. `AgentsCurrent` is `True/Current`, `False/Outdated` or `Unknown/Unreported`; it is informational and must not itself make a cluster unready or degraded.

Expose `POST /clusters/{cluster}/upgrade-agents` as a durable `upgrade-agents` operation, authorized by `clusters:update` on the cluster and protected by normal CSRF and idempotency handling. Optional `includeCurrent` defaults to false; return `409 AgentsCurrent` if no outdated or unknown member needs work. Engine drivers advertise support in their operation catalog.

Freeze the target build in the checkpoint when the operation is claimed. Redeploy the current executable through `engine.Deploy` to the **same runtime and volume**, at the **unchanged database version**. Skip members already at the target unless explicitly included. Use existing one-member-at-a-time disruption journals, allow cancellation between members, and retain native gates:

- [PSMDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) checks safe restart, primary step-down and catch-up.
- [RabbitMQ](https://www.rabbitmq.com) checks quorum, drains and revives through its agent-change action.
- [Elasticsearch](https://www.elastic.co/elasticsearch) uses its rolling allocation, flush and recovery gates.
- [PostgreSQL](https://www.postgresql.org) rolls replicas first, switches away from a primary and waits for streaming replication.
- [Supabase](https://supabase.com) rolls optional component runtimes after the PostgreSQL core.

Engine driver implementations decode the enqueue-frozen `engine.AgentUpgradePayload` (`includeCurrent`, `targetBuild`, `targetVersion`) with `engine.AgentUpgradeTarget(op)` and persist it in their rolling checkpoint at claim. `engine.MemberNeedsAgentUpgrade(member, target)` selects outdated or unreported members, or all initial candidates when `includeCurrent` is set. The checkpoint's completed-member journal takes precedence over selection, so an explicit refresh never repeats completed members. Compare the frozen build with `engine.CurrentAgentBuild().Build` before starting a deployment; if the control plane has changed again, fail before disruption rather than deploying different bytes under the old identity.

If the control plane changes builds after an Elasticsearch member has already been admitted, finish recovery using its existing artifact before retiring the old rollout. Corrected configuration may be published at that member's existing engine version without redeploying another executable. Restore allocation and native readiness, then fail the frozen rollout explicitly; do not strand its member or silently retarget subsequent members.


Installation setting `agentRollout` is managed through `GET/PUT /settings/agent-rollout`, with `settings:read/update`, an `automatic` flag defaulting to false and `maxConcurrentClusters` in 1–5, default 1. Responses include the current version, commit and build. Every 60 seconds, automatic scheduling considers outdated clusters except deleting, suspended and managed drill targets. It enqueues at most the installation-wide concurrency limit, without duplicating pending/running rollouts. Use a deterministic operation ID derived from namespace, cluster, incarnation and target build. A failed automatic rollout is not re-enqueued for that build; an operator may retry manually. Queued rollouts respect maintenance windows.

Failed ordinary desired-state synchronization must not prevent claiming an
agent rollout that can repair the old agent. After a failed synchronization,
the serialized reconciler may claim only `upgrade-agents`, retaining provider
claim policy and transactional maintenance admission. Running work still takes
precedence, and the rollout retains its native health gates and failure budget.
Other Pending actions continue to require successful synchronization. Frozen
targets are never silently changed to match a newer control-plane executable.


### Maintenance policy and disruption boundaries

`spec.maintenance` contains an IANA `timeZone` (default UTC) and at most 16 windows. Each window pairs a five-field cron **start schedule** with an explicit [Go](https://go.dev) duration of 15 minutes to 168 hours. Validate both with the scheduler's cron parser and timezone loader. No windows means always open. A shared evaluator returns whether the policy is open, the current end and the next start; cluster responses expose these under `status.maintenance`.

Gate new disruptions for `restart`, `rotate-tls`, `upgrade-agents`, `raise-fcv`, user-requested replacement or move, engine upgrades, rolling resource changes, restart-required configuration and scale-in. Automatic database leaf-certificate renewal is gated only while more than seven days remain before expiry.

Exempt healing and system lost-volume replacement, delete/suspend/resume, create/scale-out, backup/restore/initial restore, credential rotation, agent transport-certificate renewal and certificates within seven days of expiry. Window closure must never interrupt a member already past its stop/drain boundary.

Accept `ignoreMaintenanceWindow=true` on the gated imperative endpoints and cluster PUT/PATCH. Persist it on the operation, or as the server-managed override generation for desired-state convergence; an update override applies only to that generation.

Gate imperative work before claim: it remains Pending, consumes no attempt or execution budget, and records a `MaintenanceWindowWait` message/event once per operation. Engine drivers also call the shared disruption gate before **each** member's disruptive step. While waiting, publish `MaintenanceWindow=False/WaitingForWindow` with the next start and requeue within the lesser of five minutes and the time to that start. Maintenance waits are excluded from execution budgets, including waits encountered after an operation has started.

Only an invocation-local gate receipt matching the active operation pauses a running imperative operation's execution budget. Persisted desired-state conditions cannot pause unrelated exempt operations, member recovery, cancellation or failure cleanup.

### Placement evidence and explicit move

Report `placement.source` as `backend`, `best-effort` or `unavailable`. Set `guaranteedAntiAffinity` only when the provider advertises Placement, every member has an authoritative group and all groups are distinct. Fingerprint separation alone never establishes that guarantee or rack/zone independence. Emit `Warning MembersCoLocated` on transition into co-location, deduplicated by group set, and `Normal MembersSeparated` when it clears. UI guidance must reflect provider capabilities rather than use fixed platform wording.

Status persistence compares the previous and next co-located group sets and
commits the transition event with the status/checkpoint transaction. The stored
placement observation is the deduplication journal: restarting the control plane
does not re-emit an unchanged set, and unknown groups never count as co-location.

Expose `POST /clusters/{cluster}/members/{member}/move`, authorized by `clusters:update`, as `replace-member` with `member`, `relocate:true` and `avoidGroup` equal to the observed current group. It accepts the maintenance override. Supported members are PSMDB, RabbitMQ, Elasticsearch data/master members and PostgreSQL HA data members; unsupported members retain replacement's 409/422 refusal semantics.

Pass the exclusion as `VolumeService.SpreadAvoid`. [Nomad](https://developer.hashicorp.com/nomad) and [Dokploy](https://dokploy.com) exclude those nodes during volume selection. If no eligible node remains, fail with `infra.ErrPlacementUnsatisfiable` **before any destructive step**, explaining that no other eligible node exists. On a provider without Placement, including [Zerops](https://zerops.io), a move is an explicit best-effort runtime-and-volume replacement: state that no placement guarantee exists and re-observe the fingerprint afterwards. Do not introduce automatic relocation loops.

## Consequences

Fleet identity and database readiness remain distinct. Safe rolling progress can pause for native health, placement capacity or maintenance; a window is permission to start disruption, not a deadline to finish it. Automatic rollout is opt-in and bounded, and failures require an explicit retry rather than repeatedly disrupting a cluster.

All rollout and relocation deployment work uses normal engine driver reconciliation and provider admission caps, ownership checks and bounded retry accounting. Neither automatic scheduling nor a maintenance override bypasses those safeguards. Maintenance waiting does not exhaust execution budgets.

This refines ADR 0016 for placement-capable providers without strengthening Zerops' contract. Moving a member requires temporary replacement capacity and native data-transfer safety; it is not a generic host-migration primitive. Unknown placement remains usable but uncertain.

## Alternatives considered

- **Use the deployment retry epoch as the rollout epoch:** retry permission is not executable identity and cannot describe mixed running builds or freeze a target across restarts.
- **Restart all members, or run a generic parallel redeployer:** restart need not install new bytes, and parallel deployment bypasses engine-specific quorum, leadership and recovery gates.
- **Cron-only maintenance windows without duration:** a start instant cannot express an interval in which disruption is allowed.
- **Check maintenance only at API admission:** misses queued work, desired-state convergence and later member steps; forcibly stopping at window end can strand a database mid-transition.
- **Unconditionally refuse non-anti-affine placement on Zerops:** the provider cannot enforce it; refusal would block otherwise correct database operation without creating a new failure domain.
- **Keep recreating until fingerprints differ:** heuristic observations cannot justify an unbounded destructive placement search.
