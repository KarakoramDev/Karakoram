<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL operations

Use the shared [operation/generation model](../clusters.md#operations-and-status). A successful request or operation is not a database health guarantee. Keep client reconnect logic and capacity for replacement/rebuild work.

## Health and configuration

```sh
karactl -n NS clusters status appdb -o json
karactl -n NS clusters members appdb -o json
karactl -n NS operations list
```

Inspect the current primary, replica streaming/replay lag, etcd quorum access, member conditions and archive state. Status flags stopped/non-streaming replicas, replay lag above 16 MiB and loss of linearizable etcd access; those thresholds are not application-specific SLOs. Built-in metrics include connections/limits, database activity/size, replication and slot-retained WAL, archive failures/age, Patroni role/timeline and proxy pools. Monitoring still depends on control-plane collection.

Prometheus exposition uses `karakoram_postgres_*`; cluster metrics use canonical
`postgres_*` names. Historical counter series are reset-aware rates in
operations/second, including committed/rolled-back transactions (TPS), rather
than cumulative counts.

Allowed `engineOptions.parameters` and synchronous settings are rendered into native configuration. HA dynamic configuration is patched after bootstrap; changing bootstrap YAML alone would not update Patroni's stored settings. Reloadable changes do not restart PostgreSQL. Most postmaster-required changes roll replicas first, switch the writer, then restart the former primary. Single members reload where possible and restart for pending postmaster settings. PostgreSQL shutdown uses fast mode, terminating sessions rather than waiting indefinitely for idle pools. Proxy-only settings do not restart databases. See [TLS and access](access-and-tls.md) for certificate renewal and proxy session effects.

In HA, a member's restart-required signal comes from Patroni's `/patroni`
`pending_restart` field, which reflects its desired configuration. PostgreSQL's
raw `pg_settings.pending_restart` can retain recovery-target flags after Patroni
adopts a running postmaster, even when no setting would change; those flags must
not trigger a later unrelated switchover. If Patroni status is unavailable, native
flags are not substituted as a restart request. Single topology has no Patroni
and continues to use `pg_settings.pending_restart`.

The shared-memory settings `max_connections`, `max_prepared_transactions`,
`max_locks_per_transaction`, `max_wal_senders` and `max_worker_processes` require
direction-aware HA restarts. Increases restart standbys before switching the
writer. Decreases restart the current primary in place without switching to an
old-value standby, wait for replicas to replay the primary's WAL barrier on the
same timeline, then restart replicas. Removing an override uses its managed
default when choosing direction. Mixed increases/decreases use two durable
phases: first raise every value to the old/new maximum, then apply decreases.
Every member renders the same phase into Patroni's dynamic configuration, and
running GUC values, not just a cleared pending-restart flag, confirm completion.
Queued imperative operations wait while this journal owns convergence. Desired
scale-up can be edited during the rollout, but new member identities are not
allocated until its durable phases finish. A control-plane restart preserves
that ordering; ordinary learner provisioning then converges the requested size.

## Scale and replace

Change `spec.topology.type`, `spec.components.engine.replicas`, resources and `spec.components.extra.proxy` through a complete edit or merge patch. Only these data transitions are supported:

| From | To | Behavior |
| --- | --- | --- |
| single, 1 | ha, 3 | Preserve the original volume; start co-located etcd and have Patroni adopt existing PGDATA; add replicas |
| ha, 3 | ha, 5 | Add/catch up etcd learners one at a time, promote them and join database replicas |
| ha, 5 | ha, 3 | Move writer leadership before removing selected database/DCS members |
| ha | single | Rejected |

During single-to-HA conversion, only the original member may initialize the
Patroni scope. Empty new replicas wait for that member instead of racing to
bootstrap an unrelated database while its runtime is preparing.

HA requires at least one proxy; only single permits zero. Multiple stateless proxy replicas require backend horizontal scaling. Resource changes use replica-first ordering. Persistent disks only grow, and growth needs backend `diskGrowth`; requested size is not a quota on backends without quota enforcement.

If resource bounds prevent a runtime from starting, correct the bounds through
the cluster spec. Reconciliation supersedes a failed obsolete resize and applies
the latest requested resources to a runtime the backend reports as failed,
without requiring its unavailable agent to become healthy first. A disconnected
but still-running member does not bypass normal role and switchover safety.

Failed infrastructure receipts are handled independently of their completion
flag. A failed member or upgrade deployment is cleared for a fresh deployment
attempt rather than left pending indefinitely after a placement timeout.

The shared `replace-member` operation adds a new monotonically named replica, waits for streaming/DCS catch-up, removes the old etcd member, promotes the learner and retires the old runtime/volume. Maintain quorum and spare resource/disk capacity. Failed volume deletion is tracked by the orphan-volume janitor. The internal `reinit-member` operation rebuilds a non-primary replica; it is not a force-promote or arbitrary command endpoint.

Terminal replacement failure transfers its existing journal to ordinary
reconciliation instead of releasing an abandoned operation owner. Before any
predecessor-retirement intent, recovery removes the unused learner, stops/deletes
its new runtime and retires its volume while preserving the original member.
Once predecessor retirement has begun, recovery completes the same fenced
cutover; it never blindly discards partially executed membership changes or
durable stop holds. This recovery survives control-plane restarts, holds queued
imperative work until complete, and then permits a new replacement operation.

Replacement and 5→3 scale-in share a checkpointed retirement fence. A reachable
retiring writer first switches to a healthy streaming candidate (preferring a
synchronous standby) while still untagged, and waits to become a streaming
replica. Only then does it receive durable local Patroni tags `nofailover: true`,
`noloadbalance: true` and `clonefrom: false`; reload and `/cluster` confirm the
exclusion. If it regains leadership at any checkpoint, the control plane first
clears and confirms those tags before switching away again. Keeping a primary
untagged preserves Patroni's member replication slots with its default slot
retention settings. The agent also guards local rendering/reload against tagging
a primary or an unknown role. The agent then persists a stop hold before stopping
Patroni and PostgreSQL. Configuration pulls and agent restarts preserve that hold.
Membership removal and runtime/volume deletion require a fresh stopped
observation and a surviving primary, not just an earlier leadership check.
An agent that remains reachable while PostgreSQL briefly stops during demotion
waits for a streaming replica; a failed PostgreSQL action alone does not select
the unreachable-runtime fencing path. A responding agent preserves the original
reload/action/status error for retry diagnostics without advancing its retirement
checkpoint. Application errors from the generic status probe are reported
together with that original cause; only loss of transport selects backend fencing.

If the old agent is unreachable, retirement instead requires a surviving
member's `/cluster` view to identify a different primary with DCS quorum access.
The backend then durably stops the old runtime, suppressing automatic
rescheduling, and the control plane waits for terminal stop completion before
removing etcd membership or deleting infrastructure. A disconnected runtime
whose stop cannot yet be confirmed remains pending; loss of agent connectivity
alone is never proof that PostgreSQL has stopped. Both retirement paths resume
from their persisted checkpoints after a control-plane restart.

Learners cannot serve etcd's member-list RPC locally. Their known membership
identity is retained while PostgreSQL becomes ready, and promotion resolves the
identity from a voting member by its peer endpoint. A missing local identity does
not prevent a caught-up learner from being promoted.
Membership changes use a responding joined voter, exclude the replacement's
predecessor and learners, and try another voter if agent dialing or the membership
query fails. An unreachable predecessor does not prevent a replacement from joining.

A data backend is published to ProxySQL only after it has joined successfully
and its runtime has an operational `ACTIVE` or `RUNNING` status. Planned,
placement-blocked and deploying identities
are not monitored by the proxy. Each successful data-member join publishes a new
proxy configuration version. During initial bootstrap or single-to-HA conversion,
the proxy may temporarily run with no published backend until a data member joins.

ProxySQL places newly discovered backends in its reader hostgroup until the
native role monitor identifies the writer. Server-list reloads preserve existing
runtime hostgroup decisions rather than temporarily routing writes to replicas.
Before a data runtime is stopped or retired, its backend is removed from the
proxy configuration at a durable configuration version. The control plane
enumerates the logical proxy service's live instances and waits for **every**
`ACTIVE`/`RUNNING` instance to report that applied version. A successful reload
request, one replica's acknowledgement, or repeated requests to service DNS do
not authorize retirement. Proxy readiness and cluster-ready configuration
checks also observe all live instances; the reported applied version is their
minimum, not that of an arbitrary answering replica.

Instance probes use Zerops container addresses, Nomad allocation CNI addresses,
and Dokploy's individual Swarm task DNS names. They keep the logical member's
pinned CA, TLS hostname and authentication, including namespace relay transport.
For relayed namespaces the tunnel keeps the policy-allowed logical member and
agent port, with a separate instance selector. Before opening each instance
tunnel the hub verifies the selected address against that namespace backend's
fresh live-container inventory for that member. The relay permits IP/task-DNS
destinations only on this authenticated instance path, still rejects prohibited
addresses, and never substitutes a load-balanced logical acknowledgement.
Infrastructure inventory is refreshed on every retry: gone or non-running
instances no longer block retirement, while an unreachable still-running
instance or unavailable inventory remains a safety barrier. A newly started
instance pulls the current configuration before starting ProxySQL. No
acknowledgement survives by logical service identity alone. An empty live
inventory cannot make a proxy ready, but has no running monitor left to drain.
The old data hostname remains resolvable throughout this handoff, so stale
monitoring of a deleted hostname cannot stall checks of the healthy writer.

ProxySQL's PostgreSQL local DNS cache uses a 5-second TTL and a 1-second refresh
interval, with the resolver queue setting retained at 128. These settings are
written at bootstrap and reconciled into runtime and disk on configuration Apply.
The asynchronous cache remains enabled: platform service discovery can move a
member's address without changing its hostname, and connection retries alone do
not invalidate a stale cached address. Healthy monitor checks are therefore not
proof that new application connections can reach the writer.

After upgrading an existing proxy, an already cached address can retain its old
expiry (roughly the previous 5-minute TTL, plus jitter and resolver scheduling)
until the next resolution. Allow that
transition before testing address changes, or restart the proxy during a planned
connection interruption. Once the new policy is active, DNS propagation,
resolution time, backend readiness, role monitoring and connection retry/shunning
can still add recovery delay; the 5-second TTL is not an end-to-end outage bound.

On the current Nomad backend, proxy replicas share one runtime job. Port/exposure,
resource and replica-count mutations use the durable stop-before-start operation:
the backend stops **all** allocations, waits for them to become terminal, then
registers the replacement job. The job's `MaxParallel=1` update setting does not
make this path rolling. Two proxy replicas therefore do not guarantee private
endpoint continuity during these changes. A native lab exposure enable/disable
with two replicas measured maximum acknowledged-write gaps of 27.521/24.784
seconds (90/79 failed reconnect attempts); these are observations, not bounds.
Schedule an interruption for these mutations until the backend provides
serving-gated serial replacement. Normal availability also requires clients to
reconnect to another healthy proxy rather than pinning one allocation address.

## Upgrades

Use catalog paths from `karactl providers get postgres -o json`. Minor changes redeploy exact packages through replicas, switch over and redeploy the former primary; single topology interrupts service.

PostgreSQL-version-only changes, minor or major, do not redeploy ProxySQL.
Its pinned package and advertised protocol version are independent of the
PostgreSQL package release; the proxy does not emulate the backend's version.
Proxy member version metadata converges through the normal configuration
hot-reload path, which continues to reconcile backends, users and TLS settings.
Major upgrades still interrupt database availability during their offline window,
even though the proxy allocation remains in place.

Use at least two proxy replicas when zero downtime is required for proxy resource,
listener/port, exposure or package changes, together with a backend that rolls
allocations one at a time and waits for the replacement to serve before proceeding.
A sole proxy necessarily interrupts clients when it must restart or be replaced;
existing sessions on a replaced proxy can also be interrupted with multiple
replicas, so clients must reconnect. PostgreSQL version changes are not an agent
binary refresh mechanism: updating the control-plane binary alone does not replace
agents already running in member allocations.

Adjacent major upgrades (16→17, 17→18) require a completed backup of the current version and an offline maintenance window. The checkpointed workflow installs old/new packages, durably pauses Patroni elections in DCS and waits for every data member to acknowledge maintenance mode before publishing shutdown intent. It then records the paused writer and timeline, stops and confirms all replicas first, verifies the recorded writer is still authoritative on the same timeline, and stops that writer last. Writes may continue on that writer until its fast shutdown; no replica may promote during the handoff. Single topology stops its sole writer without Patroni.

Every member must acknowledge durable stopped process intent and be freshly observed stopped before either `pg_upgrade --check` or `--link` can run on the final writer. Paused Patroni can leave PostgreSQL running with inherited log pipes, so applying stopped intent first fast-stops the postmaster and only then waits for Patroni to stop. This lets the member acknowledge the offline configuration without hanging on those pipes. The journal and DCS pause survive control-plane restarts; an unexpected writer/timeline change fails closed for recovery rather than upgrading a stale seed. Cached member configuration preserves an intentionally absent PGDATA between activation renames.

The workflow then resets the Patroni scope (including its maintenance pause), starts the upgraded writer, rebuilds replicas, upgrades the pgBackRest stanza and queues a full backup. Final redeployment removes old-major packages. Pending imperative work, including scheduled/manual backups, restores, replacement and restarts, remains queued until major maintenance releases ownership; queued wait does not consume execution attempts or the execution deadline. Upgrade progress therefore cannot be preempted by backup retries against stopped databases. Backup source metadata uses the observed source version and native pgBackRest inventory, never the desired upgrade target. Supabase allows only PostgreSQL 17 minor paths.

Upgraded replicas rebuild from the new primary with basebackup. Backups of the
previous major are not eligible replica seeds; pgBackRest replica creation is
re-enabled only after a full backup of the new major completes.

**Do not start the old data directory after link-mode activation.** Restore the pre-upgrade backup when rollback is needed; package downgrade is not recovery. Skipped majors and downgrades are rejected. A queued post-upgrade full must succeed before treating it as a new recovery base.

## Failover, suspension and deletion

Patroni and etcd, not the control plane, establish HA writer authority. A majority outage can intentionally make the database unavailable. Strict synchronous mode can block writes without an eligible standby. Rewind and automatic divergent-directory rebuild help replicas rejoin; neither replaces backups nor guarantees data survival under every durability override. Proxy endpoints stay stable, but sessions/transactions can be interrupted.

Retained-volume suspension removes runtimes, including the stateless proxy, while preserving PostgreSQL and etcd data/identity. Resume recreates those identities from retained volumes. `keepBackups` is independent of volume retention. See [delete/suspend](../clusters.md#delete-or-suspend), [recovery](backups.md) and [orphan cleanup](../operations.md#orphaned-platform-volumes). Do not erase state or manually reset DCS to bypass a pending maintenance/deletion operation.
