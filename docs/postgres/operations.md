<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL operations

Use the shared [operation/generation model](../clusters.md#operations-and-status). A successful request or operation is not a database health guarantee. Keep client reconnect logic and capacity for replacement/rebuild work.

## Agent rollout and maintenance windows

`karactl -n NS clusters upgrade-agents appdb` redeploys the current Karakoram
executable to the existing runtimes and volumes without changing [PostgreSQL](https://www.postgresql.org/)'s
applied version or data/[etcd](https://etcd.io/) identities. Unlike a process restart, this updates
the member agent. HA replicas precede the primary; the primary must pass the
normal local-etcd and safe-candidate switchover checks before redeployment.
Each deployment waits for readiness, streaming catch-up and the frozen target
agent build before advancing. Standalone and proxy runtimes incur an interruption.

Already-current members are skipped. `--include-current` explicitly refreshes
them too, but never repeats a journaled completed member after a control-plane
restart. Cancellation finishes any in-flight member and stops between members.
The operation freezes its target build; a different control-plane build refuses
to start the next disruption. Re-enqueue against the new build instead.
Deploy admission caps and deployment retry budgets still apply.

`spec.maintenance` gates the start of desired engine-version changes, major
offline upgrades, rolling resource changes, restart-required settings (including
directional shared-memory restarts), and scale-in. Each new member disruption
waits independently; work already stopped/deployed continues to readiness after
the window closes. Automatic database leaf renewal waits only while more than
seven days remain. Agent transport renewal, emergency database leaf renewal,
healing/runtime recovery, creation/scale-out and recovery operations remain
ungated. Restart, TLS rotation and agent-rollout operations wait before claim.
`MaintenanceWindow=False/WaitingForWindow` explains a delayed convergence;
waiting does not consume the operation budget. No configured windows means
always open. The shared [operations guide](../operations.md) describes policy
syntax and `--ignore-maintenance-window` overrides.

## Health and configuration

```sh
karactl -n NS clusters status appdb -o json
karactl -n NS clusters members appdb -o json
karactl -n NS operations list
```

Inspect the current primary, replica streaming/replay distance, [etcd](https://etcd.io/) quorum and **each local voter**, member conditions and archive state. Status flags stopped/non-streaming replicas, replay distance above 16 MiB and loss of linearizable etcd access; those thresholds are not application-specific SLOs. Replica distance compares the primary's current LSN with the replica's replay LSN only on matching system IDs and timelines. Missing/incompatible samples are unknown, not zero. The replica-local `replay_backlog_bytes` metric measures received-but-not-replayed WAL and deliberately does not claim to measure transport lag. The primary's `replication_lag_bytes` measures its maximum downstream replay distance.

Members advertise each implemented health observation independently of whether
its latest sample succeeded. Older member binaries that omit those capabilities
retain their supported readiness checks after a control-plane-only upgrade.
New slot, WAL/archive, local-DCS, cross-member WAL-distance and pause conditions
report `Unknown/Unsupported` for such members rather than treating missing JSON
fields as failed probes. Streaming, reported legacy lag and explicit observed
faults still apply. A member that advertises a capability but cannot obtain the
required sample remains fail-closed and makes a formed cluster `Degraded`.
A control-plane upgrade or configuration reload does not upgrade member binaries;
refresh those binaries deliberately to enable the additional observations.
`CredentialRecovery=False/LegacyAgent` likewise recommends a binary refresh for
durable credential replay and is informational, not a legacy readiness gate.

Read-only member/role observations have a four-second deadline, independent of
long-running engine actions. Direct-data readiness also bounds its subsequent
generic agent-status read independently to four seconds; expiry withholds readiness
and identifies the member whose responsiveness/connectivity should be inspected.
Reload and restart mutations retain their original budgets. Data-member status
probes and maintenance role discovery run concurrently, so an unreachable former
primary does not serialize healthy peers behind the engine action timeout.
Publication still follows the reconcile interval and infrastructure/API latency;
this is not a failover SLA.

On IPv6-capable providers, PostgreSQL listens on both available address families
(`listen_addresses='*'`, including Patroni-managed instances). Service DNS can
publish both A and AAAA records: a proxy may select either address, while a
direct client can silently fall back from one to the other. A PostgreSQL `::`
listener alone does not accept IPv4 connections. This does not change TLS,
SCRAM or client-CIDR enforcement, or the IPv6 listener policy for etcd and
Patroni REST. Existing IPv6-only PostgreSQL listeners need an agent rollout
containing this correction; a control-plane-only update or proxy restart does
not change a running PostgreSQL listener.

On graceful **agent runtime termination**, an HA leader first requests [Patroni](https://patroni.readthedocs.io/)
`/switchover` while Patroni and the local etcd voter are still alive, before
draining the agent HTTP server. The handoff has at most four seconds, including
waiting for local configuration/action serialization. It selects the streaming,
unfenced standby with the highest received WAL position on the leader timeline;
in synchronous modes only synchronous/quorum standbys are eligible. Candidate
fencing follows Patroni's `nofailover`/`failover_priority` precedence. Patroni
revalidates the request. Local or CP maintenance, an explicit Patroni stop hold,
desired processes excluding Patroni, and retirement suppress this extra handoff:
the lifecycle operation retains its own pause, writer-last and retirement ordering.
Paused clusters and missing safe candidates fall back to ordinary
dependency-ordered shutdown, never forced promotion or leader-key deletion.
The runtime-stop path does not remove/re-add etcd membership.

The Patroni HA loop is one second; the 20-second DCS lease and five-second retry
timeout remain unchanged (`loop_wait + 2 * retry_timeout <= ttl`). Strict
synchronous commit is not relaxed. [Nomad](https://developer.hashicorp.com/nomad) delivers SIGTERM through the same
agent path and renders a 120-second kill timeout; its client
`max_kill_timeout` must allow that value. [Zerops](https://zerops.io/) does not expose a stop-grace
setting in Karakoram's renderer, and its public deployment lifecycle docs do
not specify SIGTERM-to-kill timing. A platform stop request includes platform
queueing before signal delivery: neither this bounded handoff nor the HA loop
guarantees a five-second request-to-acknowledgement SLA.

Rolling resource/configuration changes re-read the primary and standby health
before each handoff. Only running, streaming, unfenced replicas are eligible;
with synchronous replication enabled, the candidate must be synchronous.
A concurrent graceful stop or Patroni failover does not pin the old leader or
candidate: reconciliation waits for a safe candidate and resumes from the new
topology without consuming the operation's failure-retry allowance for native
leader/candidate changes. Retirement and shared-memory rolling changes use the
same handoff checks, including for [Supabase](https://supabase.com/)'s [PostgreSQL](https://www.postgresql.org/) members.
An advertised local-DCS health failure blocks a handoff. Older agents that omit
that capability or leader observation retain role-based, Patroni-validated
handoffs; an unsupported probe is not treated as failed quorum. An explicit
leader mismatch still blocks the request, as does missing leader evidence from
an agent advertising modern health observations.

If Patroni is paused or a current agent explicitly classifies a rejected handoff,
the update fails with an actionable diagnostic rather than repeatedly submitting
the same unsatisfiable request. `Switchover=False/SwitchoverBlocked` withdraws `Ready` and
identifies the blocking state. Inspect Patroni pause, candidate health,
synchronous membership and DCS access before retrying the change. Only bounded,
allowlisted native rejection classes are exposed; raw Patroni responses are not.
Legacy plaintext conflicts and unknown protocol diagnostics remain retryable
under the existing operation policy: they may represent transient failures and
cannot establish a permanent native rejection. Every attempt rediscovers topology.
No path forces promotion, relaxes synchronous commit or bypasses retirement's
single-writer fences.

Status visits up to eight proxy instances concurrently, with a four-second
budget for each instance's combined generic/native status observation inside
the five-second observation sweep. Every live rollout instance is included.
A hung native endpoint withdraws proxy health and reports the affected instance,
but responsive agents remain connected and healthy database members retain their
fresh native evidence; instance deadlines no longer accumulate serially.
Data-member and proxy-instance HTTP observations start together; only evaluation
of proxy routes waits for the current primary identified by that same sweep.

Proxy health requires an `ONLINE` writer in runtime hostgroup 10 once providers
are published. A listening proxy with only `SHUNNED` writers or online readers
is unhealthy, and the cluster is `Degraded` with `ProxyWriter=False` even if PostgreSQL itself is healthy.

Exposure changes and proxy rollouts are not complete when a port update is merely
accepted or an old proxy is still serving. Generation acknowledgement and `Ready`
wait for the infrastructure rollout to finish, then freshly check the applied
configuration and native writer health on **every live proxy instance**. On Nomad,
a running canary is not a successful deployment: readiness remains withdrawn
through canary promotion and serial replacement, without stopping the serving
predecessors or disabling native auto-revert. A failed rollout cannot become ready
just because one of its tasks is running.
Configuration acknowledgement remains separate from this final rollout barrier:
healthy serving predecessors may apply corrected configuration or proxy counts
after a failed rollout, but cannot complete the generation or restore `Ready`
until the resulting infrastructure rollout succeeds.
With proxy count zero, the direct PostgreSQL endpoint's runtime rollout and fresh
native/configuration observations provide the same completion barrier, including
public-access removal. Suspension and deletion use independent cleanup paths;
configuration-only proxy drain acknowledgements do not wait for rollout health.

When a listening proxy has no `ONLINE` writer, the agent also makes a bounded,
authenticated read-only request through that proxy's writer route
(`SHOW transaction_read_only`). This exercises native demand-driven recovery
of automatically shunned providers on an otherwise idle proxy. TLS-enabled
frontends still require the cluster CA and proxy hostname to verify. A failed
request, a read-only provider, or a runtime read-back without an `ONLINE` writer
remains unhealthy; the probe never forces a server online or promotes a replica.

Each runtime provider optionally reports `diagnostics` in the private PostgreSQL
agent status: the last failed native connect, ping and read-only check as an
allowlisted error class and Unix-microsecond timestamp, plus summed connection
pool `ConnERR` and whether that counter was observed. Native messages, SQL,
credentials and connection strings are never returned. Collection is read-only,
limited to 32 provider identities and a shared 250 ms deadline; missing native
tables do not change readiness. `ProxyWriter=False` includes a bounded summary
from failing directly observed proxy instances. Older agents omit diagnostics.
The timestamps may precede the outage: compare them and counter deltas across
samples rather than treating an old monitor error as the current root cause.

**Proxy reconnect capacity.**

Each proxy's per-server provider pool is derived from the smallest old/desired
PostgreSQL `max_connections` during a rolling change, not from the frontend
connection limit. With `N` proxy instances, the limit is
`floor((max_connections - 8) / N) - 3`: three PostgreSQL superuser-reserved slots,
five management/backup slots and three monitor connections per proxy are kept
outside application pools. `N` reserves the maximum simultaneously serving set:
Nomad and [Dokploy](https://dokploy.com/) reserve `max(old replicas, desired replicas) + 1`, including
ordinary resource/configuration rollouts; an unknown provider surge bound
(including Zerops) reserves both complete old and desired sets. Thus a default
100-connection PostgreSQL server permits 43 application connections per proxy
during a singleton replacement, not 89. An insufficient budget is rejected
rather than silently borrowing reserved slots. Direct clients must also respect
this headroom; the proxy cannot cap external direct connections.

Reduced budgets are published before proxy scaling, resource rollouts and
PostgreSQL capacity decreases; every existing live proxy must acknowledge the
configuration before infrastructure may introduce overlap. Writer/reader hostgroups remain exclusive
(`monitor_writer_is_also_reader=false`) so one writer does not receive two pools.

Reconnects use two retries with 100 ms spacing, a one-second per-attempt timeout
and a three-second total acquisition timeout. New provider connections are
throttled to 20 per second per hostgroup. Three connection failures can shun a
provider, but automatic recovery eligibility is one second (the pinned version's
integer-time comparison can add a second). This bounds retry amplification
without disabling native failure detection or replaying failed transactions.
The idle pool retains at most ten percent of its bounded capacity; connection
warming stays disabled. Although the idle-ping interval is configured to one
second, [ProxySQL](https://proxysql.com/) 3.0.11's PostgreSQL idle ping performs no socket I/O and cannot
detect dead pooled connections. Connect/ping monitors run every second, role
checks every 500 ms, with 500 ms timeouts and two consecutive ping/role-timeout
failures before fencing.

Independently of client traffic and CP readiness polling, the proxy agent probes
each provider's `pg_postmaster_start_time()` with monitor credentials and the
configured provider TLS trust, on a one-second maintenance cadence. A changed
previously observed epoch fences that server `OFFLINE_HARD`, loads runtime,
waits up to three seconds for both free and used pools to empty, then restores
its original `ONLINE` status and loads runtime again. Restoration is attempted
with a separate bounded context even on cancellation or failure; failed flushes
retain the old epoch for another attempt. Before fencing, the agent durably
records ownership in `postgres/proxy-epoch-recovery.json`. Failed restoration
is retried before epoch queries or ONLINE guards, including after agent restart,
and ownership is cleared only after restoration succeeds. Each global server
load carries forward observed runtime hostgroups and native non-ONLINE states,
so flushing a restarted replica does not reload stale roles for a healthy writer.
Temporary status changes for other servers are journaled before mutation too;
their configured status is restored without another runtime load. Failed resets
retain durable ownership and are repaired before any later load or epoch probe,
including after an agent restart.
First observations and unreachable
providers do not trigger a flush, and native `SHUNNED`/offline states are left to
native recovery rather than forced online. Unchanged postmaster epochs during
Patroni failover do not trigger flushing. The flush serializes with configuration
and lifecycle changes and emits one structured event with server, old/new epoch,
duration, connection count and error.

This prevents connections from a dead postmaster epoch persisting indefinitely
in an apparently healthy idle pool after a fast restart. A single-provider proxy
briefly has no provider during the flush; clients must retain the documented
connection/transaction retry contract, including requests racing restart detection.

These settings are reconciled in admin memory, runtime and disk, not merely
written to the first-boot configuration. ProxySQL 3.0.11's PostgreSQL
`shun_and_killall` also reads the **[MySQL](https://www.mysql.com/)** monitor ping interval when setting its
recovery timestamp; Karakoram sets that interval to one second to prevent the
unrelated default from extending PostgreSQL recovery. See the
[PostgreSQL variables](https://proxysql.com/documentation/global-variables/pgsql-variables/),
[native monitor documentation](https://proxysql.com/documentation/postgresql-backend-monitoring/)
and [pinned implementation](https://github.com/sysown/proxysql/blob/v3.0.11/lib/PgSQL_HostGroups_Manager.cpp).
Actual recovery time depends on PostgreSQL accepting authenticated connections,
TLS/CPU capacity and client reconnect behavior; retain acknowledged-write
evidence when validating a connection-storm recovery.

`Failover=False/UnexpectedPatroniPause` means Patroni elections are disabled.
Ordinary reconciliation preserves native pauses: absence of a Karakoram journal
does not prove that an operator did not intentionally pause the cluster. Verify
cluster-wide maintenance intent before explicitly resuming Patroni. A
Karakoram-owned major-upgrade pause is resumed only by that upgrade's durable
operation, never by ordinary health reconciliation.

`EtcdMembers=False/LocalEtcdUnavailable` identifies failed local DCS probes even
when `EtcdQuorum=True` through another voter. Restore the member's etcd process or
network dependency. If its store is persistently corrupt, use `replace-member`
while quorum survives; Karakoram does not erase/reseed an unclassified store.
`EtcdQuorum=False` requires restoring a majority of the existing voters. If that
is impossible, preserve surviving data and follow an explicit etcd snapshot/backup
disaster-recovery procedure; automatic `force-new-cluster` is never attempted.

A failed data-member native observation withdraws member health and Ready, with
member, Ready-condition and cluster messages naming the member and inspection
steps. `Replication`, `EtcdMembers` and `Failover` become
`Unknown/ObservationUnavailable` when required evidence is missing, unless fresh
positive failure evidence already makes that condition False. A successful
linearizable probe from another member still proves `EtcdQuorum=True`; without
such evidence an unavailable member makes quorum Unknown, not proven unavailable.
Inspect agent/native PostgreSQL/Patroni health and connectivity before failover
or maintenance; a timeout alone never proves a DCS outage or a Patroni pause.

`EtcdReconfiguration=False/MembershipChangePending` identifies the replacement's
`remove-etcd` or `promote` checkpoint. A second voter fault may prevent strict
reconfiguration; restore the failed voter rather than bypassing etcd's checks.

With backup storage configured, `WALArchive=False/ArchiveFailing` identifies the
writer and repository dependency. `WALCapacity=False/DataDiskPressure` reports
less than 10 percent free data-filesystem space. Restore repository access or grow
the volume, and monitor `wal_size_bytes` and `archive_pending_segments`; do not
remove WAL files or disable archiving to clear the condition. PITR observation
errors report `PITR=Unknown` without replacing current database/quorum health or
retaining an old recovery window.

`ReplicationSlots=False/SlotWALRetention` names slots retaining over 1 GiB or whose
required WAL is lost/unreserved. This blocker also reports
`Ready=False/SlotWALRetention`, not `MembersStarting`, unless another
higher-priority readiness diagnosis applies. Serving member processes can remain
healthy while slot retention prevents overall readiness and generation convergence.
Per-slot `slot_retained_wal_bytes`, `slot_active`
and `slot_wal_unavailable` metrics include the consumer identity. Recover the
consumer or explicitly rebuild/remove only a verified obsolete slot. The native
`max_slot_wal_keep_size` default remains unlimited: imposing a finite cap can
invalidate logical consumers and discard their recovery history. You may
explicitly set it through `engineOptions.parameters` (for example `"4GB"`), after
planning consumer rebuild/recovery. It is enforced at checkpoints, not a hard
filesystem quota. Karakoram never drops or advances slots automatically.

User-created logical replication slots are not synchronized or preserved across
an HA failover. Karakoram does not configure Patroni permanent logical slots or
PostgreSQL failover-slot synchronization; Patroni's managed physical member slots
do not preserve a logical consumer's position. The consumer owns slot recovery:
verify its slot on the new primary, recreate it if absent, and re-synchronize the
consumer's data before relying on the stream again.

Built-in metrics also include connections/limits, database activity/size, archive
failures/age, Patroni role/timeline and proxy pools. Monitoring depends on
control-plane collection; unavailable archive/slot samples are explicit unknowns.

[Prometheus](https://prometheus.io/) exposition uses `karakoram_postgres_*`; cluster metrics use canonical
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
[Patroni](https://patroni.readthedocs.io/) scope. Empty new replicas wait for that member instead of racing to
bootstrap an unrelated database while its runtime is preparing.

HA requires at least one proxy; only single permits zero. Multiple stateless proxy replicas require provider horizontal scaling. Resource changes use replica-first ordering. Persistent disks only grow, and growth needs provider `diskGrowth`; requested size is not a quota on providers without quota enforcement.

If resource bounds prevent a runtime from starting, correct the bounds through
the cluster spec. Reconciliation supersedes a failed obsolete resize and applies
the latest requested resources to a runtime the provider reports as failed,
without requiring its unavailable agent to become healthy first. A disconnected
but still-running member does not bypass normal role and switchover safety.

An initial placement-blocked runtime, including [ProxySQL](https://proxysql.com/), can also accept corrected
bounds before its original deployment completes. Volume-member admission requires
proof of never acquiring an allocation; stateless ProxySQL can also use an exact
retained blocked-evaluation chain after the original evaluation is collected,
because its correction registers a canary-first update without a provider-issued
predecessor stop; [Nomad](https://developer.hashicorp.com/nomad) replaces remaining predecessors destructively after
promotion.
Reconciliation persists the replacement deployment receipt
and keeps Ready false until placement, configuration and application health
converge. A spec correction does not re-run a completed initial restore.
The admission and exact receipt are checked by the provider. Previously initialized
members retain normal role/switchover ordering. For a never-ready volume member,
the provider stops the old job to fence placement, then rechecks allocation
history: if placement raced admission, replacement is refused with
`UnplacedResizeDeferred` rather than blindly restarting the new allocation.
That narrow initial-bootstrap window can stop a newly placed task; it is not an
atomic scheduler check-and-stop. The stale receipt is retired and the failure
blocks ordinary redeployment until explicit `clusters retry-deployments` re-arm.
Engine driver/provider applied bounds are restored together; re-arm synchronizes current
desired capacity before deploying. Inspect its diagnostic before re-arming.

Safe owned-volume growth runs before database health and maintenance gates.
After disk exhaustion stops [PostgreSQL](https://www.postgresql.org/), increase the desired disk bounds on a
provider with `diskGrowth`; reconciliation does not require working SQL to
request that growth. The same capacity path remains available during a
shared-memory rollout, major upgrade or restore recovery. Provider-confirmed
failed runtimes can receive corrected resource bounds without clearing those
journals or their stop holds. This does not permit shrinking volumes, resizing
an unreachable running writer outside switchover rules, or bypassing a
major-upgrade data fence with replacement/reinit.
During maintenance, a stopped PostgreSQL child with a still-running agent may
also receive corrected bounds when another fresh quorum-backed primary proves
it is not the writer, or when its durable offline configuration is acknowledged.
An unavailable probe by itself is not evidence that resizing a writer is safe.

Failed infrastructure receipts are handled independently of their completion
flag. Retryable failures release the member or upgrade deployment for another
attempt. [Zerops](https://zerops.io/) exact-version deployment retries have a hard budget: exhaustion
records `DeploymentRetriesExhausted`, clears the pending handle without declaring
the requested version applied, and stops further submissions for that target.
Other members and the proxy remain observable and independently provisionable;
quorum and maintenance fences still prevent unsafe rolling progress. After
correcting the platform/runtime failure, run
`karactl --namespace NS clusters retry-deployments NAME` to grant a new budget.

The shared `replace-member` operation adds a new monotonically named replica, waits for streaming/DCS catch-up, removes the old [etcd](https://etcd.io/) member, promotes the learner and retires the old runtime/volume. Maintain quorum and spare resource/disk capacity. Failed volume deletion is tracked by the orphan-volume janitor. The internal `reinit-member` operation rebuilds a non-primary replica; it is not a force-promote or arbitrary command endpoint.

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
together with that original cause. Provider fencing is also permitted when fresh
generic status explicitly reports a stopped or failed Patroni supervisor with no
owned PID; it still requires the surviving primary and terminal provider-stop
evidence below. A transient demotion or failed engine action alone is insufficient.

If the old agent is unreachable, retirement instead requires a surviving
member's `/cluster` view to identify a different primary with DCS quorum access.
The provider then durably stops the old runtime, suppressing automatic
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

A data provider is published to ProxySQL only after it has joined successfully
and its runtime has an operational `ACTIVE` or `RUNNING` status. Planned,
placement-blocked and deploying identities
are not monitored by the proxy. Each successful data-member join publishes a new
proxy configuration version. During initial bootstrap or single-to-HA conversion,
the proxy may temporarily run with no published provider until a data member joins.

ProxySQL places newly discovered providers in its reader hostgroup until the
native role monitor identifies the writer. Server-list reloads preserve existing
runtime hostgroup decisions rather than temporarily routing writes to replicas.
Before a data runtime is stopped or retired, its provider is removed from the
proxy configuration at a durable configuration version. The control plane
enumerates the logical proxy service's live instances and waits for **every**
`ACTIVE`/`RUNNING` instance to report that applied version. A successful reload
request, one replica's acknowledgement, or repeated requests to service DNS do
not authorize retirement. Proxy readiness and cluster-ready configuration
checks also observe all live instances; the reported applied version is their
minimum, not that of an arbitrary answering replica.

Instance probes use Zerops container addresses, Nomad allocation CNI addresses,
and [Dokploy](https://dokploy.com/)'s individual Swarm task DNS names. They keep the logical member's
pinned CA, TLS hostname and authentication, including namespace relay transport.
For relayed namespaces the tunnel keeps the policy-allowed logical member and
agent port, with a separate instance selector. Before opening each instance
tunnel the hub verifies the selected address against that namespace provider's
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

Each ProxySQL Apply compares desired stable settings with admin memory, runtime
and disk independently. Runtime-only loss and a failed SAVE are retried even when
memory already matches the intent. Native monitor hostgroup decisions are
preserved, and unknown servers enter the reader group until classified. Readiness
requires an online writer when providers are published and all configured users
to be active in runtime; a frontend TCP handshake alone is not sufficient.

After upgrading an existing proxy, an already cached address can retain its old
expiry (roughly the previous 5-minute TTL, plus jitter and resolver scheduling)
until the next resolution. Allow that
transition before testing address changes, or restart the proxy during a planned
connection interruption. Once the new policy is active, DNS propagation,
resolution time, provider readiness, role monitoring and connection retry/shunning
can still add recovery delay; the 5-second TTL is not an end-to-end outage bound.

On the current Nomad provider, proxy replicas share one runtime job. Port/exposure,
resource and replica-count mutations use the durable stop-before-start operation:
the provider stops **all** allocations, waits for them to become terminal, then
registers the replacement job. The job's `MaxParallel=1` update setting does not
make this path rolling. Two proxy replicas therefore do not guarantee private
endpoint continuity during these changes. A native lab exposure enable/disable
with two replicas measured maximum acknowledged-write gaps of 27.521/24.784
seconds (90/79 failed reconnect attempts); these are observations, not bounds.
Schedule an interruption for these mutations until the provider provides
serving-gated serial replacement. Normal availability also requires clients to
reconnect to another healthy proxy rather than pinning one allocation address.

## Upgrades

Use catalog paths from `karactl providers get postgres -o json`. Minor changes redeploy exact packages through replicas, switch over and redeploy the former primary; single topology interrupts service.

At minor- and major-upgrade completion, configured profile extensions are
updated on the writable primary in each connectable non-template database.
Only installed names are altered; absent extensions are not created. Extension
update failure keeps completion pending and surfaces an error for retry.
Repeated `ALTER EXTENSION ... UPDATE` calls are idempotent.

[PostgreSQL](https://www.postgresql.org/)-version-only changes, minor or major, do not redeploy [ProxySQL](https://proxysql.com/).
Its pinned package and advertised protocol version are independent of the
PostgreSQL package release; the proxy does not emulate the provider's version.
Proxy member version metadata converges through the normal configuration
hot-reload path, which continues to reconcile providers, users and TLS settings.
Major upgrades still interrupt database availability during their offline window,
even though the proxy allocation remains in place.

Use at least two proxy replicas when zero downtime is required for proxy resource,
listener/port, exposure or package changes, together with a provider that rolls
allocations one at a time and waits for the replacement to serve before proceeding.
A sole proxy necessarily interrupts clients when it must restart or be replaced;
existing sessions on a replaced proxy can also be interrupted with multiple
replicas, so clients must reconnect. PostgreSQL version changes are not an agent
binary refresh mechanism: updating the control-plane binary alone does not replace
agents already running in member allocations.

Adjacent major upgrades (16→17, 17→18) require a completed backup of the current version and an offline maintenance window. The checkpointed workflow installs old/new packages, durably pauses [Patroni](https://patroni.readthedocs.io/) elections in DCS and waits for every data member to acknowledge maintenance mode before publishing shutdown intent. It then records the paused writer and timeline, stops and confirms all replicas first, verifies the recorded writer is still authoritative on the same timeline, and stops that writer last. Writes may continue on that writer until its fast shutdown; no replica may promote during the handoff. Single topology stops its sole writer without Patroni.

Every member must acknowledge durable stopped process intent and be freshly observed stopped before either `pg_upgrade --check` or `--link` can run on the final writer. Paused Patroni can leave PostgreSQL running with inherited log pipes, so applying stopped intent first fast-stops the postmaster and only then waits for Patroni to stop. This lets the member acknowledge the offline configuration without hanging on those pipes. The journal and DCS pause survive control-plane restarts; an unexpected writer/timeline change fails closed for recovery rather than upgrading a stale seed. Cached member configuration preserves an intentionally absent PGDATA between activation renames.

The workflow then resets the Patroni scope (including its maintenance pause), starts the upgraded writer, rebuilds replicas, upgrades the [pgBackRest](https://pgbackrest.org/) stanza and queues a full backup. Final redeployment removes old-major packages. Pending imperative work, including scheduled/manual backups, restores, replacement and restarts, remains queued until major maintenance releases ownership; queued wait does not consume execution attempts or the execution deadline. Upgrade progress therefore cannot be preempted by backup retries against stopped databases. Backup source metadata uses the observed source version and native pgBackRest inventory, never the desired upgrade target. [Supabase](https://supabase.com/) allows only PostgreSQL 17 minor paths.

Upgraded replicas rebuild from the new primary with basebackup. Backups of the
previous major are not eligible replica seeds; pgBackRest replica creation is
re-enabled only after a full backup of the new major completes.
Replica rebuild progress is member-owned: one unavailable replica does not
prevent another healthy stopped replica from rebuilding from the available
new-major primary. Every replica must still pass fresh new-major readiness
before the upgrade advances to stanza/backup finalization. Failed members
remain fenced; this never restarts old-major data or clears maintenance.

**Do not start the old data directory after link-mode activation.** Restore the pre-upgrade backup when rollback is needed; package downgrade is not recovery. Skipped majors and downgrades are rejected. A queued post-upgrade full must succeed before treating it as a new recovery base.

An upgrade action failure reports its durable native checkpoint without exposing
command output or credentials. At `linking`, command completion is ambiguous:
keep PostgreSQL and Patroni stopped, preserve both data directories and the
receipt, and restore a validated pre-upgrade backup into a separate cluster
before an explicit cutover. Never clear that receipt or retry the link to bypass
the refusal. Replayable checkpoints retain the same receipt for retry after
repairing the member dependency; native logs remain in the protected upgrade
work directory.
If the target exists before its creation ownership was durably recorded,
the action identifies the `planned` checkpoint and refuses to adopt or erase
that unverified directory. Preserve it for offline inspection, or recover the
validated backup into a separate cluster.

## Failover, suspension and deletion

[Patroni](https://patroni.readthedocs.io/) and [etcd](https://etcd.io/), not the control plane, establish HA writer authority. A majority outage can intentionally make the database unavailable. Strict synchronous mode can block writes without an eligible standby. Rewind and automatic divergent-directory rebuild help replicas rejoin; neither replaces backups nor guarantees data survival under every durability override. Proxy endpoints stay stable, but sessions/transactions can be interrupted.

A **frozen primary postmaster** (for example, suspended with `SIGSTOP`) while
Patroni remains healthy is a failover boundary: Patroni keeps renewing the leader
lock while the postmaster process still exists, so no automatic failover occurs.
Karakoram reports `Degraded`, `Ready=False` and no writable primary rather than
claiming availability. Remove the underlying process freeze and allow the
postmaster to resume; recovery occurs when it becomes responsive again. This is
not retained-volume suspension: `karactl clusters resume` is not a process-thaw
action. Do not delete the leader lock or force a second writer to bypass it.

Retained-volume suspension removes runtimes, including the stateless proxy, while preserving [PostgreSQL](https://www.postgresql.org/) and etcd data/identity. Resume recreates those identities from retained volumes. `keepBackups` is independent of volume retention. See [delete/suspend](../clusters.md#delete-or-suspend), [recovery](backups.md) and [orphan cleanup](../operations.md#orphaned-platform-volumes). Do not erase state or manually reset DCS to bypass a pending maintenance/deletion operation.

Data members pass readiness and publish their provider identities before proxy
writer readiness is required, regardless of their original allocation order.
This also applies when resume elects a different primary. A running proxy process
alone does not mean it can serve writes: the actual current writer route and
credential-publication checks must still pass before readiness completes.

Runtime-only recovery can reattach an existing retained volume without the
ordinary import journal. Its retained-volume inventory marker is cleared after
the recreated member passes readiness and rejoins; a failed or unready reattach
remains retained and never authorizes empty-volume creation.

New provisioning records the exact import batch before submitting it. If setup
is interrupted after the provider reserves a service, reconciliation replays that
original batch, including completed siblings, with its original resources and
replica count before applying newer desired changes. A runtime-only import never
adds its existing volume to that batch. A service reservation is not proof of
completed setup and cannot advance to deployment. Preserve both control-plane
and provider import state; an incomplete reservation without its original import
intent requires recovery of that intent, not adoption or empty-volume creation.
Deletion and retained-volume suspension also complete an incomplete reserved
batch before removing services, without deploying the imported member. An accepted
import handle does not hide unfinished setup: its frozen batch is resumed before
waiting for completion.
