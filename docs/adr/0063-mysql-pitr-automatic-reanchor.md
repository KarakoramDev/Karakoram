# 0063 — MySQL/PXC automatic PITR full-backup re-anchoring

Status: Accepted

## Context

Physical XtraBackup snapshots bind replay to the source server UUID and exact
binary-log position. Group Replication failover, rolling maintenance, resource
relaunch and PXC archive-source changes can leave healthy uploads that cannot
extend the previous physical baseline. Requiring an operator or the next full
schedule to establish a replacement unnecessarily prolongs that frozen window.

## Decision

Re-anchor by an **ordinary automatic full backup**, never by joining histories
across physical writers. Only the MySQL engine's mutating Sync path queues it;
status and recovery-window reads remain read-only. With PITR, backup storage and
`spec.backup.pitr.autoReanchor` enabled (default true when unset), compare the
current archive writer UUID against the newest successful full's verified source
UUID using the existing archive-writer, sealed-manifest and coverage mechanisms.
A mismatch or missing baseline requires a replacement. Returning to the newest
full's source does not require a new full.

Persist the observed writer and first-seen timestamp in engine-owned durable
cluster state. Require five minutes of continuous writer observation, no
pending/running non-backup operation of any kind, and five minutes since the
latest such operation finished. In-progress maintenance and unavailable writer
identity restart stability observation. Unknown operation or baseline evidence
fails closed without queuing a full.

Agent upgrades and member replacements, including experimental opt-in automatic
lost-volume replacement (disabled by default since v0.4.7; see
[0067](0067-mysql-lost-volume-replacement.md)), use the same operation deferral and
post-completion stability period. Confirmed or observed member volume loss and unsettled proxy drain
flags also defer re-anchoring independently of operation inventory. Runtime
recovery and orphaned-drain settlement run first; a busy or failed pass resets
writer stability observation before returning.

Deduplicate by cluster, physical writer UUID and newest-full name. Use trigger
`pitr-reanchor`, block while any backup is pending/running, and permit at most one
automatic re-anchor full per cluster per hour. Failed attempts remain deduplicated
for that epoch; a manual full is still available. Preserve post-restore automatic
full behavior unchanged. Durable inventory also recovers cooldown after a crash
between enqueue and state persistence.

### Coverage and trust

Enqueueing or starting a full is not recovery evidence. Existing windows,
admission, `anchorReplayLog` and `trustedReplayHistory` remain unchanged. Only a
successful full with verified coverage can establish the new forward interval.
The time gap from the old frozen endpoint to that snapshot remains unrestorable.

### Retention

Successful re-anchor fulls count with the full backup schedule having the largest
positive retainCount; lexical schedule name resolves ties. Disabled schedules
retain their count policy consistently with existing schedule retention. Without
such a schedule, keep the two newest successful re-anchor fulls. Post-restore and
manual fulls are not added to this automatic-full budget.

Delete via existing retention admission and normal MySQL deletion, preserving
incremental chains and binlog-GC safety. Never prune the newest successful full
(the live/extending interval's anchor) or a backup referenced by a pending/running
restore or clone. Protected backups may exceed the count until safe deletion
becomes possible. Older recovery ranges expire with their fulls just as for
scheduled retention; protecting every historical disjoint interval indefinitely
would prevent the bounded retention budget from reclaiming any such baseline.

### Observability

Record `PITRReanchorTriggered` with the queued backup name. Record reason-specific
`PITRReanchorSkipped…` events for no storage, unavailable writer/operation/baseline
evidence, active operation, active backup, cooldown and writer instability.
Persist the last reason and emit skips only on reason changes, not every pass.

## Consequences

Automatic fulls reduce the frozen period without changing replay trust or
requiring an agent protocol change. They add object storage, IO and memory peaks
on the writer; capacity planning and the explicit opt-out are required. Failover
flapping, long operations, failed fulls, unknown observations and cooldown can
still prolong the gap. Control-plane downtime pauses observation and scheduling;
this is not a recovery-point or availability guarantee.

## Alternatives considered

- **Cross-writer GTID-chain bridging:** rejected. Agent replay explicitly rejects
  foreign server UUIDs. Matching GTID prefixes do not establish physical lineage;
  a control-plane-only claim would disagree with replay, while a partial or
  mixed-version rollout risks fail-open recovery admission. This decision does
  not weaken either side of that trust boundary.
- **Wait for scheduled or manual fulls:** safe but needlessly delays recovery
  coverage after automatic failover and planned operations.
- **Keep automatic fulls indefinitely:** rejected because writer churn must not
  create unbounded retained fulls. Existing chain and in-use deletion guards
  remain authoritative over the count target.

## References

- [MySQL engine](0052-mysql-engine.md), [PXC engine](0053-pxc-engine.md)
- [MySQL backups](../mysql/backups.md#automatic-writer-re-anchoring)
- [Upgrade guidance](../upgrade.md#upgrading-to-v047)
