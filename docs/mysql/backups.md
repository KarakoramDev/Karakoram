<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backups and point-in-time recovery

[Overview](README.md) · [Access](access-and-tls.md) · [Operations](operations.md)

Replication is not a backup. Register and verify an [S3](https://aws.amazon.com/s3/) backup storage, select it on the cluster, schedule full backups and test recovery into an independent cluster. Use the shared [backup storage and retention guide](../backups.md) for permissions and storage registration; [MySQL](https://www.mysql.com/)/[PXC](https://docs.percona.com/percona-xtradb-cluster/) use the engine driver-specific rules below.

## Physical backup chains

Both engine drivers stream [XtraBackup](https://docs.percona.com/percona-xtrabackup/) through `xbcloud` to S3 without staging the full archive locally. Types are `full` and LSN-based `incremental`, not `logical`, `physical`, `diff` or `incr`. Each incremental depends on a successful parent in the same repository incarnation and server major. Preserve the full root and every intervening incremental. The pinned compatible XtraBackup release performs its normal server-version check; numerical patch values across the two products are not directly comparable.

```sh
karactl -n NS backups create CLUSTER baseline --type full --storage ARCHIVE
karactl -n NS backups list CLUSTER -o json
karactl -n NS backups get CLUSTER baseline -o json
```

Creation returns an operation; observe that existing operation and terminal backup inventory rather than submitting again after a timeout. A streaming backup is not complete merely because one side of the upload finished. Interrupted backups are reported as failed, not silently repeated. Backups use DDL locking; schedule with DDL contention, donor load and available capacity in mind. Group Replication normally prefers an online secondary; when binary-log PITR is enabled, the backup must anchor the current archive writer instead.

Streaming backups explicitly use one XtraBackup copy thread and 1 MiB read/chunk
buffers, with two concurrent xbcloud uploads. The server's default buffer-pool
sizing reserves non-pool and backup headroom, including for 1 GiB singletons;
custom buffers and workload concurrency still need capacity planning.
`--use-memory` is a prepare-phase setting, not a backup-time RSS limit.
The bounded streaming options require the corrected member agent; a control-plane
upgrade alone only updates server sizing. MySQL/PXC do not support `upgrade-agents`,
so replace agents through a supported healthy-member lifecycle or re-create the
deployment to obtain the new backup command.

Full backup receipts report XtraBackup's native checkpoint type
`full-backuped`; the control plane validates it against the requested API type
`full`. Incremental receipts report `incremental`. Metadata validation failures
identify the inconsistent field without including its value.

Pipeline failures retain their reason-code prefix (for example,
`pipeline_stream_failed`) and include XtraBackup/xbcloud exit statuses or signals.
SIGKILL is reported as possible OOM, not proof of it; pipeline cleanup can also
kill children after a stream failure. Bounded native stderr tails are normalized
to secret-free error summaries such as connection loss, upload failure or disk
exhaustion. Arbitrary native text, credentials and URL query keys are omitted.
An unclassified tail is explicitly reported as omitted rather than echoed.

Provenance includes engine driver, source version/topology, cluster incarnation, repository, tool/server versions, GTID set, LSN range, chain dependencies and completion time. MySQL repositories use `<storage-prefix>/mysql/<namespace>/<cluster>-<uid>`; PXC uses the corresponding **`pxc`** prefix. They share a subtree structure, not an interchangeable backup identity:

- `base/<label>`: streamed physical backup.
- `base/<label>.meta/`: readable checkpoints, binlog coordinates and backup metadata.
- `binlogs/<server_uuid>/<file>` plus JSON sidecar: archived binary log, checksum, GTID coverage, event/commit times and transaction boundaries.

Restore validates recorded provenance, not a filename resemblance. `mysql` and `pxc` backups cannot restore into each other, and source/target server majors must match. A restore is not an alternative major-upgrade path.

### Integrity and repository write access

S3 write access can substitute backup bytes, not only delete them. A completed
backup records an exact physical-object manifest (keys, available version IDs,
sizes and SHA256 values); its digest is retained in control-plane-sealed
recovery provenance independently of the repository. Restore rejects substituted,
missing or extra objects before extraction/preparation. The prepared physical
file tree and replay files are frozen under one plan digest, rechecked before
member STOP and immediately before installation. A failed integrity check leaves
the serving datadir untouched and fails the operation; it is not a reason to
silently fetch a different backup or broaden the plan.

The pre-STOP recheck runs asynchronously and records its progress in the durable
restore receipt, so hashing large datasets does not hold an HTTP request open.
STOP requires successful verification for the current persisted fence-attempt
token; an earlier success or an interrupted recheck cannot authorize it. Tokens
identify verification attempts, not restore contents, and therefore do not change
the immutable request hash or its pinned plan digest. Installation still repeats
the frozen-plan verification immediately before replacing the datadir.

The sealed manifest protects the selected **physical snapshot** against
repository substitution; frozen-plan checks also detect later staging changes.
Continuous binary-log archives remain trusted repository input before initial
preflight, as [PostgreSQL](https://www.postgresql.org/) WAL and [PSMDB](https://docs.percona.com/percona-server-for-mongodb/)/[PBM](https://docs.percona.com/percona-backup-mongodb/) oplog archives do. Their self-checksums
are not off-repository authentication against a writer replacing both logs and
sidecars. Source/position binding rejects incompatible honest histories, not a
forged canonical stream. See the [shared continuous-archive trust boundary](../security.md#backup-repositories-and-continuous-log-writers).

These controls do not protect against compromise of the control plane, its
sealing key, or the member runtime's operating-system account. Keep backup
prefixes immutable and restrict archive writers; separate retention/deletion
credentials from ordinary upload credentials where the storage service permits.

## Binary-log PITR

Enable `spec.backup.pitr.enabled` and retain binary logs longer than the archive interval. Archiving follows the routed writer and uploads closed logs. Coverage requires both an exact physical-source lineage anchor and a continuous **GTID set**; neither matching filename sequences nor matching numeric GTIDs across different writers prove continuity. A source change requires a fresh full baseline from the current writer, as described below.

PITR requires a completed restorable full backup, its required chain and continuous archive coverage beyond it. The displayed window ends at the last safely archived contiguous transaction—not the latest application commit, upload time or requested target. Rotation bounds eligibility for upload; storage failure can still increase archive lag. Local log expiry is independent of upload success. Archiving does not delete local logs.

Unchanged configuration refreshes do not restart the archive interval. Changing
the interval reschedules the deadline; replacing repository configuration cancels
the previous attempt before starting the replacement loop. After a clone or
restore completes, archiving resumes in the independent target repository.
Until the writer has archived a transaction, **BinlogArchive** reports
`False/ArchivePending`, not healthy zero lag. A disabled or unavailable archive
observation reports `Unknown/ObservationUnavailable`. A full-backup-only
zero-width window is a snapshot recovery point, not evidence of continuous
archiving; verify that the endpoint advances after post-backup writes.

Each archive pass releases its repository connection pool when it completes or
fails; backup, restore and coverage observations do the same. This prevents
short archive intervals from accumulating abandoned idle S3/TLS connections.
This lifecycle fix requires updated member agents, not only a control-plane
upgrade, and is not a limit on the combined mysqld and agent RSS.

Pending native files are persisted separately from per-file recovery summaries,
so draining a large backlog does not rewrite its entire pending inventory after
each upload. A completion receipt still becomes authoritative only after its
summary is durable. That summary also records the most recent completion
identity, so a crash before receipt publication cannot turn an already archived
native file's later expiry into a false gap. Before replacing that identity, an
agent preserves an independent durable completion proof if its receipt is still
missing; later passes therefore neither re-upload it nor forget it after expiry.
Updated agents migrate the local archive index to format 2 before using the
split inventory.

Coverage refresh belongs to a single background worker in each member agent,
not to a short status HTTP request. Status polls read compact state without
listing or downloading archived history. Active roots are reverified about
every 30 seconds; a successful status result is usable for at most 60 seconds.
A newer refresh failure withdraws that result instead of returning an older
success. Cold or expired coverage reports `Unknown/CoveragePending`, with the
stage and processed/total sidecar counts and no claimed recovery window.

The worker uses bounded parallel reads and ordered 16-sidecar batches. It
persists compact GTID/window progress and a separate object-key inventory, so
an agent restart resumes its verified prefix rather than redownloading history
from zero. A new primary with a different local state directory builds its own
proof. Cancelling a status request does not cancel/restart this work. Memory
includes the key inventory, a bounded batch and exact GTID/window complexity,
not every historical transaction. Appends read new sidecars only; deletion,
replacement or out-of-order insertion forces a verified rebuild.
At most 32 root entries reside in memory; eviction retains their compact durable
proofs and resumable checkpoints. Disk GC expires roots unrequested for 24 hours
and keeps up to 256 inactive roots, without deleting the current working set.
Pending coverage excludes only that full backup's window: other completed roots
remain available while it converges.
A restore target already covered by a completed root remains admissible. An
uncovered target returns pending rather than out-of-range while any eligible
root is still verifying or queued; out-of-range requires completed observations.

With a reachable source agent, restore/clone admission does **not** use the
status freshness lease or a remembered window alone: it waits under the operation
deadline for verification started after that admission request. The existing
totally dead-source disaster-recovery path may select a previously proven safe
window without a source agent; it does not extend remembered continuity.
In either path, the target member's restore preflight/recheck freshly reads and
verifies the sealed inputs and replay checksums before destructive activation.
Missing or invalid restore inputs fail closed.
Updated agents expose pending/progress and freshness metadata. During a mixed-
version rollout, a legacy agent's successful synchronous coverage response is
still fresh for that request; it cannot provide the background convergence fix.
The control plane explicitly negotiates pending responses with `acceptPending`.
A legacy control plane receives an observation error while coverage is pending
or expired, matching a legacy agent's unsuccessful synchronous request; it can
read completed windows normally. Existing PXC remembered-safe-window fallback
on observation errors and disaster-recovery behavior are unchanged.

Upgrade the control plane before agents; do not downgrade the CP below v0.4.3
while v0.4.3 agents run. Old-CP/new-agent operation is not supported: an old CP
retains its released PXC error-to-remembered-window fallback and cannot implement
the updated pending/freshness contract.

MySQL singletons and Group Replication can restore in place or clone with a PITR
time even when no source member is an ONLINE writer. Keep at least one admitted
data-member agent reachable: it verifies each sealed full baseline against the
object-store archive index without connecting to the source mysqld. Restore uses
the archived physical backup and binary logs, not the failed server's local logs.
Targets outside proven contiguous coverage are rejected; an unavailable archive
or all unreachable agents cannot establish coverage. This does not clear GTID
divergence or automatically replace a member.

An observed log purged before upload is a gap. The continuous endpoint freezes at the last safe event even if later uploads succeed; later data does not heal missing GTIDs. Inspect the **BinlogArchive** condition, archive-lag monitoring and PITR window. Preserve needed objects and establish a new valid backup/coverage chain after resolving the cause; never infer recovery coverage from a green upload alone.

Time targets resolve to the last complete transaction whose original commit timestamp is at or before the target. Microsecond commit metadata and transaction end positions matter: second-resolution `mysqlbinlog --stop-datetime` cannot distinguish commits within the same second. The recovery implementation preserves GTIDs and excludes already recovered transactions; low-level GTID/position replay must never skip a missing dependency. Public restore workflows use their documented backup/time target fields; an internal GTID boundary is not a separate CLI/API target flag.

A fresh full backup establishes a new **replay-lineage anchor**, not just a
completion timestamp. Retained pre-full binary logs are not eligible merely
because they contain a GTID absent from that full: the same old row can have
been recorded under another SID or a shifted sequence number. Latest, GTID and
time recovery must all prove the snapshot-to-archive boundary before replay.
This rule applies to [Percona Server](https://docs.percona.com/percona-server/) singletons, Group Replication and PXC.
Unproven lineage stops the advertised recovery window; it is not repaired by
skipping unknown events or resetting GTID history.

The anchor records the snapshot source UUID (the `server_uuid` XtraBackup writes
into `backup-my.cnf` inside the sealed stream; the datadir `auto.cnf` is never
streamed) and exact binary-log file/position.
Older files and events at or before that position are excluded on the original
stream. A different writer's numeric GTID prefix cannot prove that its counters
describe the same transactions. Foreign-source streams are therefore refused
as replay contributors, even if their GTID sets appear compatible. After an
archive writer/source change, v0.4.4 automatically queues a full backup from the
current writer to establish its own exact anchor; normal service and full backups
remain available while the previous PITR window is frozen.

When the current writer differs from a baseline's physical source and live
coverage finds foreign-writer archived transactions beyond the newest
recoverable endpoint, **PITR** remains `True` for the existing window but uses
reason `WindowFrozenWriterChanged`. Its message names the frozen window
end and asks you to **take a full backup to extend point-in-time recovery**.
**BinlogArchive** can still be healthy: its message labels the current writer's
archive lag separately and states the recoverable endpoint, rather than implying
that newer uploads are recoverable. This applies to Group Replication and PXC.
After a successful full from the new writer establishes its own safe coverage,
PITR returns to `True/WindowAvailable`. The old window remains usable; the new
full does not fill any uncovered interval between the two windows. Elapsed time
alone, including an idle writer, does not trigger the writer-change reason.
If the original writer returns, its pending uploads are normal archive lag,
not proof that a new full is required. Its contiguous, trusted uploads can
extend the original baseline again; retained foreign uploads do not prevent it.

### Automatic writer re-anchoring

`spec.backup.pitr.autoReanchor` defaults to `true` when omitted (MySQL and PXC).
Set it to `false` to manage replacement full baselines manually. With PITR and
backup storage configured, reconciliation compares the current physical writer
UUID with the newest verified successful full's source UUID. A mismatch, or no
matching full baseline, queues an ordinary full backup with trigger
`pitr-reanchor`; read-only status and PITR-window requests never queue work.

The writer must be observed continuously for at least five minutes. Any pending
or running non-backup cluster operation, including rolling maintenance, resource
relaunch, scale, member replacement and agent upgrades, restarts that wait; at
least five minutes must also elapse after the operation finishes. An unavailable
writer restarts observation. Returning to the newest full's original source
needs no replacement backup. The writer observation is durable across control-
plane restarts. An existing pending/running backup blocks the automatic full,
and re-anchor fulls are limited to one per cluster per hour. A deterministic
name deduplicates each cluster/writer/newest-full epoch, including failed attempts;
manual full backups remain available if intervention is needed.
Admission rechecks active backups, cluster operations and the five-minute
post-operation grace in the same transaction that inserts the automatic full,
so work queued and even cancelled after observation still defers it.
Only after these inexpensive gates pass does automatic re-anchoring request
baseline verification. Its verified physical source UUID is remembered durably
by immutable backup incarnation and physical provenance (including storage,
repository, generation and sealed object identity), not a reusable backup name.
Only the current baseline is cached; observing no eligible baseline clears it.
Unchanged baselines need no repeated coverage request for this decision.
Pending or failed verification retries no more often than every
five minutes. This cache is scheduling evidence only, never restore admission
or recovery-window evidence.

`PITRReanchorTriggered` events name the queued full. Skip events use
`PITRReanchorSkipped` with a reason suffix (`NoStorage`, `WriterUnavailable`,
`OperationsUnavailable`, `OperationInProgress`, `BaselineUnavailable`,
`BackupInProgress`, `Cooldown`, or `WriterNotStable`). They are emitted only
when the reason changes, not every reconciliation.

Automatic full backups incur ordinary full-backup storage, IO and writer memory
peaks. They do not extend recovery coverage until the full succeeds and its
coverage is verified; the gap after the old frozen endpoint remains unrestorable.
They also do not live forever: with full schedules, they count alongside the
schedule having the largest positive `retainCount` (lexical schedule name breaks
ties). Without such a schedule, only the two most recent successful automatic
re-anchor fulls are retained. Normal backup deletion preserves required chains,
the newest successful full (the live/extending interval's anchor), and pending/
running restore or clone references; protected backups may temporarily exceed
the count. Older recovery ranges expire with the fulls removed by retention.
Post-restore automatic fulls keep their existing retention behavior.
See [ADR 0063](../adr/0063-mysql-pitr-automatic-reanchor.md).


```sh
karactl -n NS backups pitr-window CLUSTER -o json
```

The endpoint and CLI expose every proven range in `intervals`, including the
full backup's name in `backup` when known. Ranges belonging to different full
backups remain separate even when they overlap; entries are ordered by start,
end, then backup name. The restore picker and backups page show all ranges and
allow a target inside any one of them, never an uncovered gap. Legacy
`available`, `start`, `end`, and `message` are unchanged: `start`/`end` still
describe the latest-ending merged interval. Cluster status keeps that summary.
Remembered PXC windows have no per-full attribution and omit `backup`.


## PXC GTID continuity

The same mapping defect can occur at first formation or concurrent SST/IST joins,
not just after restore. PXC follower admission is serialized until each preceding
joiner is Synced. Updated agents additionally certify a replicated probe before
the admitted formation becomes Ready; only lifecycle-proven empty joiners with no
unique frozen history can receive bounded seed-pinned SST remedies. A unique-data
or unproven member fails closed with operator guidance. Legacy agents report
`CertificationUnavailable` and retain the comparator without acquiring new holds.
Neither this certificate nor SST clears an existing GTID incident. Once members
are admitted, a history/epoch change does not re-enter formation certification
or stop service. Managed replacement, full acknowledgement, restore and deletion
remain available with the history latch set. A fresh full acknowledges history
differences; it does not repair native SID selection. Acknowledgement separately
checks current direct/inverse namespace compatibility without client holds:
members must share namespace presence, with at most one namespace differing by
nested prefixes and the other identical frozen history. Current incompatible
accounting or retained mixed probe evidence refuses acknowledgement even if the
old birth certificate was consistent. The full artifact can still succeed while
the history latch remains set.
No separate retry API is needed for a withdrawn empty join: scale back to the
previously admitted count or cancel the not-yet-retired replacement. Only the
unadmitted cohort is removed; its controls are released and prior member/history
proofs are preserved. A prior real latch remains Degraded. A cleanup-complete
blocked formation or restore admits a new trusted-source restore; a disposable
blocked birth may instead be deleted/re-created.


[Galera](https://galeracluster.com/) `Synced` and matching rows are not sufficient proof of MySQL GTID
continuity. On the pinned PXC 8.0.46-38.1 and 8.4.11-11.1 releases, a missing-write
IST rejoin can assign a replicated application row a different GTID on the
receiver, including a SID that is the bitwise inverse of the Galera state UUID.
This is not an extra application transaction or a restore-time account write,
but it can invalidate asynchronous-replica and binary-log recovery continuity.
PXC's [SID initialization](https://github.com/percona/percona-xtradb-cluster/blob/33f7510189c5719c852837d7ad9ba5e7d6431f4d/sql/wsrep_mysqld.cc#L673-L693)
and [automatic GTID assignment](https://github.com/percona/percona-xtradb-cluster/blob/33f7510189c5719c852837d7ad9ba5e7d6431f4d/sql/binlog.cc#L1787-L1809)
distinguish cluster and local GTID identities; the inverse SID alone is not an
errant-transaction test. Compare complete executed histories against quorum.
Upstream tracks the same complemented-SID inconsistency in
[PXC-4493](https://perconadev.atlassian.net/browse/PXC-4493), verified against PXC
8.0 and 8.4. No released fix or supported configuration workaround is recorded
there. Disabling durable binary logging is not a continuity repair.

Karakoram records a persistent **GTIDHistorySafe** condition when histories
conflict. Incremental chains and PITR/binlog replay cannot cross the recorded
unsafe interval. Detection time is not a promise that every earlier target is
safe: only the recorded safe boundary and independently verified archive
coverage establish that. Pre-boundary recovery remains available from its
original backup chain.

Availability probes agent metadata, native MySQL state and platform inventory in
parallel. Successful reads are immutable and shared by Sync, Status and PITR
within one reconcile pass, including the Degraded retry. Proxy instance reads
retain their individual runtime identities; neither payloads nor timestamps are
shared across clusters or passes. Membership, formation, generation, configuration
and restore checkpoint changes invalidate samples. Reuse retains the original
sample boundary: a retry cannot renew availability by relabelling cached metadata.

Read-only work shares one absolute four-second deadline per cluster/pass,
including agent dialing and GTID observation-lock waits. Optional quorum GTID
sampling probes data members concurrently and has at most one second per sweep;
an unsupported or failing member endpoint may be retried once within the pass.
Budget exhaustion after identity invalidation is an unknown observation, not
evidence that every member failed: last-published health keeps its original
timestamps and epoch, cannot establish readiness or renew availability, and does
not mask genuine observed failures. A quorum assembled from individually cached
responses uses the oldest member sample as its safety boundary, never the later
retry's start time.
Transport failures are not cached. Full-backup acknowledgement and restore
certification/activation use explicit fresh observations under the same
per-cluster observation/commit lock, outside the read-only budget. Native mutation
boundaries and incident creation invalidate prior observations.

Normal database service and **full backups remain available**. After preserving
evidence and repairing/replacing affected members from a trusted source, take a
new full backup. A successfully completed full backup started after detection
establishes a new PITR baseline and clears the active latch after a complete
healthy same-component member observation. Its per-member frozen differences
are acknowledged durably. Complete successful observations permanently retire
healed or shrinking differences; later regrowth is new divergence, including
after SST or controller restart. The original sample remains immutable evidence.
Usable allowances are scoped to formation epoch/group, managed data/runtime
identities and the observed native server UUID; replacement/re-formation
invalidates them. New/growing or reversed differences latch again. This does not
repair member data, certify asynchronous replicas, make an old incremental chain
continuous, or authorize replay through the old interval. Older journals without
identity-bound narrowing allowances remain readable and grant no exceptions.
A completed native full is not failed by a later GTID observation, journal or
audit outage. Its operation completes with `done-baseline-pending` and an
explicit message if acknowledgement could not finish; the active latch or
pending audit remains durable. Verify `GTIDHistorySafe` and agent compatibility,
then take a fresh full after repair if still latched. Native full-backup polling
does not repeatedly sample quorum histories; successful fulls with no incident
need no extra GTID sweep.
Shared restored historical namespaces are legitimate. Before any new client
fence/hold, PXC restore negotiates `mysqlRestoreCertification` through agent
status on all deployed data members/proxies. With complete support it durably
holds ProxySQL processes and fences ordinary direct SQL with `offline_mode`,
then certifies an ordinary replicated managed-schema probe.
Negotiating capability is planning only, not acquisition of a certification
fence. A rejected preflight still releases untouched recovery ownership, while
native restore polling failures retain the frozen native checkpoint and its
original mysqld holds. If client fencing was acquired before certification
began, terminal cleanup releases those client controls without replacing the
native recovery checkpoint or declaring certification blocked.
Followers whose advancing SID mapping differs from the restored seed are
re-provisioned by SST from that seed, at most twice per member; persistent
disagreement blocks activation with checkpointed evidence. Terminal cleanup and
successful activation release the native fence and explicitly restart every
held proxy; terminal cleanup does not leave healthy proxies held merely because
a data agent is unavailable. This restore-only authorization never silently
clears an existing incident.
Terminal cleanup has its own durable pending checkpoint: every recovery pass,
including initial-restore Sync after CP restart, retries release until done
before settling failed certification blocked. Pre-certification cleanup instead
resumes its original checkpoint. GTID journal/audit outages do not gate cleanup.

A fully cleaned `certify-blocked` restore whose queue operation is terminal
Failed/Cancelled is not an eternal maintenance owner. Its only non-delete remedy
is an explicitly requested **new restore**: a kind-filtered queue claim may pass
older blocked backup/restart requests, then ownership transfers only after the
new Running restore resolves a valid frozen source. The new restore negotiates
and performs certification again, including its bounded SST remedy; it does not
inherit the failed certification or clear the sealed GTID incident. The old
failed operation remains auditable. Backup, restart and replace-member remain
blocked until certification; the condition says **restore again or delete**.
Cluster deletion follows its existing teardown ownership path.
A remedy that fails before acquiring ownership finishes its queue cleanup without
touching the previous blocked owner. If a handed-off remedy is rejected before
native installation, cleanup retains the unresolved certification restriction:
another new restore may proceed, but backup/restart/replace do not become safe
merely because that remedy failed preflight.
Every new restore records its own stop intent before changing data runtime.
An inherited Offline flag or an earlier SST stop hold does not prove that the
new request crossed that boundary. Once its own stop intent is committed, cleanup
cannot roll back to the predecessor; older checkpoints without that receipt keep
their conservative recovery rules.
An active Running restore, unfinished cleanup, or a pre-certification native
recovery checkpoint cannot authorize this handoff and retains its original
exclusive frozen recovery path.

After activation, use explicit
trusted-source replacement/SST followed by a fresh full; a mysqld restart cannot
repair incompatible GTID numbering. A persistent prefix difference at equal
Synced wsrep positions is also fenced after independent confirmation beyond the
in-flight observation window. Equal histories after SST or re-formation do not
clear the latch by themselves.

Agents from v0.4.2 that omit the optional capability use legacy restore/PITR
without new certification holds, native fence fields or probe endpoints.
`RestoreCertificationUnavailable` explains the limitation and recommends
re-creating the cluster for certified restores. This compatibility path does
not relax post-restore GTID comparison: real divergence remains latched and
Degraded. Its automatic post-restore full cannot silently acknowledge that
incident; explicit fresh-full acknowledgement still requires complete native
member identity observations.
See the [operator procedure](operations.md#pxc-gtid-history-safety). Never reset
GTIDs, inject empty transactions or attach an asynchronous replica merely to
make a continuity check pass.

## Offline restore and clone

MySQL and PXC new-cluster readiness requires the persisted initial restore completion receipt. Healthy members and an observed spec generation alone do not prove that the requested source data was imported; completed receipts survive control-plane restarts without replaying the import.

In-place restore is destructive and makes the database unavailable. Take a fresh
backup if the current state may be needed. Before any member STOP or datadir
replacement, the recovery operation preflights the physical chain and complete
replay plan: exact lineage, replay-closed target, required objects, sizes and
SHA256 values. A failed preflight leaves the serving data untouched and reports
the verification error. The durable receipt freezes the verified objects and
target; the destructive phase must consume that plan, not recompute a broader
one from later archive contents.

After successful preflight, fence every data member and install the prepared
full/incremental chain. Preparation uses redo-only mode while incrementals
remain, merges them in LSN order, and completes preparation before installation.
PITR replays only the verified prefix up to the chosen boundary. A time cutoff
or explicit GTID target that omits an earlier dependency is refused, not applied
as a partial history. Do not reset GTID history as a routine restore step.

Do not write through privileged direct sessions during PXC restore, including
external SQL runners. `CONNECTION_ADMIN`/`SUPER` can bypass `offline_mode`; the
managed administrator intentionally retains its privileges. ProxySQL itself is
held stopped during certification so proxy administrator sessions cannot bypass
that window. Ordinary managed application grants do not include those global
privileges. Managed account actions are refused while restore owns the cluster,
and archival loops are disabled/cancelled until activation and native fence
release. Startup account recovery is local, non-wsrep and non-binlogged; the
explicit replicated certification probe is the intentional exception.

A restored server gets a **new `server_uuid`** while keeping recovered GTID history. Restored Group Replication uses a **new group UUID**, and other members clone from the restored seed. PXC starts a **new Galera history**, with no predecessor peer list, and the other members join by SST. Prepared `xtrabackup_galera_info` can contain a zero UUID and sequence `-1`; file presence is not a trustworthy bootstrap position. Do not use backup coordinates to rank crashed live members.

Restore owns the process-stop holds created while fencing the target. During
re-formation it explicitly starts the restored seed only after the new
configuration is acknowledged, then releases follower holds after the seed has
formed. Configuration reloads alone never release durable process-stop holds.
Follow the existing restore operation after a control-plane restart; do not
manually start fenced followers or submit a second restore to bypass a wait.

Group Replication recipients fence client connections with `offline_mode` before
temporarily lifting `super_read_only` for native clone's removal of old user
data. A rejected clone restores the write fence before admitting clients again;
an ambiguous or successful copy remains offline until its supervised restart.
Recovery retries a failed native start only when its matching native progress
positively shows that file copying never started. Stale receipts, missing
progress and active or partially completed copies never authorize another clone.
This behavior requires updated data-member agents, not just a control-plane
update. MySQL does not support `upgrade-agents`; use supported agent/member
replacement on a healthy group or re-create disposable clusters with the updated
agent build before testing in-place recovery.

The UI offers **Restore in place** and **Restore into new cluster**. New-cluster recovery keeps the source unchanged but still requires matching engine driver and major. Target system credentials are re-keyed; source endpoints, writer identity and replication history must not be reused. Target archiving stays inactive until its own repository is active. Follow the target operation, member state, observed generation, recovered data and application authentication before reopening traffic. Never make a restored copy a member of the still-running predecessor cluster.

## Accounts after recovery

The physical backup includes MySQL accounts; managed proxy passwords additionally require the sealed account snapshot recorded with that backup. Keep the control-plane secret store and encryption key recoverable as well as the S3 objects. Missing account provenance is not safely replaced by an empty account list.

Recovered managed accounts use the selected backup's sealed current/retained-password snapshot, filtered to accounts present after replay. Accounts deleted by PITR are not recreated just to satisfy the proxy inventory. Password changes or new users after that snapshot are not automatically reconstructed as recoverable proxy credentials from binary logs. Inspect account state and rotate/reconcile required application identities before restoring traffic; test old/new password behavior, locks and database grants through the proxy. System accounts use target-cluster credentials, not source secrets. Do not publish snapshot contents as recovery evidence.

## Retention and deletion

Retention is chain-aware: deleting a full backup also deletes dependent incrementals; deleting an intermediate backup can invalidate descendants. Review the deletion closure and keep an independently tested recovery point. Scheduled retention must not strand retained chain leaves. Retaining object bytes alone is insufficient if inventory, sealed account snapshots, storage credentials or encryption keys are lost. Cluster deletion/retained-backup choices follow the shared [backup lifecycle](../backups.md); inspect remaining inventory and storage explicitly.
