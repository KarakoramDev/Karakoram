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

Full backup receipts report XtraBackup's native checkpoint type
`full-backuped`; the control plane validates it against the requested API type
`full`. Incremental receipts report `incremental`. Metadata validation failures
identify the inconsistent field without including its value.

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

Live coverage admission and backup selection use each full backup root's own
GTID-bounded coverage. When live coverage is unavailable, the remembered-window
fallback retains newest-safe backup selection; a newer incompatible root does
not invalidate an older root whose own verified stream covers the target.

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
archive writer/source change, take a fresh full backup from the current writer
to establish its own exact anchor; normal service and full backups remain
available while the previous PITR window is frozen.

```sh
karactl -n NS backups pitr-window CLUSTER -o json
```

## PXC GTID continuity

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

Normal database service and **full backups remain available**. After preserving
evidence and repairing/replacing affected members from a trusted source, take a
new full backup. A successfully completed full backup started after detection
establishes a new PITR baseline and clears the active latch; it does not make an
old incremental chain continuous or authorize replay through the old interval.
Equal histories after SST or re-formation do not clear the latch by themselves.
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
