<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backups and PITR

Use [PBM](https://docs.percona.com/percona-backup-mongodb/) for consistent [MongoDB](https://www.mongodb.com/) snapshots, point-in-time recovery and clone recovery.

## At a glance

| Capability | Requirement / behavior |
| --- | --- |
| `logical` | PBM logical snapshot; CLI backup-type default |
| `physical` | PSMDB physical snapshot; recovery replaces the data directory |
| PITR | Successful base snapshot plus continuous PBM-observed oplog slices |
| Transfer | Members ↔ S3 directly, never through the management relay |
| Isolation | Effective storage prefix includes a stable cluster UID; preserve each snapshot's recorded namespace/prefix |

Register and verify [S3](https://aws.amazon.com/s3/) storage using [Backups and storage](../backups.md), which owns common inventory, schedule/count-retention and retained-backup flows. Verification from the control plane does not prove every member can reach S3. Preserve state, original encryption key and storage credentials; object storage alone cannot reconstruct recovery ownership.

## On-demand backups and schedules

```sh
karactl -n default backups create demo beforechange --type logical --storage archive
karactl -n default backups create demo physicalbase --type physical --storage archive
karactl -n default backups list demo
karactl -n default backups get demo beforechange
karactl -n default operations cluster demo
```

UI: **Cluster → Backups → Backup now**; storage is required. Follow the operation and inventory to **Succeeded**, not just request acceptance/[PBM](https://docs.percona.com/percona-backup-mongodb/) initiation. Inventory includes PBM name, type/storage, times, size/error where available, and `location`: the snapshot's object prefix `<PBMPrefix>/<PBMName>/` within its recorded storage bucket; `trigger` is `manual`, `schedule` or `restore` (automatic post-restore base). The prefix identifies snapshot data, not sibling `<PBMPrefix>/<PBMName>.pbm.json` metadata or shared PITR slices. It is populated when the native snapshot name is known, including inventory-discovered logical, physical and incremental snapshots and PITR base snapshots. Legacy empty locations are filled when the snapshot is next observed by inventory using its recorded prefix (or the prefix adopted by inventory if none was recorded); unobserved snapshots remain unchanged. Existing nonempty locations are preserved. Deletion continues to use the recorded PBM prefix/name, not `location`.

Manual requests require a formed active cluster with current generation observed. Creating/suspended/deleting/unconverged clusters return **409** with phase/generation context, even if an older Ready phase remains visible. Failed/expired/cancelled/retired operations mark unfinished backup inventory Failed. Never-started or Failed records without a started PBM snapshot can be removed immediately when no matching active operation exists; started snapshots need backend cleanup before name reuse. Missing storage returns **404** (`backupStorage`); duplicate name **409** `already_exists`; unsupported type **400** listing valid types.

Both PBM types can be scheduled. Create-time shorthand:

```sh
# Add to clusters create:
--backup-storage archive --schedule 'nightly=0 2 * * *,type=logical,retain=7,storage=archive'
```

For edits use `spec.backup.schedules` in an API-shaped patch; see [schedule rules](../backups.md). PBM deletes excess successful scheduled snapshots durably. Base/oplog deletion can shorten PITR coverage; inspect the observed window after retention/storage changes.

## PITR

1. Configure primary `spec.backup.storage` and complete a base backup.
2. Set `spec.backup.pitr.enabled: true`; `oplogSpanMinutes` is integer minutes, minimum 1, default 10.
3. Run `karactl -n default backups pitr-window demo`; choose a UTC RFC3339 timestamp inside a reported **continuous** window.

Future/outside/gap timestamps return **400**, `details.field: source.pitrTime`, reason `out_of_range`, with UTC `earliest`/`latest`; bounds do not imply intervening gaps are covered. Initialization/no coverage returns **409** `invalid_state`, including on the window endpoint. Coverage is rechecked during execution; permanently invalid targets fail terminally. Once a set is ready with [PBM](https://docs.percona.com/percona-backup-mongodb/) storage configured, convergence during scaling/upgrades/reloads does not itself hide established coverage; agent/storage failures still can.

Quota, slicing failure, history gaps, base deletion and storage changes affect recovery. After restore, PITR stays disabled until a fresh snapshot on **primary storage** succeeds. The required automatic base cannot be cancelled. If it fails/disappears, at most three replacement attempts are queued with warnings without blocking cluster convergence. After exhaustion, take a successful manual primary-storage backup; a snapshot queued after restore also satisfies the gate. Check the new timeline before trusting recovery.

## In-place restore

**Warning:** Stop application writers and plan downtime. Restore overwrites data and may restore application users/roles; preserve a current recovery copy first.

```sh
karactl -n default restores create demo recoverbase --backup beforechange --yes
# Replace the illustrative time with an observed covered point:
karactl -n default restores create demo recovertime --time 2026-01-15T12:00:00Z --yes
karactl -n default restores get demo recoverbase
```

Use exactly one `source.backup` / `source.pitrTime`; both/neither return **400**, field `source`, reason `conflict`. Missing backup returns **404** with source context; duplicate restore name **409** `already_exists`, before enqueueing. Source defaults to target; require matching provider/topology/engine major and a successful compatible snapshot, particularly for physical recovery. Missing provider metadata denotes legacy [PSMDB](https://docs.percona.com/percona-server-for-mongodb/), not a provider-neutral backup.

Logical restore is **not full rollback**: replica-set restore replaces backed-up namespaces but leaves extra collections/databases untouched. Sharded logical restore drops every database present in the backup, removing later collections within those databases; databases absent from the backup remain. For an exact copy, quiesce/drop extras deliberately, use physical recovery, or restore into a new cluster.

Karakoram serializes disruptive work, disables PITR and holds sharded routers/balancer. Physical restore suppresses automatic mongod/[PBM](https://docs.percona.com/percona-backup-mongodb/) restart; mongod may be offline and PBM may exit successfully. Progress uses PBM storage configuration while [MongoDB](https://www.mongodb.com/) is unavailable. Recovery restarts processes, clears maintenance, resets system users, resyncs target storage and restores router/balancer state. Logical restore does not use this offline lifecycle. Phase is **Restoring** with checkpoint message, then health observation on success or Degraded/error on terminal failure.

Sharded restore captures the original balancer state durably before disabling it. A balancer that was intentionally disabled remains disabled; cleanup only re-enables one that this operation observed enabled. Older interrupted checkpoints without a captured state leave the balancer disabled rather than guessing operator intent; inspect and enable it explicitly if appropriate.

Validate all members/agents, application authentication, collection data, balancer, inventory and fresh base before reopening writers. PBM `done` alone is insufficient.

## Clone and cross-namespace recovery

```sh
karactl -n default restores create demo cloneproof --backup beforechange --into-new democopy
# PITR clone: substitute a real covered timestamp
karactl -n default restores create demo clonepoint --time 2026-01-15T12:00:00Z --into-new pointcopy
karactl -n default restores list democopy
```

Clone leaves the source running. Alternatively create a compatible cluster with immutable `spec.dataSource` containing `cluster`, exactly one `backup`/`pitrTime`, and optional source `namespace`; see [generic clone flow](../backups.md). Destination initialization queues a deterministic restore. After initial recovery, dataSource is provenance only; later changes do not need the source. It is not continuous replication.

[PBM](https://docs.percona.com/percona-backup-mongodb/) restores source application users. Logical recovery preserves its performing backup account, then uses it to reset destination system users. Physical snapshots retain an independently encrypted snapshot-time `userAdmin` recovery identity. Legacy physical recovery can protect a still-existing source's unchanged snapshot-era userAdmin; unavailable/changed credentials block recovery before mutation. Modern logical/physical snapshots can survive source deletion; missing provenance/topology/version or unrecoverable physical identity returns **409**. Never manually back up a clone while PBM still points at the source prefix.

PITR clone still requires a live source and observed window. A deleted source needs an explicit complete create file rather than `--into-new` (which copies a live spec), or restore into an existing compatible target with `--source-cluster`.

For cross-namespace recovery, global `-n` selects target, `--source-namespace` source; source needs `backups/read` **and** `backups/export`, plus target mutation permission. PBM temporarily uses the recorded source-namespace storage; target members must reach it directly, then return to their isolated configuration. Convenience clone omits copied cross-namespace backup settings: configure target-local storage afterward. See [cross-namespace flow](../backups.md).

## PBM recovery and cancellation

[Operation limits](../clusters.md#operations-and-status) apply. Restore compensation has a separate six-hour generic failure-cleanup window (with the shorter fail-closed observation rules below); it is not rollback. Backup polling exhaustion and legacy failed-start cleanup enter **read-only recovery with a two-minute observed-execution budget**, persisted across CP restarts. Legacy cleanup checkpoints receive one fresh observation window on upgrade. Each provider attempt is capped at two minutes; with the default one-minute worker backoff, failure is published within five minutes of entering this recovery window (excluding CP downtime, queue scheduling and unavailable state storage).

Before initiating a backup, Karakoram waits for [PBM](https://docs.percona.com/percona-backup-mongodb/) status to report at least one healthy primary/secondary from each participating replica set, including config servers. A Ready [MongoDB](https://www.mongodb.com/) primary alone does not prove PBM storage readiness: a first configuration/resync can finish before PBM refreshes its health record, causing an empty native nomination. The operation reports `ready` progress while waiting and fails without initiation after a persisted two-minute observed-execution readiness budget. Status outages or malformed/missing readiness data do not authorize initiation.

A failed, unknown, or malformed initiation response without a native name now fails the requested record and operation immediately with **initiation outcome unconfirmed** and operator guidance, rather than repeatedly returning the immutable failed receipt for the ordinary retry budget. This does not assert that PBM rejected or stopped the command. No automatic re-initiation, cancellation, deletion, worker restart, or reconfiguration is issued; inspect native work before retrying. The durable `done` checkpoint prevents replay after CP restart.

Backup recovery never replays a failed/unknown start receipt and never cancels, deletes, restarts workers or reconfigures a possibly-running backup. Recovery may claim a snapshot only when the native name came from **this operation's actual successful structured initiation result**, not merely from a matching timestamp, type, storage or serialized queue. For example, an accepted/named backup whose later polling exhausted retries can still complete truthfully after native observation. Pinned PBM 2.16 JSON mode emits its initiation result only after its acceptance wait; there is no supported partial progress-name capture. A CLI interrupted before that JSON result leaves no attributable name. Full-cluster scope is independently checked: selective `namespaces` metadata cannot satisfy a full backup request.

For an operation-attributed full backup, positive native success completes both record and operation Succeeded; native failure completes both Failed. Without an operation-specific identity, the original record and operation fail closed with candidate artifact names and operator guidance. Ordinary inventory reconciliation then adopts an unclaimed native snapshot under its **own separate native-name record**; it must not claim that a manual backup fulfilled the failed request. Native polling also expires Failed with operator guidance if the outcome remains unconfirmed within the observation budget. Repair/verify native work before submitting another backup. An unnamed initiation receipt cannot gain ownership proof from a CP or member restart. Cluster deletion's verified-prefix sweep (below) covers unlisted artifacts or reports `BackupArtifactsRetained`.

After ordinary restore timeout, cleanup observes an already-started PBM restore to terminal state before restarting databases. PBM `done` can leave Restore Succeeded while Operation records the limit failure; fresh-base/PITR gating still applies. Cleanup expiry does not prove PBM stopped or authorize starting database processes.

For a logical restore, an orphaned native `running` status is not progress.
Karakoram checkpoints the native command/lock owner's supervisor session and
successful child-launch counter while observing the restore. A later, higher
launch counter **in the same supervisor session** proves the previous child
was reaped before replacement. If that restore's lock heartbeat and native
progress transition are also at least **two minutes** old, Restore/Operation
fail with the native name, launch evidence, possible OOM/restart diagnosis and
recovery action. Create-from-backup also fails rather than presenting an empty
healthy target as Ready. This is a genuine lost-worker abort, not just an
expired create tracker. No restore is re-initiated, and holds remain because
other native workers may still need inspection.

Wall-clock process birth ordering is **never** replacement proof, even on a
single host: clocks can skew or step relative to MongoDB cluster time.
A paused live process keeps its launch counter and cannot be classified dead.
First observation, legacy agents without launch identity, a different
supervisor session, missing lock ownership and slow progress are ambiguous.
They retain ordinary bounded operation deadlines, not inferred native failure.
Deploying CP alone cannot retroactively prove an old unobserved worker died;
do not restart a member merely to manufacture evidence.
Physical restores are excluded because their process restarts are intentional.
With an established comparable identity and reachable observers, classification
occurs on the first poll after the two-minute stale window. Observer-only
authentication/transport failures are diagnostics, not restore retry-budget
failures; normal native polling remains authoritative. Observer authentication
uses the active restore-scoped PITR identity when present.

Ambiguity is visible, not silent: if both the named restore's native progress
and lock heartbeat are stale for the two-minute window, Karakoram publishes
`RestoreWorker / Unknown / RestoreWorkerUnconfirmed` on the cluster and a
`RestoreWorkerUnconfirmed` event associated with the Restore operation and its
active create operation. The Restore operation message explains that the worker
cannot be confirmed alive, native restoration is not re-initiated, and the
operator may inspect native logs/memory or delete the target. This warning also
works after a CP-only upgrade on legacy agents with no launch identity.
It is **not terminal failure or abort evidence**. Fresh heartbeat/native progress
or a classified terminal outcome clears the warning; repeated stale polls do not
emit duplicate transition events.

PBM's supervised Go runtime also receives an explicit **soft** memory target.
For each participating mongod, the control plane derives `GOMEMLIMIT` from its
**applied** minimum allocation (`minRam`), minus its **active** WiredTiger cache,
256 MiB for mongod non-cache memory, 64 MiB for agents/CLI and 64 MiB of
allocation headroom. A missing active-cache field uses the agent's historical
0.25 GiB cache default, never a recomputation from pending desired `maxRam`.
With normally applied provider cache defaults, fixed 1 GiB therefore gets a
384 MiB PBM target; fixed 2/4/8 GiB members get 896/1920/3968 MiB.
An unapplied desired RAM/cache increase cannot inflate the current target.
Go's limit controls heap/GC pacing, **not a hard process-RSS ceiling**: live
buffers, non-Go memory, native engine growth and GC CPU pressure still matter.
`GOGC` is not overridden.

Applied sizing travels with every replica/shard/config-server mongod in the
PBM environment. Legacy members without persisted applied RAM are checked
against the backend's authoritative current service bounds, not desired spec.
If those bounds are unavailable or unknown, new logical recovery is refused
with `PBMRestoreMemoryBudgetUnknown`; no 1 GiB allocation is invented.
Existing native work keeps its nonterminal legacy diagnostic instead of failing
because an optional sizing observation is unavailable.

New logical restore/clone recovery validates the **minimum applied PBM budget
across every participating mongod** against a **320 MiB floor** before source
configuration or native restore initiation. Below it, the operation fails with
`PBMRestoreMemoryBudgetTooSmall`, naming the limiting member/component and budget.
Correct applied `minRam`/`maxRam` before recovery. This floor reserves room
for live archive/BSON/bulk/driver state; it is not a claim that every possible
workload fits at the boundary. Physical restores are not subject to this
logical admission rule. Already-named or uncertain legacy native work is not
retroactively rejected or re-initiated by a new sizing policy.

Logical concurrency uses that same minimum applied member budget, allowing
256 MiB of BSON reader queue per collection and 128 MiB per
insertion worker. Two collections are used only if two one-worker budgets fit,
then workers are capped at ten per collection. Fixed 1 GiB uses one/one,
2 GiB two/one, 4 GiB two/five and 8 GiB two/ten. Batch size remains 500:
pinned mongo-tools flushes below its approximately 48 MB wire byte limit.
These concurrency settings alone do not account for Go GC slack.
The 64 MiB download-buffer setting applies to PBM's configurable downloader;
pinned 2.16 logical namespace reads instead use the ordinary one-worker 8 MiB
[S3](https://aws.amazon.com/s3/) arena, plus archive composition/decompression/BSON buffers.

New agent binaries apply the target **only on the next actual PBM process
launch**, reporting it as `processes.pbm-agent.goMemoryLimitBytes` in agent
status. A config pull or budget change never restarts a possibly-mutating PBM
restore. Existing agents and already-running children may lack an applied
target; Restore operation diagnostics then report `PBMMemoryBudgetUnknown`
at the first restore-time observation instead of claiming enforcement.
Deploy new agents on fresh recovery targets to exercise the bounded-runtime
path; do not restart a held restore to retrofit a limit.

Small members trade parallelism/GC CPU for headroom, not data correctness.
Inspect allocation OOM events, observed PBM launch limit and native progress;
choose larger RAM before recovery when the workload requires it.
Increasing RAM or restarting CP cannot resume an already-killed native restore.
Keep writers quiesced and use explicit fresh-target recovery below, never replay
the failed receipt.


A published native restore failure remains **Restore Failed / Operation Failed**
after compensating cleanup. The error retains the native restore name and
original PBM error. Partial data or authentication changes cannot be excluded:
PBM 2.16 does not provide affirmative no-mutation proof for a logical restore
that fails during primary loss, even if the last sampled status was `starting`
or mongorestore reports zero document counters. Karakoram does not automatically
re-initiate it, change its native identity or issue another request identity.
`Ready` describes database health, not successful restore or unchanged data.
A native failure with uncertain consistency leaves the cluster **Degraded /
operator action required**, even after processes and members become healthy;
runtime liveness alone does not clear that durable safety state.

The operator must keep application writers quiesced, verify that native restore
workers have stopped, and assess collection data and application/system
authentication before choosing recovery. Explicitly authorize a **new restore
with a new restore name** (duplicate names are rejected), or recover a verified
backup into a fresh compatible cluster. A new request is a new destructive
execution, not a resume or automatic retry. Only a later successful, verified
restore under a fresh restore name completes recovery and clears the
consistency-uncertain safety state. Preserve the failed Restore, Operation and
receipts as evidence; compensation is not rollback.

`karactl operations cancel ID` supports ordinary backups and restores **before data mutation**, including source-inventory waits; initiating/applying restore cannot be cancelled. Required post-restore base cancellation is rejected; legacy missing/unreadable trigger inventory returns **409** until safety can be established. Cancelled backup/restore inventory is Failed with the cancellation summary. Wait for cooperative cleanup; cancellation is not undo.

PBM initiation pins a command member and durable request identity. Observation and idempotent configuration/resync commands can use surviving eligible members (config-server members for sharded clusters), including offline physical-restore progress. Once an attributable native operation name is durably recorded, backup/restore resumes polling without replaying initiation, even if the receipt-owning member is unavailable.

An interrupted receipt **without** a recorded native name reports `PBMReceiptUnknown`, naming the operation request and member. Karakoram cannot safely attribute an unrelated timestamp-matching native operation to that request or prove that an absent inventory entry means initiation never happened. Inspect `pbm list --out=json`, `pbm list --restore --out=json`, matching `describe-backup` / `describe-restore`, PBM logs and the persisted Operation using a surviving eligible member and the recorded storage prefix. Preserve receipts, storage and physical-restore holds; never erase receipts, change request identity, switch initiation nodes or launch a duplicate restore. If provenance cannot be established, recover a verified backup into a new cluster and keep the ambiguous target fenced. There is no automatic destructive retry or supported receipt-editing shortcut.

Missing restore metadata is tolerated as **restore starting** only for one minute after successful acceptance, with a persisted deadline; later disappearance and unrelated command failures are errors.

Logical named restores also consult `.pbm.restore/` in their recorded source
storage. An empty `pbm list --restore`, historical non-primary skip, listener
log, or reconstructed process birth timestamp never proves that an old command
cannot execute later.

PBM's storage-backed `describe-restore --config` can return exit success with
only a name/type and an empty status even for an empty restore prefix. That
synthetic shell is not native progress or completion. For a never-observed
logical restore it must still pass the complete absence and active-fence checks
below; a stored nonempty status continues through native-outcome polling.

For a logical replica-set restore that has **never** published metadata, a
bounded absence path requires exact not-found from every eligible member,
complete storage absence, the unique native command at least one minute old,
and no conflicting operation in either `pbmLock` or `pbmLockOp`. All operation
types (backup, PITR, replay, resync, restore and unknown types) block admission;
only this restore's own stale restore lock is exempt. These observations must
remain complete across a persisted further one-minute window. Admission is
refreshed immediately before **each** member restart, so work that appears
after the initial window or between restarts prevents the next restart.
The product then **actively
restarts PBM agents on every member**, using stable identified supervisor
requests—not another destructive restore command. Each agent must confirm its
old child exited through the supervisor's wait and a replacement started.
Immediately before launch and on acknowledgment, the member clock must be at
least the original command timestamp plus **30 seconds**; a clock behind that
guard keeps the old process stopped. Identified durable receipts prevent a
lost response from repeating the restart. Every complete final observation
revalidates all receipts for the current supervisor session, process generation,
capability and clock before accepting metadata/storage absence. No per-member
verification cursor survives a failed or interrupted observation.

After all members confirm replacement, Karakoram rechecks every member's
metadata, all PBM operation locks and complete recorded storage listing. Only
continued absence permits compensating recovery to restore target
configuration and release holds. Restore and Operation then become **Failed**
with `RestoreNeverStarted` and “data unchanged; re-run the restore”; ordinary
fresh health observation can return the cluster to Ready. Karakoram never
resubmits the destructive command or reports Succeeded from absence.

If native metadata appears during restart admission or final absence checking,
Karakoram consumes that observation through normal native-outcome polling
instead of continuing absence fencing. Native `done` or explicit failure enters
the corresponding existing recovery path; running, unclassified or subsequently
missing metadata keeps the restore holds in place. No further agent restarts
or destructive restore initiation are allowed from that observation, and it
cannot be converted into `RestoreNeverStarted`.

Any unconfirmed exit/start, changed supervisor/process generation, clock guard
failure, transport error or unknown restart outcome keeps the restore fenced.
Unknown restart state is durable; later healthy observations cannot revive it
as a successful fence. Local stopped intent prevents config pulls or automatic
backoff from bypassing a rejected launch. Physical/sharded restores, previously
observed native restores, active locks and partial/unavailable metadata/storage
observations do not use this terminal-absence path.

The `pbmRestoreFence` capability and explicit restart acknowledgment require
updated member agents as well as the control plane. Legacy agents remain
fenced under the default 20-failure or six-hour restore-execution limit,
followed by a separate six-hour failure-cleanup bound. Expiry does not prove
PBM stopped, establish unchanged data, or authorize destructive retry.
Every unsuccessful attempt invalidates its absence window, including
dependency resolution/restore validation before provider entry and
inventory/storage/runtime-recovery failures inside the provider.

## Delete backup data and recover pending deletion

**Warning:** [PBM](https://docs.percona.com/percona-backup-mongodb/) deletion is irreversible and can shorten PITR coverage. Keep registered storage/location/credentials valid until cleanup completes.

`karactl backups delete demo beforechange --yes` on a live cluster waits asynchronously for PBM inventory absence. Active matching work returns **409** naming backup/state; cancel it first. After source deletion, snapshot DELETE is synchronous **204**, agent-free, and removes only recorded `<prefix>/<pbm-name>/...` and `<prefix>/<pbm-name>.pbm.json`, then identity/record. Other snapshots, PITR and namespace markers remain. Partial failure leaves Deleting; repeat DELETE safely, including after restart. Already absent returns 204. Never-started retained Failed records need no storage; unsafe provenance returns 409.

Cluster deletion without backup retention first removes runtime services and (unless retained) volumes. It never authenticates to [MongoDB](https://www.mongodb.com/)/PBM: broken managed users or unreachable members do not block deletion. Once every database runtime is authoritatively absent, the control plane uses its registered backup-store binding to sweep only the verified cluster-incarnation prefix, including unlisted snapshots, PITR and markers. Agent unreachability alone never authorizes a sweep. Recorded backup prefixes must match the binding; earlier-incarnation retained snapshots remain independent. `--keep-backups` skips all artifact and inventory deletion. Active restore/clone dependency fences still return **409** before deletion is accepted.

Object cleanup has a durable **two-minute total window**, starting after runtime/volume retirement. A storage failure, unsafe/missing ownership or exhausted window emits **`BackupArtifactsRetained`**, identifying the storage/bucket/exact prefix when known and manual removal guidance; deletion still completes and affected backup inventory remains available under the original name/namespace. Inspect events as well as terminal success: success means cluster removal, not a promise that every remote artifact was removed. Restore the original storage binding and remove only the recorded incarnation prefix with object-store tooling; never remove a source/other incarnation prefix. Then remove retained backup records through the API. Suspended clusters need not resume for verified-prefix cleanup. Resume clears cleanup checkpoints and recreates PBM initialization markers even with unchanged backup settings.

Version-aware cleanup uses `s3:ListBucketVersions` / `s3:DeleteObjectVersion` to remove versions/markers. Unavailable/denied version listing falls back to ordinary visible-key deletion: noncurrent versions require separate credentials/lifecycle cleanup. Successful listing followed by denied version deletion remains an error. Empty-namespace confirmation uses that listing mode; large namespaces progress over bounded passes. If storage prefix changed, restore the original prefix before retrying.

For pending cluster deletion, inspect same-namespace cluster status, operations and events. Repair the named [S3](https://aws.amazon.com/s3/) bucket/prefix/write-delete access, [namespace relay](../namespaces.md#relay-health), or platform process; unformed clusters may never have had usable PBM. Observe the existing operation; do not repeatedly DELETE, erase state/volumes or recreate the name. Retry only failed/unaccepted deletion with the **same intentional retention choices**, following [Delete or suspend](../clusters.md#delete-or-suspend). Check retained inventory and [orphan volumes](../operations.md#orphaned-platform-volumes) separately; keep namespace management available until cleanup finishes.
