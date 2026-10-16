<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backups and PITR

Use PBM for consistent MongoDB snapshots, point-in-time recovery and clone recovery.

## At a glance

| Capability | Requirement / behavior |
| --- | --- |
| `logical` | PBM logical snapshot; CLI backup-type default |
| `physical` | PSMDB physical snapshot; recovery replaces the data directory |
| PITR | Successful base snapshot plus continuous PBM-observed oplog slices |
| Transfer | Members ↔ S3 directly, never through the management relay |
| Isolation | Effective storage prefix includes a stable cluster UID; preserve each snapshot's recorded namespace/prefix |

Register and verify S3 storage using [Backups and storage](../backups.md), which owns common inventory, schedule/count-retention and retained-backup flows. Verification from the control plane does not prove every member can reach S3. Preserve state, original encryption key and storage credentials; object storage alone cannot reconstruct recovery ownership.

## On-demand backups and schedules

```sh
karactl -n default backups create demo beforechange --type logical --storage archive
karactl -n default backups create demo physicalbase --type physical --storage archive
karactl -n default backups list demo
karactl -n default backups get demo beforechange
karactl -n default operations cluster demo
```

UI: **Cluster → Backups → Backup now**; storage is required. Follow the operation and inventory to **Succeeded**, not just request acceptance/PBM initiation. Inventory includes PBM name, type/storage, times, size/error where available; `trigger` is `manual`, `schedule` or `restore` (automatic post-restore base).

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

Future/outside/gap timestamps return **400**, `details.field: source.pitrTime`, reason `out_of_range`, with UTC `earliest`/`latest`; bounds do not imply intervening gaps are covered. Initialization/no coverage returns **409** `invalid_state`, including on the window endpoint. Coverage is rechecked during execution; permanently invalid targets fail terminally. Once a set is ready with PBM storage configured, convergence during scaling/upgrades/reloads does not itself hide established coverage; agent/storage failures still can.

Quota, slicing failure, history gaps, base deletion and storage changes affect recovery. After restore, PITR stays disabled until a fresh snapshot on **primary storage** succeeds. The required automatic base cannot be cancelled. If it fails/disappears, at most three replacement attempts are queued with warnings without blocking cluster convergence. After exhaustion, take a successful manual primary-storage backup; a snapshot queued after restore also satisfies the gate. Check the new timeline before trusting recovery.

## In-place restore

**Warning:** Stop application writers and plan downtime. Restore overwrites data and may restore application users/roles; preserve a current recovery copy first.

```sh
karactl -n default restores create demo recoverbase --backup beforechange --yes
# Replace the illustrative time with an observed covered point:
karactl -n default restores create demo recovertime --time 2026-01-15T12:00:00Z --yes
karactl -n default restores get demo recoverbase
```

Use exactly one `source.backup` / `source.pitrTime`; both/neither return **400**, field `source`, reason `conflict`. Missing backup returns **404** with source context; duplicate restore name **409** `already_exists`, before enqueueing. Source defaults to target; require matching provider/topology/engine major and a successful compatible snapshot, particularly for physical recovery. Missing provider metadata denotes legacy PSMDB, not a provider-neutral backup.

Logical restore is **not full rollback**: replica-set restore replaces backed-up namespaces but leaves extra collections/databases untouched. Sharded logical restore drops every database present in the backup, removing later collections within those databases; databases absent from the backup remain. For an exact copy, quiesce/drop extras deliberately, use physical recovery, or restore into a new cluster.

Karakoram serializes disruptive work, disables PITR and holds sharded routers/balancer. Physical restore suppresses automatic mongod/PBM restart; mongod may be offline and PBM may exit successfully. Progress uses PBM storage configuration while MongoDB is unavailable. Recovery restarts processes, clears maintenance, resets system users, resyncs target storage and restores router/balancer state. Logical restore does not use this offline lifecycle. Phase is **Restoring** with checkpoint message, then health observation on success or Degraded/error on terminal failure.

Validate all members/agents, application authentication, collection data, balancer, inventory and fresh base before reopening writers. PBM `done` alone is insufficient.

## Clone and cross-namespace recovery

```sh
karactl -n default restores create demo cloneproof --backup beforechange --into-new democopy
# PITR clone: substitute a real covered timestamp
karactl -n default restores create demo clonepoint --time 2026-01-15T12:00:00Z --into-new pointcopy
karactl -n default restores list democopy
```

Clone leaves the source running. Alternatively create a compatible cluster with immutable `spec.dataSource` containing `cluster`, exactly one `backup`/`pitrTime`, and optional source `namespace`; see [generic clone flow](../backups.md). Destination initialization queues a deterministic restore. After initial recovery, dataSource is provenance only; later changes do not need the source. It is not continuous replication.

PBM restores source application users. Logical recovery preserves its performing backup account, then uses it to reset destination system users. Physical snapshots retain an independently encrypted snapshot-time `userAdmin` recovery identity. Legacy physical recovery can protect a still-existing source's unchanged snapshot-era userAdmin; unavailable/changed credentials block recovery before mutation. Modern logical/physical snapshots can survive source deletion; missing provenance/topology/version or unrecoverable physical identity returns **409**. Never manually back up a clone while PBM still points at the source prefix.

PITR clone still requires a live source and observed window. A deleted source needs an explicit complete create file rather than `--into-new` (which copies a live spec), or restore into an existing compatible target with `--source-cluster`.

For cross-namespace recovery, global `-n` selects target, `--source-namespace` source; source needs `backups/read` **and** `backups/export`, plus target mutation permission. PBM temporarily uses the recorded source-namespace storage; target members must reach it directly, then return to their isolated configuration. Convenience clone omits copied cross-namespace backup settings: configure target-local storage afterward. See [cross-namespace flow](../backups.md).

## PBM recovery and cancellation

[Operation limits](../clusters.md#operations-and-status) apply; backup/restore recovery has a separate **six-hour failure-cleanup window**, durable across restarts. Cleanup retries beyond ordinary limits, then releases the queue as Failed with the original error; expiration warns which manual recovery checkpoint remains. It is not rollback. Never start dependent work until PBM/processes/holds/target configuration are repaired.

After ordinary restore timeout, cleanup observes an already-started PBM restore to terminal state before restarting databases. PBM `done` can leave Restore Succeeded while Operation records the limit failure; fresh-base/PITR gating still applies. Cleanup expiry does not prove PBM stopped or authorize starting database processes.

`karactl operations cancel ID` supports ordinary backups and restores **before data mutation**, including source-inventory waits; initiating/applying restore cannot be cancelled. Required post-restore base cancellation is rejected; legacy missing/unreadable trigger inventory returns **409** until safety can be established. Cancelled backup/restore inventory is Failed with the cancellation summary. Wait for cooperative cleanup; cancellation is not undo.

PBM initiation pins a command member and durable request identity. An interrupted receipt with unknown outcome reports **“PBM initiation outcome unknown after interrupted execution; reconcile before issuing another request”**. Inspect PBM metadata/logs/storage and the persisted Operation; never erase receipts, change request identity, switch command nodes or launch a duplicate restore. Missing restore metadata is tolerated as **restore starting** only for one minute after successful acceptance, with a persisted deadline; later disappearance and unrelated command failures are errors.

## Delete backup data and recover pending deletion

**Warning:** PBM deletion is irreversible and can shorten PITR coverage. Keep registered storage/location/credentials valid until cleanup completes.

`karactl backups delete demo beforechange --yes` on a live cluster waits asynchronously for PBM inventory absence. Active matching work returns **409** naming backup/state; cancel it first. After source deletion, snapshot DELETE is synchronous **204**, agent-free, and removes only recorded `<prefix>/<pbm-name>/...` and `<prefix>/<pbm-name>.pbm.json`, then identity/record. Other snapshots, PITR and namespace markers remain. Partial failure leaves Deleting; repeat DELETE safely, including after restart. Already absent returns 204. Never-started retained Failed records need no storage; unsafe provenance returns 409.

Cluster deletion without backup retention disables PITR, resyncs/deletes through PBM **before** removing agents/data. Only once all database runtimes are absent can direct S3 cleanup sweep the exact recorded incarnation, including unlisted snapshots, PITR and markers. Agent unreachability alone never authorizes this sweep. Final storage checkpoints complete before credential removal; earlier-incarnation explicitly retained snapshots remain independent. Suspended clusters need not resume for verified-prefix cleanup; legacy unknown prefixes require resume/PBM cleanup or intentional retention. Resume recreates PBM initialization markers even with unchanged backup settings.

Version-aware cleanup uses `s3:ListBucketVersions` / `s3:DeleteObjectVersion` to remove versions/markers. Unavailable/denied version listing falls back to ordinary visible-key deletion: noncurrent versions require separate credentials/lifecycle cleanup. Successful listing followed by denied version deletion remains an error. Empty-namespace confirmation uses that listing mode; large namespaces progress over bounded passes. If storage prefix changed, restore the original prefix before retrying.

For pending cluster deletion, inspect same-namespace cluster status, operations and events. Repair the named S3 bucket/prefix/write-delete access, [namespace relay](../namespaces.md#relay-health), or platform process; unformed clusters may never have had usable PBM. Observe the existing operation; do not repeatedly DELETE, erase state/volumes or recreate the name. Retry only failed/unaccepted deletion with the **same intentional retention choices**, following [Delete or suspend](../clusters.md#delete-or-suspend). Check retained inventory and [orphan volumes](../operations.md#orphaned-platform-volumes) separately; keep namespace management available until cleanup finishes.
