// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/*
Package pbm implements the provider's PBM subsystem.

# Durability contract

The provider/reconciler must commit OpResult.Checkpoint after every call, including
calls that return errors. PBM start requests use op.ID as the node agent's durable
RequestID. The selected member is committed before starting; never change that
member when retrying an initiation with a lost reply. Agent receipt storage must
survive control-plane restarts. An agent's explicitly ambiguous receipt is an
operator-visible error, not permission to issue a second restore. Env.Nodes and
Env.Routers must retain their order for the lifetime of a restore, and provider
Sync must not alter topology or PBM storage during an active operation. For a
sharded cluster the command node belongs to the cfg replica set.
RunRestore also merges Internal.pbmMaintenance (member -> bool) and
Internal.pbmStopped (member -> process names). Each maintenance/router action
first changes desired state in a separate call without issuing the command; the
provider must persist Cluster.Internal together with the returned checkpoint
before the next call and feed these maps into MemberConfig. This prevents periodic
config pulls from restarting stopped routers or clearing physical-restore
suppression. Clear maintenance only after database/PBM process recovery, and
router suppression only after restored users and target-prefix resync are ready.

Checkpoints are secret-free JSON: step (next action), member (receipt owner),
pbmName (PBM operation/snapshot name), snapshot (restore base), physical (offline
progress/lifecycle mode), source (clone source), storage/prefix (recorded source
destination), recoveryIdentity (encrypted secret scope reference), targetTime,
index (next node/router action), failure (terminal PBM failure carried through cleanup),
routerOp (current infrastructure process), routerRetry (failure count, retry deadline
and message), freshBaseBackupRequired (provider follow-on action), and destination
(cleanup-only hash binding progress to storage name/endpoint/bucket/effective prefix).
Configuration YAML and storage credentials are loaded at invocation time and never checkpointed.

Backup: configure -> start -> poll -> target-configure/resync/inventory -> done.
An initial call commits the selected member, immutable destination/topology/version
provenance, and a reference to encrypted snapshot-time userAdmin credentials.
Start stores PBMName; poll maps size, last-write, start/finish and terminal phase.
For nonterminal describe metadata, poll also checks the exact snapshot in PBM's
heartbeat-aware status inventory. An orphaned owner's persisted running metadata
does not hide a native stale-heartbeat failure or cause a second backup initiation.
Inventory entries can omit finish timestamps; subsequent polls preserve the
recorded completion time instead of replacing it with the current poll time.

Delete-backup: configure -> resync -> inventory -> delete -> observe ->
target-configure/resync/inventory -> done. Resolve the snapshot's recorded
destination before interpreting missing PBM metadata as deletion. The inventory
record and protected recovery identity are removed only after destination cleanup.
A retry after record removal is complete.

Restore: configure-source -> source-resync -> source-inventory ->
disable-pitr -> balancer-off -> routers-off -> maintenance-on -> start -> poll ->
mongod-start -> pbm-start -> maintenance-off -> reset-users -> configure-target ->
target-resync -> target-inventory -> routers-on -> balancer-on -> complete -> done.
Initial resolution commits the newest successful base at/before a PITR target,
the command member and source/type. PITR targets must belong to a backed continuous
range. Balancer/router steps are no-ops for replica sets; maintenance and database
process steps are no-ops for logical restores. Per-node steps persist index and
use idempotent process start actions, not disruptive repeated restart actions.
Physical polling supplies storage YAML with --config while MongoDB is offline.
Clone config prefers the immutable full prefix/storage on the Backup record and
does not require a live source cluster. Legacy backups fall back to live source
prefix provenance. PITR base selection excludes snapshots in other storages
because their oplog ranges do not belong to the primary PITR destination.
Every successful restore resets target system credentials via the preserved PBM
account, with encrypted snapshot-time userAdmin fallback for physical recovery.
Router Stop/Start acts on the entire infrastructure service (all containers),
observing the platform process before proceeding. Failed or canceled router processes
are cleared in the checkpoint and retried with the shared platform backoff (30 seconds,
doubling up to 10 minutes). After backoff, the service state is observed first: an
already-stopped/active service completes the requested action without reissuing it.
The restore does not advance past a router until its action completes. Both destination
resyncs are nonblocking and observed using status plus an available inventory. Terminal
PBM errors still traverse database/router/maintenance/config cleanup before the Restore
record is marked Failed and the error is returned.

The provider enqueues an initial fresh base backup after a successful
restore when freshBaseBackupRequired is true, derived from the target cluster's
desired PITR state rather than a temporarily disabled restore environment.
Keep Env.PITR.Enabled false for normal config reconciliation until that backup
or a later snapshot on primary storage succeeds. The provider retries failed,
cancelled or missing bases within a bounded budget, without blocking Sync,
and permits a later manual snapshot to clear the gate after exhaustion.
EnsureConfig independently
gates initial PITR activation on a successful base after first configuring PBM
with PITR disabled. It cannot tell
whether an old snapshot predates a restore, so the provider's pending requirement
is necessary. Clone prefixes must never receive backups while pointing at source
storage. Alternate backups/deletions configure and resynchronize the primary
destination before returning success; the provider also invalidates its config hash.

DeleteAll merges pbmCleanup into Cluster.Internal: configure destination with
PITR disabled -> resync -> inventory -> delete-pitr -> observe-pitr -> snapshots ->
delete-snapshot -> observe-snapshot (repeat) -> done. Its destination hash resets
progress when switching storage/prefix, so the provider can clean multiple storages
sequentially. The provider must persist Internal between calls and keep agents alive
until Done before removing runtime services. Unrelated Internal keys are retained.
Missing protected recovery secrets are already deleted, so a restart between deleting
a snapshot's recovery secret and removing its inventory record can finish cleanup.
*/
package pbm
