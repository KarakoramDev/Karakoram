# 0004 — Level-triggered reconciliation and durable operations

Status: Accepted

## Context

[Zerops](https://zerops.io) lifecycle actions are asynchronous Processes without a generic watch/webhook interface. A request can succeed externally before Karakoram crashes or stores its response. Database changes must preserve quorum and never rely on an HTTP request handler staying alive.

## Decision

Persist desired cluster specifications separately from observed status and engine driver-owned internal state. Reconcile on changes and timed requeues. Engine driver `Sync` is level-triggered and idempotent, performs at most one disruptive step per call, and returns a requeue hint; `Status` reports observation rather than desired success.

Represent imperative backup, deletion of backups, restore, FCV raise, rolling restart, replacement and secret/certificate rotation as durable `Operation` records. `RunOperation` is re-entrant and resumes from a persisted checkpoint. Specification-driven creation, scaling, resources, version, exposure, PITR and schedules remain `Sync` work. Serialize `Sync`, cleanup and operations with per-cluster locks; different clusters can make independent progress.

Persist step intent and known platform/[PBM](https://docs.percona.com/percona-backup-mongodb/) identifiers, poll completion without blocking a worker indefinitely, and checkpoint observed results. After a crash or uncertain submission, inspect stable service identities and engine/PBM state before resubmitting. No exactly-once claim is made across [SQLite](https://sqlite.org) and external systems. Preserve deletion intent until cleanup or explicit retention is complete.

Existing member availability also has a bounded read-only loop while ordinary
reconciliation is busy, independent of lifecycle phase. It loads an isolated
durable snapshot instead of sharing the lifecycle worker's mutable state. Its
status-only update compares the inspected status, provider state, generation and
incarnation. Running imperative operations exclude independent publication, but
ordinary desired-state convergence trackers do not; the distinction is checked
under the same writer transaction using the domain operation classification.
Availability never advances applied generation or completes a non-Ready lifecycle
phase/negative readiness fence, including during deferred maintenance. New clusters
without members or prior observations have no availability to renew. Suspended
and deleting workloads must remain stopped; restoring state/native receipts retain
serialized ownership, so those intents exclude the independent loop. Creating
with existing members, Initializing, Updating, Scaling, Upgrading and failed
requests can still report fresh member health without being promoted to Ready.
Both paths arbitrate each complete observation through its publication using a
shared per-worker gate; a completed probe alone does not release publication
ownership. This prevents the ordinary writer from replaying an older healthy
sample over a newer failure and prevents either writer from shortening a newer
lease. Only a successful pre-Sync publication releases the gate before Sync.
If that write fails, ownership remains through the caller's error-path state
publication and deferred release. Final observation retains ownership through
the lifecycle checkpoint. No health/lifecycle merge or wall-clock comparison is
used to order failed (zero-lease) samples.

Record durable events and publish them through `/events/stream` SSE. Reconnects replay authorized events using event IDs; in-process delivery is an acceleration, not the source of truth.

## Consequences

Management survives request disconnects and process restarts. Partial failure remains visible with actionable status, not false completion. Checkpoints and external observation are essential: locks alone do not make a platform call atomic. Long restore/upgrade work serializes other disruptive work on that cluster. Event visibility is subject to the same per-cluster RBAC as resource reads.

Long successful synchronization cannot starve availability merely by occupying
the lifecycle worker. Actual observation/platform/publication failures still
expire positive availability under the existing lease. The additional bounded
reads share backend observation admission; they do not reserve mutation slots or
turn cached health into fresh evidence. Checkpoint conflicts discard the sample
until a subsequent sweep rather than merging an old observation into new intent.

## Alternatives considered

- **Request-driven scripts:** simpler happy path but lose progress and safety on disconnect/crash.
- **Edge-triggered queue alone:** missed notifications leave drift unresolved.
- **External workflow engine:** durable orchestration, but adds infrastructure beyond the single-control-plane requirement.
