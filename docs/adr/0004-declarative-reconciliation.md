# 0004 — Level-triggered reconciliation and durable operations

Status: Accepted

## Context

Zerops lifecycle actions are asynchronous Processes without a generic watch/webhook interface. A request can succeed externally before Karakoram crashes or stores its response. Database changes must preserve quorum and never rely on an HTTP request handler staying alive.

## Decision

Persist desired cluster specifications separately from observed status and provider-owned internal state. Reconcile on changes and timed requeues. Provider `Sync` is level-triggered and idempotent, performs at most one disruptive step per call, and returns a requeue hint; `Status` reports observation rather than desired success.

Represent imperative backup, deletion of backups, restore, FCV raise, rolling restart, replacement and secret/certificate rotation as durable `Operation` records. `RunOperation` is re-entrant and resumes from a persisted checkpoint. Specification-driven creation, scaling, resources, version, exposure, PITR and schedules remain `Sync` work. Serialize `Sync`, cleanup and operations with per-cluster locks; different clusters can make independent progress.

Persist step intent and known platform/PBM identifiers, poll completion without blocking a worker indefinitely, and checkpoint observed results. After a crash or uncertain submission, inspect stable service identities and engine/PBM state before resubmitting. No exactly-once claim is made across SQLite and external systems. Preserve deletion intent until cleanup or explicit retention is complete.

Record durable events and publish them through `/events/stream` SSE. Reconnects replay authorized events using event IDs; in-process delivery is an acceleration, not the source of truth.

## Consequences

Management survives request disconnects and process restarts. Partial failure remains visible with actionable status, not false completion. Checkpoints and external observation are essential: locks alone do not make a platform call atomic. Long restore/upgrade work serializes other disruptive work on that cluster. Event visibility is subject to the same per-cluster RBAC as resource reads.

## Alternatives considered

- **Request-driven scripts:** simpler happy path but lose progress and safety on disconnect/crash.
- **Edge-triggered queue alone:** missed notifications leave drift unresolved.
- **External workflow engine:** durable orchestration, but adds infrastructure beyond the single-control-plane requirement.
