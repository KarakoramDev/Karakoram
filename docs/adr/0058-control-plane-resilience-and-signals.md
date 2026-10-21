# 0058 — Control-plane recovery, durable notifications and backup drills

Status: Accepted

## Context

Database backups do not recover the coordinator's desired state, operation journals, credentials or PKI. SQLite WAL mode also makes a raw database-file copy an unsafe online backup. Rebuilding the control plane requires a consistent snapshot and separately preserved external configuration and master key.

Durable events need bounded, restart-safe delivery to external systems. A successful backup record alone does not demonstrate restorability, and engine metrics alone cannot describe control-plane maintenance, placement, recovery or delivery failures. These features must share existing authorization, outbound safety and infrastructure reconciliation rather than introduce privileged alternate paths.

## Decision

### Control-plane snapshots and rebuild

Implement `Store.Snapshot(ctx, dstPath)` with `VACUUM INTO` on the writer connection, using a mode-0600 temporary file inside the data directory, then run `PRAGMA integrity_check`. This is the shared transactionally consistent, WAL-safe snapshot primitive for scheduled and local exports.

Create a persistent random 16-hex-character installation ID at startup if absent. Derive a key fingerprint as `kkfp1:` plus the first 16 bytes of HKDF-SHA256 in hexadecimal, using master-key bytes, salt `karakoram/secrets/v1` and info `karakoram/master-key-fingerprint/v1`. Export the fingerprint, never the master key.

Before export, authenticate the supplied sealer against persisted encrypted state. A valid-length but incorrect key must not create a successful archive with a misleading fingerprint.

Manage installation policy through `GET/PUT /control-plane/backup-policy`, authorized by `settings:read/update`. Policy contains `enabled`, an existing S3 storage reference, UTC cron schedule (default `0 3 * * *`) and retention count 1–1000 (default 14). Storage need not have been verified, but must exist and be S3; refuse deletion while the policy references it. Schedule at most one catch-up run.

`POST /control-plane/backups` queues a manual run; `GET /control-plane/backups` lists cursor-paginated records. Persist trigger, Pending/Running/Succeeded/Failed phase, storage/object identity, compressed size/SHA-256, schema and Karakoram versions, key fingerprint, timestamps and sanitized error. Run one backup at a time: snapshot, gzip and stream through the guarded backup-storage client. Store objects beneath `<prefix>/karakoram-control-plane/<installationId>/<UTC timestamp>-<id>.db.gz` with an adjacent JSON manifest containing format `karakoram-state/v1`, installation ID, creation time, version/commit, last migration name, fingerprint, SHA-256 and size. Retain only this installation's newest successful runs up to the policy count. Emit `Warning ControlPlaneBackupFailed` on failure.

Provide `karakoram admin backup-state --out FILE` for local export from a stopped or running control plane, using the same snapshot primitive and writing the archive plus `FILE.json`. Provide offline `restore-state` from `--file`, or an S3 endpoint/bucket/key with optional region/path-style selection. S3 restore credentials come only from `KARAKORAM_RESTORE_S3_ACCESS_KEY_ID` and `KARAKORAM_RESTORE_S3_SECRET_ACCESS_KEY`.

Before publication, verify manifest SHA-256, gzip decoding, SQLite integrity, that the snapshot schema is not newer than the binary and that its fingerprint matches `KARAKORAM_MASTER_KEY`. Refuse a running server's locked database. Refuse an existing database unless `--replace`; checkpoint it out of WAL mode, refuse remaining sidecars, and preserve a timestamped hard-link backup with file/directory fsync before replacing the still-populated authoritative path atomically. Publish through a temporary file, file fsync, atomic rename and directory fsync; an interrupted replacement must never present an absent database as a fresh installation.

Rebuild guidance must cover Zerops, Nomad and Dokploy and distinguish restored SQLite state (including sealed secrets and PKI) from external prerequisites: the master key, environment-supplied backend tokens and endpoint configuration. Restore before normal startup creates new installation identities. Resources created after an old snapshot become orphans; in-flight operations can resume or fail against changed infrastructure and require review.

### Durable notifications and outbound safety

Manage named webhook, Slack and email channels through `/notification-channels` CRUD and `/{channel}/test`, authorized by `settings:read/update`. Names match `^[a-z][a-z0-9-]{0,39}$`. Channels contain enabled state, category and namespace filters, destination configuration and read-only delivery status. Empty category filters mean all categories; empty namespace filters include all namespaces and installation-wide events. Tests return delivered/error status.

Seal channel secrets with AAD `notification/<name>/<field>`. URL, signing-secret and password inputs are write-only; responses expose configured flags, a webhook host only and sanitized status, never stored secrets. Omitted secrets on replacement retain their value only if destination identity is unchanged: URL host, or SMTP host/port/username.

Transport and test delivery load configuration plus sealed credentials from one record snapshot, releasing the read before decryption and network work. A concurrent replacement cannot combine one destination with another's credentials; concurrent deletion is normal lifecycle rather than a controller failure. Forms clear already-entered credentials when destination identity changes.

Classify durable events using **reason, type and operation target**, not severity alone. Supported categories are `backup-failed`, `cluster-degraded`, `cluster-recovered`, `volume-deletion-refused`, `certificate-expiring`, `deployment-retries-exhausted`, `operation-failed`, `members-co-located`, `backup-drill-failed`, `control-plane-backup-failed` and `agents-outdated`. Normalize terminal backup/scheduler failure events; emit degradation/recovery transitions, ownership-refused or stuck orphan-volume deletion events, daily-deduplicated certificate expiry warnings within 30 days and outdated-agent warnings once per cluster/target build. Cover database, internal/server and relay certificates.

Advance a durable event cursor and insert outbox rows transactionally, unique by `(channel,event_id)`. Subscribe before draining durable history and drain periodically every 15 seconds. Per-channel delivery workers retry with exponential backoff from 30 seconds to one hour and give up after 24 hours, retaining failed state and delivery timestamps/errors/counts. Delivery is **at least once**, with the event ID available to recipients for deduplication.

Webhook JSON contains ID, category, warning/info severity, namespace, cluster, reason, message, creation time and source `karakoram`; send `X-Karakoram-Event-Id`. Optional HMAC-SHA256 signing uses `X-Karakoram-Signature: t=<unix>,v1=<hex>` over `<t>.<body>`. Slack uses incoming-webhook text. Email supports required STARTTLS or implicit TLS, TLS 1.2 or newer, PLAIN authentication only over TLS and 1–20 recipients.

Share the backup-storage endpoint guard through `internal/netguard`: address policy, DNS-rebinding-safe dialing, no proxy, no redirects, TLS 1.2 or newer and explicit allowlisting. Notification destinations use `KARAKORAM_NOTIFY_ENDPOINT_ALLOWLIST`; webhook/Slack require HTTPS unless explicitly allowlisted. Bound requests to ten seconds and response-body draining to 4 KiB. Sanitize errors so URLs and secrets are never echoed. Preserve existing backup-storage guard behavior.

### Backup drills

Add an optional backup drill policy with enabled state, cron schedule, optional source backup-schedule filter and timeout (default six hours, range 30 minutes–48 hours). Expose cluster-scoped create/list/get under `/clusters/{cluster}/backup-drills`, authorized by `restores:create/read`. A manual request may name a backup; otherwise select the latest successful eligible backup.

Persist Pending/Provisioning/Restoring/Verifying/CleaningUp/Succeeded/Failed reports, backup identity/type, target identity, timestamps, restore/total duration, data age, checks, limitations, errors and cleanup/orphan-volume status. Retain the last 50 reports per source after target deletion. Installation concurrency defaults to one; `KARAKORAM_BACKUP_DRILL_CONCURRENCY` permits 1–4, with excess work queued.

Create a disposable **same-namespace** target through the same validated cluster-create path using the source backup as `dataSource`. Copy the source spec but strip schedules, drill policy and public exposure, and disable monitoring. Use a compact random `dr`-prefixed name and server-managed `ManagedBy{kind:backup-drill, cluster, id}` ownership; reject user-supplied ownership and preserve it on updates. Mark managed targets in the UI and exclude them from automatic agent rollout and drill scheduling.

Success requires both Ready cluster state and successful native restore inventory. RabbitMQ must confirm definitions import and explicitly report **definitions only, no messages**. Measure restore duration from restore start/completion when available, otherwise provisioning start to readiness; data age is drill start minus backup last-write time, falling back to completion time.

Persist the report **before** cleanup. Use normal deletion with `keepVolumes=false` and `keepBackups=false`, and record orphaned volumes. Never delete a target not marked for that exact drill ID. Emit `BackupDrillFailed` on failure/timeout and `BackupDrillSucceeded` on success; cleanup failure remains visible separately.

Source deletion retains active drill journals and cleanup bookkeeping until the owned target can be retired. Missing reports do not release managed-target mutation ownership.

### Metrics, dashboards and alerts

Expose control-plane state metrics on existing `/metrics` with unchanged authentication/filtering: cluster series require `monitoring:read` on that cluster; installation series require `settings:read`. Emit cluster state even when engine monitoring is disabled. The metric catalog is:

- `karakoram_cluster_info{provider,version,phase}`, `karakoram_cluster_ready`, `karakoram_cluster_degraded`, `karakoram_cluster_condition{type,status,reason}`;
- `karakoram_cluster_members_co_located`, `karakoram_member_agent_outdated{member}`, `karakoram_deployment_retries_exhausted`;
- `karakoram_backup_last_success_timestamp_seconds{schedule}`, `karakoram_backup_last_failure_timestamp_seconds`, `karakoram_certificate_expiry_timestamp_seconds{certificate}`;
- `karakoram_backup_drill_last_success_timestamp_seconds`, `karakoram_backup_drill_last_restore_duration_seconds`;
- `karakoram_control_plane_backup_last_success_timestamp_seconds`, `karakoram_notification_deliveries_failed_total{channel}`.

Ship overview, MongoDB, RabbitMQ, Elasticsearch, PostgreSQL and Supabase Grafana dashboard JSON under `deploy/grafana/dashboards/`, with datasource and namespace/cluster/member variables and reset-aware rates. Ship Prometheus alerts with runbook annotations in `deploy/prometheus/karakoram-rules.yaml`. Automated artifact checks validate JSON/YAML and require every referenced `karakoram_*` metric to exist in engine catalogs or the state metric list. Document installation and interpretation in the monitoring guide.

## Consequences

A state snapshot restores coordinator knowledge, not database data or external configuration. Operators must preserve a strong master key separately; the fingerprint detects a mismatched key but is **not protection for a weak key** and provides a key-guess verification signal. Old snapshots create infrastructure orphans rather than magically discovering post-snapshot ownership.

Drills consume real infrastructure and backup bandwidth. They use normal provider reconciliation, backend admission caps, ownership fences and bounded retries; they do not bypass capacity safeguards. Maintenance-window waits are excluded from execution budgets. A successful drill proves the recorded restore/check scope, not application correctness or a universal recovery-time guarantee.

Notifications can be duplicated, delayed or permanently failed after the retry horizon; recipients must deduplicate by event ID. Outbox and backup/drill journals add durable state. Metrics and dashboards improve visibility without making delivery, placement or recovery guarantees stronger than the underlying backend.

## Alternatives considered

- **Raw file copy of SQLite under WAL:** can omit committed WAL state or capture inconsistent files; use SQLite's snapshot primitive and integrity verification.
- **Export desired specs only:** loses PKI, sealed recovery material and operation history needed to rebuild trust and resume safely.
- **Put the master key in the snapshot:** defeats independent key custody and exposes every sealed secret with one object-store compromise.
- **Event-type-only notification filtering:** warning/normal severity cannot distinguish actionable categories, successful recovery or operation targets.
- **Best-effort in-memory delivery or exactly-once claims:** memory loses work on restart; remote acknowledgement races prevent exactly-once guarantees without recipient cooperation.
- **Gang/parallel drills without admission:** multiply platform load and recovery contention; bounded queued execution through normal admission preserves backend limits.
- **Treat backup completion as restore proof:** does not exercise provisioning, native restore, readiness or cleanup; definitions-only RabbitMQ recovery must remain explicit.
