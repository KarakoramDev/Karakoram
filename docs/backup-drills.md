<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backup drills

A successful backup is not proof that it can be restored. Backup drills exercise provisioning, native restore and readiness without modifying the source cluster. A successful drill proves the recorded checks, not application correctness or a guaranteed recovery time. Engine backups and [control-plane state backups](control-plane-backup.md) protect different assets.

## Lifecycle and safety

A manual or scheduled drill selects a successful eligible backup and creates a disposable target in the **source namespace**, named `dr` followed by ten lowercase letters/digits. The target copies the source spec and resource sizes, but strips backup schedules, drill policy and public exposure, and disables monitoring. It uses the normal validated cluster-create path and backend deploy admission, including per-project caps. Server-owned `managedBy` identifies its source and exact drill ID; users cannot supply this marker. Managed targets are excluded from drill scheduling and automatic agent rollout.

Reports progress through `Pending`, `Provisioning`, `Restoring`, `Verifying`, `CleaningUp`, then `Succeeded` or `Failed`. Verification requires both cluster Ready and successful native restore inventory (RabbitMQ confirms definitions import). The report is persisted before cleanup. Cleanup uses ordinary cluster deletion with `keepVolumes=false` and `keepBackups=false`; it never deletes a target without matching drill ownership. Inspect cleanup phase and orphaned-volume inventory even after verification succeeds. The last 50 reports per source are retained after target deletion.

Deleting the source does not delete its drill reports or active cleanup bookkeeping. The controller records source disappearance as a failure for unfinished verification and continues ownership-fenced target cleanup; a drill already cleaning up preserves its recorded outcome. Reports remain retained after source deletion, subject to the same 50 completed reports per-source limit. Managed targets with missing reports remain protected from user mutation because completion cannot be established.

Cleanup is bounded to three delete-admission attempts and 30 minutes; exhausting either bound records Failed cleanup and orphaned-volume hostnames for operator review.

**Resource and cost:** every drill creates a real cluster with the source's resources and uses backup bandwidth and storage. Allow spare quota/capacity and budget for its lifetime; admission may delay work. Installation-wide execution defaults to **one** drill; configure `KARAKORAM_BACKUP_DRILL_CONCURRENCY` on the control plane to an integer **1–4**. Excess drills remain queued.

Invalid concurrency values fail control-plane startup. Queued `Pending` time does not consume the drill's server-side timeout.

## Scheduled policy

Policy lives at `spec.backup.drill`:

```yaml
spec:
  backup:
    drill:
      enabled: true
      schedule: '0 4 * * 0'
      backupSchedule: nightly
      timeout: 6h
```

| Field | Rule |
| --- | --- |
| `enabled` | Enables automatic runs; omit the policy or set false to disable scheduling. Manual runs remain separate. |
| `schedule` | Five-field cron in **UTC**, required for enabled scheduling. |
| `backupSchedule` | Optional source backup-schedule name; only successful backups from that schedule are eligible. Omit for any eligible successful backup. |
| `timeout` | Go duration, default `6h`, inclusive range `30m`–`48h`; bounds server-side execution from provisioning start, excluding the pending queue. |

Configure an existing cluster with a merge patch (other backup policy fields remain unchanged):

```sh
karactl -n production clusters patch orders -f drill-policy.yaml
# Or use convenience flags:
karactl -n production clusters patch orders --drill-enabled \
  --drill-schedule '0 4 * * 0' --drill-backup-schedule nightly --drill-timeout 6h
```

The same four flags are available on cluster creation. Explicit flags update only their policy fields; `--drill-enabled=false` disables future scheduled drills. A request file remains authoritative over convenience flags. Disabling scheduling does not cancel accepted work.

## Manual runs and reports

```sh
karactl -n production backup-drills run orders
karactl -n production backup-drills run orders --backup nightly-20261021 --wait --timeout 6h
karactl -n production backup-drills list orders --limit 20
karactl -n production backup-drills get orders <drill-id>
karactl -n production backup-drills get orders <drill-id> --wait --timeout 6h
karactl -n production backup-drills get orders <drill-id> -o json
```

Without `--backup`, the server chooses the latest successful eligible backup. Run returns the accepted report immediately unless `--wait` is supplied. List supports `--limit` and `--cursor`. Get includes checks, limitations, error, cleanup phase and orphaned volumes; JSON/YAML retain all structured fields. `--wait` polls this report through terminal status, exits nonzero for Failed, and defaults to a six-hour client timeout. Timeout/interruption does not cancel server work: use the printed `get ... --wait` command without creating another drill. The client timeout is independent of policy timeout.

API equivalents are `POST /clusters/{cluster}/backup-drills` with optional `{ "backup": "name" }`, `GET /clusters/{cluster}/backup-drills`, and `GET /clusters/{cluster}/backup-drills/{drill}` under `/api/v1`, using the namespace query parameter. **RBAC:** run requires `restores:create` on the source cluster; list/get/poll require `restores:read`. Waiting therefore needs both permissions. Policy changes use normal cluster create/update authorization.

### Timing fields

- `createdAt`: report acceptance; `startedAt`: execution start after queuing.
- `readyAt`: restored target ready; `completedAt`: drill completion.
- `restoreDurationSeconds`: native restore `StartedAt` to `CompletedAt` when available, otherwise provisioning start to target readiness. It can include provisioning in the fallback case.
- `totalDurationSeconds`: overall recorded execution duration, distinct from restore duration.
- `dataAgeSeconds`: drill start minus backup `LastWriteTime`, falling back to backup completion. This is the age of the recovery data, not a promise about future recovery point objectives.

Missing timing fields mean the milestone or measurement is not yet available, not a zero-duration restore.

## Engine scope and limitations

| Engine | Drill scope |
| --- | --- |
| MongoDB / PSMDB | Restore the selected supported PBM backup and confirm native inventory and cluster readiness; not an application query/assertion suite. |
| RabbitMQ | **Definitions only, no messages.** Confirms definitions import; cannot prove recovery of queued messages. The report explicitly records `definitions only, no messages`. |
| Elasticsearch | Restore the selected native snapshot and confirm inventory/readiness; not an application indexing or query benchmark. |
| PostgreSQL | Restore the selected pgBackRest backup and confirm inventory/readiness; this is backup restore, not an arbitrary PITR exercise. |
| Supabase | PostgreSQL backup restore scope; not proof of recovery of every external integration or non-database asset. |

Drills explicitly reject Keycloak, OpenBao, Temporal, MySQL, PXC, Neo4j, Weaviate and unknown providers before provisioning. Their isolated clone/restore drill contracts are not implemented. In particular, a drill must not restore a binding-backed service's shared PostgreSQL source in place. A persisted unsupported drill records a clear failure without creating a target; ordinary engine backup and restore support does not imply drill support.

See [engine backup capabilities](backups.md#engine-recovery-capabilities) for backup types and recovery boundaries.

## Events and notifications

Success emits Normal `BackupDrillSucceeded`; failure or timeout emits Warning `BackupDrillFailed` on the source cluster. The failure notification category is **`backup-drill-failed`**. Configure [notification channels](notifications.md) with that category (or all categories), and inspect the durable report for checks and cleanup details. Cleanup/orphaned-volume state remains visible independently of verification results.
