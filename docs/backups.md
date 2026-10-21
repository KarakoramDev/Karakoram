<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backups and storage

Register S3 destinations, manage backup inventory and schedules, and recover into existing or new clusters.

## Engine recovery capabilities

| Engine | Backup types and scope | PITR | Backup data path |
| --- | --- | --- | --- |
| [MongoDB](mongodb/backups.md) | PBM `logical` (CLI default) / `physical`; incremental type not exposed; providerless legacy records belong here | Yes | Members write directly to S3 |
| [RabbitMQ](rabbitmq/backups.md) | `definitions` only; **no queued messages**; restore merges definitions | No | Control plane exports through Management API, then compresses/uploads to S3 |
| [Elasticsearch](elasticsearch/snapshots.md) | Native `snapshot` | No | Nodes write directly to S3 |
| [PostgreSQL / Supabase](postgres/backups.md) | pgBackRest `full`, `diff`, `incr`; PostgreSQL data only | When enabled | Current primary writes backups and WAL directly to S3 |
| [OpenBao](openbao/backups.md) | Native `raft-snapshot`, up to 4 GiB; requires source seal material for recovery | No | Control plane streams native HTTPS snapshot to S3; unavailable with PostgreSQL storage |
| [Temporal](temporal/operations.md#backups) | Source PostgreSQL `full`, `diff`, `incr` through the binding plus a manifest in Temporal cluster state | No Temporal timestamp restore | PostgreSQL source owns archives/retention; deleting a Temporal record does not delete source archives |

Backup payload transfer to S3 does not use the management relay; remote administration requests may. Control-plane storage verification does not prove member connectivity. Engine backups and [control-plane state backups](operations.md#control-plane-state-backup-and-recovery) protect different assets. Zerops live volume archives are not consistent engine backups.

## Register S3 storage

Create the bucket separately. In **Settings → Backup storages**, register an existing S3-compatible destination in the cluster's namespace. Cluster backup settings and schedules resolve storage names within that namespace.

| Request field | Value / rule |
| --- | --- |
| `name` | New registration: 2–63 lowercase letters/digits/hyphens, starting with a letter; trailing hyphen allowed |
| `type` | `s3` |
| `endpointURL` | HTTP(S) origin, e.g. `https://storage-<region>.zerops.io`; optional port and root `/` allowed, no endpoint path prefix |
| `region` | Required S3 signing region |
| `bucket` | Existing bucket name |
| `prefix`, `description` | Optional |
| `forcePathStyle` | Defaults to `true`; `false` prefers virtual hosting for DNS endpoints, with automatic path-style fallback for IP endpoints and dotted HTTPS buckets |
| `verifyTLS` | Defaults to `true`; keep enabled with HTTPS |
| `credentials.accessKeyId`, `credentials.secretAccessKey` | Required credential pair; bucket listing plus object read/write/delete access scoped to the configured prefix |

Storage prefixes ignore leading and trailing slashes (`/` alone means the bucket root), while new registrations reject empty inner components, `.`/`..` components and backslashes; existing bindings retain their engine's historical namespace mapping, shared by writers, inventory and cleanup.

Prepare an API-shaped YAML/JSON file using these fields; protect it with owner-only permissions and never commit or trace credentials.

```sh
karactl -n <namespace> storages create -f /private/path/storage.yaml
karactl -n <namespace> storages verify archive
karactl -n <namespace> storages get archive
karactl -n <namespace> storages set archive -f /private/path/storage-update.yaml
```

Set replaces the full configuration: include `type`, `endpointURL`, `region`, `bucket`, `forcePathStyle`, `verifyTLS` and any retained optional fields; omit `name`. Omit `credentials` entirely to retain the sealed pair, or supply both keys to rotate. Verify before revoking old credentials. Reads expose `credentialsConfigured`, never keys. Existing legacy names remain editable/readable/deletable.

Registration does not verify access. Verification performs temporary put/get/delete under the prefix; **HTTP 200 can contain `verified: false`**. The latest result/check time is persisted, not a history. Connection/credential edits clear it; description-only edits preserve it. UI **Options** describe transport, not successful **Verification**.
Verification status records only the last explicit `storages verify` result and the control-plane clock at that time; backups and restarts do not refresh it, so after correcting host clocks or credentials, run `storages verify` again.

Inventories and restore-absence checks also require complete `s3:ListBucket`
access for the configured prefix; a successful put/get/delete verification does
not establish listing permission. Preserve `s3:GetObject`/HEAD access to existing
metadata. Some S3 policies return HEAD403 rather than404 for a missing key when
listing is restricted. A403 alone never proves absence: restore fencing requires
an independently successful complete listing and all other native checks. Denied
or partial listings retain the restore holds; repair the scoped policy rather
than bypassing the fence.

Storage registration accepts DNS hosts, IPv4 literals and bracketed unscoped IPv6 literals, with an explicit `http://` or `https://` scheme and an optional port from 1–65535. Bare `host` / `host:port`, scoped IPv6, dotted IPv4-mapped IPv6 literals, URL credentials, query strings and fragments are rejected. For mapped IPv6, use hex-only notation or the equivalent IPv4 literal. Endpoint mount paths such as `/s3` are **not supported**: pgBackRest does not preserve the endpoint URL's path. Supply an origin-level S3 endpoint instead. `prefix` scopes **object keys**, not an HTTP reverse-proxy mount, so copying `/s3` into `prefix` does not repair a mounted S3 service.

With `forcePathStyle: true`, requests use `https://endpoint/bucket/key`. With `false`, ordinary DNS endpoints use `https://bucket.endpoint/key`, requiring bucket-subdomain DNS and a matching certificate. Literal IPv4/IPv6 endpoints always use path style; dotted bucket names over HTTPS also use path style to avoid wildcard-certificate mismatch. HTTP dotted buckets may use virtual hosting. The Go object client, PBM, pgBackRest and Supabase Storage share these rules; Elasticsearch intentionally always uses path style. TLS validation still requires a certificate matching the effective hostname (or literal IP), and HTTP remains plaintext.

Old registrations with unsupported endpoint shapes remain readable and deletable. Their get/list response exposes `verification.verified: false`, `failureReason: endpoint_not_allowed` and shape-specific remediation even if a historical check succeeded. This is a current configuration diagnosis, not a new network probe. `storages verify` returns and persists the same actionable failure without making a request; the CLI exits nonzero. Repair the origin with `storages set`, then verify again. No stored endpoint, bucket, object key or retained-backup location is silently rewritten.

### Endpoint verification policy

Use an HTTP(S) origin: no credentials, non-root path, query, fragment or redirects. Public global-unicast destinations on 80/443 are permitted by default. For private S3 or another port, set `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` on the **control-plane service**, then restart/redeploy. Entries are comma-separated exact `host` or `host:port`; host-only permits 80/443, host:port only that port. Do not broadly allowlist project services. The built-in exception is exactly `storage-prg1.zerops.io` on 80/443, including its private resolution; other regional private endpoints require configuration.

Loopback, link-local/metadata, unspecified/non-global destinations and `100.64.0.0/10` remain blocked even when allowlisted. Every connection validates all DNS answers and dials a checked numeric address. Proxies and redirects are disabled; the probe deadline is 30 seconds. Keep HTTPS/certificate verification enabled; plaintext HTTP is not made safe by a successful probe. This policy also guards control-plane retained-object cleanup, not all engine egress.

| `failureReason` | Check |
| --- | --- |
| `access_denied` | Credentials, read/write/delete permissions, signing region |
| `bucket_not_found` | Bucket, regional endpoint |
| `clock_skew` | Synchronize the control-plane host clock, then run `storages verify` again |
| `endpoint_not_allowed` | Origin, allowlist, DNS addresses, port, redirects |
| `tls_error` | Hostname/certificate chain; do not hide it by disabling verification |
| `unreachable`, `timeout` | Control-plane DNS/network/firewall or storage responsiveness |
| `unknown` | Configuration and storage-service diagnostics; do not publish credentials |

## On-demand backups and inventory

Register storage first, then use **Cluster → Backups → Backup now** or:

```sh
karactl -n <namespace> backups create <cluster> beforechange --type <type> --storage archive
karactl -n <namespace> backups list <cluster>
karactl -n <namespace> backups get <cluster> beforechange
karactl operations wait <operation-id>
```

Select an explicit type from the engine table. Acceptance is not completion: follow **View operation**, then inspect backup phase (`Pending`, `Running`, `Succeeded`, `Failed`), timestamps, storage, type, native location and errors. Only `Succeeded` backups are recovery bases. Inventory distinguishes manual/scheduled/recovery-triggered work. A manual request needs a formed, active cluster with its current generation observed; unconverged/suspended/deleting clusters return `409`. See [operation and health semantics](clusters.md#operations-and-status).

### Protect backups used by restores and clones

An accepted restore or create-from-backup reserves its source backup while the
operation is in flight, including the clone's initial member-provisioning phase.
Deleting that backup (live or retained) returns `409 invalid_state` naming the
dependent operation and target cluster. PostgreSQL expiration also protects
referenced differential/incremental descendants of the requested set.
Deleting the source cluster without retaining backups likewise returns `409`;
wait for the dependent operation to complete or cancel it safely, or delete the
source with `keepBackups=true` (`karactl clusters delete --keep-backups`).
Retention sweeps cannot bypass this fence.

PITR restores/clones select their base set during execution. Until the dependent
operation is terminal, Karakoram conservatively refuses deletion of any backup
on that source and refuses source-cluster deletion even with `keepBackups=true`:
retaining objects alone does not preserve the live source required by PITR.
If source deletion or backup expiration is accepted first, PITR admission is
refused until that destructive intent finishes and an available recovery point
can be selected. Likewise, a named PostgreSQL child restore/clone is refused
while any of its full/differential ancestors is already Deleting.

Admission and deletion intent are serialized: if deletion was accepted first,
a new restore/create referencing that backup is refused instead. Retained,
successful backups remain usable when the source was deleted with backups kept.

If inventory or the stored snapshot is authoritatively absent (for example,
objects were removed outside Karakoram), the restore/create operation reaches
`Failed` with source-backup guidance rather than staying `Restoring`. Inspect
the operation and restore error, select another verified successful backup, and
retry the restore or create a new target. Storage authorization, transport,
timeout and service errors are not proof of absence and retain existing bounded
retry/compensation behavior; do not delete restore holds or receipts manually.

## Schedules and count retention

Configure **Backups** policy or patch `spec.backup`: `storage` selects the primary destination; `schedules` entries contain `name`, `cron`, provider-supported `type`, optional override `storage`, `retainCount` (at least 1), and `enabled`.

```sh
karactl -n <namespace> clusters patch <cluster> -f backup-settings.yaml --wait
```

Cron uses five fields in **UTC**. Retention keeps the newest successful backups **of that schedule only**, not manual/failed/pending backups. Excess backups are deleted through durable operations; this is count retention, not age/GFS or a guaranteed recovery duration. A patch replaces the schedules array: include every entry to retain. Set `enabled: false` to stop new runs.

Each retention sweep is fenced to the cluster incarnation and policy generation
it inspected. A policy edit or source-cluster replacement abandons stale deletion
candidates; a later sweep evaluates the current policy.

Unchanged enabled schedules retain a durable last-admitted occurrence. A failed
enqueue does not consume that occurrence: the scheduler retries after storage
recovers, including across control-plane restarts. Backup inventory, operation
and cursor advancement commit atomically. Catch-up admits at most one overdue
occurrence per schedule per reload (normally every 30 seconds) or cron callback,
so recovery can queue delayed backups rather than running them at their original
times. A new or edited schedule starts from its first successful observation;
there is no retroactive guarantee for time before that cursor exists. Disabling
and re-enabling a schedule starts a new cursor epoch once the disabled policy has
been observed. Backward clock corrections do not repeat admitted occurrences.
Engine-specific continuity requirements remain in the linked recovery guides.

## Restore and clone

**Warning:** restore mutates target state and can affect application access. Quiesce writers, plan downtime and preserve a recovery copy before in-place recovery; exact overwrite/merge semantics depend on the engine.

While an in-place restore runs, cluster `phase` is `Restoring` and the `Ready`
condition is `False` with reason `Restoring`, even if a native health probe can
still reach the old database processes. This applies to every engine, including
definition-merge restores. Availability observations continue but cannot certify
restore completion. A terminal restore operation leaves `Ready=False` with reason
`RestoreFinished`; ordinary reconciliation must then observe database health
before publishing `phase=Ready` and `Ready=True`. A successful operation alone is
not an application-availability guarantee.

1. Select a successful compatible backup, or a provider-supported observed recovery time. Provider must match; topology/version/identity checks are engine-specific. Providerless legacy records are not engine-neutral artifacts; see the engine table.
2. Restore into an existing target, or clone into an independent new cluster without stopping the source:

   ```sh
   karactl -n <namespace> restores create <cluster> recovery --backup beforechange --yes
   karactl -n <namespace> restores create <cluster> clone --backup beforechange --into-new <new-cluster>
   karactl -n <namespace> restores list <cluster>
   ```

3. Follow the operation and destination restore/cluster records; verify actual recovered data and application authentication before reopening traffic. Acceptance is not a recovery drill.

Explicit cloning uses a complete cluster-create request with immutable `spec.dataSource` containing `cluster` and `backup` (or supported `pitrTime`), plus optional `namespace`. After initial recovery, dataSource is provenance, not a continuing dependency.

The initial data source remains a durable requirement across control-plane
restarts and desired-generation changes. `Ready` requires positive native
restore-completion evidence, not merely healthy database processes. An aborted
initial restore leaves the target `Failed` with `RestoreIncomplete`; it never
silently falls back to an empty database. Delete the target and create a new one
from the preserved source to abandon that failed attempt.
Failure withdraws readiness, not ownership of safe compensation: already-running
native cleanup and any retained frozen PostgreSQL recovery journal continue to
release their process/maintenance/router holds. Their failure observation cannot
block that recovery, authorize a fresh restore, or turn the failed target Ready.

A create operation can also fail only its observation deadline/retry budget while
an already-claimed native restore continues without abort or teardown. That is
not a restore abort: positive native completion can make the target Ready, with
an `InitialRestoreCompletedAfterTrackerFailure` audit event; the original create
operation remains Failed. By contrast, explicit cancellation, native failure or
partial/cancelled outcome, compensation, teardown, or failure before native claim
durably aborts the initial restore. Such a target stays Failed/non-Ready even if
a late native success appears. This evidence survives health refresh and
suspension independently of the native success marker.
The native claim is the first durable, incarnation-scoped bootstrap/import
receipt, not the later creation of a bookkeeping Restore operation. Its original
time is preserved across retries and restart. PostgreSQL/Supabase can therefore
finish an already-claimed bootstrap if the create tracker expires before DATA
members join and the initial Restore record is enqueued. This continuation
cannot change the source or reopen conflicting edit admission.

Retry diagnostics on a Running restore (for example a polling connection error)
are not terminal failure evidence. They remain retryable; only actual terminal
failure/cancellation or native compensation evidence records an abort, and
positive completion clears the stale retry diagnostic.

Before native recovery is claimed, placement-only corrections (component
replicas/resources) amend the **same create operation**, preserving the recovery
source and its audit history. This permits correcting an unplaceable proxy
without cancelling recovery. Source/target point, version, credentials, topology
kind and other changes are refused with `409 invalid_state`. Once native recovery
has been claimed, conflicting edits/actions are refused with `409` naming the
owning operation until recovery completes. Target deletion remains allowed.
Source backup deletion stays fenced until positive initial restore completion,
including after a failed or cancelled create; deleting the target releases that
dependency.

PostgreSQL and Supabase initial recovery joins and recovers DATA members without
waiting for proxy deployment/placement. An unplaceable proxy therefore does not
keep the recovery mutation fence closed indefinitely: after native recovery
completes, an ordinary proxy placement/replica PATCH is admitted even while the
create tracker is still waiting. Desired proxies remain an independent `Ready`
requirement; successful data recovery alone does not make an unplaced proxy
healthy. Supabase optional-service placement likewise does not gate core recovery.

Upgrade behavior: successful pre-marker PostgreSQL/Supabase and MongoDB clones
are migrated only from matching completed native restore receipts; an old
`Cancelled` create tracker caused by PATCH is not itself a failed restore.
Matching native work already in flight is allowed to finish. Elasticsearch's
existing completed marker and RabbitMQ's successful definitions-import marker
remain valid; an Elasticsearch accepted-only native submission is not promoted
to success.

A legacy superseded create with no restore record or completion marker is
different: the target may already contain user writes. It reports
`Ready=False / LegacyRestoreUnproven`, naming the old create operation and the
delete-and-recreate escape, without replaying initial restoration or authorizing
empty initialization. Normal runtime self-healing, managed credentials, own
backup scheduling and data-plane service/proxy exposure continue; withdrawing
Ready is not a request to stop or unregister the database. Its initial-source
obligation remains unproven and source deletion remains fenced. To recover the
requested source data, delete that target and create a new target from the
preserved source; do not discard its live data or force an implicit replay.

Own backup operations remain queueable because they do not supersede the restore
owner and share the serialized native worker; this also permits the required
post-restore fresh-base backup without reopening conflicting mutation admission.
OpenBao foreign/deleted-source recovery additionally needs the source's unseal/recovery material. The top-level write-only `openbaoUnsealShares` field is accepted in restore and cluster-create requests, not persisted in desired spec. See [Raft restore, clone and supplied-share examples](openbao/backups.md); a snapshot alone cannot recover a sealed server.

### Cross-namespace recovery

Global `-n` selects the **target**; `--source-namespace` selects the source and defaults to the target when omitted:

```sh
karactl -n dev restores create orderscopy recovery \
  --source-namespace prod --source-cluster orders --backup baseline --yes
```

New-cluster requests use `spec.dataSource.namespace`; in-place REST requests use `sourceNamespace`. Recovery reads the source backup's recorded storage; ensure the applicable data path can reach it. Target backup settings must use target-local storage. The live-source `--into-new` convenience clone omits copied backup settings across namespaces; configure them afterward.

### PostgreSQL and Supabase recovery

See the focused [PostgreSQL recovery guide](postgres/backups.md) and [Supabase recovery boundaries](supabase/operations.md#recovery-boundaries). The shared rules below apply to both providers.

Use an explicit `--type full`, `--type diff` or `--type incr`; the generic CLI
default `logical` belongs to MongoDB. A differential requires an existing full;
an incremental requires an existing consistent backup chain. The registered
storage's endpoint scheme, path-style addressing and TLS verification policy
apply on every potential primary. Repository paths are isolated by namespace and
cluster incarnation: `<storage-prefix>/postgres/<namespace>/<cluster>-<uid>`,
with stanza `kk`. Keep storage credentials and the control-plane master key.

pgBackRest automatic expiration is disabled. The Karakoram scheduler controls
retention through `delete-backup` operations, but defers the newest full until a
newer full exists and preserves ancestors of retained sets across schedules and
manual requests. Skipped sets are reconsidered on later passes; actual counts
can exceed the configured limit while dependencies require them.
Explicitly deleting a full **also deletes its dependent differential and
incremental sets**, including records from other schedules or manual requests;
deleting a differential also deletes incrementals that reference it. The newest
full cannot be deleted while the cluster is live. Rejected or failed expiration
fails its operation and releases the queue for subsequent backups.
WAL preceding the oldest remaining full chain is pruned by expiration.
After source deletion, retained-backup deletion
uses the recorded repository and native backup dependency metadata, rewrites
checksummed pgBackRest inventory, and removes the whole isolated repository when
its last backup is deleted.

Enable `spec.backup.pitr.enabled` with a default storage. `oplogSpanMinutes`
controls PostgreSQL `archive_timeout` (one minute when omitted); this bounds WAL
switching during low activity, not an exact commit-time recovery guarantee. The
reported conservative interval starts at the oldest successful consistent
backup's stop time and ends at the current primary's last successful archive
completion time. No interval is advertised when PITR is disabled or archiving
is failing. Archive completion is not a precise last-commit timestamp; missing
objects or externally modified repositories can still make recovery fail.

In-place restore is destructive and offline. Single-member recovery stops
PostgreSQL, performs a delta restore, starts the server and waits for promotion.
HA recovery fences and stops every Patroni/PostgreSQL member, resets only that
cluster's DCS scope, restores a seed through Patroni custom bootstrap, and
rebuilds the remaining members before completing. Recovery continues archiving
to the target's stanza on a new timeline. When restoring from a different source
repository, archiving remains disabled until the target repository configuration
is published and the bootstrap archive override is removed on every member.
For a different-source restore into an existing target repository, stanza-upgrade
appends the restored system-ID history while retaining the target's prior backup
sets; it does not overwrite them with the source repository's inventory.
When PITR is enabled a new full backup is queued afterward; recovery is not
protected by that new base until it succeeds.
Recovery adopts the backup's sealed system-role credentials after all existing
members are stopped, before restarting the restored seed. This also applies to
same-cluster backups taken before a system-password rotation; reveal connection
credentials again after recovery rather than reusing an earlier password.

A clone's `dataSource` may name a backup or a PITR time in the same or another
namespace. Source and target providers must match **exactly**:
`postgres` cannot consume a `supabase` backup or vice versa. PostgreSQL major
versions must match; source and destination topologies may differ. Bootstrap
reads the source repository with archiving disabled, then switches to the
target's own repository and queues a full backup. Target-local backup storage
must be configured in the clone request to establish that independent recovery
history. Retained backup clones
use the snapshot's sealed system credentials, not a deleted source's live
credential scope. Supabase backups protect PostgreSQL data, not objects stored
by its separate Storage service.


## Permissions

| Action | Required resource/action on namespace-qualified object |
| --- | --- |
| Register/read/edit or verify/delete storage | `backup-storages/create`, `read`, `update`, `delete` respectively |
| Create/read/delete backups | `backups/create`, `read`, `delete` on source cluster |
| Save backup policy | `clusters/update` |
| Restore into existing / clone into new | Target `restores/create` / `clusters/create`, plus source `backups/read` |
| Cross-namespace recovery | Additionally source `backups/export`; export alone is insufficient |

## Delete or preserve recovery objects

Backup deletion is destructive; live-cluster cleanup is asynchronous and provider-specific. Active matching work can block deletion until cancelled/terminal. Use `backups delete <cluster> <backup> --yes` and follow its outcome. See engine guides for shared-object and continuity rules, especially [MongoDB backup-data cleanup](mongodb/backups.md).

If a backup-deletion operation fails, expires, or is cancelled before completion, its inventory leaves `Deleting` for `Failed` with the cleanup error and its original remote identity preserved. It is not a usable restore base. Repair the reported dependency and submit deletion again; the new operation safely retries cleanup of the same target instead of leaving an unretryable `Deleting` record. Startup also repairs historical inventory stranded by terminal deletion operations.

Suspended clusters still process backup-deletion operations without resuming ordinary runtime reconciliation. Object-store cleanup can therefore finish while runtimes remain absent. A provider whose native cleanup requires a running member can report that dependency failure; preserve the recorded target, resume/repair the dependency where needed, and retry deletion rather than removing inventory by hand.

Namespace/backend failures during suspended cleanup are reported in conditions
and messages without clearing `Suspended`; only an explicit resume or deletion
changes that lifecycle intent. After source-cluster deletion, failed backups
with a recorded remote location or PostgreSQL repository metadata retry through
direct retained-object cleanup, never through an operation stranded on the
absent cluster. Inventory is removed only after remote cleanup succeeds.

[Cluster deletion](clusters.md#delete-or-suspend) intentionally preserves backups with `--keep-backups`, independently of volumes. MongoDB, Elasticsearch, RabbitMQ and PostgreSQL/Supabase retire runtime writers and delete (or intentionally retain) volumes before control-plane artifact cleanup. Elasticsearch, RabbitMQ and PostgreSQL/Supabase retry artifact access failures within a durable two-minute cleanup window; restarts do not reset that window. If exact owned cleanup cannot finish safely, cluster removal completes with affected inventory retained and a `BackupArtifactsRetained` warning (`SnapshotsRetained` for Elasticsearch). Successful destinations are checkpointed and are not deleted again after restart. Read-only source repositories and other incarnations remain untouched. Dependency admission still returns `409` before destructive intent when another operation needs the backups. Inspect retained-artifact warnings and exact-prefix removal guidance rather than treating terminal cluster success as proof of empty storage. Retained `backups list/get/delete` use the original name/namespace and source `backups/read`/`delete`. Recovery after source deletion is provider-specific: use an explicit create file where supported, or restore into an existing target following the engine guide. `--into-new` needs a live source spec.
Retained listing returns `404` after the deleted source's final backup record is removed.

Preserve the state database, master key, storage configuration and credentials: S3 objects alone do not reconstruct lost control-plane state. Retained inventory recreates neither the source resource nor its volumes. Storage removal returns `409` while referenced by clusters, schedules or backup records; deleting its registration does not empty the bucket.

### When cluster deletion stays pending

A wait timeout does not undo deletion. Inspect the same namespace's cluster, operation and events; repair the reported token/relay, S3 credential/bucket or platform-process dependency. Do not erase state, recreate the name or delete volumes to hide pending cleanup. Observe the existing operation; retry only after failure/nonacceptance, keeping intentional retention choices. Keep storage and namespace management access until cleanup finishes. Check cluster absence (or `Suspended`), retained inventory and [orphaned volumes](operations.md#orphaned-platform-volumes) separately; orphan disks can still consume capacity and incur charges.
