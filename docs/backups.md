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

Backup payload transfer to S3 does not use the management relay; remote administration requests may. Control-plane storage verification does not prove member connectivity. Engine backups and [control-plane state backups](operations.md#control-plane-state-backup-and-recovery) protect different assets. Zerops live volume archives are not consistent engine backups.

## Register S3 storage

Create the bucket separately. In **Settings → Backup storages**, register an existing S3-compatible destination in the cluster's namespace. Cluster backup settings and schedules resolve storage names within that namespace.

| Request field | Value / rule |
| --- | --- |
| `name` | New registration: 2–63 lowercase letters/digits/hyphens, starting with a letter; trailing hyphen allowed |
| `type` | `s3` |
| `endpointURL` | Object Storage origin shown in Zerops, e.g. `https://storage-<region>.zerops.io` |
| `region` | Required S3 signing region |
| `bucket` | Existing bucket name |
| `prefix`, `description` | Optional |
| `forcePathStyle` | Defaults to `true` |
| `verifyTLS` | Defaults to `true`; keep enabled with HTTPS |
| `credentials.accessKeyId`, `credentials.secretAccessKey` | Required credential pair; bucket read/write/delete access |

Prepare an API-shaped YAML/JSON file using these fields; protect it with owner-only permissions and never commit or trace credentials.

```sh
karactl -n <namespace> storages create -f /private/path/storage.yaml
karactl -n <namespace> storages verify archive
karactl -n <namespace> storages get archive
karactl -n <namespace> storages set archive -f /private/path/storage-update.yaml
```

Set replaces the full configuration: include `type`, `endpointURL`, `region`, `bucket`, `forcePathStyle`, `verifyTLS` and any retained optional fields; omit `name`. Omit `credentials` entirely to retain the sealed pair, or supply both keys to rotate. Verify before revoking old credentials. Reads expose `credentialsConfigured`, never keys. Existing legacy names remain editable/readable/deletable.

Registration does not verify access. Verification performs temporary put/get/delete under the prefix; **HTTP 200 can contain `verified: false`**. The latest result/check time is persisted, not a history. Connection/credential edits clear it; description-only edits preserve it. UI **Options** describe transport, not successful **Verification**.

### Endpoint verification policy

Use an HTTP(S) origin: no credentials, non-root path, query, fragment or redirects. Public global-unicast destinations on 80/443 are permitted by default. For private S3 or another port, set `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` on the **control-plane service**, then restart/redeploy. Entries are comma-separated exact `host` or `host:port`; host-only permits 80/443, host:port only that port. Do not broadly allowlist project services. The built-in exception is exactly `storage-prg1.zerops.io` on 80/443, including its private resolution; other regional private endpoints require configuration.

Loopback, link-local/metadata, unspecified/non-global destinations and `100.64.0.0/10` remain blocked even when allowlisted. Every connection validates all DNS answers and dials a checked numeric address. Proxies and redirects are disabled; the probe deadline is 30 seconds. Keep HTTPS/certificate verification enabled; plaintext HTTP is not made safe by a successful probe. This policy also guards control-plane retained-object cleanup, not all engine egress.

| `failureReason` | Check |
| --- | --- |
| `access_denied` | Credentials, read/write/delete permissions, signing region |
| `bucket_not_found` | Bucket, regional endpoint |
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

## Schedules and count retention

Configure **Backups** policy or patch `spec.backup`: `storage` selects the primary destination; `schedules` entries contain `name`, `cron`, provider-supported `type`, optional override `storage`, `retainCount` (at least 1), and `enabled`.

```sh
karactl -n <namespace> clusters patch <cluster> -f backup-settings.yaml --wait
```

Cron uses five fields in **UTC**. Retention keeps the newest successful backups **of that schedule only**, not manual/failed/pending backups. Excess backups are deleted through durable operations; this is count retention, not age/GFS or a guaranteed recovery duration. A patch replaces the schedules array: include every entry to retain. Set `enabled: false` to stop new runs. The control plane runs scheduling/retention; downtime has no missed-run catch-up guarantee. Engine-specific continuity requirements remain in the linked recovery guides.

## Restore and clone

**Warning:** restore mutates target state and can affect application access. Quiesce writers, plan downtime and preserve a recovery copy before in-place recovery; exact overwrite/merge semantics depend on the engine.

1. Select a successful compatible backup, or a provider-supported observed recovery time. Provider must match; topology/version/identity checks are engine-specific. Providerless legacy records are not engine-neutral artifacts; see the engine table.
2. Restore into an existing target, or clone into an independent new cluster without stopping the source:

   ```sh
   karactl -n <namespace> restores create <cluster> recovery --backup beforechange --yes
   karactl -n <namespace> restores create <cluster> clone --backup beforechange --into-new <new-cluster>
   karactl -n <namespace> restores list <cluster>
   ```

3. Follow the operation and destination restore/cluster records; verify actual recovered data and application authentication before reopening traffic. Acceptance is not a recovery drill.

Explicit cloning uses a complete cluster-create request with immutable `spec.dataSource` containing `cluster` and `backup` (or supported `pitrTime`), plus optional `namespace`. After initial recovery, dataSource is provenance, not a continuing dependency.

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

[Cluster deletion](clusters.md#delete-or-suspend) preserves backups only with `--keep-backups`, independently of volumes. Retained `backups list/get/delete` use the original name/namespace and source `backups/read`/`delete`. Recovery after source deletion is provider-specific: use an explicit create file where supported, or restore into an existing target following the engine guide. `--into-new` needs a live source spec.
Retained listing returns `404` after the deleted source's final backup record is removed.

Preserve the state database, master key, storage configuration and credentials: S3 objects alone do not reconstruct lost control-plane state. Retained inventory recreates neither the source resource nor its volumes. Storage removal returns `409` while referenced by clusters, schedules or backup records; deleting its registration does not empty the bucket.

### When cluster deletion stays pending

A wait timeout does not undo deletion. Inspect the same namespace's cluster, operation and events; repair the reported token/relay, S3 credential/bucket or platform-process dependency. Do not erase state, recreate the name or delete volumes to hide pending cleanup. Observe the existing operation; retry only after failure/nonacceptance, keeping intentional retention choices. Keep storage and namespace management access until cleanup finishes. Check cluster absence (or `Suspended`), retained inventory and [orphaned volumes](operations.md#orphaned-platform-volumes) separately; orphan disks can still consume capacity and incur charges.
