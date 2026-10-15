<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backups, PITR, restore and clone

Backup types and recovery capabilities come from the cluster's provider catalog (`backupTypes`). The MongoDB provider uses PBM for consistent replica-set and sharded recovery; the MongoDB/PBM workflows below apply to that provider. Zerops Local Storage live archives are **not** consistent database backups. Engine backup objects are independent of [control-plane state backups](../operations.md#control-plane-state-backup-and-recovery).

## Register S3 storage

Create the bucket separately: Karakoram's BackupStorage resource describes an existing S3-compatible destination. Zerops Object Storage and other S3-compatible services are supported. In **Settings → Backup storages**, supply name, endpoint URL, signing region, bucket, optional prefix and access credentials. Path-style addressing and TLS verification default to true. Do not disable TLS verification to hide an incorrect endpoint or certificate chain.

Storages belong to a [namespace](namespaces.md): select it in the UI or use
`karactl -n dev storages create -f FILE`. Names are unique per namespace; cluster
backup settings and schedules resolve storage names in their own namespace.
Verification runs from the control plane's network, while backup payloads go
directly between members and S3, not through the relay.

New storage names must contain 2–63 lowercase letters, digits or hyphens and
start with a letter. A trailing hyphen is allowed. Violations return **400**
naming `name` and the rule. Existing stored names remain readable, editable and
deletable. Missing configuration or credential fields return **400** with the
specific field; an existing storage name returns **409** `already_exists`.

For the CLI, prepare a protected API-shaped YAML/JSON request and pass it with `-f`:

```yaml
name: archive
type: s3
endpointURL: https://storage-prg1.zerops.io
region: us-east-1
bucket: your-existing-bucket
prefix: production
forcePathStyle: true
verifyTLS: true
credentials:
  accessKeyId: YOUR_S3_ACCESS_KEY
  secretAccessKey: YOUR_S3_SECRET_KEY
```

These are placeholders, not usable credentials. Supply real values through a secret manager/private file, never a committed example or shell trace.

```sh
karactl storages create -f /private/path/storage.yaml
karactl storages verify archive
karactl storages get archive
```

Verification performs a temporary put/get/delete under the prefix; read-only S3 credentials cannot pass or support normal recovery-object lifecycle. Storage reads return metadata and `credentialsConfigured`, never keys. The effective PBM prefix adds a stable per-cluster UID namespace, avoiding collisions among clusters sharing the bucket. Preserve storage configuration/credentials and control-plane state (with its encryption key) as part of recovery.

### Edit storage and replace credentials

Use **Settings → Backup storages → Edit**, or `karactl storages set archive -f storage-update.yaml`. The update file contains the full configuration (`type`, `endpointURL`, `region`, `bucket`, `forcePathStyle`, `verifyTLS`, plus optional prefix/description); omit `name`. Omit the entire `credentials` object to retain the existing encrypted pair. To rotate credentials, include both `credentials.accessKeyId` and `credentials.secretAccessKey`, using a protected file as for creation. Verify after every edit or rotation before revoking the old keys.

Registration saves configuration, not proof of S3 access. A verification failure still returns **HTTP 200**, with `verified: false`, `failureReason` and a secret-free message; success omits `failureReason`. The probe checks put/get/delete from the control plane, not connectivity from every database member.

The UI separates **Options** (HTTP without TLS, or HTTPS with certificate
verification on/off) from **Verification** (not checked, checking, successful or
failed). The API's optional `verification` field stores the latest result and
check time, not a history. Changing credentials, endpoint, region, bucket,
prefix, addressing style or TLS settings clears that result immediately;
description-only edits preserve it. Run Verify again after a connection change.
HTTPS/certificate verification settings alone do not prove credentials or
bucket access, and a successful probe does not make plaintext HTTP safe.

| `failureReason` | What to check |
| --- | --- |
| `access_denied` | Access key/secret pair and bucket read, write and delete permissions; expired credentials or a signature/region mismatch |
| `bucket_not_found` | Existing bucket name, regional endpoint and signing region |
| `endpoint_not_allowed` | HTTP(S) origin only; no credentials, non-root path, query, fragment or redirects. Loopback/metadata destinations are forbidden; private addresses and nonstandard ports need the operator allowlist below |
| `tls_error` | Endpoint hostname, certificate chain and TLS handshake; repair certificates rather than disabling verification |
| `unreachable` | DNS, endpoint port, network routes and firewall from the control plane |
| `timeout` | Endpoint responsiveness and network connectivity; retry after the service is reachable |
| `unknown` | Endpoint/region/bucket configuration and read/write/delete access; inspect storage-service diagnostics without publishing credentials |

### Endpoint verification policy

The verification endpoint must be an HTTP(S) origin without embedded credentials, a non-root path, query or fragment. Public global-unicast destinations on **80/443** are permitted by default. For a private S3 server or nonstandard port, configure the **control-plane service** environment `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` with comma-separated exact `host` or `host:port` entries, then restart/redeploy that service. Host-only entries allow ports 80/443; host:port allows that exact port. `storage-prg1.zerops.io` on 80/443 is already trusted for Zerops storage's private addresses.

Loopback, link-local/metadata-style addresses, unspecified/non-global addresses and shared range 100.64.0.0/10 stay blocked even when allowlisted. The verifier checks all DNS answers on every connection, disables proxies, rejects redirects and has a thirty-second deadline. Use the actual regional S3 origin, not a redirecting website URL; keep HTTPS/TLS verification enabled. This is the verifier's policy, not a guarantee that PBM uses the same outbound sandbox. See [security](../security.md#s3-verifier-outbound-policy) and [installation settings](../install.md#required-secrets-and-generated-settings).

## On-demand backups and inventory

Inventory `trigger` distinguishes `manual`, `schedule` and `restore` snapshots.
`restore` identifies the automatic post-restore base backup used to resume PITR.

```sh
karactl backups create demo beforechange --type logical --storage archive
karactl backups create demo physicalbase --type physical --storage archive
karactl backups list demo
karactl backups get demo beforechange
karactl operations cluster demo
```

Both types execute on nodes via PBM; physical snapshots require PSMDB. A create request returns acceptance, not completion. Watch Backup phase (`Pending`, `Running`, `Succeeded`, `Failed`) and the matching Operation. The inventory carries PBM name, storage, type, timestamps, last-write time, size and error. Only a successful backup is a recovery base.

Backup and restore responses always identify their operation as `operationId`, also returned in `Karakoram-Operation-Id`; use `karactl operations wait ID`. Inspect their own records as well as the operation and verify actual recovered data in a drill. Do not treat an HTTP 2xx or PBM command initiation as a successful recovery test.

Accepted backup/restore actions in the UI include **View operation** links in
their success toasts. Follow the linked [operation detail page](web-ui.md#operation-details-and-cancellation),
then inspect the inventory's final outcome. For catalog upgrades marked
`requiresBackup`, only a Succeeded backup completed within 24 hours and no
earlier than the last known convergence to the current version qualifies;
see [the server-enforced gate](scaling-and-upgrades.md#required-pre-upgrade-backups).

Manual backup requests require a formed, active cluster with its current generation observed. Creating, suspended, deleting or otherwise unconverged clusters return **409** naming the phase and, when applicable, the pending and observed generations instead of leaving a Pending record. A cluster may still report Ready while a desired change is pending; wait for `observedGeneration` to reach `generation` before requesting a backup. If an operation fails, expires, is cancelled or is retired by cluster deletion, its unfinished backup inventory becomes Failed with the same message. Failed records without a started PBM snapshot, and never-started records without active work, can be deleted immediately and their name reused; snapshots that did start still need asynchronous backend cleanup. Backups have a 20-consecutive-failure/two-hour ordinary-work limit; restores have a six-hour deadline. Successful provider steps reset the failure count. Expired/exhausted Running work first completes compensating cleanup, retrying with backoff beyond that limit, before the operation becomes Failed with the original reason. Restore cleanup restarts database/PBM processes, clears maintenance/router holds and restores target configuration; this is recovery, not rollback of restored data. See the [API async contract](api.md#declarative-updates-and-asynchronous-work) for configuration and safe cancellation checkpoints.

Failure compensation has its own six-hour window from entry into recovery,
including observation of an already-running PBM restore. If it expires, the
operation releases the queue as Failed with a Warning naming the incomplete
compensation step and requiring manual recovery. This does not prove PBM is
terminal or undo data mutation. Repair and verify PBM/process state and any
maintenance/router holds before reopening traffic or starting dependent work.

After full cluster removal, deleting a retained Failed/never-started record without a PBM snapshot is synchronous (**204**) and does not require reachable storage. An active matching operation still blocks deletion until it is cancelled or terminal. Removal also deletes the record's protected recovery credential. Cleanup of a cluster that never configured PBM removes never-started owned records without waiting for nonexistent database agents; ambiguous durable start receipts remain subject to remote snapshot cleanup instead.

For applications outside this repository, see the [Go client installation and usage walkthrough](api.md#clientssdk), including bearer authentication, structured errors and operation polling. The public client exposes backup, PITR-window, restore and clone request methods from the same OpenAPI contract; after polling, inspect the recovery resource and validate the recovered data as described above.

Missing storage dependencies return **404** with `details.resource: backupStorage` and the requested name. A duplicate backup name returns **409** `already_exists`; state conflicts identify the backup and its current state. Unsupported `type` returns **400** naming the field and listing the provider's supported types; do not assume every engine supports logical/physical backups. A PITR-window request for an engine without PITR returns an explicit unsupported-capability error naming that engine.

Active work is matched to the operation's recorded backup target. When it blocks deletion, the **409** error retains `details.resource: backup`, the backup name and its current `details.state`, and directs you to cancel the operation first.

## Schedules and count retention

Without cluster-update permission, the UI's backup policy is read-only, even
when you can read backup inventory. Saving schedules, storage or PITR settings
requires that permission; backup creation/deletion have their own permissions.

Cluster `spec.backup.storage` is the primary destination. Schedules may select another registered storage. Configure them in the UI or submit a cluster merge patch:

```yaml
spec:
  backup:
    storage: archive
    schedules:
      - name: nightly
        cron: "0 2 * * *"
        type: logical
        storage: archive
        retainCount: 7
        enabled: true
    pitr:
      enabled: false
      oplogSpanMinutes: 10
```

```sh
karactl clusters patch demo -f backup-settings.yaml --wait
```

Schedules use five-field cron **in UTC**; `retainCount` must be at least one. Retention counts successful backups of that schedule, not failed/pending attempts or manual backups. Excess snapshots are removed through durable PBM deletion operations. It is **not** age-based retention, GFS, or a guaranteed PITR duration. Deleting base snapshots or oplog history can shorten recoverable ranges; choose count/quota with your recovery objective and inspect the observed window after retention.

A merge patch replaces arrays in full: include every schedule you want to retain when changing `schedules`. Set `enabled: false` to stop a schedule. The control plane owns cron; no missed-run catch-up guarantee is provided while it is unavailable. Already running PBM jobs are node-side.

The CLI also offers create-time convenience syntax, for example `--backup-storage archive --schedule 'nightly=0 2 * * *,type=logical,retain=7,storage=archive'`. Use API-shaped files for subsequent schedule edits.

## PITR

PITR records oplog slices into the cluster's primary backup destination. It needs a successful base snapshot **and continuous PBM-observed history**, not merely `pitr.enabled: true`.

1. Configure a primary storage and take a successful base backup.
2. Set `spec.backup.pitr.enabled: true` and `oplogSpanMinutes` (integer minutes, minimum one; default ten).
3. Inspect the observed restorable range:

   ```sh
   karactl backups pitr-window demo
   ```

4. Select a UTC RFC3339 time **inside** the advertised continuous window. Do not assume that every timestamp between cluster creation and now is recoverable.

The API validates `source.pitrTime` **before accepting** a restore. A future time, a time before/after the observed coverage, or a gap between continuous windows returns HTTP **400** with `details.field: source.pitrTime`, `reason: out_of_range`, and UTC `earliest`/`latest` bounds for the current coverage. Those bounds do not imply that intervening gaps are recoverable. A source that is initializing or has no coverage returns **409** with reason `invalid_state` and explains the base-snapshot/oplog prerequisite; the PITR-window endpoint also reports initialization as a state conflict, not an internal error. Refresh the window and select a covered time once prerequisites complete. Coverage is checked again during execution; a permanently invalid target fails its Restore and Operation with a reason rather than retrying forever.

Once a replica set is ready and PBM storage is configured, scaling, binary upgrades, resource changes, member joins and reloads do not hide established coverage merely because the cluster is converging. The PITR-window endpoint and restore preflight continue to query PBM; actual agent/storage failures or missing history still affect availability.

Enabling PITR alone does not create proof of a usable window. Gaps, quota exhaustion, failed slicing, base deletion or storage changes can remove recovery coverage. After restore Karakoram holds PITR disabled until a fresh snapshot on primary storage succeeds. Its automatic post-restore base backup cannot be cancelled because it protects PITR continuity. If that backup fails, is cancelled by older software, or its record disappears, Karakoram queues at most three replacement attempts and emits warning events without blocking cluster convergence. If those attempts are exhausted, take a successful manual backup on primary storage; a snapshot queued after the restore also satisfies the fresh-base requirement. Check the backup and new timeline before trusting recovery coverage again.

## In-place restore

**Stop/quiesce application writers and plan downtime.** A restore overwrites data and may restore application users/roles. Keep a recovery copy of the current state if you may need to return to it.

```sh
karactl restores create demo recoverbase --backup beforechange
# Or select a timestamp observed in this cluster's PITR window:
karactl restores create demo recovertime --time 2025-01-15T12:00:00Z
karactl restores list demo
karactl restores get demo recoverbase
karactl operations cluster demo
```

The timestamp is illustrative; replace it with a real point in your reported window. Exactly one of backup or time is required. Restore requests accept a source cluster, with the target used by default. Use matching topology/engine major and a successful snapshot; physical restores especially depend on binary/data compatibility.

An in-place **logical restore is not a full rollback**: PBM drops/replaces the
namespaces contained in the backup, but leaves databases and collections that
did not exist in that backup untouched, including their documents. On sharded
clusters PBM drops every database present in the backup before restoring it, so
collections added later to those databases are removed as well; only databases
absent from the backup are preserved there. To obtain an
exact copy of the backed-up data, stop writes and drop those extra databases or
collections before restoring, or use a physical backup/restore (which replaces
the data directory) or restore into a new cluster. See
[PBM logical backups and restores](https://docs.percona.com/percona-backup-mongodb/latest/features/logical.html).

REST uses `source.backup` or `source.pitrTime`: set exactly one. Both selectors or neither return **400**, `details.field: source`, reason `conflict`. A nonexistent backup returns **404** with its name and source cluster/namespace; the same dependency context is returned for clone creation. Duplicate restore names return **409** `already_exists`. These errors are synchronous: no restore operation is accepted.

Karakoram serializes disruptive cluster work. It disables PITR and, for sharding, coordinates balancer/router holds. Physical restore suppresses automatic mongod/PBM restart on all relevant nodes. Mongod may be offline and PBM may exit successfully as part of recovery; progress is read with PBM storage configuration while MongoDB is unavailable. Karakoram then starts mongod/PBM, releases maintenance, restores system users, resyncs target storage and restores router/balancer state. Logical restores do not use the offline physical lifecycle.

While an in-place restore is Running, cluster phase is **Restoring**, with the operation checkpoint exposed in its message. Successful completion transitions to health observation; terminal failure exposes Degraded with the error. Cancellation is cooperative only before data mutation (including waiting for PBM source inventory); already-started restore is not an undoable operation. Wait for cancellation cleanup and inspect member/agent health before reopening traffic.

If the operation exhausts its ordinary work limit after PBM starts, recovery
continues to observe PBM until a terminal status before starting database
processes. A PBM `done` outcome remains a Succeeded restore even if recovery
exceeded that limit; the operation records the limit failure separately. The
fresh-base backup is still enqueued and PITR remains gated until it succeeds.

After completion verify every member/agent, application authentication, collection data, balancer state, backup inventory and the fresh post-restore base. Do not reopen writers solely because PBM reported `done`. See [physical-restore troubleshooting](../operations.md#troubleshooting-by-symptom).

## Clone into a new cluster

Clone leaves the source cluster running and provisions an independent destination. Source/target must match topology type and engine major. For the common “copy source spec” path:

```sh
karactl restores create demo cloneproof --backup beforechange --into-new democopy
karactl clusters get democopy
karactl restores list democopy
```

Alternatively create a cluster with immutable `spec.dataSource`:

```yaml
dataSource:
  cluster: demo
  backup: beforechange
# Use pitrTime instead of backup for a point-in-time clone.
```

After the initial restore completes, the immutable source is provenance only:
later cluster changes do not require the source cluster or backup to remain.
Delete or retain those recovery objects according to your recovery policy.

This fragment belongs under `spec` in a normal cluster create request, not a standalone create payload. The destination is initialized, then Karakoram queues a deterministic restore. Inspect destination cluster/restore/operation readiness before connecting.

PBM restores source users except the backup account that performs a logical restore. Karakoram uses that account to reset destination system users to its own generated credentials. Physical snapshots additionally retain a separately encrypted snapshot-time `userAdmin` recovery identity, independent of the source cluster's secrets. Recovery uses the recorded storage/prefix, topology and engine version, then switches back to the destination's isolated prefix. Modern successful logical and physical snapshots can therefore be cloned after their source cluster has been fully deleted. Application users/data restored from the source remain application administration concerns. Never manually take clone backups while PBM still points at the source prefix.

### Cross-namespace recovery

Select the source namespace in the restore/clone dialog, or pass
`--source-namespace`. The global `-n` selects the **target**:

```sh
karactl -n dev restores create orderscopy recover \
  --source-namespace prod --source-cluster orders --backup baseline
# Live-source convenience clone into a new cluster in dev:
karactl -n dev restores create orders cloneproof \
  --source-namespace prod --backup baseline --into-new orderscopy
```

For explicit cluster creation, set `spec.dataSource.namespace: prod` along with
`cluster` and `backup`/`pitrTime`; in-place REST restore uses `sourceNamespace`.
Omitting source namespace means the target namespace. Cross-namespace recovery
requires source `backups/read` and **`backups/export`** on `prod/orders`, plus
target restore/cluster create. Export alone is not sufficient.

PBM uses the backup's recorded **source namespace storage** during recovery,
then returns to the target's isolated configuration. Target members must reach
that S3 destination directly. Normal topology/version and protected snapshot
identity checks still apply. The CLI convenience clone omits copied source
backup settings across namespaces; configure a target-local storage afterward.
See [namespaces](namespaces.md#backup-storages-and-cross-namespace-recovery).

## Delete and preserve recovery objects

```sh
karactl backups delete demo beforechange
karactl backups list demo
```

For a live cluster, deletion is asynchronous and observes PBM inventory absence before removing the record. PBM backup deletion is not reversible. Review PITR dependencies first. Cluster deletion removes owned backup/PITR history unless `--keep-backups` is explicitly selected; `--keep-volumes` is a separate choice.

### Recover when cluster deletion stays pending

Deletion is server-side reconciliation, not a foreground CLI job. A wait timeout
or lost HTTP reply does not undo it. Do not recreate the same cluster name,
manually erase its Karakoram state, or delete database volumes just to make the
inventory disappear.

1. Inspect the cluster and the accepted deletion operation in the **same
   namespace**, then inspect recent events:

   ```sh
   karactl -n default clusters get demo -o json
   karactl -n default clusters status demo
   karactl -n default operations cluster demo
   karactl -n default events list --cluster demo
   # Use the ID returned by deletion or found in the cluster's operations.
   karactl operations get OPERATION_ID -o json
   ```

   Read phase, message, conditions, operation target and event diagnostics rather
   than treating `Deleting` as progress by itself. The deletion response exposes
   its Operation and `Karakoram-Operation-Id` header. If a client lost the
   response, find the operation through the cluster operation list; do not
   initiate unrelated recovery work. Use API event pagination (`nextCursor`) or
   newest-first `order=desc` when investigating a long history.

2. Repair the specific prerequisite: an unavailable namespace needs a valid
   project token/connected relay; failed S3 cleanup needs the recorded bucket,
   prefix and write/delete credentials; a blocked Zerops process needs platform
   diagnosis. For an unformed cluster, PBM may never have become reachable:
   inspect the failed cleanup step instead of waiting for database readiness
   after deletion has begun. The control plane retries transient platform
   failures with backoff; let an already-running platform process finish before
   retrying the mutation.

3. After repairing the cause, observe the existing operation with
   `karactl operations wait OPERATION_ID`. If deletion failed or the original
   request was not accepted, repeat deletion **with the same intentional
   retention choices** and follow the returned operation:

   ```sh
   # Destructive retry: removes volumes and owned backups.
   karactl -n default clusters delete demo --wait
   ```

   Do not repeatedly submit DELETE while a deletion operation is still active.
   Check whether the cluster is already absent before retrying after response
   loss. A missing cluster is not proof that every orphaned volume is gone.

4. Decide retention before deleting, not as a blind workaround for failed
   cleanup. To suspend a cluster while keeping both live data and recovery
   objects, use:

   ```sh
   karactl -n default clusters delete demo --keep-volumes --keep-backups --wait
   # Only a completed retained-volume suspension can be resumed.
   karactl -n default clusters resume demo --wait
   ```

   `--keep-volumes` retains volume identity/secrets and results in `Suspended`;
   `--keep-backups` separately preserves backup objects/records. Neither can
   recover data already deleted or reverse a submitted platform deletion.
   Backup records marked Failed are not usable recovery bases. Keep the storage
   registered until retained records have been dealt with.

5. Inspect **Settings → Orphaned volumes** or
   `karactl -n default orphaned-volumes list -o json`. An orphan record means the
   runtime was removed but Zerops refused the volume deletion. Karakoram retries
   automatically; the retained disk can still consume capacity and incur charges.
   Manual removal requires access to the **database namespace's Zerops project**
   outside Karakoram: delete the recorded volume in the Zerops GUI or run
   `zcli service delete <hostname>` with zcli authenticated for that project.
   Confirm the recorded service ID and that no runtime needs the volume before
   removing it. If Zerops still refuses deletion, give its support the volume
   service ID and failing delete Process ID. Karakoram cannot bypass platform
   mount/process restrictions.

After deletion completes, check cluster absence (or `Suspended` if retaining
volumes), retained backup inventory and the orphan inventory separately. See
[orphan cleanup and retry timing](../operations.md#orphaned-platform-volumes)
and [troubleshooting](../operations.md#troubleshooting-by-symptom). Do not remove
the namespace token/relay until owned-resource cleanup is complete.


### Retained backups after full cluster deletion

Deleting volumes with `--keep-backups` removes the cluster resource, but preserves backup records, immutable snapshot provenance and protected recovery identities. Continue using the original cluster name **and namespace** with the backup API/CLI; permissions remain `backups` read/delete on **`<source namespace>/<source cluster>`**, not the destination or storage:

```sh
karactl clusters delete demo --keep-backups --wait
karactl backups list demo
karactl backups get demo beforechange
```

To clone from this retained snapshot, create a **new** compatible cluster using a complete cluster-create file with `spec.dataSource.cluster: demo` and `spec.dataSource.backup: beforechange`, as in the fragment above:

```sh
karactl clusters create democopy -f /private/path/retained-clone.yaml --wait
karactl restores list democopy
```

The `restores create ... --into-new` convenience form copies a live source's spec, so use the explicit create file when the source no longer exists. Alternatively restore into an existing compatible destination with `karactl restores create democopy recoverbase --source-cluster demo --backup beforechange`. Verify recovered application data before deleting the retained snapshot. Logical recovery does not require source-cluster secrets; physical recovery requires its protected snapshot recovery credential. Legacy records without snapshot/topology/version provenance, or physical records without a recoverable identity, are rejected up front with **409** rather than starting destructive work. PITR cloning still requires a live source and its observed continuous window.

```sh
karactl backups delete demo beforechange
karactl storages delete archive
```

For a deleted source, backup DELETE is synchronous (**204**): the control plane signs requests using the registered storage credentials and removes only `<recorded-pbm-prefix>/<pbm-name>/...` and `<recorded-pbm-prefix>/<pbm-name>.pbm.json`, then the recovery credential and inventory record. No agents are needed. Other snapshots, PITR slices and namespace markers are left untouched; removing a storage resource does not empty its bucket. The same outbound/TLS policy as storage verification applies. A timeout or partial S3 failure leaves the record `Deleting`; repeat DELETE to resume safely, including after a control-plane restart. Already absent records return 204. Keep the registered storage endpoint/bucket/prefix and credentials valid until cleanup finishes; unsafe/missing snapshot provenance is rejected with 409.

Storage deletion remains blocked while any cluster, schedule or backup record references it; after all references are removed it returns 204. Backup list returns 404 once a deleted cluster name has no retained records. Retained records do not recreate the original cluster or its volumes, and S3 objects alone do not reconstruct a lost control-plane database: maintain a tested state+key+PBM recovery plan.
