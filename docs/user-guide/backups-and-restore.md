<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Backups, PITR, restore and clone

Karakoram uses PBM for consistent MongoDB recovery, for both replica sets and sharded clusters. Zerops Local Storage live archives are **not** consistent MongoDB backups. PBM backup objects are independent of [control-plane state backups](../operations.md#control-plane-state-backup-and-recovery).

## Register S3 storage

Create the bucket separately: Karakoram's BackupStorage resource describes an existing S3-compatible destination. Zerops Object Storage and other S3-compatible services are supported. In **Settings → Backup storages**, supply name, endpoint URL, signing region, bucket, optional prefix and access credentials. Path-style addressing and TLS verification default to true. Do not disable TLS verification to hide an incorrect endpoint or certificate chain.

Storages belong to a [namespace](namespaces.md): select it in the UI or use
`karactl -n dev storages create -f FILE`. Names are unique per namespace; cluster
backup settings and schedules resolve storage names in their own namespace.
Verification runs from the control plane's network, while backup payloads go
directly between members and S3, not through the relay.

New storage names must not contain `:` or `/`. Existing storages in namespace
`default` with either character in their name remain readable and updatable;
this creation-only restriction does not rename or invalidate legacy storages.

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

### Endpoint verification policy

The verification endpoint must be an HTTP(S) origin without embedded credentials, a non-root path, query or fragment. Public global-unicast destinations on **80/443** are permitted by default. For a private S3 server or nonstandard port, configure the **control-plane service** environment `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` with comma-separated exact `host` or `host:port` entries, then restart/redeploy that service. Host-only entries allow ports 80/443; host:port allows that exact port. `storage-prg1.zerops.io` on 80/443 is already trusted for Zerops storage's private addresses.

Loopback, link-local/metadata-style addresses, unspecified/non-global addresses and shared range 100.64.0.0/10 stay blocked even when allowlisted. The verifier checks all DNS answers on every connection, disables proxies, rejects redirects and has a thirty-second deadline. Use the actual regional S3 origin, not a redirecting website URL; keep HTTPS/TLS verification enabled. This is the verifier's policy, not a guarantee that PBM uses the same outbound sandbox. See [security](../security.md#s3-verifier-outbound-policy) and [installation settings](../install.md#required-secrets-and-generated-settings).

## On-demand backups and inventory

```sh
karactl backups create demo beforechange --type logical --storage archive
karactl backups create demo physicalbase --type physical --storage archive
karactl backups list demo
karactl backups get demo beforechange
karactl operations cluster demo
```

Both types execute on nodes via PBM; physical snapshots require PSMDB. A create request returns acceptance, not completion. Watch Backup phase (`Pending`, `Running`, `Succeeded`, `Failed`) and the matching Operation. The inventory carries PBM name, storage, type, timestamps, last-write time, size and error. Only a successful backup is a recovery base.

If a mutation returns an Operation ID, use `karactl operations wait ID`. For backups/restores, inspect their own records as well as the operation and verify actual recovered data in a drill. Do not treat an HTTP 2xx or PBM command initiation as a successful recovery test.

For applications outside this repository, see the [Go client installation and usage walkthrough](api.md#clientssdk), including bearer authentication, structured errors and operation polling. The public client exposes backup, PITR-window, restore and clone request methods from the same OpenAPI contract; after polling, inspect the recovery resource and validate the recovered data as described above.

## Schedules and count retention

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

Enabling PITR alone does not create proof of a usable window. Gaps, quota exhaustion, failed slicing, base deletion or storage changes can remove recovery coverage. After restore Karakoram holds PITR disabled until its required fresh base snapshot succeeds; check that backup and the new timeline before trusting recovery coverage again.

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

Karakoram serializes disruptive cluster work. It disables PITR and, for sharding, coordinates balancer/router holds. Physical restore suppresses automatic mongod/PBM restart on all relevant nodes. Mongod may be offline and PBM may exit successfully as part of recovery; progress is read with PBM storage configuration while MongoDB is unavailable. Karakoram then starts mongod/PBM, releases maintenance, restores system users, resyncs target storage and restores router/balancer state. Logical restores do not use the offline physical lifecycle.

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
