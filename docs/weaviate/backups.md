<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Weaviate backups and recovery

[Weaviate](https://weaviate.io/) uses native `backup-s3` **full** backups, with members writing directly to [S3](https://aws.amazon.com/s3/). No incremental chain, replica deduplication or point-in-time recovery is supported. Backups protect native collection/schema, object/vector data and native user/role state; they do not replace [control-plane state protection](../operations.md#control-plane-state-backup-and-recovery).

## Bind storage

[Register and verify S3 storage](../backups.md#register-s3-storage) in the cluster namespace, then set `spec.backup.storage`. At most one storage is bound. `backup-s3` is managed automatically and enabled when storage is bound; without a bucket it is not enabled at native startup. Do not put it in `engineOptions.modules`.

```yaml
spec:
  backup:
    storage: archive
    schedules:
      - name: nightly
        cron: "0 2 * * *"
        type: full
        retainCount: 7
        enabled: true
```

This is a merge-patch body for an existing cluster, not a complete create request. Schedules require a positive retention count and the cluster's bound storage. Binding or changing storage configuration requires a member roll. Every member needs S3 connectivity; a successful control-plane probe does not establish that path.

S3 endpoint, bucket, prefix, region and access keys are rendered from encrypted configuration. The native endpoint is host[:port] without a scheme; TLS is selected separately from the registered origin. Per-operation endpoint overrides cannot redirect native backups to another service: the native S3 client uses its startup endpoint. Backups naming a storage other than the bound storage are rejected.

Storage credentials must permit reading objects under the recorded backup prefix as well as native backup writes/restore reads. The control plane reads `<path>/<native-ID>/restore_config.json` to identify restore attempts; write-only backup credentials are insufficient.

## Create and observe

```sh
karactl -n default backups create vectors baseline --type full --storage archive
karactl -n default operations wait OPERATION_ID
karactl -n default backups get vectors baseline
karactl -n default backups list vectors
```

Always specify `--type full`: the generic CLI default `logical` is not a Weaviate type. Creation requires a Ready cluster with all members healthy. Submission alone is not success: Karakoram checks existing native status before submission and polls through native `SUCCESS`, `FAILED` or `CANCELED`; inspect both the operation and backup inventory.

Each backup records an immutable endpoint, bucket, path, native ID, cluster UID, source version and ordered source member identities/count. Objects live below:

```text
<storage-prefix>/<namespace>/<cluster>/<cluster-UID>/<native-backup-ID>/...
```

The native ID is derived from the Karakoram backup name, not the user-facing name itself. The recorded location is `s3://<bucket>/<path>#<native-backup-ID>`. Keep the recorded location and storage registration intact; do not infer old locations from a changed current prefix or a newly created same-named cluster.

Backup inventory reports size in **bytes**, converting the native GiB value; non-finite or out-of-range sizes are not recorded.

## Restore and clone

**In-place restore is destructive:** Karakoram deletes existing collections before native restore submission. Stop writers and take a separate recovery backup first. A native restore needs an empty collection target; it does not merge existing collection data.

```sh
karactl -n default restores create vectors recovery --backup baseline --yes
karactl -n default operations wait OPERATION_ID
karactl -n default restores get vectors recovery
```

To create a new cluster from the same source:

```sh
karactl -n default restores create vectors clone --backup baseline --into-new copied
```

Follow the returned operation and new cluster until generation/health converge. For a create request, use immutable `spec.dataSource` with `cluster`, `backup` and optional source `namespace`; the target binds the source backup storage. Cross-namespace recovery remains subject to shared [source-export permissions](../backups.md).

Recovery constraints:

- Source must be a succeeded `full` Weaviate backup with complete recorded metadata.
- Target member count must equal the recorded source count. Member names are mapped by their recorded order to target members through native `node_mapping`; this is **not resharding**.
- Target bound storage must use the recorded source endpoint and bucket. Clone/data-source admission requires the source backup's storage name; retain suitable namespace-local registrations and credentials.
- Version must be unchanged or move forward on a supported upgrade path: patch-forward or exactly one minor forward. No downgrade or minor skipping; only catalog-supported targets are admitted.
- The target must be healthy before clearing/submitting, and healthy again before completion.
- In-place restore into the same cluster incarnation restores users and roles (`usersOptions: all`, `rolesOptions: all`). Clone/new-incarnation restore uses `noRestore` for both, preserving fresh target identities. Recreate application users/roles deliberately on a clone; source keys are not its login credentials.
- Native operation config uses capitalized `Bucket`/`Path`; Karakoram supplies them from the recorded location. Do not substitute a REST `Endpoint` override for binding storage.

Restore attempt identity comes from the backup prefix's `restore_config.json` `startedAt`, not native restore GET status (which lacks timestamps). Before capturing that baseline or deleting collections, Karakoram acquires durable ownership of the source backup for the target, restore and operation. Other targets wait; successful/failed/cancelled restores and deleted targets release ownership or allow atomic reclamation. Karakoram persists the previous identity (or confirmed absence) before submission and requires a different identity to accept subsequent status, so an old `SUCCESS` cannot complete a new restore. The identity need not be later: native clocks can move backwards. Unreadable metadata, a missing required attempt identity or ambiguous acceptance fails closed. An accepted restore whose status becomes unavailable is not blindly resubmitted: preserve its operation/receipt, source storage and target data; inspect `restore_config.json` and target collections before retrying. Do not restore the same source backup concurrently outside Karakoram: external native clients do not participate in its ownership fence.

## Delete backups and retained objects

```sh
karactl -n default backups delete vectors baseline --yes
```

Deletion observes or cancels any active native writer before removing objects. Karakoram recursively deletes only the recorded `<path>/<native-ID>/` prefix, then removes inventory; retention cleanup uses the same isolated identity. Keep S3 delete permissions and the matching storage registration available until cleanup completes. Retained backups can be cleaned without a live source runtime, but pending/running backups cannot be treated as writer-free retained objects.

Cluster deletion follows shared `--keep-backups` and `--keep-volumes` semantics. Retaining volumes is not a backup; retaining backup objects is not a control-plane state backup. See [deletion and suspension](../clusters.md#delete-or-suspend) and inspect operations, retained inventory and orphaned volumes separately.
