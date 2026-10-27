<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Definitions backups

[RabbitMQ](https://www.rabbitmq.com/) backups recover broker configuration, not queued message data.

## At a glance

| Property | Behavior |
| --- | --- |
| Supported type | `definitions` only; no messages, physical broker snapshots or PITR; no PBM deployed |
| Captured | Topology, application users/password hashes, grants, policies, runtime parameters |
| Excluded | `karakoram`, `admin`, `monitor`, `guest` and their vhost/topic grants |
| Restore | Management API import **merges** objects; unrelated objects/existing messages are not rewound or removed |
| Bound | At most **64 MiB uncompressed** definitions; corrupt/unsupported sources fail |

The **control plane** exports through the broker Management API, filters reserved users/grants, gzips JSON and uploads to [S3](https://aws.amazon.com/s3/); payloads do not go directly from the broker to S3. Restore downloads/decompresses in the control plane and repeats the filter, including older backups, preserving target system credentials.

**Warning:** application password hashes and runtime parameters are sensitive; gzip is not encryption. Restrict bucket/S3 access, use appropriate storage encryption, and keep exports out of logs/issues. Definitions remain trusted administrative input. Message replay/migration needs an independent source.

## Create and schedule

[Register and verify namespace-local storage](../backups.md) first; these examples use `archive`. In **Cluster → Backups → Backup now**, choose definitions/storage and follow the operation until inventory says **Succeeded**. Without registered storage, the action is disabled. The UI warns about password hashes for manual backups and schedules.

```sh
karactl -n NS backups create brokers before-change --type definitions --storage archive
karactl -n NS backups get brokers before-change -o json
karactl -n NS operations list --cluster brokers -o json
```

Backup creation is asynchronous; inspect the recorded Operation and backup phase/error. The CLI default type is `logical`, so specify `--type definitions`.

Save `schedule.yaml`, preserving any other schedules because the array is replaced:

```yaml
spec:
  backup:
    storage: archive
    schedules:
      - name: daily
        cron: "0 2 * * *"
        type: definitions
        retainCount: 7
        enabled: true
```

```sh
karactl -n NS clusters patch brokers -f schedule.yaml --wait
```

Cron is UTC. Retention counts successful backups **of this schedule**, not manual backups; shared [scheduling/retention rules](../backups.md) apply.

## Restore or clone

Only a successful RabbitMQ definitions backup is valid; PITR requests and incompatible sources are rejected before queueing (unsupported already-queued sources fail terminally).

**Warning:** in-place import can replace matching application definitions even though it does not delete unrelated objects. Review users/grants/policies first.

```sh
karactl -n NS restores create brokers restore-definitions --backup before-change --yes
karactl -n NS restores get brokers restore-definitions -o json
karactl -n NS restores create brokers --backup before-change --into-new brokerscopy
karactl -n NS clusters wait brokerscopy --timeout 30m
```

Observe the restore record until **Succeeded**, then inspect imported objects and target health. For cloning, the CLI copies the source spec; creation-time `dataSource: {cluster: brokers, backup: before-change}` imports after target readiness. Use a complete create file to customize the clone or recover from a deleted source; see [generic restore/clone and cross-namespace recovery](../backups.md).

`dataSource` is immutable provenance, not continuous replication. Keep backup/storage available until import completes; afterward deletion/pruning of the source backup does not trigger re-import or block spec updates. Retained backups remain usable after source-cluster deletion.

## Cleanup

Successful scheduled-backup retention and explicit deletion remove recorded S3 objects:

```sh
karactl -n NS backups delete brokers before-change --yes
```

Choose `keepBackups` independently of `keepVolumes` when [deleting/suspending](../clusters.md#delete-or-suspend). Retained objects require control-plane state, the original master key and storage credentials. Retained broker volumes preserve secrets for coordinated resume, not portable message backups; see [operations](operations.md).

Whole-cluster deletion first retires owned broker runtimes and deletes volumes (or
retains them for suspension). It then deletes this incarnation's recorded
definitions objects directly through the control-plane storage binding, without
requiring working broker credentials. Object cleanup has a durable **two-minute**
window shared across backups and controller restarts; transient failures retry
within that window. `keepBackups=true` bypasses object and inventory removal,
independently of `keepVolumes`; backups from earlier incarnations remain untouched.

If object cleanup cannot finish safely within that window, deletion still removes
the cluster record (or completes suspension when retaining volumes). A
`BackupArtifactsRetained` warning event names the retained backup, storage, recorded
object location and bucket when available; its inventory is preserved. This means
**cluster removal with retained artifacts**, not successful object removal.
Restore the original storage binding, verify the recorded object's exact ownership,
remove only that object with object-store tooling, then remove its retained backup
record. Never guess a missing or ambiguous location or delete a shared prefix.
Runtime ownership, backend deletion and control-plane persistence failures still
block unsafe cleanup. Active restore/clone dependency fences still reject
destructive source deletion with HTTP 409 before any cleanup begins.
