<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Neo4j backups and recovery

[Overview](README.md) · [Access and TLS](access-and-tls.md) · [Operations](operations.md)

## Recovery boundary

The only backup type is `dump`: a full offline copy of both `system` (native users/passwords) and `neo4j` (graph data). **The server is unavailable for the whole backup**, including upload and restart. There is no Community online backup, incremental backup or PITR. Storing a copy of live database files is not a supported substitute.

Register and verify an [S3](https://aws.amazon.com/s3/)-compatible destination using [Backups and storage](../backups.md). Members stream directly to S3; a successful control-plane storage probe does not prove member connectivity. Protect dump objects as credentials as well as data: the system database carries native authentication state.

```sh
karactl -n NS backups create graphdb --type dump --storage archive
karactl -n NS backups list graphdb
```

Use `dump` explicitly; the generic CLI default `logical` is for [MongoDB](https://www.mongodb.com/).

## Offline dump sequence

The member places a durable stop hold, stops [Neo4j](https://neo4j.com/), then runs `neo4j-admin database dump --to-stdout` for `system` and `neo4j`. Each stream is hashed and multipart-uploaded. The manifest is written **last**, then the server is restarted and the hold released. A prefix without a manifest is incomplete and must not be restored.

Object layout within the storage bucket:

```text
<storage prefix>/neo4j/<namespace>/<cluster>-<incarnation>/<backup>/system.dump
<storage prefix>/neo4j/<namespace>/<cluster>-<incarnation>/<backup>/neo4j.dump
<storage prefix>/neo4j/<namespace>/<cluster>-<incarnation>/<backup>/manifest.json
```

The manifest identifies format `karakoram-neo4j-dump/v1`, Community edition, source version, creation time, and each dump's object key, size and SHA-256. Do not move objects independently of their manifest. Polling an operation's durable receipt does not run the dump again. A native store lock failure fails the backup rather than copying a live store.

## Schedules mean outages

Any enabled schedule requires:

```yaml
engineOptions:
  acceptBackupDowntime: true
backup:
  storage: archive
  schedules:
    - name: nightly
      cron: "0 2 * * *"
      type: dump
      enabled: true
      retainCount: 7
```

Cron is UTC; the scheduler has no separate maintenance-window concept. Set the flag only after arranging application downtime and reconnect behavior. It does not make dumps online or bound their duration. Retention and namespace/storage rules are described in the [shared recovery guide](../backups.md).

## Restore and clone

An in-place restore replaces the target store and is offline/destructive. A clone creates a new Neo4j cluster from the backup; it is not an additional replica or a continuous copy. Use the shared [restore and clone interfaces](../backups.md), selecting a Neo4j backup and an equal or newer supported target version. Dumps from a newer Neo4j version than the target are rejected; older stores are upgraded on startup. Do not use restore as a downgrade mechanism.

The loader fetches each dump while checking its size and SHA-256, loads both into a staging data directory and swaps only after every load succeeds. The previous store is retained until credential recovery and normal server launch succeed. Interrupted uncommitted swaps roll back; a committed swap retains the replacement.

The `system` dump preserves source application users and their passwords. The managed `neo4j` password is reset to the **target's** managed credential, not the source's, through an authentication-disabled recovery start bound only to loopback on separate ports. The recovery child is stopped before normal authenticated listeners start. Reveal target credentials after recovery; do not expect the source administrator password to authenticate to a clone.

For an in-place restore, the dump may already contain the target's managed password. Credential recovery accepts Neo4j's explicit confirmation that the old and new passwords are identical; it does not roll the restored store back merely because no password change is needed.

Backup scope does not include the control-plane database, secret encryption key, namespace registrations or storage credentials. Protect those separately with [control-plane state backups](../operations.md#control-plane-state-backup-and-recovery).

## Capacity and failures

Backup streams do not require a full local `.dump` file, but native tools, multipart buffers and temporary files still consume resources. Restore requires room for the current store **and** the fully loaded replacement at the same time, plus transaction logs and temporary overhead. Size disk from observed store size and free space rather than compressed object size; grow it before recovery and never assume a fixed compression ratio. The JVM and offline tools also need memory headroom.

An interrupted background backup is not blindly resumed. Receipt recovery starts the server again and marks the interrupted job failed; submit a new operation after investigating the failure. If safe restart or rollback fails, recovery remains blocked rather than reporting success. A missing manifest or checksum/size mismatch is a failed recovery input, not an empty database. Observe operation status and cluster readiness independently before allowing application traffic.

A failed restore remains **Failed** even if the original store is successfully restarted: member readiness does not prove that the backup was restored. After rollback and authenticated readiness are verified, the restore operation terminates without retrying the same failed receipt. Inspect the restore and cluster status, correct the recovery input, storage access or capacity problem, and submit a restore with a **new name**. Do not remove receipt files or stop holds to force a replay. If rollback or restart is still pending, keep application traffic stopped and resolve the member's recovery failure before trying another restore.
