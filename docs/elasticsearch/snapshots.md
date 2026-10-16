<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Snapshots

Back up Elasticsearch to S3 and recover data, global state and feature states through native snapshots.

## Create and schedule

Elasticsearch supports **`snapshot`**, not logical/physical backups or PITR. [Register storage](../backups.md) first. Nodes send snapshot data directly to S3. Always pass `--type snapshot`; the CLI defaults to `logical`.

```sh
karactl backups create search beforechange --type snapshot --storage archive
karactl backups get search beforechange
karactl backups list search
karactl operations cluster search
```

Wait for both Backup and Operation success. Partial/failed native snapshots are not recovery bases. Snapshots include indices/data streams, global state and feature states, including security/Kibana; they exclude node files, private keys, keystores and repository registrations. Preserve protected control-plane state, its original encryption key and storage configuration separately.

Use Karakoram schedules/count retention, not Elastic SLM; the [create example](README.md#create) includes a schedule. [Scheduling rules](../backups.md) cover UTC cron, successful-backup counts, array replacement and control-plane downtime. ILM controls live-index retention, not backups.

Each referenced storage gets a named S3 client and verified repository with a cluster-UID-unique base path and one writer. Adding/removing clients or changing static endpoints requires rolling maintenance; keystore credential changes reload without restart. A removed storage retires only after no inventory, queued/running restore or unfinished initial-restore source references it. Unregistration does not delete snapshots; already deleted, unreferenced storage does not block retirement/configuration pulls. Never attach another writer to that prefix.

Elasticsearch 9.3+ requires S3 conditional writes. Verify actual repository semantics; do not bypass failures with unsafe compatibility settings. See [snapshot rationale](../adr/0028-elasticsearch-security-and-snapshots.md).

## In-place restore

**Warning:** Quiesce external writers and plan downtime. Restore overwrites the destination; take a current recovery snapshot if needed.

```sh
karactl restores create search recovery --backup beforechange --yes
karactl restores get search recovery
karactl operations cluster search
```

Karakoram stops Kibana/Logstash/APM services at platform level, deletes all destination data streams and non-system indices (including hidden/closed objects), restores global state, aliases and feature states, waits for recovery, re-establishes managed credentials through the unaffected file realm and restarts components with current configuration. System indices restore through feature states, not blanket deletion. Application users, roles, API keys and persistent settings may revert. Verify data, authentication and delivery before reopening writers. Do not duplicate a restore after a client timeout.

## New cluster from backup

Create a complete destination request using the [canonical shape](README.md#create) and this immutable `spec` fragment:

```yaml
dataSource:
  cluster: search
  backup: beforechange
```

```sh
karactl clusters create searchcopy -f clone.json --wait
karactl clusters status searchcopy
karactl operations cluster searchcopy
```

The source must still exist, use Elasticsearch and have a successful snapshot. Destination version cannot be older and may be at most one major ahead; [snapshot/index compatibility](https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore) still applies. This is neither downgrade nor continuous replication. The destination forms, registers the source repository read-only, restores, establishes its own managed credentials and then uses its unique writable prefix.

Keep source backup/storage available through initial restore. After success, unrelated destination updates do not re-resolve source records; immutable `dataSource` remains historical provenance. For cross-namespace sources, set `dataSource.namespace`; see [permissions and recovery flow](../backups.md). Nodes require direct S3 reachability.

For a **deleted** source, create the destination without `dataSource`, wait for formation, then reference the retained backup's original source:

```sh
karactl clusters create searchcopy -f destination.json --wait
karactl restores create searchcopy recovery --source-cluster search --backup beforechange --yes
karactl restores get searchcopy recovery
```

Add `--source-namespace NAME` when needed. Preserve its inventory, storage and protected control-plane state.

## Cleanup and interrupted restore

`Backup.location` is `s3://<bucket>/<base_path>?repository=<repo>#<snapshot>`, a repository/snapshot reference shown in Backup details, **not a per-snapshot object prefix**. Snapshots share blobs: delete through Elasticsearch. Direct retained-backup deletion after source deletion is allowed only for the sole snapshot in a retired repository with no writer; otherwise register it elsewhere and use the native snapshot API.

With `keepBackups: false`, cluster cleanup deletes native snapshots only in owned writable repositories before infrastructure, removing inventory only after confirmed absence. Read-only sources are untouched. If Elasticsearch is unreachable, `SnapshotsRetained` warns that infrastructure deletion continues while snapshots/inventory remain. `keepBackups: true` explicitly preserves both; see [deletion choices](../clusters.md#delete-or-suspend).

Suspending with `keepVolumes: true` during restore abandons it. Resume records `RestoreAbandoned`, clears restore-owned destructive settings after rejoin, restores component intent and skips interrupted initial-restore checkpoints. Retained data may be partially restored: inspect before workloads; request a fresh explicit restore if necessary. Initial restore is not automatically retried.
