<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Snapshots

Back up [Elasticsearch](https://www.elastic.co/elasticsearch) to [S3](https://aws.amazon.com/s3/) and recover data, global state and feature states through native snapshots.

## Create and schedule

Elasticsearch supports **`snapshot`**, not logical/physical backups or PITR. [Register storage](../backups.md) first. Nodes send snapshot data directly to S3. Always pass `--type snapshot`; the CLI defaults to `logical`.

```sh
karactl backups create search beforechange --type snapshot --storage archive
karactl backups get search beforechange
karactl backups list search
karactl operations cluster search
```

Wait for both Backup and Operation success. Partial/failed native snapshots are not recovery bases. Snapshots include indices/data streams, global state and feature states, including security/[Kibana](https://www.elastic.co/kibana); they exclude node files, private keys, keystores and repository registrations. Preserve protected control-plane state, its original encryption key and storage configuration separately.

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

Karakoram stops Kibana/[Logstash](https://www.elastic.co/logstash)/APM services at platform level, deletes all destination data streams and non-system indices (including hidden/closed objects), restores global state, aliases and feature states, waits for recovery, re-establishes managed credentials through the unaffected file realm and restarts components with current configuration. System indices restore through feature states, not blanket deletion. Application users, roles, API keys and persistent settings may revert. Verify data, authentication and delivery before reopening writers. Do not duplicate a restore after a client timeout.

Restore submission uses a durable per-operation receipt, not the presence of an index with a snapshot name. If the control plane loses the native response or cannot persist its successful response, it refuses to submit again and keeps managed components stopped. The Restore error requests reconciliation: inspect native `GET /_recovery?active_only=false&detailed=true`, the source snapshot, expected indices and acknowledged data. Do not delete the receipt or blindly retry. If a full restore cannot be established, keep external writers fenced, suspend with retained volumes to abandon the interrupted operation as described below, then resume and explicitly request a fresh restore. Legacy in-flight submission checkpoints without these receipts also require reconciliation rather than automatic replay.

Without durable native successful completion, the submission outcome remains **unknown**, even when the final HTTP response looks like a validation rejection (including HTTP 500 `snapshot_restore_exception` naming an open conflicting index, or a typed 4xx). Karakoram preserves the native error as evidence on the intent receipt, not as proof that restoration never started. Restore/Operation fails immediately with reconciliation guidance instead of spending a retry budget on a receipt that can never authorize another POST. Replaying an intent-only receipt after a crash or lost response is likewise terminal immediately; transient receipt-store read/write errors remain retryable.

In Elasticsearch 9.x, [RestoreService validation](https://github.com/elastic/elasticsearch/blob/v9.3.0/server/src/main/java/org/elasticsearch/snapshots/RestoreService.java#L1747-L1793) runs before restore routing is published **for that execution**. However, [TransportMasterNodeAction retries after master disconnect/NodeClosedException](https://github.com/elastic/elasticsearch/blob/v9.3.0/server/src/main/java/org/elasticsearch/action/support/master/TransportMasterNodeAction.java#L251-L274). A previous execution of the same HTTP request may already have started restoration before the retried execution rejects its now-open indices. One control-plane POST does not guarantee one native execution. Elastic's [restore guidance](https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/restore-snapshot#delete-restore) also requires stopping indexing to prevent external index re-creation.

Terminal unknown-outcome does **not** undo the preceding destination deletion or release holds. Restore holds and stopped managed components remain in place; healthy client-created indices do not prove restoration and must not make the cluster Ready. Preserve the receipt, fence external writers, inspect the named conflict/reason, native `GET /_recovery?active_only=false&detailed=true`, source snapshot and acknowledged data, and preserve any needed destination data. To recover through Karakoram, suspend with `keepVolumes: true`, resume as described below, resolve the reported cause (including writers recreating indices), then explicitly request a fresh restore. For a conflicting index recreated by a client, first copy or snapshot any of its documents you need, then remove it with native `DELETE /<index>` and confirm `HEAD /<index>` returns 404 before the fresh restore. Do not delete receipts or restart managed components to bypass the hold. If error evidence cannot be persisted, the existing intent still remains outcome-unknown.

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

Changing a storage registration's prefix applies to future snapshots. Existing backups and interrupted backups retain their recorded base path for restore, deletion and continued polling; the recorded bucket and storage identity must still match. Keep the original bucket contents and access permissions available. A prefix edit does not migrate or copy objects, and changing a bucket/endpoint requires a separate storage migration plan.

With `keepBackups: false`, whole-cluster cleanup first retires runtime writers and deletes volumes (or records intentional `keepVolumes` retention). It then uses the control-plane S3 binding to remove only complete owned writable repository prefixes, including shared blobs, without contacting Elasticsearch. Read-only source repositories and prior incarnations are untouched. A durable two-minute artifact-cleanup window bounds storage/authentication/binding retries across controller restarts; successful repositories are checkpointed before inventory removal. On exhaustion, `SnapshotsRetained` names the recorded repository prefix and manual-removal guidance; cluster deletion completes while affected inventory remains. Restore the original binding, verify ownership, remove only that complete retired repository prefix, then remove retained inventory. Never delete individual shared snapshot blobs or guess ambiguous ownership. `keepBackups: true` preserves objects and inventory without a failure warning, independently of `keepVolumes`; accepted restore/clone dependencies still fence destructive deletion with `409`. See [deletion choices](../clusters.md#delete-or-suspend).

Suspending with `keepVolumes: true` during restore abandons it. Resume records `RestoreAbandoned`, clears restore-owned destructive settings after rejoin, restores component intent and skips interrupted initial-restore checkpoints. Retained data may be partially restored: inspect before workloads; request a fresh explicit restore if necessary. Initial restore is not automatically retried.
