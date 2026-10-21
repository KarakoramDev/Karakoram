<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Distributed PostgreSQL with Citus

`postgres` topology `distributed` runs one coordinator (group **0**) and **1–8 worker groups**. Each group has its own persistent data members and Patroni replication/failover scope. `components.engine.replicas` is the count **per group**, not the total: 1 or 3, default 3. Three-member groups provide group-local failover; one-member groups have no replica to promote. Coordinator members provide the shared etcd quorum; workers use it as clients.

ProxySQL routes application connections only to coordinator members. It never advertises workers as application endpoints. Citus routes distributed SQL from the coordinator to worker primaries; do not use worker endpoints as interchangeable connection seeds. Ordinary PostgreSQL tables remain local until the application explicitly creates distributed/reference tables using Citus SQL.

## Availability and licensing

The native-artifact catalog includes real, smoke-qualified Citus 14.2.0 archives for PostgreSQL 16, 17 and 18. Creation still requires the matching `citus-pg<major>` entry to be locked **and resolvable**: development builds need a mirror serving the exact locked bytes, and releases need their matching downloadable assets. A topology entry alone does not establish availability. Inspect `karactl providers get postgres -o json` and the release artifact lock before deployment. Supabase does not offer this topology.

Karakoram remains Apache-2.0. Citus is an **AGPL-3.0-only** component, separately downloaded as an unmodified artifact, not bundled into the Karakoram binary. Its corresponding source is the upstream Citus tag matching the locked artifact (see [Citus upstream](https://github.com/citusdata/citus)); retain the artifact's license notices and source/tag reference and assess your obligations for your deployment. See [native artifacts](../native-artifacts.md).

## Create

Save a cluster request, choosing a PostgreSQL version with the matching locked artifact:

```yaml
name: distdb
spec:
  provider: postgres
  version: 17.11.1
  topology:
    type: distributed
  components:
    engine:
      replicas: 3
    extra:
      proxy:
        replicas: 1
  engineOptions:
    citus:
      workerGroups: 2
      database: app
```

```sh
karactl -n NS clusters create -f distributed.yaml --wait
karactl -n NS clusters status distdb -o json
karactl -n NS clusters members distdb -o json
```

Defaults are two worker groups and database `app`. A configured database name must be nonempty and at most 63 bytes. A proxy is required for replicated groups. The wizard exposes worker groups and the Citus database; the detail topology view lists group roles, leaders and members. Member `group` is read-only (0 coordinator); `CitusTopology` conditions distinguish matching metadata from drift. Missing observation is not evidence of matching metadata.

## Scale worker groups

The per-group count `components.engine.replicas` is fixed at creation. Changes in either direction (1 to 3 or 3 to 1) are rejected: group-local replica scaling is not supported for existing distributed clusters. Choose the required replication count when creating the formation; changing `engineOptions.citus.workerGroups` remains supported.

PATCH the desired worker-group count, leaving the per-group replication count unchanged:

```sh
karactl -n NS clusters patch distdb -f scale-workers.json --wait
```

```json
{"spec":{"engineOptions":{"citus":{"workerGroups":3}}}}
```

Scale-out provisions and registers worker groups. Scale-in first **drains shard placements**, removes the worker from Citus metadata, and only then retires its runtime/volumes. It may be long-running and require spare placement capacity. Do not manually delete worker services or volumes to scale in. Inspect the accepted operation and group/drift status before proceeding. Scale-out does not imply that existing shards have been evenly redistributed; request rebalance when appropriate.

Drain is durably checkpointed and irreversible once started. Raising the desired count during a drain does not cancel it: the draining group finishes retirement before replacement members are created with fresh monotonic identities.

## Rebalance

```sh
karactl -n NS postgres rebalance distdb --wait
karactl -n NS postgres rebalance distdb --strategy by_shard_count --wait
```

The optional strategy must exist in the installed Citus catalog. Omit it to use Citus's default. The API is `POST /api/v1/clusters/{cluster}/actions/rebalance?namespace=NS` with `{}` or `{"strategy":"by_shard_count"}`. It requires `clusters:update`, standard CSRF for browser sessions, and supports `Idempotency-Key`. It returns **202** with an Operation of type `rebalance`. Non-PostgreSQL providers are rejected with 400; non-distributed PostgreSQL clusters with 409. The UI action requires confirmation.

Shard movement adds database load. Track the returned operation rather than resubmitting after a timeout. Rebalance has durable receipts to avoid duplicate work after a lost response or coordinator promotion; cancellation is cooperative and does not undo placements already moved.

## Coordinated backup and PITR

A distributed backup is a coordinated formation backup, not an independently restorable coordinator snapshot. Each group has its own pgBackRest data/archive path and backup manifest entry. Backup work precedes the coordinated marker; named restore points and WAL/archive barriers establish a common recoverable boundary across all groups.

PITR uses persisted **named coordinated restore points**, not arbitrary wall-clock timestamps. **Timestamp restore targets are rejected** for distributed formations. Select an available named point from the recovery inventory and preserve the complete formation backup and all required group archives. The coordinator LSN returned by Citus is exact; sampled **worker LSNs are conservative post-point upper bounds**, used to fence archival coverage, not exact per-worker restore-point positions. They must not be presented as exact marker LSNs or used as independent targets.

Restore fences the entire formation, restores every group to the same named point, rebuilds replicas and checks Citus metadata before releasing access. A clone must have the source formation's worker-group count. Never mix groups from unrelated backups or restore only one group into a running formation. See [PostgreSQL backups](backups.md) for storage, retention and archive requirements.

Completed marker creation has a durable name receipt and replays its original response. Uncertain partial creation fails closed: retrying the same name never creates duplicate coordinated markers.

## Failover and limits

Worker agents receive client-only etcd endpoints (`host:2379`); they do not add etcd voters. Internal DCS reset is group-scoped by default (`/karakoram/<scope>/<group>/`) and atomically requires an absent leader. Whole-formation reset must be explicitly requested with `Formation: true`, requires no observed leader in any group and uses a revision-fenced atomic delete; it is not a shortcut around restore fencing.

Patroni elects a leader **within each group**. Worker failover changes that group's registered primary, not the coordinator role; coordinator promotion retains the formation metadata. Coordinator-only proxies refresh routing to the coordinator leader. Client transactions may fail during promotion and require application-level handling; Citus does not make a failed in-flight distributed transaction transparently succeed. Metadata drift withdraws healthy topology status until reconciled.

- No conversion to or from `single`/`ha`; create a separate formation and migrate application data explicitly.
- No distributed PostgreSQL major upgrade. Minor maintenance remains group-aware, workers before coordinator.
- No distributed topology for Supabase or its composed services.
- No arbitrary timestamp PITR or partial-group restore.
- Group-local replication does not imply separate infrastructure failure domains or unlimited shard movement capacity.
