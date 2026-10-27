<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Temporal operations

Start with the [prerequisites and options](README.md) and [access/TLS](access-and-tls.md). [Temporal](https://temporal.io/) operations depend on a healthy bound [PostgreSQL](https://www.postgresql.org/) source; additional stateless replicas do not replace database HA or establish independent failure domains.

## Workflow namespaces

Declare namespaces in `spec.engineOptions.namespaces`:

```json
[{"name":"orders","retention":"168h","description":"Order workflows"}]
```

Reconciliation creates missing namespaces and updates retention/nonempty descriptions through a frontend's local HTTPS API using the internode client identity. `default` is always ensured with 72h retention; a declaration of `default` must retain that value. Removing a declaration does **not** delete the namespace or its workflows. Temporal's API treats an empty description update as unchanged, so descriptions cannot be cleared this way.

```sh
karactl -n NS temporal namespace list workflows
# Remove the declaration first, then explicitly request destructive deletion:
karactl -n NS temporal namespace delete workflows orders --yes
```

Listing includes retention, description, state and whether a namespace is managed by options. Deletion calls `DELETE /api/v1/clusters/{cluster}/temporal/namespaces/{temporalNamespace}?namespace=NS` and returns **202** when accepted. Temporal deletes workflows asynchronously; acceptance is not completion. Karakoram refuses deletion of `default`, `temporal-system`, or any namespace still declared in options (**409**). Invalid names, missing namespaces and unavailable frontends also fail rather than silently succeeding.

Deletion uses the frontend **gRPC operator API**: upstream's HTTP gateway has no namespace-delete binding. These are workflow namespaces, not Karakoram project namespaces.

## Restart and scaling

```sh
karactl -n NS clusters restart workflows --wait
karactl -n NS clusters patch workflows -f scale.json --wait
```

For a split cluster, `scale.json` can contain:

```json
{"spec":{"components":{"extra":{"history":{"replicas":3}}}}}
```

Use a spec patch/file for extra components: generic `clusters scale --component` convenience flags only accept the standard engine/configServer/router slots.

Use split-role names only in split topology (`history`, `matching`, `frontend`, `worker`); use `server` in combined topology. `ui` is independent in both. Configure replica/resource changes in the selected `components.extra` entry. Scaling one role does not reconfigure/restart other roles. Stay within the [component bounds](README.md#components-and-presets); topology itself is immutable.

Rolling restart uses **history → matching → frontend → worker → server → ui**, skipping disabled components. It waits for all instances of each component to apply the desired configuration and become Ready before advancing, with a one-minute settle period after history or combined server restart. A restart can target a component/member through the operation payload; TLS CA rotation must include all components. A singleton role is unavailable during restart; multiple replicas are not a zero-disruption guarantee.

## Server upgrades and schema

The current catalog path is **1.31.4 → 1.32.1**. Admission requires locked artifacts. The version policy permits forward patches within a minor or exactly one minor forward; skipping minors and downgrades are rejected. Do not bypass the advertised `UpgradePaths`.

```sh
karactl -n NS clusters upgrade workflows --version 1.32.1 --wait
```

Before starting migration, reconciliation waits until every current component is healthy. It rolls history → matching → frontend → worker → server → ui, checking all instances at the desired version/configuration before advancing. Split history gets a one-minute settle period for shard ownership. The UI binary remains release-locked even when server version changes.

Every server member ensures schema before starting. The **first upgraded member** applies migrations with its bundled `temporal-sql-tool`; a PostgreSQL advisory lock on the persistence database serializes schema setup/update for both persistence and visibility. Repeated starts converge rather than racing migrations. Temporal's startup schema compatibility checks remain in force.

The locked **1.31.4 and 1.32.1 both embed persistence schema 1.19 and visibility schema 1.14**. This specific upgrade therefore does not imply a schema-version increment. Inspect `TemporalSchema` and `TemporalUpgrade` conditions, observed version, members and generation—not just HTTP acceptance.

## Backups

Temporal has no separate snapshot mechanism. `backup` asks the PostgreSQL binding's optional backup interface for a source PostgreSQL `full`, `diff` or `incr` backup and waits for its outcome. It requires a binding service that supports backups. Both Temporal databases reside on that source and are covered together; source backup/storage configuration and retention remain PostgreSQL-owned.

Karakoram also records a non-secret manifest: server version, observed schema versions, history shard count, Temporal cluster name, namespace declarations, database names, source reference and CA fingerprint. **The manifest lives in the Temporal cluster's internal state**, keyed by its Temporal backup name. The generic backup record's `location` is the binding backup ID; backup records have no engine-metadata manifest field.

```sh
karactl -n NS backups create workflows daily --type full
karactl -n NS backups list workflows
```

Deleting a Temporal backup removes **only its Temporal inventory record and manifest**. It does not delete PostgreSQL archives; source retention owns those. Conversely, a Temporal record cannot guarantee its source archive is still retained. Preserve [control-plane state/master key](../operations.md#control-plane-state-backup-and-recovery) alongside source backups; manifests are not independently recoverable from an archive.

## Restore

A Temporal restore is an offline, destructive **in-place source PostgreSQL restore**, not a workflow-only import. It rolls back all databases/consumers covered by that source restore. Temporal requires a recorded, complete manifest with a source backup ID; PITR timestamps are rejected.

Compatibility checks require:

- The same Temporal cluster name and `historyShards`.
- The original PostgreSQL source namespace/cluster and both original database names.
- The same server version or a supported forward upgrade path from the recorded server version. Schema versions are recorded in the manifest; compatibility is gated by this version policy and startup schema checks, not arbitrary schema conversion.

If the binding reports a shared source, restore refuses unless **`acknowledgeSharedSource: true`** is included at the top level of the restore request. A protected API-shaped request file can be used with the CLI:

```json
{
  "name": "recover",
  "source": {"backup": "daily"},
  "acknowledgeSharedSource": true
}
```

```sh
karactl -n NS restores create workflows -f restore.json --yes
```

Acknowledge only after coordinating **every** source consumer. The operation stops all enabled roles, restores the source through the binding, then starts components in roll order. Server startup ensures schema; all components must become Ready and namespace declarations are synchronized before success. Workflow replay may execute application activities again: application workers must preserve their idempotency guarantees.

The manifest's namespace declarations are recovery evidence, not a replacement for the target's current desired options. Its CA fingerprint is not a backup of private keys or sealed UI secrets. Inspect operation outcome and recovered workflow state before resuming application traffic.

## Delete or retain

Cluster deletion removes components in reverse roll order, then releases the PostgreSQL binding. Plain deletion drops the two bound databases and owner role on the source. `--keep-volumes` passes retained-data semantics to the binding: **keep the databases and disable the role**. Temporal roles themselves have no durable volumes to preserve. The source cannot be deleted while its Temporal binding exists.

```sh
karactl -n NS clusters delete workflows --keep-volumes --yes --wait
```

Review [shared deletion/retention rules](../clusters.md#delete-or-suspend) before deleting. Retained PostgreSQL archives are separate from Temporal inventory and cluster-state manifests; keeping a backup record alone is not a portable Temporal recovery package.

## Known limitations

- No multi-cluster replication/global namespaces or per-namespace authorization.
- Namespace descriptions cannot be cleared through Temporal's API; removal from options never deletes workflows.
- Client revocation requires a full [three-phase CA rotation](access-and-tls.md#revocation-and-rotate-tls).
- Source restores affect other consumers; arbitrary cross-source clones and Temporal PITR timestamps are unsupported.
- Backup manifests live in Temporal cluster state; preserve that state, not only database archives.
