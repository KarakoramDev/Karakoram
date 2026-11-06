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

## Agent upgrades

```sh
karactl -n NS clusters upgrade-agents workflows --include-current --wait --timeout 30m
```

This is also the explicit operator mechanism for moving an existing UI to the
current release-locked UI version. Equivalent API request:

```http
POST /api/v1/clusters/workflows/upgrade-agents?namespace=NS
Content-Type: application/json

{"includeCurrent":true}
```

Track the returned `202` operation. No separate UI-version endpoint or
`spec.version` change is needed; that field selects the Temporal **server**,
not its independently locked UI.

This replaces member agents with the frozen control-plane build, retaining the
applied Temporal server version, component resources, replica counts and PostgreSQL
binding. The separately release-locked UI is prepared and activated at the
current supplied version during its deployment; ordinary control-plane-only
reconciliation retains the existing UI's persisted installed version and
executable directory. Use `--include-current` to rerun preparation on current
agents. Server sealed configuration and restart tokens remain unchanged.
An active version upgrade, restore recovery, import or other pending structural
work blocks admission.

An absent UI version field is recovered from its sealed applied configuration;
an unknown incumbent installation does not fall back to the current release
lock. Target UI configuration is published only after a successful deployment
submission. A rejected or lost response retains the previous configuration,
and the already-admitted UI preparation can be retried without requiring the
unprepared replacement to become ready first.

The durable order is **history → matching → frontend → worker → server → ui**,
skipping disabled roles and already-current agents (unless `--include-current`).
Each component has a persisted deployment intent and receipt, followed by native
readiness, applied-configuration checks and exact target-build acknowledgement
from every live instance. History and combined server retain the restart path's
one-minute settle period. Replaying a completed component never redeploys it.
Each new deployment respects the maintenance window; an already-deployed
component finishes its readiness checks even after the window closes.

Failed deployment/preparation receipts are retired into the already-admitted
preparation phase, not polled forever and not gated on the missing executable's
readiness. A durable per-component budget permits **four failed attempts** for
the same locked native payload and agent build; platform placement/backpressure
waits do not consume it. Exhaustion publishes `DeploymentRetriesExhausted`,
marks the cluster Degraded and stops further preparation submissions. Correct
the cause, then explicitly re-arm:

```sh
karactl -n NS clusters retry-deployments workflows
karactl -n NS clusters upgrade-agents workflows --include-current --wait --timeout 30m
```

An existing in-flight rollout can resume after the retry epoch advances; if its
operation already ended, the new explicit rollout repairs the retained failed
preparation without requiring an unavailable UI to become healthy first. Native
readiness and exact target-build checks still fence successful advancement.

The backend contract deploys a whole logical component service; it has no
per-instance deployment primitive. Platform replacement strategy therefore
controls multi-instance rollout within that component. **Singleton components
have downtime, and additional replicas do not guarantee uninterrupted service.**
The next component never starts until all expected instances are ready on the
target build. A different control-plane executable cannot replace the frozen
target; re-run the operation using the intended build.

Cancellation stops between components or before deployment intent is recorded.
An intent without a receipt may have reached the backend. If the applied old
configuration is still healthy, cancellation completes without submitting a UI
upgrade—even when the installed UI is older than the current lock, or a prior
submission was rejected. If an ambiguous replacement is no longer serving,
only its already-admitted bounded preparation is replayed to restore readiness.
A submitted component with a receipt must acknowledge the exact target build
before cancellation completes; no subsequent component is deployed and completed
work is not rolled back. Exhausted disrupted preparations retain the actionable
condition and require explicit retry re-arm before recovery can finish.
Component recovery/import journals defer operation admission until normal
recovery finishes.
With `--include-current`, a lost deployment receipt after platform acceptance
can cause an additional same-build redeployment of that component on replay.
The backend contract has no request-identity receipt lookup to distinguish a
completed deployment from untouched current agents. Normal readiness/window
gates still apply. Without the flag, all expected instances must report the
exact frozen target build before replay adopts the completed component.


## Server upgrades and schema

The current catalog offers **1.31.4 → 1.31.5**, **1.32.1 → 1.32.2**, and either 1.31 patch → either 1.32 patch. Prefer the security-patched **1.31.5** or **1.32.2** (default); 1.31.4 and 1.32.1 are deprecated but retained for existing clusters. Admission requires locked artifacts. The version policy permits forward patches within a minor or exactly one minor forward; skipping minors and downgrades are rejected. Do not bypass the advertised `UpgradePaths`.

```sh
karactl -n NS clusters upgrade workflows --version 1.32.2 --wait
```

Before starting migration, reconciliation waits until every current component is healthy. It rolls history → matching → frontend → worker → server → ui, checking all instances at the desired version/configuration before advancing. Split history gets a one-minute settle period for shard ownership. The UI binary remains release-locked even when server version changes.

Every server member ensures schema before starting. The **first upgraded member** applies migrations with its bundled `temporal-sql-tool`; a PostgreSQL advisory lock on the persistence database serializes schema setup/update for both persistence and visibility. Repeated starts converge rather than racing migrations. Temporal's startup schema compatibility checks remain in force.

The locked **1.31.4, 1.31.5, 1.32.1 and 1.32.2 all embed persistence schema 1.19 and visibility schema 1.14**. Official server archives carry schemas embedded in `temporal-sql-tool`, not separate schema directories. The upstream PostgreSQL schema trees are byte-for-byte unchanged across each patch pair, so the existing `postgresql/v12/temporal` and `postgresql/v12/visibility` migration selectors and advisory-lock handling remain applicable. These upgrades do not imply a schema-version increment. Inspect `TemporalSchema` and `TemporalUpgrade` conditions, observed version, members and generation—not just HTTP acceptance.

### v0.4.7 compatibility notes

[Server 1.31.5](https://github.com/temporalio/temporal/releases/tag/v1.31.5) and [1.32.2](https://github.com/temporalio/temporal/releases/tag/v1.32.2) tighten **`ValidateWorkerDeploymentVersionComputeConfig` to require namespace Write permission** because it has persistent side effects. Clients previously authorized only for namespace reads can now receive permission-denied responses for that API; review their authorization policy before upgrading. This is not a blanket requirement for all namespace reads. Karakoram's frontend mTLS identity configuration does not itself install an upstream namespace authorizer.

The UI is upgraded from 2.56.0 to **2.58.0** for security maintenance, including the upstream go-jose remediation. The [2.57](https://github.com/temporalio/ui/releases/tag/v2.57.0) and [2.58](https://github.com/temporalio/ui/releases/tag/v2.58.0) changes retain the rendered OIDC, frontend mTLS, `publicPath`, listener and `--root` / `--config` / `start` contract. New banner settings (`banner.text` / `banner.color`, or upstream Docker-template variables `TEMPORAL_UI_BANNER_TEXT` / `TEMPORAL_UI_BANNER_COLOR`) are optional; Karakoram does not render a banner or use the upstream Docker environment template. No UI configuration migration is required. Verify private UI access, authenticated public OIDC callback/logout and namespace/workflow views after rollout.

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

Creation with `spec.dataSource` is rejected: Temporal does not implement initial clone recovery. Use the recorded in-place restore workflow below; a healthy new Temporal deployment is not proof that source workflow data was imported.

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
