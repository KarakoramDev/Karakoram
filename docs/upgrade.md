<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Upgrade and recover Karakoram

Upgrade the control plane and recover its state; engine-version changes are separate operations documented under each engine.

## Upgrading from v0.1.x to v0.2.0

**Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY` before starting v0.2.0.** Use the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup) below, not a live copy of the SQLite main file. Let engine operations settle and retain the old revision and backup for recovery.

Existing Zerops namespaces and clusters retain their backend, resource identities, secrets and desired specifications; no cluster recreation or backend migration is required. Stored namespaces without a backend kind normalize to `zerops`, and omitted namespace-create kinds still default to Zerops. PostgreSQL and Supabase are new engines; upgrading the control plane does not change an existing cluster's provider or engine version.

### Automatic state changes

Startup applies the following migrations added after v0.1.2, in order, without a separate migration command:

| Migration | Purpose |
| --- | --- |
| `010_lifecycle_recovery.sql` | Marks failed delete tracking successful when the matching cluster incarnation is already absent; releases backup inventory stuck in `Deleting` after terminal failed/cancelled deletion so deletion can be retried. |
| `011_metrics_retention.sql` | Adds the `metrics(timestamp,id)` index for bounded retention scans. |
| `012_sanitize_diagnostics.sql` | Rewrites legacy diagnostic JSON and event details using the current sanitizer in bounded, resumable batches; leaves secret values and row metadata intact. |

Migration 011 builds an index over existing metrics; migration 012 rewrites legacy diagnostics. These can extend first startup on large stores. Measure on an isolated copy and allow enough readiness time; see [Monitoring](monitoring.md#upgrading-an-existing-metrics-database). Migrations are forward-only: an older binary is not a schema rollback.

### API and specification compatibility

The `/api/v1` API and existing Zerops cluster spec shapes remain. The OpenAPI changes are principally additive, but clients must account for these response and admission changes:

- Namespace responses now require `backend` and `capabilities` and may include non-secret `config`; namespace creation accepts optional `backend` and `config`. `projectId` remains the field name for the backend target ID. Strict clients that reject new response properties need updated models.
- `GET /backends` lists compiled-in backend metadata and requires `namespaces/read` on global `*`.
- Cluster create/PUT/PATCH reject missing backend capabilities with `400 invalid_request`. A newly selected public IP family that fresh backend information proves unavailable is rejected; unchanged legacy exposure is preserved and unknown availability does not reject admission.
- `POST /clusters/{cluster}/retry-deployments` explicitly re-arms exhausted deployment budgets. `spec.retryEpoch` is server-managed/read-only, not an ordinary spec-edit control.
- Backup or source-cluster deletion can return `409 invalid_state` while an in-flight restore/create depends on it; PITR dependencies also protect the source/history. Wait for completion or safe cancellation rather than bypassing the guard.
- PostgreSQL role/grant and Supabase credentials, SMTP, functions and JWT-rotation endpoints are added. Topology, backup-type, member-role and credential enums gain engine-specific values; member responses may include `componentVersion`, and agent status may include `message`. Some engine-administration reads now document `503` for unavailable dependencies.
- Restore readiness is explicitly false during recovery and remains false with `RestoreFinished` until ordinary reconciliation observes database health. Do not infer database readiness from operation completion alone.

Use the matching v0.2.0 CLI/SDK models; see the [API reference](api.md) and [OpenAPI contract](../api/openapi.yaml). No existing path or request property was removed in the v0.1.2-to-v0.2.0 contract diff; expanded enums, stricter admission and deletion guards are notable behavior changes, not a blanket promise that every old client handles new responses.

### Deploy on Zerops

After the state/key backup, deploy the published tag from the checkout root:

```sh
git fetch origin --tags
git checkout v0.2.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Set `PROJECT_ID` as below. Preserve the existing volume, master key, secrets, public origin and single-writer deployment settings. Then follow [deployment and readiness](#deployment-and-readiness) and [member-agent updates](#member-agent-updates); upgrading the control-plane binary alone does not replace running member binaries.

### New optional backends

Zerops remains the reference backend. Dokploy and Nomad are additional options, not an automatic migration for existing installations. Review [Backends](backends.md), then the [Dokploy installation](dokploy.md#install-the-control-plane-on-dokploy) or [Nomad installation](nomad.md#install-the-control-plane-on-nomad). Keep the existing default namespace on its original target; additional targets use explicit backend selection and credentials through [Namespaces](namespaces.md).

## Prerequisites

For new installations use [Install](install.md). The concrete deployment, maintenance-container and ownership commands below are for the **Zerops backend**. [Dokploy](dokploy.md#install-the-control-plane-on-dokploy) and [Nomad](nomad.md#install-the-control-plane-on-nomad) use their guides' singleton/stop-first deployment paths with the same consistent-state and master-key backup requirements. Zerops upgrades need the target checkout, authenticated [zcli](https://docs.zerops.io/references/cli) with deployment access, runtime secrets and Local Storage maintenance-container access. Use `zcli login` with a Zerops token; avoid shared terminals/shell history.

From the checkout root, set:

```sh
PROJECT_ID='<project-id>'
```

Commands assume default service names; substitute customized names consistently.

## Before deploying

1. Read target-release compatibility notes. Retain the previous source revision/application version.
2. Let provisioning, backup, restore, scaling and upgrade operations settle. Avoid desired-state changes during maintenance. Engine processes continue independently, but reconciliation, new operations, schedules and monitoring pause while the control plane is down; choose a window that will not miss required backups.
3. Take the consistent state backup below; preserve `KARAKORAM_MASTER_KEY` separately. State without its original key cannot decrypt credentials.
4. Keep the same volume, secrets, project ID and public origin. Never rerun the installer or regenerate the master key.

## Consistent pre-upgrade state backup

**Warning:** A live copy of SQLite's main file or a live volume tarball can omit committed WAL changes. There is no `karakoram admin backup-db` command.

1. Stop the runtime:

   ```sh
   zcli service stop karakoram -P "$PROJECT_ID"
   ```

   Wait until the stop finishes and the runtime is `STOPPED`; no control-plane writer may remain.
2. Open **karakoramdata → Web shell**. The maintenance container mounts state at `/data` (runtime: `/mnt/karakoramdata`). Archive outside it:

   ```sh
   umask 077
   tar -czf /tmp/karakoram-state-before-upgrade.tar.gz -C /data .
   ```

   Include the whole directory: database, remaining WAL/SHM sidecars and persisted identities. Never attach another running writer.
3. Download through the maintenance container's **File browser**. Store encrypted off-platform, record version/time and restrict access: the archive contains authentication state and encrypted infrastructure secrets. `/tmp` and same-volume copies are not durable protection against host/volume loss.
4. Leave stopped for immediate deployment; otherwise restart the old version:

   ```sh
   zcli service start karakoram -P "$PROJECT_ID"
   ```

See [Zerops Local Storage maintenance](https://docs.zerops.io/local-storage/how-to/manage). Automated consistent backups can use SQLite's native backup API/tooling; do not assume the runtime includes `sqlite3`.

## Deploy

### Prebuilt release (default)

After state/key backup, select the published release:

```sh
git fetch origin --tags
git checkout v0.2.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Choose your target tag, or update `main`: [`deploy/zerops/RELEASE`](../deploy/zerops/RELEASE) pins the latest published release, not newer source. Tags pin themselves. **Never edit the pin by hand**; only the [release tool](../CONTRIBUTING.md#releasing) changes it.

The default setup verifies the manifest's SSH signature, archive SHA-256 and binary version; allow seconds, with no Go/Node toolchain. `--workspace-state clean` uploads only committed `HEAD`; do not add `--no-git`, which uploads the whole checkout.

For Git-connected services, redeploy the chosen tag or `main` through Zerops controls; its ref determines the pin. Installer `buildFromGit` is a one-time import, not webhook configuration. Never rerun the helper or reimport to upgrade.

Download, signature/checksum or build failure never reaches deployment and leaves the running version untouched. A service left stopped after backup remains stopped until started or successfully deployed.

### Source builds and provenance

Use `--setup karakoram-source` for the exact checked-out commit, including UI and static Go binary (minutes): forks, development or unreleased fixes. Git imports select `zeropsSetup: karakoram-source`.

Before source pushes without Git metadata, merge literal `git describe --tags --always` / `git rev-parse HEAD` output into the source setup's `build.envVariables`; see [source provenance](install.md#build-provenance-for-local-pushes). Explicit commit overrides discovery; the pipeline records UTC build time. These are build inputs, not runtime settings. Prebuilt binaries already carry provenance; missing source metadata appears as `unknown (built without git metadata)` in the API/UI.

Prefer `--workspace-state clean` and commit intended source/pipeline changes before pushing. `--no-git` uploads the whole checkout: keep private imports outside it and exclude other sensitive files first. The [source helper](install.md#one-command-helper) uses this no-Git path.

### Deployment and readiness

Both setups replace the executable in the existing runtime. Keep `deploy.temporaryShutdown: true` and one-container maximum: stop the old writer before starting the new one. Brief UI/API downtime is intentional; do not use overlapping deployments. Automatic state migrations need no separate command. Deployment probes `/healthz` on port 8080 (120-second failure timeout, 10-second retries). Wait for deployment completion and `ACTIVE`, then check:

```sh
curl --fail --show-error https://example.com/healthz
karactl --context production health
karactl --context production version
```

Check clusters, operations, backup inventory, member agents and logs without exposing secrets. Healthy control-plane readiness does not prove engine recovery works.

## Existing single-project installations

Existing clusters, events and metrics enter namespace `default`; existing secrets and master key remain valid. Stored policy is normalized: `*` becomes `**`, while bare cluster patterns such as `orders*` become `default/orders*`, retaining their old project scope. Namespace rules keep bare names. Review [access control](access-control.md).

Upgrades retain stored policy rather than adding fresh-install namespace-read defaults. If operators/viewers should enumerate namespaces, merge these rules into the **complete existing policy**, preserving administrator access; use narrower grants where appropriate:

```csv
p, role:operator, namespaces, read, **
p, role:viewer, namespaces, read, **
```

Without namespace-read grants the UI shows only `default`, even with remote-cluster access. Remote relays update automatically; expect brief management interruptions and check desired/reported versions and connected state in [Namespaces](namespaces.md#relay-health).

## Member-agent updates

A control-plane restart alone does not update existing member agents. Members receive the current executable on their next control-plane-managed member deployment. Plan for mixed agent versions, check compatibility before upgrading, and inspect health/version after rollout. Do not copy binaries into running containers or add another supervisor.

## Rollback

- A pre-deployment build failure leaves the current application unchanged. After temporary shutdown, inspect service/process state; start/redeploy the previous version if necessary.
- Redeploy an old binary only if it supports the current schema. Migrations are forward-only; a pre-namespace binary requires pre-upgrade state.
- Otherwise stop **every writer**, preserve failed-version state, and restore the complete pre-upgrade archive into an **emptied** state directory through the maintenance container. Never merge stale WAL/SHM or identity files. Restore `zerops` ownership, restrictive permissions and the matching master key; deploy the old binary before starting it.
- State rollback rewinds specifications, operations, tokens, sessions and inventory—not external services or engine data. Review changes and in-flight operations before resuming reconciliation. It does not undo engine-level upgrades or compatibility changes; see [MongoDB](mongodb/operations.md), [RabbitMQ](rabbitmq/operations.md) and [Elasticsearch operations](elasticsearch/operations.md), and [Backups and storage](backups.md) for data recovery.

Retain the archive, master key and previous revision until the upgrade and agent transition are accepted.

## Version notes

### v0.1.2

- Default Zerops deployments use signed prebuilt releases pinned by the checkout, not unreleased source; forks/unreleased fixes require `karakoram-source`. Preserve the state volume, master key and single-writer deployment settings.
- Signed CLI checksum manifests start with v0.1.2; [verify the signature and checksum](install.md#install-karactl) and match the deployed server version.
- Cluster specification changes record `ClusterSpecChangeRequested` audit events with field-level details for `audit:read`; see [Reviewing audit activity](operations.md#reviewing-audit-activity).
- Platform resource-update rejections expose the hostname, error code and attempted bounds; see [Resources and autoscaling](clusters.md#resources-and-autoscaling).

### v0.1.1

Healthy clusters need no action. Project-scoped Full access tokens work without sudo mode; existing resources gain current ownership evidence during reconciliation.

### Legacy installations (before v0.1.1)

For unverified ownership or repeated `zerops HTTP 400 (processNotFound)`, upgrade to v0.1.1 or newer first. Expired imports have unknown outcomes: reconciliation observes existing services and imports missing ones; other expired actions use bounded retries.

If initial provisioning still reports unverified ownership, choose one recovery path:

- Delete only never-deployed, data-free runtime/volume services from the failed import so reconciliation recreates them. A member volume appends `v` to its hostname (e.g. `<cluster>m0v`). **Never delete healthy members or retained data.**
- Supply a **temporary**, project-scoped Full access sudo-mode token for **one ownership-verification reconcile only**. For `default`, change `KARAKORAM_ZEROPS_TOKEN` and restart. Then restore the non-sudo token, restart again and **revoke the temporary sudo token**. Other namespaces use [token rotation](namespaces.md#add-validate-and-rotate).

Hostnames alone never prove [service ownership](security.md#service-ownership).
