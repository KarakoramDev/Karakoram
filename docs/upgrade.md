<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Upgrade and recover Karakoram

Upgrade the control plane and recover its state; engine-version changes are separate operations documented under each engine.

## Upgrading from v0.4.0 to v0.4.1

Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY`
before replacing the control plane. Engine packages and member agents are separate
from the control-plane executable; follow [member-agent rollout](#rolling-out-member-agents).

### PostgreSQL exact versions and existing version drift

In v0.4.0, installing PostgreSQL profiles could upgrade a previously exact-pinned
server as an APT dependency. For example, a cluster assigned Percona PostgreSQL
17.10.1 with PostGIS could actually run 17.11.1 while member status still reported
17.10.1. This affects the shared PostgreSQL/Supabase core on all backends.
Native-artifact mirrors do not freeze the separate APT repositories.

**Before refreshing existing member agents, changing profiles/resources, or
replacing a data runtime, inventory every member with `SELECT version()` over a
verified connection.** Old agents echo assigned versions, so v0.4.0 status is not
proof of the installed release. Record SQL results and take a backup. If a
member runs a newer catalog release than requested, explicitly update the
cluster's desired release to that observed release (or a supported newer one)
before any runtime preparation. Do not install older packages over the existing
data directory to make the binary match the old specification.

For PostgreSQL 16, SQL and older agents may report only the upstream release
`16.15`, with no Percona package revision. Select catalog version `16.15.1`
for that release (or a newer supported same-major catalog version). Missing
product revisions are not treated as downgrades or unresolved drift; an explicitly
reported newer product revision still blocks an older pin. The corrective engine
upgrade rolls members normally, including when they still run v0.4.0 agents.
Wait for its generation to converge before `upgrade-agents`. An agent-only rollout
before correcting the stale database pin is rejected rather than reinstalling
older database packages. Public member versions retain the actual SQL release,
which can remain `16.15` after alignment to catalog `16.15.1`.

v0.4.1 keeps the requested server package as the APT candidate throughout profile
installation. Updated member agents report the running Percona product release,
not configuration intent. An unavailable SQL version observation is empty.
An unexpected observed data-member release raises `VersionDrift=True` with the
member, observed/desired releases and recovery guidance, and the cluster is not
Ready. Expected source releases during a forward rollout remain Updating
instead of producing drift incidents. A confirmed mismatch survives missing
observations as `VersionDrift=Unknown` until fresh SQL confirms the target;
missing observations alone do not create drift on unaffected members. The
specification is never rewritten automatically. Preparing a runtime below its
observed server release is rejected, including agent-only redeployment.

For an affected 17.10.1-assigned/17.11.1-running cluster, explicitly select
17.11.1, complete the same-major reconciliation and agent refresh, verify SQL and
member status agree on every member, then take a new backup. Only then request an
adjacent major upgrade. Major-upgrade admission reports unresolved assignment/
observation drift explicitly rather than rejecting an actual-release backup as
though no current backup existed. Review [PostgreSQL version-drift recovery](postgres/README.md#existing-version-drift)
before proceeding. This upgrade neither downgrades engines nor recreates clusters.

### PostgreSQL restores stuck at access

v0.4.0 accidentally sent every managed-role recovery password request through
the binding mutation scope. The agent rejected it before SQL execution because
normal binding mutations require an OID-fenced journal. This affects every
backend, including single-member and HA PostgreSQL/Supabase; it requires a
managed login role present in the recovered catalog. It is not an IPv6, TLS,
extension-profile or PostgreSQL minor-version error. v0.2.0 and v0.3.0 do not
contain this regression.

An affected restore is Failed and the cluster Degraded at `access`, with client
HBA rules and proxy users intentionally held closed. **Do not submit another
restore or edit HBA files.** Preserve the control-plane database/master key,
backup objects and sealed recovery credentials.

For a stuck cluster with only ordinary managed roles (no `kb_` binding roles),
deploy the fixed control plane first, preserving its state and master key.
The fixed control plane uses the original password-only protocol accepted by
v0.4.0 member agents: the existing journal automatically resumes, rekeys roles,
publishes access and releases client holds. No new API action is required.
Wait for `RestoreRecovery` to clear, restore-resource success, working verified
SQL and fresh cluster readiness before refreshing agents. The original Failed
operation remains historical evidence. If version drift also exists, resolve
the actual/desired release mismatch as described above **after recovery and
before any member redeployment**; this recovery does not downgrade PostgreSQL.

New control-plane/member-agent pairs also support password-only recovery of
binding identities, gated by the member's client-held state. **An already stuck
v0.4.0 source containing recovered `kb_` roles cannot use `upgrade-agents` as an
escape:** its restore journal owns the operation queue. Skipping binding rekey
is not safe: normal binding ownership checks do not reapply a password rotated
since the backup, and could leave a nominally repaired consumer unable to log
in. There is no in-place legacy-agent bypass. Preserve the backup and use the
supported delete/new-target recovery in
[failed restore and client holds](postgres/backups.md#failed-restore-and-client-holds).
Remove dependent consumers/bindings through their supported lifecycle first if
they prevent source deletion. Do not copy executables into running containers.

Before another restore, complete a supported member-agent rollout on a healthy
cluster. A failed destructive restore never opens incomplete data merely
because a retry budget expires: repair and automatic continuation, or explicit
target abandonment and new-target recovery, are the supported outcomes.

## Upgrading from v0.3.0 to v0.4.0

**Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY` before starting v0.4.0.** Use the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup); retain the old binary and state together for recovery. Existing namespaces, clusters, credentials and policies are retained; upgrading does not migrate their provider or opt them into new engine topologies.

### Automatic state changes

Opening the store at startup applies these migrations in filename order, skipping recorded migrations. Each SQL migration and its completion record commit in one transaction; no separate migration command is needed.

| Migration | Purpose |
| --- | --- |
| `013_postgres_bindings.sql` | PostgreSQL consumer bindings, source identity and dependency metadata |
| `014_ops_cluster_fields.sql` | Adds managed-cluster ownership and maintenance-override generation fields |
| `015_notifications.sql` | Durable notification delivery outbox and retry indexes |
| `016_control_plane_backups.sql` | Control-plane backup run records and single-active-run index |
| `017_backup_drills.sql` | Backup drill reports and cluster/active-run indexes |

Migrations are **forward-only**. Downgrading an upgraded store to v0.3.0 is not supported; an older executable is not a schema rollback. To roll back, stop the new control plane and restore the consistent pre-upgrade state with its original master key and matching old binary. This does not roll back engine data or infrastructure changes made after the snapshot.

### PostgreSQL binding database access

[PostgreSQL](https://www.postgresql.org/) bindings are new in v0.4.0. Databases Karakoram creates for [PostgreSQL bindings](postgres/bindings.md) do not keep PostgreSQL's default PUBLIC `CONNECT` and `TEMPORARY` privileges: Karakoram revokes both from PUBLIC and grants them only to the binding's owning role; explicit grants Karakoram manages, such as Data [MCP](https://modelcontextprotocol.io/) readers scoped to that database, are kept. Another binding's role cannot connect to a consumer's database.

Karakoram re-checks these privileges on reconcile, not through the [SQLite](https://sqlite.org/) migration: if PUBLIC access reappears (for example, granted by hand or restored from older data), it is revoked again and sessions of roles without `CONNECT` are disconnected. Applications must use the owning binding identity or deliberately managed grants. This concerns binding databases only, not a blanket revocation on unrelated PostgreSQL databases.

### Source-build prerequisites

[`go.mod`](../go.mod) now declares **`go 1.26.0`**, with no separate `toolchain` directive. Source builders need [Go](https://go.dev/) 1.26 or newer; the [README build prerequisites](../README.md#build-from-source) also require [Node](https://nodejs.org/) 22.6+, [npm](https://docs.npmjs.com/) and [Make](https://cgit.git.savannah.gnu.org/cgit/make.git/). `make build-all` builds the UI before both executables; `make build` alone does not build UI assets. Prebuilt installations need no Go or Node toolchain.

### New runtime environment variables

`internal/server/config.go` adds no settings in this release. These new variables are read by the operational controllers/transports or offline recovery command:

| Variable | Default | Effect |
| --- | --- | --- |
| `KARAKORAM_BACKUP_DRILL_CONCURRENCY` | `1` | Installation-wide concurrent drill limit, integer 1–4; invalid values fail startup |
| `KARAKORAM_NOTIFY_ENDPOINT_ALLOWLIST` | unset | Comma-separated exact `host` or `host:port` exceptions for notification destinations; see [outbound safety](notifications.md#delivery-and-outbound-safety) |
| `KARAKORAM_RESTORE_S3_ACCESS_KEY_ID` | unset | S3 access key for offline `karakoram admin restore-state` |
| `KARAKORAM_RESTORE_S3_SECRET_ACCESS_KEY` | unset | S3 secret key for offline `karakoram admin restore-state` |

Restore credentials are needed only for [S3](https://aws.amazon.com/s3/) recovery, not ordinary startup. `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` and `KARAKORAM_ARTIFACT_MIRROR` already existed; they are not new settings. Native-test binary paths and development hooks are not installation requirements. See [notifications](notifications.md), [backup drills](backup-drills.md) and [offline state restore](control-plane-backup.md#local-export-and-offline-restore).

### New native artifacts

Installations mirroring native artifacts must add the following exact filenames from [`lock.json`](../internal/engine/nativeartifacts/lock.json), preserving their locked SHA-256 and size. Artifacts are selected by engine/version/profile, not all downloaded at control-plane startup.

| Feature | New files |
| --- | --- |
| OpenBao | `openbao_2.7.1_linux_amd64.tar.gz`, `openbao-plugin-database-mongodb_linux_amd64_v1.tar.gz`, `openbao-plugin-kms-aws_linux_amd64_v1.tar.gz` |
| Keycloak | `keycloak-26.8.1.tar.gz` |
| TimescaleDB | `timescaledb-pg16-2.31.0-ubuntu24.04-amd64.tar.gz`, `timescaledb-pg17-2.31.0-ubuntu24.04-amd64.tar.gz`, `timescaledb-pg18-2.31.0-ubuntu24.04-amd64.tar.gz` |
| Citus | `citus-pg16-14.2.0-ubuntu24.04-amd64.tar.gz`, `citus-pg17-14.2.0-ubuntu24.04-amd64.tar.gz`, `citus-pg18-14.2.0-ubuntu24.04-amd64.tar.gz` |
| Temporal | `temporal_1.31.4_linux_amd64.tar.gz`, `temporal_1.32.1_linux_amd64.tar.gz`, `ui-server_2.56.0_linux_amd64.tar.gz` |
| Percona Server | `Percona-Server-8.4.11-11-Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-Server-8.4.10-10-Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-Server-8.0.46-37-Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-Server-8.0.45-36-Linux.x86_64.glibc2.35-minimal.tar.gz` |
| PXC | `Percona-XtraDB-Cluster_8.4.11-11.1_Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-XtraDB-Cluster_8.0.46-38.1_Linux.x86_64.glibc2.35-minimal.tar.gz` |
| XtraBackup | `percona-xtrabackup-8.4.0-7-Linux-x86_64.glibc2.35-minimal.tar.gz`, `percona-xtrabackup-8.0.35-36-Linux-x86_64.glibc2.35-minimal.tar.gz` |
| Neo4j / Java | `neo4j-community-2026.09.0-unix.tar.gz`, `neo4j-community-2026.10.0-unix.tar.gz`, `OpenJDK21U-jre_x64_linux_hotspot_21.0.12.1_1.tar.gz` |
| Weaviate | `weaviate-v1.40.1-linux-amd64.tar.gz`, `weaviate-v1.39.12-linux-amd64.tar.gz` |

[TimescaleDB](https://www.timescale.com/) and [Citus](https://www.citusdata.com/) bundles are Karakoram-built; development builds need a mirror for them. Other additions have official upstream URLs. [Weaviate](https://weaviate.io/) is upstream-only, mixed-license and **not redistributed as a Karakoram release payload**. A private mirror is not permission to redistribute upstream software. See [native artifacts](native-artifacts.md) for resolution order, provenance and licensing.

### Deploy on Zerops

After the state/key backup, deploy the published tag from the checkout root:

```sh
git fetch origin --tags
git checkout v0.4.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Set `PROJECT_ID` as below, then follow [deployment and readiness](#deployment-and-readiness) and [member-agent rollout](#rolling-out-member-agents). Review [engine/provider status](providers.md#engine-availability) and the [verified-offline Ops features](operations.md) before enabling new functionality.

[Temporal](https://temporal.io/) is available on [Dokploy](https://dokploy.com/) and [Nomad](https://developer.hashicorp.com/nomad) only in v0.4.0. [Zerops](https://zerops.io/) support is pending IPv6 private networking: the current Temporal membership runtime requires a private IPv4 route to the bound PostgreSQL writer.

## Upgrading from v0.2.0 to v0.3.0

**Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY` before starting v0.3.0**, as for every upgrade ([consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup)). Namespaces, clusters, secrets, policies and API tokens are retained; no cluster recreation is required.

### Automatic state changes

No [SQLite](https://sqlite.org/) migrations are added after `012_sanitize_diagnostics.sql`. [OAuth](https://oauth.net/2/) grants and API-token restrictions use existing tables, and dry-run previews run in read-only transactions. Migrations remain forward-only; keep the pre-upgrade backup for rollback.

### Agent access is on, read-only

After the upgrade the control plane serves the [MCP server](mcp.md) at `/mcp` because `KARAKORAM_MCP` defaults to `readonly`: holders of valid API tokens, including existing unrestricted ones, can call read-only tools through the same RBAC and audit path as the REST API. Nothing becomes readable that the token could not already read. To disable the endpoint, set `KARAKORAM_MCP=off` in the control plane's service environment ([Zerops](https://zerops.io/): the service's environment variables, or `envVariables` in the import YAML) and restart; set `readwrite` only deliberately. New variables, all documented in [Install](install.md#required-secrets-and-generated-settings):

| Variable | Default | Effect |
| --- | --- | --- |
| `KARAKORAM_MCP` | `readonly` | `off`, `readonly` or `readwrite`; invalid values fail startup |
| `KARAKORAM_OAUTH` | `off` | Embedded OAuth authorization server for MCP connectors |
| `KARAKORAM_OAUTH_DCR` | `off` | Dynamic client registration, only with OAuth on |
| `KARAKORAM_OAUTH_CLIENT_HOSTS` | unset | Optional allowlist of client-metadata hosts |

Existing API tokens stay unrestricted; mint [restricted tokens](access-control.md#restricted-api-tokens) for agents with `karactl mcp config`. Stored RBAC policies are kept. The new `cluster-data` resource is covered by the administrator wildcard but not by the built-in viewer or operator roles; grant it explicitly per `<namespace>/<cluster>`. [Data MCP](data-mcp.md) stays off for every cluster until `spec.dataMcp.enabled: true` is set on that cluster.

### New native artifacts

Data MCP companions for [MongoDB](https://www.mongodb.com/), [PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) use two new entries in `lock.json`, downloaded only when a cluster enables data MCP: `mongodb-mcp-server-3.1.0-ubuntu24.04-amd64.tar.gz` is a Karakoram-built release asset with no upstream URL, and `pgedge-postgres-mcp-server_1.1.0_linux_x86_64.tar.gz` comes from the official [pgEdge](https://www.pgedge.com/) release. Operators running a `KARAKORAM_ARTIFACT_MIRROR` must add both files with their exact locked bytes; development builds need the mirror for the MongoDB bundle. See [data MCP companion artifacts](native-artifacts.md#data-mcp-companion-artifacts).

### Deploy on Zerops

After the state/key backup, deploy the published tag from the checkout root:

```sh
git fetch origin --tags
git checkout v0.3.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Set `PROJECT_ID` as below, then follow [deployment and readiness](#deployment-and-readiness) and [member-agent updates](#rolling-out-member-agents). Data MCP companions are new private services created only when a cluster opts in.

## Upgrading from v0.1.x to v0.2.0

**Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY` before starting v0.2.0.** Use the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup) below, not a live copy of the [SQLite](https://sqlite.org/) main file. Let engine operations settle and retain the old revision and backup for recovery.

Existing [Zerops](https://zerops.io/) namespaces and clusters retain their provider, resource identities, secrets and desired specifications; no cluster recreation or provider migration is required. Stored namespaces without a provider kind normalize to `zerops`, and omitted namespace-create kinds still default to Zerops. [PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) are new engines; upgrading the control plane does not change an existing cluster's engine driver or engine version.

### Automatic state changes

Startup applies the following migrations added after v0.1.2, in order, without a separate migration command:

| Migration | Purpose |
| --- | --- |
| `010_lifecycle_recovery.sql` | Marks failed delete tracking successful when the matching cluster incarnation is already absent; releases backup inventory stuck in `Deleting` after terminal failed/cancelled deletion so deletion can be retried. |
| `011_metrics_retention.sql` | Adds the `metrics(timestamp,id)` index for bounded retention scans. |
| `012_sanitize_diagnostics.sql` | Rewrites legacy diagnostic JSON and event details using the current sanitizer in bounded, resumable batches; leaves secret values and row metadata intact. |

Migration 011 builds an index over existing metrics; migration 012 rewrites legacy diagnostics. These can extend first startup on large stores. Measure on an isolated copy and allow enough readiness time; see [Monitoring](monitoring.md#upgrading-an-existing-metrics-database). Migrations are forward-only: an older binary is not a schema rollback.

### API and specification compatibility

The `/api/v1` API and existing Zerops cluster spec shapes remain. The [OpenAPI](https://www.openapis.org/) changes are principally additive, but clients must account for these response and admission changes:

- Namespace responses now require `backend` and `capabilities` and may include non-secret `config`; namespace creation accepts optional `backend` and `config`. `projectId` remains the field name for the provider target ID. Strict clients that reject new response properties need updated models.
- `GET /backends` lists compiled-in provider metadata and requires `namespaces/read` on global `*`.
- Cluster create/PUT/PATCH reject missing provider capabilities with `400 invalid_request`. A newly selected public IP family that fresh provider information proves unavailable is rejected; unchanged legacy exposure is preserved and unknown availability does not reject admission.
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

Set `PROJECT_ID` as below. Preserve the existing volume, master key, secrets, public origin and single-writer deployment settings. Then follow [deployment and readiness](#deployment-and-readiness) and [member-agent updates](#rolling-out-member-agents); upgrading the control-plane binary alone does not replace running member binaries.

### New optional providers

Zerops remains the reference provider. [Dokploy](https://dokploy.com/) and [Nomad](https://developer.hashicorp.com/nomad) are additional options, not an automatic migration for existing installations. Review [Providers](providers.md), then the [Dokploy installation](dokploy.md#install-the-control-plane-on-dokploy) or [Nomad installation](nomad.md#install-the-control-plane-on-nomad). Keep the existing default namespace on its original target; additional targets use explicit provider selection and credentials through [Namespaces](namespaces.md).

## Prerequisites

For new installations use [Install](install.md). The concrete deployment, maintenance-container and ownership commands below are for the **[Zerops](https://zerops.io/) provider**. [Dokploy](dokploy.md#install-the-control-plane-on-dokploy) and [Nomad](nomad.md#install-the-control-plane-on-nomad) use their guides' singleton/stop-first deployment paths with the same consistent-state and master-key backup requirements. Zerops upgrades need the target checkout, authenticated [zcli](https://docs.zerops.io/references/cli) with deployment access, runtime secrets and Local Storage maintenance-container access. Use `zcli login` with a Zerops token; avoid shared terminals/shell history.

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

**Warning:** A live copy of [SQLite](https://sqlite.org/)'s main file or a live volume tarball can omit committed WAL changes. There is no `karakoram admin backup-db` command.

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
git checkout v0.4.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Choose your target tag, or update `main`: [`deploy/zerops/RELEASE`](../deploy/zerops/RELEASE) pins the latest published release, not newer source. Tags pin themselves. **Never edit the pin by hand**; only the [release tool](../CONTRIBUTING.md#releasing) changes it.

The default setup verifies the manifest's SSH signature, archive SHA-256 and binary version; allow seconds, with no [Go](https://go.dev/)/[Node](https://nodejs.org/) toolchain. `--workspace-state clean` uploads only committed `HEAD`; do not add `--no-git`, which uploads the whole checkout.

For [Git](https://git-scm.com/)-connected services, redeploy the chosen tag or `main` through [Zerops](https://zerops.io/) controls; its ref determines the pin. Installer `buildFromGit` is a one-time import, not webhook configuration. Never rerun the helper or reimport to upgrade.

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

## Rolling out member agents

Upgrading or restarting the control plane does **not** replace existing member agents. A member receives the current executable on its next control-plane-managed deployment. Do not copy binaries into running containers or add another supervisor.

Inspect `karactl clusters get NAME` or `clusters status NAME`: `status.agents` reports `targetBuild`, `targetVersion`, and the `outdated` and `unknown` member names. Member output includes `agent.version`, `agent.commit` and `agent.build`. The `AgentsCurrent` condition is informational: `True/Current`, `False/Outdated`, or `Unknown/Unreported`; it never makes a cluster unready or Degraded. An `AgentsOutdated` event signals outdated member agents.

```sh
karactl clusters upgrade-agents NAME --wait --timeout 30m
# Include agents already reporting the current build:
karactl clusters upgrade-agents NAME --include-current --wait
```

The durable `upgrade-agents` operation redeploys the current executable at the **same engine version**, using each engine's safe, one-member-at-a-time rolling sequence and health/quorum gates. Unknown/legacy agents are eligible for a manual run. If every agent is current, the API returns HTTP 409 with code `AgentsCurrent` unless `--include-current` is set. Cancel with `karactl operations cancel ID`; cancellation is cooperative between members, not a rollback of completed deployments.

Rollouts respect [maintenance windows](operations.md#maintenance-windows). Use `--ignore-maintenance-window` for an explicit override; this does not bypass engine safety gates. Timing out or interrupting `--wait` does not cancel the rollout; retain the operation ID and use `operations wait ID`.

Agent rollout requires an advertised engine driver operation backed by a one-member disruption journal. [Keycloak](https://www.keycloak.org/), [OpenBao](https://openbao.org/), [Temporal](https://temporal.io/), [MySQL](https://www.oracle.com/mysql/), [PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster), [Neo4j](https://neo4j.com/) and [Weaviate](https://weaviate.io/) do not currently implement that contract: automatic rollout reports unsupported agents without scheduling any disruption, and manual rollout is rejected. Enabling automatic rollout never opts these engines into a generic restart or redeployment fallback.

Automatic rollout is **off by default**:

```sh
karactl settings agent-rollout get
karactl settings agent-rollout set --automatic=true --max-concurrent-clusters 2
karactl settings agent-rollout set --automatic=false --max-concurrent-clusters 1
```

These installation-wide settings include a read-only `currentBuild` (`version`, `commit`, `build`). `set` replaces the settings; omitted flags default to `automatic=false` and `maxConcurrentClusters=1` (allowed range **1–5**).

When enabled, the controller checks every **60 seconds**, enqueuing rollouts only for clusters with **outdated** agents, bounded by `maxConcurrentClusters` across the installation and respecting maintenance windows. Unknown/legacy agents alone do not trigger automatic rollout: run manually. A failed rollout for a target build is **not retried automatically** for that build; fix the cause and re-run manually.

## Rollback

- A pre-deployment build failure leaves the current application unchanged. After temporary shutdown, inspect service/process state; start/redeploy the previous version if necessary.
- Redeploy an old binary only if it supports the current schema. Migrations are forward-only; a pre-namespace binary requires pre-upgrade state.
- Otherwise stop **every writer**, preserve failed-version state, and restore the complete pre-upgrade archive into an **emptied** state directory through the maintenance container. Never merge stale WAL/SHM or identity files. Restore `zerops` ownership, restrictive permissions and the matching master key; deploy the old binary before starting it.
- State rollback rewinds specifications, operations, tokens, sessions and inventory—not external services or engine data. Review changes and in-flight operations before resuming reconciliation. It does not undo engine-level upgrades or compatibility changes; see [MongoDB](mongodb/operations.md), [RabbitMQ](rabbitmq/operations.md) and [Elasticsearch operations](elasticsearch/operations.md), and [Backups and storage](backups.md) for data recovery.

For [Weaviate](https://weaviate.io/), follow [native upgrade operations](weaviate/operations.md#restart-and-upgrade): only cataloged patch-forward or one-minor-forward changes, never downgrades or skipped minors. Members roll with quorum/health gates; restoring the control-plane archive does not reverse native data changes.

Retain the archive, master key and previous revision until the upgrade and agent transition are accepted.

## Version notes

### Unreleased: fleet operations and recovery

- Member build identity and explicit rolling agent upgrades keep executable rollout separate from database-version upgrades. Automatic rollout is opt-in; existing and legacy agents are not redeployed merely because the control plane restarts.
- Optional cluster maintenance windows defer new disruptive work. Emergency healing and near-expiry certificate recovery remain exempt; explicit overrides never bypass engine safety gates.
- Placement reports distinguish authoritative anti-affinity from best-effort evidence. Member relocation uses native replacement and synchronization, and cannot promise a new host on providers without placement support.
- Control-plane backup policy, durable notification deliveries and backup-drill reports add forward-only [SQLite](https://sqlite.org/) migrations. Preserve a consistent pre-upgrade snapshot and the original master key; an older binary must not open newer state.
- State snapshots do not contain the operator master key or external provider credentials. Rebuild with the original key, endpoint identity and valid credentials; review external changes since the snapshot before resuming reconciliation.
- Import the [Grafana dashboards and Prometheus rules](monitoring.md) and configure [notification channels](notifications.md) deliberately. Existing installations retain their RBAC policy; installation-wide settings still require explicit settings access.

### v0.1.2

- Default [Zerops](https://zerops.io/) deployments use signed prebuilt releases pinned by the checkout, not unreleased source; forks/unreleased fixes require `karakoram-source`. Preserve the state volume, master key and single-writer deployment settings.
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
