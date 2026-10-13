<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->

# Karakoram end-to-end suite

These tests operate a **deployed Karakoram in your own Zerops project**, not an in-process server. Requests use the generated REST client over verified HTTPS. MongoDB checks use the MongoDB driver over public TCP IPv6; RabbitMQ checks use AMQP 0-9-1 over public AMQPS IPv6. Both verify the cluster CA returned by the connection API. The test host does not need a Zerops token, zcli login or private-network DNS access.

Use a disposable deployment and bucket: tests create paid services, delete resources, restore database contents and temporarily change global authorization/SSO settings. API/CLI tests do not exercise the rendered web UI; see the [verification matrix and browser checklist](../../docs/verification.md).

## Prerequisites and environment

Install Karakoram using the [installation guide](../../docs/install.md), complete the admin password change and create an admin API token. Public data-plane scenarios require the test host to have IPv6 connectivity from the configured source CIDR. Supply credentials through your secure environment mechanism; never enable shell tracing or put tokens/passwords in command arguments.

| Variable | Meaning |
| --- | --- |
| `KARAKORAM_E2E_URL` | Your deployment's public HTTPS origin; trailing `/api/v1` is accepted. Control-plane certificate verification remains enabled. |
| `KARAKORAM_E2E_TOKEN` | Existing admin API token, supplied securely through the environment. |
| `KARAKORAM_E2E_ALLOW_CIDR` | Authorized IPv6 source CIDR (`/128` or network `/64`) of the test host. Required for public MongoDB checks and RabbitMQ data-path checks; the suite requests `IPV6` exposure with this allowlist. RabbitMQ API phases still run without it, with explicit skipped data-path subtests. |
| `KARAKORAM_E2E_ALLOWLIST_OBSERVATION_ONLY` | Optional observation-only mode: `1` logs the deny-only TLS probe in `TestSecurity/TLSExposure` rather than asserting source rejection. Unset by default. Route/spec checks and exposure deletion remain strict. A run using this option does not verify allowlist enforcement. |
| `KARAKORAM_E2E_STORAGE_NAME` | Existing HTTPS S3 backup-storage object with `forcePathStyle=true`, `verifyTLS=true`. Takes precedence over creation variables except in `TestDeletionChoices` and `TestMultiNamespace`. Existing objects are verified, not edited/deleted. |
| `KARAKORAM_E2E_STORAGE_ENDPOINT` | HTTPS S3 endpoint when creating test storage; TLS certificate verification is required. |
| `KARAKORAM_E2E_STORAGE_BUCKET` | Existing bucket; the suite never creates/deletes buckets. |
| `KARAKORAM_E2E_STORAGE_ACCESS_KEY` | S3 access key, supplied as write-only API input. |
| `KARAKORAM_E2E_STORAGE_SECRET` | S3 secret access key, supplied as write-only API input. |
| `KARAKORAM_E2E_STORAGE_REGION` | Signing region. All five storage-creation variables are required together. |
| `KARAKORAM_E2E_NAMESPACE_PROJECT_ID` | A second disposable Zerops project for `TestMultiNamespace`. Required together with its namespace token; must differ from the control plane's project. |
| `KARAKORAM_E2E_NAMESPACE_TOKEN` | Project-scoped Zerops token for that second project, supplied securely. The scenario submits it as write-only input and never logs it. |
| `KARAKORAM_E2E_NAMESPACE` | Optional namespace name for `TestMultiNamespace`; default `<KARAKORAM_E2E_PREFIX>ns`. An existing namespace is reused only if its project ID matches. |
| `KARAKORAM_E2E_PREFIX` | Optional 1–4-character lowercase alphanumeric prefix beginning with a letter; default `e2e`. Eight random hex characters keep cluster names within 12 characters. |
| `KARAKORAM_E2E_KEEP` | `1` disables final cleanup of test-owned resources. Explicit scenario actions (including deletion, suspend/resume and user/role deletion) still execute. Global RBAC/settings are always restored. |
| `KARAKORAM_E2E_KARACTL` | Already-built `karactl` executable; required by the CLI workflow. Subprocess credentials use the environment and an isolated temporary HOME. |
| `KARAKORAM_E2E_OIDC_ISSUER` | Reachable auto-approving **test-only** issuer from `test/oidc-issuer`. When configured, OIDC follows the full authorization-code/PKCE callback. Never use this issuer for production authentication. |
| `KARAKORAM_E2E_OIDC_CLIENT_ID` | Optional client ID; default `karakoram-e2e`, matching the bundled issuer. Register callback `<KARAKORAM_E2E_URL>/api/v1/auth/oidc/callback`. |
| `KARAKORAM_E2E_EVIDENCE_DIR` | Optional private output directory for JSON status/member/operation/backup/restore/metrics snapshots and test-prefix S3 object metadata. Credential/token responses are not captured. |

Missing URL/token skips deployed scenarios. Capability-specific prerequisites are checked before creating clusters. Invalid configured API values or missing advertised behavior fail; they are not silently treated as passing coverage. To exercise storage creation, omit `KARAKORAM_E2E_STORAGE_NAME` and supply all five creation variables. `TestDeletionChoices` always requires those five: it creates an isolated storage object/prefix per case and inspects its S3 snapshot objects without touching a pre-existing prefix.

`TestMultiNamespace` additionally requires the public IPv6 CIDR and all five S3 storage-creation inputs. It creates an isolated namespace-local storage regardless of `KARAKORAM_E2E_STORAGE_NAME`, then removes it before namespace deletion. The scenario reuses a matching namespace but treats it as disposable, so do not select one containing unrelated resources. Relay enrollment may take up to fifteen minutes. It creates a three-member source and a three-member clone in `default`, validates a restored marker document, checks namespace RBAC and backup export denial, and verifies that token rotation preserves the relay session. Namespace teardown waits for retained-resource guards and relay deletion; `KEEP=1` retains it and its test resources.

For OIDC setup, see the [test issuer instructions](../../docs/user-guide/rbac-and-auth.md#test-only-live-issuer). The issuer accepts public-client or basic-client authentication without validating a client secret, allowing the suite to preserve the deployment's existing write-only secret while restoring other settings. A configured issuer enables full login; no secret replacement is necessary.

## Running the suite

From the repository root, with Go 1.24 or newer:

```sh
make build
export KARAKORAM_E2E_KARACTL="$PWD/bin/karactl"
export KARAKORAM_E2E_URL=https://karakoram.example.com
# Set KARAKORAM_E2E_TOKEN and capability-specific variables securely.

go test -tags e2e -count=1 -v -timeout 24h ./test/e2e
```

To inspect local helper tests and skip deployed scenarios, leave URL/token unset:

```sh
go test -tags e2e -count=1 -v ./test/e2e/...
```

Select one deployed scenario with a slash-separated `-run` expression:

```sh
go test -tags e2e -count=1 -v -timeout 4h ./test/e2e \
  -run '^TestBackup$/Recovery$'
```

Provisioning, package downloads, backup transfers and schedule boundaries can take substantial time. Allow project capacity for all scenario members and volumes, plus any resources retained with `KEEP`. Data-bearing members normally use 1 GiB RAM, SHARED CPU and a 5 GiB volume. Resource scaling and CPU-mode scenarios temporarily change those settings. Sharded recovery creates a source and a topology-matched clone.

## Scenario selection and coverage

Use the exact `Test/Scenario` names below with the command above, for example `-run '^TestReplicaSet$/Scaling$'`.

| Scenario | Checks |
| --- | --- |
| `TestReplicaSet/Singleton` | One-member creation, status, TLS majority writes/reads and deletion. |
| `TestReplicaSet/FiveToSeven` | Direct five-member creation, scaling to seven, initial-sync data, voting configuration and continuous majority writes. |
| `TestReplicaSet/Scaling` | Three-member creation, 3→5→3 scaling, secondary replacement, initial sync, quorum, role/lag, retained-volume suspend/resume and continuous writes. |
| `TestDeletionChoices/KeepVolumesKeepBackups` | Retain volume data and snapshot objects, resume and restore the retained backup. |
| `TestDeletionChoices/KeepVolumesDeleteBackups` | Retain/resume volume data while deleting snapshot objects and inventory. |
| `TestDeletionChoices/DeleteVolumesKeepBackups` | Delete the cluster while retaining snapshots for recovery. |
| `TestDeletionChoices/DeleteAll` | Delete the cluster and its snapshot objects. |
| `TestSharding/Topology` | Config/shard/router status, shard addition, balancer state, mongos discovery and hashed-sharded data writes/reads. |
| `TestBackup/Recovery` | Storage verification, logical/physical backups, inventory, in-place restores, backup clones, PITR windows, PITR clone and in-place PITR restore. |
| `TestBackup/ScheduleRetention` | Actual scheduled backups and count-retention eviction. |
| `TestSharding/Recovery` | Logical/physical backup, in-place restore and clone, PITR clone/restore, sharded data checks and restored-clone management/backup/metrics. |
| `TestSharding/ScheduleRetention` | Sharded backup scheduling and count retention. |
| `TestScalingUpgrade/ResourcesMinor` | Observed CPU/RAM bounds, mounted disk growth, rolling minor upgrade, secondary-first/primary-last order, restart quorum and continuous writes. |
| `TestScalingUpgrade/Major` | Catalog-authorized major upgrade, explicit FCV promotion, restart quorum and data preservation. |
| `TestScalingUpgrade/CPUMode` | SHARED→DEDICATED→SHARED reconciliation, observed CPU modes and fresh metrics. Platform rate limits may extend completion time. |
| `TestSecurity/UsersRoles` | Application user/custom-role CRUD, real authentication, privileges/revocation, password update and deletion rejection. |
| `TestSecurity/SystemRotation` | Rotation of all five system accounts, old-password rejection, new-password authentication and subsequent restart/user/data/backup/metrics checks. |
| `TestSecurity/TLSExposure` | Private default, connection URI/CA, every member's chain/keyfile/TLS configuration, plaintext rejection, TLS-mode changes, allow/deny probes and public-route removal. |
| `TestMonitoring/ReplicaSet` | Fresh per-member JSON series and labelled Prometheus output. |
| `TestMonitoring/Sharded` | Fresh config/shard/router metrics and labelled Prometheus output; router-only inapplicable series are excluded. |
| `TestRBAC/Accounts` | Accounts/session login, viewer/operator permissions, object filtering, separate credential read, explicit deny and API-token CRUD/revocation. |
| `TestRBAC/OIDC` | Issuer code/PKCE/signed ID-token callback, HttpOnly session, identity/groups, viewer mapping, administrative denial and logout. |
| `TestAPICLI/Workflow` | Cluster, backup/restore, database-user/role, storage, account/token, policy/permissions, operations and events commands against the deployed server. |
| `TestMultiNamespace` | Second-project registration and relay readiness, three-member health, credential isolation, database user creation, namespace-local backup, cross-namespace clone with restored marker data, namespace RBAC/export denial, non-disruptive token rotation and deletion guard/teardown. |
| `TestRabbitMQCluster/Create` | Three healthy brokers, observed version, member/detail/status agreement, private AMQPS/management endpoints, public AMQPS endpoints when enabled, CA and credential URI consistency; confirmed persistent quorum publish/consume through every broker. |
| `TestRabbitMQCluster/Restart` | Restart operation success, stable membership, per-broker uptime resets and unchanged persistent quorum message. |
| `TestRabbitMQCluster/ReplaceMember` | Member replacement operation, old member absence, healthy three-member convergence and unchanged persistent quorum message. |
| `TestRabbitMQCluster/Access` | Vhost/user/permission/policy create, list, get, update and delete; generated-password login and changed-password login over AMQPS using the created user's vhost permissions. |
| `TestRabbitMQCluster/Definitions` | Verified S3 storage, definitions backup inventory, vhost deletion and definitions restore recovering vhost metadata. No message-data recovery claim. |
| `TestRabbitMQCluster/Upgrade` | Observed rolling 4.3.5→4.3.6 transition, healthy majority, final per-broker version and unchanged persistent quorum message. |
| `TestRabbitMQCluster/Scaling` | 3→5→3 healthy membership with the original persistent quorum message readable through every current broker after each scale. |
| `TestRabbitMQCluster/Metrics` | Fresh RabbitMQ JSON series for every broker and labelled Prometheus output. |
| `TestRabbitMQCluster/Delete` | Explicit complete cluster deletion, cluster/member/connection API absence, inventory removal and orphaned-volume evidence. |

Independent scenario processes may share a disposable deployment because resource names and S3 prefixes are randomized; the suite does not use `t.Parallel`. **Serialize `TestRBAC/Accounts`, `TestRBAC/OIDC` and `TestMultiNamespace` with each other and with any administrative policy/settings mutation.** These scenarios save/replace/restore global policy; OIDC also temporarily replaces settings and uses the fixed `oidctest` identity. Do not make unrelated administrative changes while these scenarios run.

## RabbitMQ live tests and a project-local runner

Each `TestRabbitMQCluster/<phase>` is independent: it creates a randomized prefix-named three-broker cluster, waits for `Ready` and observed-generation convergence, and cleans up its own cluster. Selecting only `Upgrade`, for example, still provisions the required 4.3.5 source cluster. The full selection creates these clusters sequentially; scaling needs capacity for five brokers, and replacement temporarily adds a fourth. No existing PSMDB scenario is changed.

Set URL/token for all phases. `Definitions` additionally needs `KARAKORAM_E2E_STORAGE_NAME` or all five S3 creation variables listed above. Storage is verified before provisioning its cluster. To exercise message persistence and created-user authentication, set `KARAKORAM_E2E_ALLOW_CIDR` to the runner's actual IPv6 source CIDR and ensure it can reach the project's public TCP endpoints. Without it, only the AMQPS subtests skip, explicitly; control-plane restart, replacement, access CRUD, upgrade, scaling, metrics and deletion still run. An invalid configured CIDR or failed advertised AMQPS endpoint is a failure, not a skip.

For a runner **inside the same disposable Zerops project**, create a separate Go service (for example `e2erunner`) with terminal access, outbound HTTPS access to Go toolchain/module registries and public IPv6 connectivity. Check out the intended Karakoram source revision there. Use Go 1.24+ or automatic toolchain selection (`GOTOOLCHAIN=auto`); Zerops `go@1` may initially provide an older Go version. Supply the variables above through secure service environment settings, including the runner's source CIDR rather than your workstation's. Open the runner service's terminal and execute from the repository root:

```sh
go vet -tags e2e ./test/e2e/...
go test -tags e2e -count=1 -v -timeout 24h ./test/e2e \
  -run '^TestRabbitMQCluster$'

# Independently runnable phase; setup and cleanup remain included.
go test -tags e2e -count=1 -v -timeout 6h ./test/e2e \
  -run '^TestRabbitMQCluster$/Upgrade$'
```

Even in a project-local runner, these tests intentionally use **public AMQPS**, not private broker DNS. Certificate verification is always enabled. Publisher confirmations prove a persistent message was accepted by a durable quorum queue; passive declarations ensure persistence checks cannot silently recreate a missing queue. The same message is read and requeued through every current broker after the disruptive action. These are before/after persistence checks, not continuous-availability or throughput measurements. The rolling-upgrade phase requires an observed mixed-version transition and fails if evidence is inconclusive.

Optional evidence uses the existing private snapshot directory and records statuses, members, operations, vhost/permission/policy metadata, backups/restores, metrics and message-proof summaries. It never records connection credentials or generated/changed passwords. `KEEP=1` retains final test-owned clusters/storage except the explicit `Delete` phase; access-object deletions still run. Cleanup verifies API removal and captures orphaned-volume records, but cannot independently enumerate every platform service or prove all retained platform volumes were physically deleted; inspect the project and orphan inventory as well.


## Capability audit and assertion boundaries

- Replica-set checks validate member roles, health, lag, voting configuration and initial-sync data, not just desired member count. Continuous acknowledged majority writes span membership changes, upgrades and restarts and are counted afterward.
- Rolling checks observe version transitions and direct member availability. Network timeouts are inconclusive about database health; reachable unhealthy members or refused connections count differently. A successful restart must have conclusive probe rounds and uptime resets.
- Recovery checks validate data inclusion/exclusion at the recovery target on both replica-set and sharded topologies. PITR targets use MongoDB's clock. A restored clone must support its own administration, subsequent backup and fresh metrics.
- Scheduling requires successful distinct snapshots and actual retention eviction, not merely a short inventory. Deletion choices inspect S3 snapshot keys/sizes before and after each action. Cluster absence does not independently enumerate all infrastructure services; inspect orphaned-volume inventory and your Zerops project as well.
- TLS checks use CA-verified handshakes and active server configuration. Source restriction is tested by temporarily allowing only the RFC 3849 documentation address `2001:db8::/128`, then restoring the test host CIDR. Observation-only mode is not enforcement coverage. Use TLS and strong credentials regardless of allowlist configuration.
- Positive public probes retry network-level failures with a bounded deadline; authentication, authorization and certificate/protocol failures remain terminal. This permits network settling without equating cluster readiness with public reachability.
- Resource tests distinguish effective autoscaling configuration from current container allocation. Metrics checks require buckets strictly newer than their baseline; rebucketing old samples does not count as fresh telemetry.
- OIDC uses the test issuer's fixed `oidctest` identity and `karakoram-admins` group. Bind that group to **viewer**, not admin. Test-created identities are cleaned unless `KEEP`; pre-existing accounts are not deleted.
- No Go scenario claims browser/DOM coverage. Exercise the [web UI checklist](../../docs/verification.md#browser-workflow-checklist) separately.

The non-deployed API and CLI inventories compare every generated operation with an implemented route and executable command:

```sh
go test ./internal/server -run '^TestEveryOperationHasImplementedHandler$'
go test ./internal/cli -run '^TestEveryOperationHasCommand$'
```

They do not replace live behavior tests or generated-source consistency checks.

## Optional snapshots, teardown and interrupted runs

Set `KARAKORAM_E2E_EVIDENCE_DIR` if you want JSON snapshots, and capture verbose test output privately. Snapshot files use mode 0600 inside 0700 directories, segregated by test/subtest. Password, connection-credential and token responses are withheld; S3 snapshots contain only keys/sizes in a unique test-created prefix. Logs/status errors may still include private deployment or database metadata. Do not publish output without reviewing and redacting it.

Replica-set data clients connect directly to the observed primary's public endpoint with majority writes, avoiding private replica-set DNS discovery. Per-member probes use each member's public endpoint. Sharded clients connect to public mongos and verify `hello.msg=isdbgrid` before writing sharded data. These connection choices are test diagnostics, not a public replica-set failover solution.

Operations have a 35-minute wait bound and independent scenarios have four-hour contexts, with longer waits where CPU-mode rate limits require them. Backups/restores must reach `Succeeded`; cluster waits require `Ready` and observed-generation convergence. Ordinary failures still invoke cleanup unless `KEEP`. Cleanup deletes owned clusters with `keepVolumes=false, keepBackups=false`, waits for removal and removes test-created storage configurations. Existing storage objects and buckets are never deleted. Deletion-choice tests clean retained S3 objects only after preservation checks unless `KEEP`. Global policy/settings are restored even with `KEEP`.

A hard timeout, process kill or host failure can interrupt cleanup. Use the exact logged random names to delete only test-owned clusters/accounts/storage objects, inspect [orphaned volumes](../../docs/operations.md#orphaned-platform-volumes), and restore saved policy/settings if interruption occurred during RBAC tests. Never perform broad prefix deletion against pre-existing infrastructure. Retained resources and backup objects can continue incurring costs.
