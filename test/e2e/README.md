<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->

# Karakoram end-to-end suite

These tests operate a **deployed Karakoram in your own Zerops project**, not an in-process server. Requests use the generated REST client over verified HTTPS. MongoDB checks use the MongoDB driver over public TCP IPv6; RabbitMQ message checks use AMQP 0-9-1 over public AMQPS IPv6. Both verify the cluster CA returned by the connection API. Private broker DNS and connectivity are additionally required only for the opt-in RabbitMQ private-listener TLS checks.

Use a disposable deployment and bucket: tests create paid services, delete resources, restore database contents and temporarily change global authorization/SSO settings. API/CLI tests do not exercise the rendered web UI; see the [verification matrix and browser checklist](../../docs/verification.md).

## Prerequisites and environment

Install Karakoram using the [installation guide](../../docs/install.md), complete the admin password change and create an admin API token. Public data-plane scenarios require the test host to have IPv6 connectivity from the configured source CIDR. Supply credentials through your secure environment mechanism; never enable shell tracing or put tokens/passwords in command arguments.

| Variable | Meaning |
| --- | --- |
| `KARAKORAM_E2E_URL` | Your deployment's public HTTPS origin; trailing `/api/v1` is accepted. Control-plane certificate verification remains enabled. |
| `KARAKORAM_E2E_TOKEN` | Existing admin API token, supplied securely through the environment. |
| `KARAKORAM_E2E_ALLOW_CIDR` | Authorized IPv6 source CIDR (`/128` or network `/64`) of the test host. Required for public MongoDB checks and RabbitMQ data-path checks; the suite requests `IPV6` exposure with this allowlist. RabbitMQ API phases still run without it, with explicit skipped data-path subtests. |
| `KARAKORAM_E2E_PRIVATE` | Set to `1` only on a runner with private RabbitMQ broker DNS and connectivity. Enables `TestRabbitMQCluster/Access/TLS`: CA/hostname verification on AMQPS and native stream TLS, CA verification on mutual-TLS Erlang distribution, and plaintext-listener rejection. Unset skips only that subtest. |
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
| `KARAKORAM_E2E_ELASTIC_CLUSTER` | Optional: run `TestElasticLifecycle` phases against an existing Elasticsearch cluster that an earlier `KEEP=1` run created and seeded, instead of creating one. Select later phases with `-run`. The suite does not delete an adopted cluster except in explicit deletion phases. |
| `KARAKORAM_E2E_KARACTL` | Already-built `karactl` executable; required by the CLI workflow and optional RabbitMQ `Access/CLI` subtest. Subprocess credentials use the environment and an isolated temporary HOME; other `KARAKORAM_` variables are removed. |
| `KARAKORAM_E2E_OIDC_ISSUER` | Reachable auto-approving **test-only** issuer from `test/oidc-issuer`. When configured, OIDC follows the full authorization-code/PKCE callback. Never use this issuer for production authentication. |
| `KARAKORAM_E2E_OIDC_CLIENT_ID` | Optional client ID; default `karakoram-e2e`, matching the bundled issuer. Register callback `<KARAKORAM_E2E_URL>/api/v1/auth/oidc/callback`. |
| `KARAKORAM_E2E_EVIDENCE_DIR` | Optional private output directory for JSON status/member/operation/backup/restore/metrics snapshots and test-prefix S3 object metadata. Credential/token responses are not captured. |

Missing URL/token skips deployed scenarios. Capability-specific prerequisites are checked before creating clusters. Invalid configured API values or missing advertised behavior fail; they are not silently treated as passing coverage. To exercise storage creation, omit `KARAKORAM_E2E_STORAGE_NAME` and supply all five creation variables. `TestDeletionChoices` always requires those five: it creates an isolated storage object/prefix per case and inspects its S3 snapshot objects without touching a pre-existing prefix.

`TestMultiNamespace` additionally requires the public IPv6 CIDR and all five S3 storage-creation inputs. It creates an isolated namespace-local storage regardless of `KARAKORAM_E2E_STORAGE_NAME`, then removes it before namespace deletion. The scenario reuses a matching namespace but treats it as disposable, so do not select one containing unrelated resources. Relay enrollment may take up to fifteen minutes. It creates a three-member source and a three-member clone in `default`, validates a restored marker document, checks namespace RBAC and backup export denial, and verifies that token rotation preserves the relay session. Namespace teardown waits for retained-resource guards and relay deletion; `KEEP=1` retains it and its test resources.

### TEST ONLY live issuer

`test/oidc-issuer` is a standalone auto-approve issuer for acceptance testing, **not a production identity provider**. It authenticates every visitor as `oidctest` with group `karakoram-admins`; bind that group only to a low-privilege test role. Its discovery/JWKS/code/token endpoints enforce the configured callback, one-minute single-use codes and S256 PKCE, and sign five-minute ID tokens with an ephemeral RSA key. Restarting the issuer invalidates pending codes and rotates that test signing key.

Build with `go build ./test/oidc-issuer`. Supply HTTPS `OIDC_ISSUER_URL` and `OIDC_CALLBACK_URL` for your deployment; the fixed client ID is `karakoram-e2e`. The dedicated `test/oidc-issuer/zerops.yaml` setup builds from the repository root. Create a disposable `oidcissuer` runtime service, push with `zcli service push oidcissuer --setup oidcissuer --zerops-yaml-path test/oidc-issuer/zerops.yaml`, then enable its HTTP subdomain. Set the issuer origin to that subdomain and the callback to your Karakoram origin followed by `/api/v1/auth/oidc/callback`. Configure Karakoram's issuer to the service's public HTTPS origin and `groupsClaim: groups`. Delete the issuer and remove its SSO settings/group binding when testing finishes.

The issuer accepts public-client or basic-client authentication without validating a client secret, allowing the suite to preserve the deployment's existing write-only secret while restoring other settings. A configured issuer enables full login; no secret replacement is necessary.

## Running the suite

From the repository root, with Go 1.25 or newer:

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
| `TestRabbitMQCluster/Create` | Three healthy brokers, observed version, member/detail/status agreement, private AMQPS/management endpoints, public AMQPS endpoints when enabled, CA and credential URI consistency; confirmed persistent quorum and stream publish/consume through every broker. |
| `TestRabbitMQCluster/Restart` | Restart operation success, stable membership, per-broker uptime resets and unchanged persistent quorum and stream messages. |
| `TestRabbitMQCluster/ReplaceMember` | Member replacement operation, old member absence, healthy three-member convergence and unchanged persistent quorum and stream messages. |
| `TestRabbitMQCluster/Access` | Vhost/user/permission/policy create, list, get, update and delete; generated-password and changed-password AMQPS login. Optional `CLI` checks RabbitMQ vhost/user/permission/policy commands against API state, including created-user login; optional `TLS` checks every private broker's secure and plaintext listeners. |
| `TestRabbitMQCluster/Definitions` | Verified S3 storage, definitions backup inventory, vhost deletion, in-place restore and one-broker `Clone` recovering vhost metadata. No message-data recovery claim. |
| `TestRabbitMQCluster/Upgrade` | Observed rolling 4.3.5→4.3.6 transition, healthy majority, final per-broker version and unchanged persistent quorum message. |
| `TestRabbitMQCluster/Scaling` | 3→5→3 healthy membership with the original persistent quorum message readable through every current broker after each scale. |
| `TestRabbitMQCluster/Metrics` | Fresh RabbitMQ JSON series for every broker and labelled Prometheus output. |
| `TestRabbitMQCluster/Delete` | Explicit complete cluster deletion, cluster/member/connection API absence, inventory removal and orphaned-volume evidence. |
| `TestRabbitMQCluster/DeleteKeepVolumes` | Definitions backup retention, retained-volume suspension/resume, healthy three-broker convergence and original quorum-message persistence, followed by complete cluster and backup deletion. |
| `TestRabbitMQCluster/DeleteKeepBackups` | Delete volumes/cluster, list/get the retained definitions backup under the deleted source name, then delete that backup and verify retained-inventory absence. |

Independent scenario processes may share a disposable deployment because resource names and S3 prefixes are randomized; the suite does not use `t.Parallel`. **Serialize `TestRBAC/Accounts`, `TestRBAC/OIDC` and `TestMultiNamespace` with each other and with any administrative policy/settings mutation.** These scenarios save/replace/restore global policy; OIDC also temporarily replaces settings and uses the fixed `oidctest` identity. Do not make unrelated administrative changes while these scenarios run.

## RabbitMQ live tests and a project-local runner

Each `TestRabbitMQCluster/<phase>` is independent: it creates a randomized prefix-named three-broker cluster, waits for `Ready` and observed-generation convergence, and cleans up its own cluster. Selecting only `Upgrade`, for example, still provisions the required 4.3.5 source cluster. The full selection creates these clusters sequentially; scaling needs capacity for five brokers, replacement temporarily adds a fourth, and `Definitions/Clone` creates a one-broker clone alongside its source. All RabbitMQ specs enable `rabbitmq_stream`. No existing PSMDB scenario is changed.

Set URL/token for all phases. `Definitions`, `DeleteKeepVolumes` and `DeleteKeepBackups` additionally need `KARAKORAM_E2E_STORAGE_NAME` or all five S3 creation variables listed above. Storage is verified before provisioning clusters. To exercise message persistence and created-user authentication, set `KARAKORAM_E2E_ALLOW_CIDR` to the runner's actual IPv6 source CIDR and ensure it can reach the project's public TCP endpoints. Without it, only the AMQPS subtests skip, explicitly; control-plane phases still run. Set `KARAKORAM_E2E_KARACTL` for `Access/CLI` and `KARAKORAM_E2E_PRIVATE=1` for `Access/TLS`; otherwise those subtests explicitly skip. An invalid configured CIDR, failed advertised endpoint or missing private-host certificate SAN is a failure, not a skip.

For a runner **inside the same disposable Zerops project**, create a separate Go service (for example `e2erunner`) with terminal access, outbound HTTPS access to Go toolchain/module registries and public IPv6 connectivity. Check out the intended Karakoram source revision there. Use Go 1.25+ or automatic toolchain selection (`GOTOOLCHAIN=auto`); Zerops `go@1` may initially provide an older Go version. Supply the variables above through secure service environment settings, including the runner's source CIDR rather than your workstation's. Open the runner service's terminal and execute from the repository root:

```sh
go vet -tags e2e ./test/e2e/...
go test -tags e2e -count=1 -v -timeout 24h ./test/e2e \
  -run '^TestRabbitMQCluster$'

# Independently runnable phase; setup and cleanup remain included.
go test -tags e2e -count=1 -v -timeout 6h ./test/e2e \
  -run '^TestRabbitMQCluster$/Upgrade$'
```

Message checks intentionally use **public AMQPS**, even on a project-local runner. Certificate verification is always enabled. Publisher confirmations prove persistent messages were accepted by durable quorum and stream queues; passive declarations ensure persistence checks cannot silently recreate missing queues. Quorum proofs read and requeue the original message through every current broker. Stream proofs set per-consumer prefetch, consume from `x-stream-offset: first` with manual acknowledgements, wait at most 30 seconds for the original payload and cancel the consumer; acknowledgements do not remove stream records. `Create/StreamPublishConsume`, `Restart/StreamPersistence` and `ReplaceMember/StreamPersistence` cover stream data.

`Access/TLS` additionally uses the private `host:port` entries in the connection document, verifying the cluster CA and published hostname on ports 5671 and 5551. It requires port 5672 and 5552 to refuse or close plaintext probes. On port 25672 it requires an observed cluster-CA-signed distribution certificate without hostname verification; rejection for a missing client certificate is expected and acceptable. A bounded plaintext Erlang `send_name` probe must not receive a distribution handshake reply. The connection API currently does not publish native stream endpoints, so native stream TLS is probed directly on each private host. TLS evidence contains booleans only.

Definitions backups recover topology/access metadata, not messages. Retained definitions backups use the ordinary `/clusters/{source}/backups` list/get/delete API even after the source cluster is gone; an absent source with no retained backups returns 404. `DeleteKeepBackups` explicitly removes its retained backup, including when `KEEP=1`.

These are before/after persistence checks, not continuous-availability or throughput measurements. The rolling-upgrade phase requires an observed mixed-version transition and fails if evidence is inconclusive. Optional evidence uses the existing private snapshot directory and records statuses, members, operations, vhost/permission/policy metadata, backups/restores, metrics and message-proof summaries. It never records connection credentials or generated/changed passwords. `KEEP=1` retains final test-owned clusters/storage except explicit `Delete`, `DeleteKeepVolumes` and `DeleteKeepBackups` actions; access-object deletions still run. Cleanup verifies API removal and captures orphaned-volume records, but cannot independently enumerate every platform service or prove all retained platform volumes were physically deleted; inspect the project and orphan inventory as well.


## Capability audit and assertion boundaries

- Replica-set checks validate member roles, health, lag, voting configuration and initial-sync data, not just desired member count. Continuous acknowledged majority writes span membership changes, upgrades and restarts and are counted afterward.
- Rolling checks observe version transitions and direct member availability. Network timeouts are inconclusive about database health; reachable unhealthy members or refused connections count differently. A successful restart must have conclusive probe rounds and uptime resets.
- Recovery checks validate data inclusion/exclusion at the recovery target on both replica-set and sharded topologies. PITR targets use MongoDB's clock. A restored clone must support its own administration, subsequent backup and fresh metrics.
- Scheduling requires successful distinct snapshots and actual retention eviction, not merely a short inventory. Deletion choices inspect S3 snapshot keys/sizes before and after each action. Cluster absence does not independently enumerate all infrastructure services; inspect orphaned-volume inventory and your Zerops project as well.
- TLS checks use CA-verified handshakes and active server configuration. Source restriction is tested by temporarily allowing only the RFC 3849 documentation address `2001:db8::/128`, then restoring the test host CIDR. Observation-only mode is not enforcement coverage. Use TLS and strong credentials regardless of allowlist configuration.
- Positive public probes retry network-level failures with a bounded deadline; authentication, authorization and certificate/protocol failures remain terminal. This permits network settling without equating cluster readiness with public reachability.
- Resource tests distinguish effective autoscaling configuration from current container allocation. Metrics checks require buckets strictly newer than their baseline; rebucketing old samples does not count as fresh telemetry.
- OIDC uses the test issuer's fixed `oidctest` identity and `karakoram-admins` group. Bind that group to **viewer**, not admin. Test-created identities are cleaned unless `KEEP`; pre-existing accounts are not deleted.
- No Go scenario claims browser/DOM coverage. Exercise the [web UI checklist](../../docs/verification.md#browser-and-installation-checklists) separately.

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

## Elasticsearch lifecycle (private-network runner)

`TestElasticLifecycle` must run **inside a Zerops runtime in the same project as
the control plane**, with private-network DNS/reachability to its test-created
Elastic Stack services. Unlike the MongoDB suites above, it does not require
`KARAKORAM_E2E_ALLOW_CIDR`, public database TCP exposure, the MongoDB driver, or
`karactl`. The Elasticsearch provider must be deployed with its security routes,
monitoring collector, and matching Stack artifacts available.

Required inputs are `KARAKORAM_E2E_URL`, `KARAKORAM_E2E_TOKEN`, and either
`KARAKORAM_E2E_STORAGE_NAME` or all five
`KARAKORAM_E2E_STORAGE_{ENDPOINT,BUCKET,ACCESS_KEY,SECRET,REGION}` variables.
Storage must satisfy the HTTPS/path-style/TLS-verification requirements above
and support Elasticsearch's S3 snapshot repository operations. Optional inputs
are `KARAKORAM_E2E_PREFIX`, `KARAKORAM_E2E_KEEP`, and
`KARAKORAM_E2E_EVIDENCE_DIR`. Set the evidence directory for private JSON timing,
health, data-preservation, operation, backup, restore, and monitoring records.
Failed phases also capture available cluster status, events, and operations;
credentials, API-key values, session cookies, and intake tokens are not saved.

The suite provisions Elasticsearch **9.5.4** (then upgrades it to 9.5.5), three combined nodes with fixed
2 GB RAM each, and one replica each of Kibana (2 GB), Logstash (1.5 GB), and APM
Server (0.5 GB). Kibana and APM receive public HTTPS subdomains. Allow capacity
for a temporary fourth Elasticsearch node (replacement/scaling) and a separate
single-node backup clone with its own volume. Provisioning, reconciliation, and
operation waits allow 60 minutes each; ingestion allows 15 minutes. The parent
context allows 24 hours. Increase the Go process timeout if necessary.

```sh
# Compile/vet without creating any live resources:
go vet -tags e2e ./test/e2e/

# Complete sequential lifecycle, from the same-project runtime:
go test -tags e2e -count=1 -v -timeout 24h ./test/e2e/ \
  -run '^TestElasticLifecycle$'

# An independently runnable phase, including prerequisite provisioning:
go test -tags e2e -count=1 -v -timeout 8h ./test/e2e/ \
  -run '^TestElasticLifecycle$/Restore$'
```

Every phase below is independently selectable with
`-run '^TestElasticLifecycle$/<Phase>$'`. Missing prerequisites are created
automatically: all phases provision a ready three-node Stack cluster and seed
twelve proof documents into a one-replica index; Restore and Clone also create
a successful snapshot. Selecting several phases in one invocation reuses that
fixture in the listed order. The first failed phase stops later mutations.

| Phase | Checks |
| --- | --- |
| `Create` | Elasticsearch 9.5.4 with combined-node topology, fixed 2 GB RAM, one Kibana/Logstash/APM replica each, green health with exactly three Elasticsearch nodes, indexed documents and exact search contents. |
| `TLSData` | Connection-details CA and elastic password, HTTPS certificate chain and node-hostname verification, exact document count and search contents. |
| `Access` | Native users/roles CRUD and lists, real allowed/denied index reads, password change, role revocation, scoped API-key authentication/list/invalidation (401), user/role deletion. |
| `Kibana` | Private `/api/status` reports 9.5.4; public HTTPS basic login with `kbn-xsrf`. |
| `Logstash` | Three HTTP-input JSON documents indexed under `logs-*`, with a logs data stream present. |
| `APM` | Secret-token NDJSON transaction intake through both private and public endpoints (202), both unique traces searchable under `traces-apm*`. |
| `Monitoring` | Elasticsearch-prefixed monitoring series with samples for every Elasticsearch member. |
| `Restart` | Durable restart succeeds, cluster returns green, exact proof data survives. |
| `ReplaceMember` | Replace the second member, observe a newly allocated member name and old-member removal, green health and exact proof data. |
| `Backup` | Native snapshot backup reaches Succeeded. |
| `Restore` | Delete the proof index, in-place restore reaches Succeeded, index returns with all twelve exact documents and green health. |
| `Clone` | Single-node cluster from `spec.dataSource`, without optional components, contains exact restored documents; delete clone and verify 404/list absence. A one-replica index may correctly make the singleton yellow. |
| `Upgrade` | Rolling patch upgrade 9.5.4→9.5.5 via merge PATCH; desired spec, all Elasticsearch nodes and Stack members report 9.5.5, Kibana reports 9.5.5, green health and exact proof data. |
| `Scale` | Engine replicas 3→4→3; each step returns green and preserves exact proof data. After scale-down, voting configuration exclusions are empty and neither persistent nor transient allocation exclusions name the removed node. |
| `DeleteKeep` | Delete with `keepVolumes=true, keepBackups=true`, reach Suspended, preserve the complete paginated backup inventory and individual backup records; resume to Ready with three green Elasticsearch nodes, six Stack members, healthy matching-version Kibana and exact proof data; backup inventory remains unchanged. |
| `Delete` | Ensure a successful snapshot exists, delete with `keepVolumes=false, keepBackups=false`, verify cluster 404/list absence, every prior backup record returns 404 and the backup-list endpoint returns 404 once no retained records remain. This checks API records, not direct S3 object removal. |
| `TestElasticDedicatedMasters` (standalone test) | Elasticsearch 9.5.4, three master-only plus two data nodes with fixed 2 GB RAM and no Stack components; Ready and green five-node health, `_cat/nodes` role counts, twelve indexed documents and exact search contents; delete with both keep flags false and verify cluster 404/list absence. No backup storage is required. |

Generic lifecycle calls use the generated API client. Elasticsearch
users/roles/API-key routes use authenticated raw HTTP with that client's base
URL and transport. Data-plane requests verify TLS against the returned cluster
CA, including the node hostname; public component HTTPS uses normal certificate
verification. Private component HTTP is reachable only through the project
network. Deletion assertions are control-plane 404/inventory evidence, not an
independent Zerops resource inventory.

Cleanup removes test-owned clusters, snapshots, and newly created storage
objects unless `KARAKORAM_E2E_KEEP=1`. As with other suites, **explicit scenario
deletions still execute with KEEP**: Clone deletes its clone and Delete deletes
its source cluster. Existing storage objects and buckets are never deleted.

## Weaviate deployed lifecycle

`TestWeaviateLifecycle` uses generated control-plane API methods and CA-verified
native HTTPS REST requests through each member's published IPv6 REST endpoint.
It requires `KARAKORAM_E2E_URL`, `KARAKORAM_E2E_TOKEN`,
`KARAKORAM_E2E_ALLOW_CIDR`, and either `KARAKORAM_E2E_STORAGE_NAME` or all five
`KARAKORAM_E2E_STORAGE_{ENDPOINT,BUCKET,ACCESS_KEY,SECRET,REGION}` inputs.
Missing CIDR/S3 prerequisites skip before member provisioning. Invalid configured
values, failed storage verification, failed TLS/authentication and missing
advertised behavior fail rather than skip. The runner needs public IPv6
connectivity from the allowlisted CIDR. Members require the private network
ports (including gossip UDP) described in the Weaviate operations guide.

Allow capacity for four members during scale-out/replacement and a simultaneous
three-member clone, with fixed 2 GiB RAM and 10 GiB disk per member.
The source starts on 1.39.12 when the deployed catalog offers it, otherwise
1.40.1; only the unavailable upgrade source skips its phase. Monitoring and
`requireTLS` are enabled; the verified S3 storage is bound at creation.
`karactl` is not required: access management uses the generated API.

```sh
go vet -tags e2e ./test/e2e/
go test ./test/e2e/ # Ordinary pure helper tests; no deployed resources.
go test -tags e2e -count=1 -v -timeout 24h ./test/e2e/ \
  -run '^TestWeaviateLifecycle$'
```

Phases share one ordered fixture and stop after the first failure. Selecting a
single phase also creates and seeds its prerequisites:

| Phase | Checks |
| --- | --- |
| `Create`, `Ready` | Three-member cluster, requested version/TLS/monitoring, Ready with observed-generation convergence, healthy native nodes. |
| `TLSData` | Protected admin credential and returned CA, verified chain/hostname, RF=3 collection, eight batch objects with explicit vectors, exact objects and nearVector results through every member using ALL consistency. |
| `Access` | Viewer DB-user key can read but cannot write; role revocation/restoration, key rotation with old-key 401, custom role create/replace/delete, protected identity and built-in-role rejection. |
| `Monitoring` | Fresh samples newer than baseline and phase start for every Weaviate catalog metric on every member. |
| `Restart`, `ReplaceMember` | Successful operations and native healthy membership, original RF=3 data remains readable through every member. Voter replacement preserves identity as required by Weaviate. |
| `Backup`, `Restore` | Full backup reaches Succeeded (native SUCCESS) with immutable location; delete an object, restore in place, recover original data. |
| `Clone` | Topology-matched three-member clone contains original data but not the source application user; root credential is fresh. |
| `Upgrade` | 1.39.12→1.40.1, native version checked on every member, original data persists. |
| `ScaleOut`, `ScaleDown` | 3→4→3, correct native member counts, ALL-consistency proof through every member and exactly three served replicas per proof shard. ScaleDown alone first scales out. |
| `RotateSystemUser`, `RotateTLS` | Root-key rotation rejects the old key; TLS rotation presents a different CA-verified leaf; original data remains accessible. |
| `DeleteKeepBackups`, `Delete` | Explicit cluster deletion, retained backup/location verification then retained-record cleanup; full deletion verifies prior backup records absent. Delete provisions a fresh fixture after DeleteKeepBackups. Assertions concern API inventory, not independent bucket/platform enumeration. |

`KARAKORAM_E2E_WEAVIATE_CLUSTER` explicitly adopts a previously seeded,
three-member suite cluster bound to the selected storage. Adoption does not
implicitly delete the source; **explicit deletion phases remain destructive**.
Use `-run '^TestWeaviateLifecycle$/Restore$'` (or another exact phase) to select
later phases. Do not adopt unrelated or production data.
`KARAKORAM_E2E_PREFIX`, `KARAKORAM_E2E_KEEP` and
`KARAKORAM_E2E_EVIDENCE_DIR` have their usual meanings. KEEP retains owned
resources except explicit deletion actions. Optional evidence includes native
node/status, data-proof summaries, operations, backup/restore records, metrics,
replica counts and timing; connection responses and API keys are never saved.
These are before/after persistence tests, not continuous availability or
rendered-browser coverage. A deployed environment is required to run them.
