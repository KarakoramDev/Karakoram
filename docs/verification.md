<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Verification

Verification checks contracts, real engine behavior and deployed workflows at different layers; none establishes production availability, physical isolation or a recovery guarantee.

## What checks establish

- **Local automated checks:** validation, authorization, state transitions, recovery checkpoints and generated-contract consistency.
- **Docker integration:** real engine/agent protocols, TLS, administration and selected snapshot/membership behavior, not backend placement or provisioning.
- **Native PostgreSQL/Supabase integration:** compiled test binaries run with real installed packages/artifacts in disposable Ubuntu 24.04 Incus/LXC system containers; no application-container runtime or image extraction is required for this path.
- **Deployed checks:** paid platform resources, engine data paths and recovery under the configured prerequisites. Skipped capabilities are unverified, not successful.
- **Browser checks:** rendered navigation, permissions and workflows. Static checks and backing-API scenarios are not browser automation; no automated browser-workflow suite is supplied.

Inspect individual assertions/skips, terminal operations, observed generations, member health and recovered data—not only exit status. Configuration acceptance is not external-network success. Observation-only allowlist mode does not verify enforcement. Persistence checks are not continuous availability under load; definitions recovery is not message recovery. API deletion is not independent enumeration of platform cleanup.

## Running the checks

Run from the repository root with Go 1.25+. Docker tests need daemon access/image pulls; host-network tests need Linux and unused ports, not a host serving important databases. Install UI dependencies before generated consistency checks. UI helper tests require Node 22+ with experimental TypeScript stripping.

```sh
# Local unit/HTTP checks:
go test ./...
# Integration-tagged checks; native cases skip without their explicit prerequisites:
go test -tags integration -count=1 -timeout 30m ./...
# UI and both executables:
make build-all
# UI static checks:
(cd ui && npm ci && npm run typecheck && npm run lint && npm run build)
# Alias-free specification and generated Go/TypeScript consistency:
make openapi-check
```

`make test-integration` selects only the integration-suite directory; the command above includes internal tagged tests too. Local recovery state machines use controlled doubles; actual data/S3 recovery needs deployed checks.

Use a disposable project/bucket, secure environment credentials and [suite prerequisites/cleanup](../test/e2e/README.md). Checks create paid services, mutate data and temporarily modify authorization/SSO. Namespace scenarios need a second project, IPv6 CIDR and all storage-creation inputs; serialize them with RBAC/OIDC/settings changes. Elasticsearch deployed scenarios need a same-project private-network runner.

```sh
export KARAKORAM_E2E_URL=https://karakoram.example.com
# Set KARAKORAM_E2E_TOKEN and capability prerequisites securely.
export KARAKORAM_E2E_KARACTL="$PWD/bin/karactl"
go test -tags e2e -count=1 -v -timeout 24h ./test/e2e
# One recovery scenario:
go test -tags e2e -count=1 -v -timeout 4h ./test/e2e \
  -run '^TestBackup$/Recovery$'
# Focused RabbitMQ checks:
go test ./internal/engine/rabbitmq/... ./internal/agent/rabbitmq \
  ./internal/server ./internal/cli
go test -tags integration -count=1 -timeout 30m ./test/integration \
  -run '^TestRabbitMQAgent$'
(cd ui && npm ci && npm test && npm run typecheck && npm run lint && npm run build)
go test -tags e2e -count=1 -v -timeout 4h ./test/e2e \
  -run '^TestRabbitMQCluster$/Restart$'
```

### Native PostgreSQL and Supabase integration

Compile on a compatible Linux host, then copy the test binaries into an isolated Ubuntu 24.04 Incus/LXC guest with native Percona packages and required locked artifacts. Run as an unprivileged runtime user, with private disposable data directories and an isolated network namespace: tests own fixed PostgreSQL/service loopback ports. Never run them against production data or alongside important host databases.

```sh
go test -c -tags integration -o /tmp/postgres-native.test ./internal/agent/postgres
go test -c -tags integration -o /tmp/supabase-native.test ./internal/agent/supabase
go test -c -tags integration -o /tmp/envoy-native.test ./internal/agent/supabase/gateway
go test -c -tags integration -o /tmp/pgbackrest-inventory.test ./internal/backupstorage
```

Inside the prepared guest, set `KARAKORAM_PG_BIN_DIR` to the installed PostgreSQL bin directory. PostgreSQL cases additionally require the native utilities used by their selected scenario (including ProxySQL/pgBackRest); major-upgrade coverage needs both major packages and `KARAKORAM_PG_UPGRADE=1`. Supabase API cases require `KARAKORAM_SUPABASE_AUTH_DIR`, `KARAKORAM_SUPABASE_REST_DIR` and `KARAKORAM_SUPABASE_GATEWAY_DIR`. Gateway conformance uses `KARAKORAM_ENVOY_BIN`; Vector uses `KARAKORAM_SUPABASE_VECTOR_DIR`. Additional component cases select installed Storage/Realtime/meta/Studio directories with the corresponding `KARAKORAM_SUPABASE_*_DIR` variables and need their actual artifacts. Storage object tests also need securely injected `KARAKORAM_SUPABASE_S3_*` settings for a disposable S3 destination.

Run the copied binary with its normal `-test.v`/`-test.run` options after provisioning these prerequisites. Consult the integration source for the exact selected case's inputs; missing variables mean **skipped**, not passed. Native artifact build/qualification commands and resource safeguards are in [native artifacts](native-artifacts.md). Remove disposable guests and objects after capturing private, redacted evidence. Native protocol tests do not establish backend provisioning, public networking, independent placement or complete restore/clone behavior; those need deployment-level scenarios.

### OpenBao coverage

OpenBao coverage has separate layers; test existence is not a claim that this checkout or your deployment passed:

- **Unit/API/CLI tests:** option admission, encrypted initialization receipts, secret-free configuration/status, reveal authorization/audit, connection target authorization, quorum-safe membership and rolling ordering, resumable snapshot backup/restore/clone, and seal/storage seams.
- **Native agent integration:** `TestNativeOpenBao` in `internal/agent/openbao/integration_test.go` runs three real OpenBao members with TLS, encrypted initialization receipt replay/acknowledgement, Raft formation, process crash/restart, explicit re-unseal, certificate hot reload and metrics. Declarative MongoDB plugin registration is optional; this is not a database-credential issuance test.
- **Native control-plane integration:** `TestNativeControlPlane` in `internal/engine/openbao/native_integration_test.go` drives provider reconciliation with real member agents/OpenBao: formation and reveal, secret-leak checks, automatic re-unseal after a child restart, rolling restart, 3→5→3 scaling, replacement, snapshot backup/restore through a signed S3 test endpoint, and a native PostgreSQL connection with real credential issuance and cleanup while dynamic leases remain outstanding. PostgreSQL target identity operations use a SQL-backed test adapter against the native server, not a deployed PostgreSQL member-agent path. Its infrastructure/S3 harness does not establish real backend or external S3 acceptance.
- **Native PostgreSQL-storage integration:** `TestNativeControlPlanePostgresStorage` uses real agents/OpenBao and a SQL-backed binding adapter to create the PostgreSQL database/role, initialize and unseal three HA peers, check native storage/HA tables, recover data after every local member volume is replaced, and verify destructive versus retained-data/NOLOGIN release. It does not establish production binding inventory, source deletion protection or credential custody; concrete PgPlatform manager injection and merged API/CLI/UI composition acceptance remain prerequisites.
- **Native deleted-source clone integration:** `TestNativeControlPlaneCloneDeletedSource` exports a snapshot from a real singleton source, deletes its processes/volumes/secrets/record, then force-restores three native members using offline supplied shares. It checks differing source/target thresholds, recovered data, explicit native seal/unseal, management authentication and cleanup. Infrastructure and the signed S3 endpoint are test fakes, not production backend or real S3 acceptance.
- **Manual native development proofs:** MongoDB TLS replica-set configuration, independent hyphenated connection/role names, root rotation and readonly/readwrite credential issuance/revocation; RabbitMQ HTTPS management and TLS AMQP issuance, readonly versus readwrite authorization, vhost isolation and revocation. Native target processes and OpenBao were real; these scratch development checks are not permanent automated-suite or deployed control-plane coverage.
- **Pending live verification:** all Nomad/Dokploy/Zerops workflows, production binding-service composition, real S3 restore/clone, transit/KMS external dependencies and external access. A newer-version rolling upgrade also awaits a newer catalog entry.

Native tests require an unprivileged Linux user, disposable directories and unused loopback ports, not Docker or production data. Set `KARAKORAM_OPENBAO_BIN` to the native `bao` executable and `KARAKORAM_PG_BIN_DIR` to the native PostgreSQL binary directory for the control-plane credential scenario. The agent's optional plugin case uses `KARAKORAM_OPENBAO_MONGODB_PLUGIN`; the control-plane harness requires the locked MongoDB plugin archive beside `bao` or this explicit executable override. Consult the integration sources for exact binary prerequisites and skip conditions. An unset opt-in variable can skip a native case; a prerequisite missing from an enabled scenario can fail it. Skipped is not passed.

```sh
go test ./internal/engine/openbao/... ./internal/agent/openbao
go test -tags integration -count=1 -timeout 30m \
  ./internal/engine/openbao ./internal/agent/openbao \
  -run '^(TestNativeControlPlane|TestNativeControlPlanePostgresStorage|TestNativeControlPlaneCloneDeletedSource|TestNativeOpenBao)$'
```

Use [OpenBao operations](openbao/operations.md), [access](openbao/access-and-tls.md) and [recovery](openbao/backups.md) as the deployment checklist. Exercise a three-member create, kill/re-unseal, restart, scale 3→5→3, replacement, catalog-available upgrade, snapshot backup/restore/clone, transit auto-unseal, PostgreSQL/MongoDB/RabbitMQ credential issuance, and delete/keepVolumes resume. Confirm both data and native authentication after recovery; require separate allowed/denied external-client checks.


## Capability matrix

Coverage descriptions, not evidence that this checkout or deployment passed:

| Engine/scope × capability | Automated | Live deployment | Browser/manual |
| --- | --- | --- | --- |
| MongoDB: replica sets/sharding, scale/upgrade/restart | Local contracts; Docker membership/bootstrap | Topologies, resources, versions/FCV, deletion choices | Wizard, topology/health, convergence |
| MongoDB: users/TLS, backups/PITR/restore/clone | Local recovery/security; Docker users/TLS | Data recovery, retention, credential rotation, access | Users/reveal, recovery controls, external clients |
| RabbitMQ: lifecycle/persistence/quorum | Local ordering/validation; three-broker Docker | Restart/replacement/upgrade, scaling, confirmed durable quorum messages | Lost-node/majority boundaries, classic-queue blockers, streams |
| RabbitMQ: access/TLS/definitions/metrics | Local API/CLI/rendering; Docker TLS/metrics | Grants, definitions restore/clone, retention, fresh metrics | TLS modes/PKI, denied clients, public management, cleanup |
| Elasticsearch/Stack: topology/security/lifecycle | Local contracts; Docker TLS/keystore/security APIs | Stack/dedicated masters, replacement, upgrades/scaling, suspend/resume | Component UI, external access, effective configuration |
| Elasticsearch: snapshots/access/metrics | Local checkpoints; Docker snapshot APIs | Restore/clone, data/credentials, fresh metrics, deletion | Repository compatibility and independent cleanup |
| PostgreSQL: access/TLS/proxy/lifecycle | Local contracts; native PostgreSQL/ProxySQL access/reload and upgrade cases | HA fencing/failover, scaling/replacement, resource changes and rolling lifecycle | Driver compatibility, external TLS/allowlists and client reconnection |
| PostgreSQL: pgBackRest/PITR/restore/clone | Local recovery checkpoints; native inventory compatibility | S3 backup chains, expiry dependencies, PITR/in-place/clone and credential recovery | Conservative-window limits, offline-major rollback, retained-object cleanup |
| Supabase: modular API/services | Local option/key/SQL/rendering contracts; native API, Envoy, Vector and available component cases | Enabled-component dependency/configuration, API data paths, service reconnect and logs | Credential/function/SMTP UI, object protection and unavailable-artifact rejection |
| OpenBao: Raft, init/unseal, TLS and membership | Unit state machines; native three-member agents and control-plane create/restart/scale/replace/snapshot workflows | Backend provisioning, real S3, catalog-newer upgrade and external seal dependencies pending | Fixture-backed browser create forms, capability gating, explicit reveal identity/async isolation and secret-cache checks |
| OpenBao: snapshots, clone and PostgreSQL storage | Native snapshot restore/replay evidence, deleted-source forced clone and SQL-backed storage/HA integration cases | Concrete shared binding-manager composition and deployment recovery acceptance pending | Supplied-share validation/clearing and restore failure/retry workflows |
| OpenBao: database credential connections | Provider/API/CLI authorization and replay tests; native PostgreSQL issuance | Deployed PostgreSQL/MongoDB/RabbitMQ connectivity and lifecycle acceptance pending | Native MongoDB/RabbitMQ TLS issuance, least privilege, rotation/revocation and cleanup development proofs |
| All: monitoring/API/CLI | Local handlers/auth/filtering; real agent integration | Namespace isolation, API/CLI workflows | Charts, events/logs, pending/error states |
| Control plane: auth/RBAC/OIDC, namespaces/relays | Local identity/policy/transport | Accounts/OIDC, second-project management/recovery | Restricted direct navigation, group grants, relay health |
| Installation/state/admin recovery | Local startup/recovery contracts | No dedicated installer scenario | Complete checklist below |

Recorded live Elasticsearch checks include Stack and dedicated-master formation, data/access, lifecycle and snapshot recovery; they are functional evidence on disposable data, not durability/load certification.

## Browser and installation checklists

With admin/restricted accounts on a disposable deployment:

1. Exercise sign-in/password change, expiry/refresh, sign-out and configured OIDC; verify restricted direct navigation and credential-reveal boundaries.
2. Create each needed topology; change settings, observe convergence and test application access. Verify backups by recovered contents independently.
3. Inspect charts/logs/events/operations; refresh nested routes, reconnect event replay and exercise request failures. Switch namespaces and verify same-named objects and source-export permissions remain isolated.
4. Confirm state mount, singleton, project-scoped token, preserved key, public HTTPS and private agent connectivity; never publish 9443.
5. Restart/redeploy and verify persistent identities/inventory. Rehearse [admin recovery](operations.md#recovering-local-admin-access) and [state recovery](operations.md#control-plane-state-backup-and-recovery), removing temporary secrets afterward. State recovery does not protect engine data.
6. Independently inspect platform resources/costs, retained storage/backups and [orphan volumes](operations.md#orphaned-platform-volumes). Remove test resources/revoke credentials; keep evidence private.
