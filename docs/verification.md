<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Verification guide and test matrix

Karakoram has local Go tests, Docker-backed integration tests and an environment-gated end-to-end suite for your own Zerops deployment. These layers answer different questions: unit tests check contracts and state transitions, integration tests exercise real MongoDB processes, and end-to-end tests exercise a deployed control plane and its database services. A skipped test does not establish coverage for your deployment.

## Running the checks

Run commands from the repository root. Use Go 1.24 or newer. Docker integration tests require a working Docker daemon and permission to start containers and pull the configured images. Tests using host networking require Linux and unused listener ports; do not run them on a host serving important databases.

```sh
# Local unit and HTTP-handler tests (no Zerops deployment required):
go test ./...

# All Docker-backed integration tests, including internal package tests:
go test -tags integration -count=1 -timeout 30m ./...

# Build the UI and both executables:
make build-all

# UI static checks (there is no automated browser-workflow suite):
(cd ui && npm ci && npm run typecheck && npm run lint && npm run build)

# Generated Go contract/client/RBAC consistency, from a Git checkout:
make openapi-check
```

`make test-integration` runs `test/integration` only; the explicit integration command above also includes tagged tests in `internal/agent`, `internal/engine/psmdb/mongoadmin` and `internal/engine/psmdb/users`. PBM backup/restore state-machine tests use controlled test doubles locally; actual database/S3 recovery is covered by the deployed suite.

For end-to-end checks, install Karakoram in a disposable Zerops project and configure the variables in the [suite guide](../test/e2e/README.md). Tests create paid services, change database contents and temporarily modify authorization settings. Supply credentials through a secure environment mechanism, not command arguments or shell traces.

```sh
export KARAKORAM_E2E_URL=https://karakoram.example.com
# Set KARAKORAM_E2E_TOKEN securely, plus capability-specific prerequisites.
export KARAKORAM_E2E_KARACTL="$PWD/bin/karactl"
go test -tags e2e -count=1 -v -timeout 24h ./test/e2e

# Select a single scenario:
go test -tags e2e -count=1 -v -timeout 4h ./test/e2e \
  -run '^TestBackup$/Recovery$'
```

The matrix uses exact top-level test/subtest names from `test/e2e/*_test.go`. A name such as `TestReplicaSet/Scaling` is selected with `-run '^TestReplicaSet$/Scaling$'`. Run listed local packages with `go test ./PACKAGE/...`; use `-tags integration` for the integration tests named below. Run all listed scenarios for complete capability coverage, not just the shortest example.

## Capability matrix

| Capability | Local unit / handler packages | Docker integration coverage | Deployed end-to-end scenarios |
| --- | --- | --- | --- |
| Replica sets | `internal/engine/psmdb` (planning, creation, membership, retention), `internal/engine/psmdb/mongoadmin`, `internal/reconciler`, `internal/store` | `test/integration: TestAgent`; `internal/agent: TestBootstrapRedirectsToElectedPrimaryAndVerifiesSecondary`; `internal/engine/psmdb/mongoadmin: TestReplicaReconfiguration` | `TestReplicaSet/Singleton`, `TestReplicaSet/FiveToSeven`, `TestReplicaSet/Scaling`; deletion choices: `TestDeletionChoices/KeepVolumesKeepBackups`, `TestDeletionChoices/KeepVolumesDeleteBackups`, `TestDeletionChoices/DeleteVolumesKeepBackups`, `TestDeletionChoices/DeleteAll` |
| Sharding | `internal/engine/psmdb` (component planning and shard validation), `internal/engine/psmdb/mongoadmin`, `internal/engine/psmdb/pbm` (router/balancer recovery coordination) | No standalone full sharded-topology integration scenario; shared member mechanisms are covered by `TestAgent` and the MongoDB administration integration tests | `TestSharding/Topology`, `TestSharding/Recovery`, `TestSharding/ScheduleRetention`; `TestMonitoring/Sharded` |
| Backups, PITR, restore and clone | `internal/engine/psmdb/pbm` (inventory, checkpoint/crash resume, recovery identities, PITR selection and cleanup), `internal/engine/psmdb`, `internal/backupstorage`, `internal/scheduler`, `internal/server` (retained inventory and write-only credentials) | `internal/engine/psmdb/users: TestPreservedPBMAccountRestoresCurrentSystemPasswords` checks restored-system-credential reconciliation, not a PBM recovery job | `TestBackup/Recovery`, `TestBackup/ScheduleRetention`, `TestSharding/Recovery`, `TestSharding/ScheduleRetention`; all four `TestDeletionChoices` scenarios above; `TestAPICLI/Workflow` |
| Scaling, upgrades and restarts | `internal/engine/psmdb` (catalog, FCV, rolling changes, resource observation), `internal/infra/zerops`, `internal/reconciler` | `internal/engine/psmdb/mongoadmin: TestReplicaReconfiguration` exercises membership changes against real mongod processes; binary upgrades require the deployed suite | `TestReplicaSet/FiveToSeven`, `TestReplicaSet/Scaling`, `TestScalingUpgrade/ResourcesMinor`, `TestScalingUpgrade/Major`, `TestScalingUpgrade/CPUMode` |
| Database users, TLS and access | `internal/engine/psmdb/users`, `internal/engine/psmdb/pki`, `internal/engine/psmdb`, `internal/agent`, `internal/agentapi`, `internal/secrets`, `internal/server` | `internal/engine/psmdb/users: TestAuthenticatedUserAndRoleLifecycle`, `TestPreservedPBMAccountRestoresCurrentSystemPasswords`; `test/integration: TestAgent` includes verified database/agent TLS and bootstrap | `TestSecurity/UsersRoles`, `TestSecurity/SystemRotation`, `TestSecurity/TLSExposure` |
| Monitoring | `internal/monitoring`, `internal/agent`, `internal/server` (authorization and member observations); e2e-tagged helper checks `TestMetricsFreshnessRejectsRebucketedHistoricalSamples` and `TestMetricsFreshnessRequiresStrictlyAfterBaselineEnd` | `test/integration: TestAgent` checks real MongoDB metrics through the authenticated agent | `TestMonitoring/ReplicaSet`, `TestMonitoring/Sharded`; `TestSecurity/SystemRotation` checks fresh metrics after rotation |
| Web UI | `internal/ui: TestSPAHandler`, `TestNotBuiltFallback`, `TestHandlerMatchesBuildTag`; UI TypeScript, ESLint and Vite checks above; `internal/server` checks backing APIs and SSE | No automated browser integration suite | No DOM/browser scenario. `TestAPICLI/Workflow` and the capability scenarios exercise backing operations, not rendered UI interactions; use the browser checklist below |
| Authentication, RBAC and OIDC | `internal/auth` (bootstrap, login, refresh, recovery, OIDC validation), `internal/rbac`, `internal/server` (CSRF, object filtering and credential-read separation), `internal/store` | HTTP/identity tests run locally; a configured test issuer is exercised by the deployed OIDC scenario | `TestRBAC/Accounts`, `TestRBAC/OIDC` |
| REST API and CLI | `internal/server: TestEveryOperationHasImplementedHandler`, plus handler tests; `internal/cli: TestEveryOperationHasCommand`, plus CLI tests; `internal/events` and `internal/tools/rbacgen` | Real agent protocol coverage in `test/integration: TestAgent`; no separate full CLI integration scenario | `TestAPICLI/Workflow`; all capability scenarios use the generated REST client |
| Installation and control-plane recovery | `internal/server: TestConfigFromZeropsEnvironment`; `internal/auth: TestBootstrapAndKeyPersistence` and admin recovery tests; `cmd/karakoram: TestServerRecoveryRunsBeforeListenersAndOnlyOnce`, `TestRecoveryEnvironmentValidation`, `TestOfflinePasswordReset`; `internal/store`, `internal/secrets` | `test/integration: TestAgent` checks static agent startup, readiness and clean shutdown, not installation of the control plane | No dedicated installer scenario. After installing, run `TestReplicaSet/Singleton` and `TestAPICLI/Workflow`; complete the installation checklist below |
| Namespaces and relays | `internal/namespaces` (validation, enrollment/rotation, ownership/deletion guards, durable deployment); `internal/relay` (mTLS rejection, policy/DNS restrictions, concurrent streams, reconnect/replacement, reverse agent TLS, bounded handshakes and EOF); `internal/store` (migration/sealed identities); `internal/rbac`, `internal/server`, `internal/cli`, `internal/monitoring` (namespace isolation, export and filtering) | Relay loopback/network tests run locally; they do not establish Zerops L7 behavior | `TestMultiNamespace` |

### Namespace-specific coverage

Migration regression `TestNamespaceMigrationPreservesLegacyData` opens legacy
state and checks default-namespace readability/decryption. Registry tests include
`TestRotationPreservesLiveRelaySession`, `TestDeleteGuardsAndOwnedRelay` and
`TestForeignRelayNeverDeleted`. Transport tests include
`TestAuthenticationFailures`, `TestTargetRefusals`, `TestInboundAgentTLS`,
`TestSessionReplacement`, `TestPendingHandshakesBounded` and
`TestResponseBeforeCloseAndEOFClosesPair`. API/RBAC/monitoring tests exercise
same-name object isolation, per-item collection/SSE filtering, policy normalization,
cross-namespace export checks and authoritative metric labels. These are
coverage descriptions, not claims that a particular checkout's checks passed.

The environment-gated **`TestMultiNamespace`** requires a disposable second
project/token, public IPv6 test CIDR and all five S3 storage-creation inputs;
see [the suite guide](../test/e2e/README.md). It registers/reuses a disposable
namespace, waits for relay enrollment, creates a three-member source, checks
credentials/database users/namespace-local backup, clones into a three-member
`default` target and verifies restored marker data. It also checks namespace
RBAC/export denial, non-disruptive token rotation and guarded teardown.
Serialize it with RBAC/OIDC scenarios and policy/settings changes. Missing
prerequisites skip the scenario, not establish success.

## Browser workflow checklist

Use the built UI against a disposable deployment, with admin and restricted accounts:

- Sign in, complete the required password change, sign out, and verify session expiry/refresh behavior. If OIDC is configured, complete a real issuer login and check group-derived permissions.
- Create replica-set and sharded clusters; inspect topology, member health, versions, resources and placement warnings. Edit supported settings and observe generation convergence.
- Configure and verify backup storage; create backups, inspect PITR coverage, restore in place and clone. Verify recovered database contents independently.
- Create/update/delete application users and roles; verify credential reveal is explicit and permission-gated. Exercise TLS and public-access controls only on disposable databases.
- Check monitoring charts, events/logs, operations and installation settings. Confirm restricted users cannot see or mutate unauthorized resources, including through direct navigation.
- Refresh and navigate directly to nested routes; reconnect live events without losing the replay cursor. Check errors and pending operations remain understandable when requests fail.
- Add a second-project namespace with invalid and valid token inputs; inspect relay health, rotate/re-validate and exercise guarded removal. Switch All/dev/prod views, refresh shared namespace links, check same-named objects remain distinct and test source-namespace restore/clone permissions with a restricted account.

Static UI checks and API tests do not replace this browser exercise.

## Installation and recovery checklist

Follow [installation](install.md) using `deploy/zerops/import-local.yaml` or a publicly reachable repository with `deploy/zerops/import.yaml` in your own project:

1. Confirm the state volume is mounted, the control plane has one container, the integration token is project-scoped, and the master key is preserved securely.
2. Verify HTTPS UI/API access and private agent connectivity; do not expose port 9443 publicly.
3. Complete first-run admin password change, create a restricted API token, and create/delete a small cluster.
4. Restart/redeploy the control plane and confirm accounts, desired state and inventory persist without overlapping SQLite writers.
5. Exercise [admin recovery](operations.md#recovering-local-admin-access), revoke temporary credentials and remove the recovery secret afterward.
6. Take a consistent control-plane state backup, retain the original master key separately, and rehearse [state recovery](operations.md#control-plane-state-backup-and-recovery). This does not back up MongoDB data; exercise PBM recovery independently.

## Interpreting results

Inspect individual assertions and skipped prerequisites, not only the process exit code. Verify recovered data, acknowledged writes, observed generations, member health and terminal operations. Configuration acceptance is not a substitute for database or external-network observation. A test-only allowlist observation option does not establish source-restriction enforcement. Keep logs and optional JSON snapshots private: status and error messages can contain deployment metadata even when secrets are withheld.

See [limitations](limitations.md), [security](security.md) and the [end-to-end suite guide](../test/e2e/README.md) for boundaries, prerequisites and cleanup.
