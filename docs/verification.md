<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Verification

Verification checks contracts, real engine behavior and deployed workflows at different layers; none establishes production availability, physical isolation or a recovery guarantee.

## What checks establish

- **Local automated checks:** validation, authorization, state transitions, recovery checkpoints and generated-contract consistency.
- **Docker integration:** real engine/agent protocols, TLS, administration and selected snapshot/membership behavior, not Zerops placement or provisioning.
- **Deployed checks:** paid platform resources, engine data paths and recovery under the configured prerequisites. Skipped capabilities are unverified, not successful.
- **Browser checks:** rendered navigation, permissions and workflows. Static checks and backing-API scenarios are not browser automation; no automated browser-workflow suite is supplied.

Inspect individual assertions/skips, terminal operations, observed generations, member health and recovered data—not only exit status. Configuration acceptance is not external-network success. Observation-only allowlist mode does not verify enforcement. Persistence checks are not continuous availability under load; definitions recovery is not message recovery. API deletion is not independent enumeration of platform cleanup.

## Running the checks

Run from the repository root with Go 1.25+. Docker tests need daemon access/image pulls; host-network tests need Linux and unused ports, not a host serving important databases. Install UI dependencies before generated consistency checks. UI helper tests require Node 22+ with experimental TypeScript stripping.

```sh
# Local unit/HTTP checks:
go test ./...
# All Docker integration checks, including internal tagged tests:
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
