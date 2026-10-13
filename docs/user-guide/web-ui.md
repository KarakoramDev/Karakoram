<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Web UI

The React SPA is embedded in the Karakoram server when production assets are built. Open the installation's HTTPS origin; no separate frontend deployment or browser Zerops token is needed. The UI calls the same REST API as `karactl` and listens to durable event SSE for live view invalidation.

## Sign in

Use local username/password or the configured SSO button. First bootstrap and reset-password users are directed to **Change password** before ordinary pages. After a local password change, sign in again. Browser credentials are HttpOnly cookies, not local-storage bearer tokens; refresh and CSRF handling are built into the client. [Authentication](rbac-and-auth.md) explains local/OIDC setup and authorization.

A missing/disabled control can be an RBAC restriction, not missing functionality. The server enforces permissions regardless of what the UI displays. Unauthorized resources do not become visible just by typing a URL.

## Namespace selection

The app-shell switcher offers **All namespaces** and readable namespace names
with token/relay health indicators. Selection is persisted in `?namespace=` and
a non-secret localStorage preference. Clusters, backups, events, operations and
storages follow it; cluster detail routes carry the namespace so same-named
clusters remain distinct. Choose an explicit namespace for creation. The
switcher may be absent in a single-namespace view; direct links still require
server authorization. See [namespaces](namespaces.md).

## Clusters and creation

**Clusters** lists authorized clusters and their current phases. **Create cluster** opens the topology/provider-driven wizard at `/clusters/new`. Select a name, exact catalog version, replica-set or sharded topology, component replicas/resources and the relevant backup, PITR, TLS, exposure and monitoring settings. Review costs and placement warnings before submitting.

The wizard uses provider metadata/presets; it does not promise a “production” preset supplies physical anti-affinity. Name, topology, backup dependencies and unsafe transitions are validated by the server. Creation acceptance starts reconciliation; keep observing until member health and desired-generation convergence are reported. [Replica sets](replica-sets.md), [sharding](sharding.md) and [limitations](../limitations.md) describe the constraints.

Percona member resources require at least **0.5 GiB RAM** and **5 GiB data disk**. The resource editor applies these limits before submission; an initial cluster may not yet have connection hosts, in which case Overview shows the initializing message rather than a connection URI.

## Cluster detail tabs

| Tab | Use |
| --- | --- |
| Overview | Phase/conditions, password-free connection summary and explicit credential reveal |
| Topology | Member roles/health/lag/agent status, resource/membership controls, replacement/placement, member logs |
| Upgrades | Catalog-based binary changes, FCV promotion and rolling restart |
| Backups | Backup inventory, on-demand backup, storage/schedule/PITR configuration and observed window |
| Restores | In-place recovery and restore into a new cluster, restore status/errors |
| Users | Application users/custom roles and managed system-user rotation |
| Monitoring | Per-member time series and status context |
| Events | Cluster activity and diagnostic messages |
| Operations | Durable asynchronous operation progress/failure |
| Delete | Destructive deletion or retained-volume/backups choices |

Use operation and resource status together. Accepted mutations are not completed provisioning/upgrades/restores, and local TCP readiness does not prove replica-set health. Physical restore intentionally stops database processes; do not use UI restart buttons to undo a restore's maintenance holds.

Restore/clone dialogs support selecting a source namespace; cross-namespace
recovery checks source `backups/export` as well as backup read and target
mutation. Source storage is used during recovery; target backup settings must
refer to target-local storage. See [cross-namespace recovery](backups-and-restore.md#cross-namespace-recovery).

**Reveal credentials** is an explicit action requiring `cluster-credentials:read`. Its response is not stored in the query cache; treat displayed/copied URI/password/CA material according to [users/TLS/access](users-tls-access.md). Normal Overview data does not reveal application/system passwords. Use your own application account rather than the revealed administrative identity for workloads.

When adding a database user under **Users**, leave the password empty to have the server generate one. The success dialog displays that generated password exactly once; copy it into a secret manager before closing. Alternatively, supply your own password of at least eight characters.

## Installation settings

**Settings** contains:

- **Backup storages:** S3 destination/credential registration and verification.
- **Namespaces:** project/token/relay health, Validate and add, detail/description, token rotation, re-validation and guarded removal. Tokens are write-only; default-token rotation requires a service-secret update/restart.
- **Accounts:** local account lifecycle, enable/disable and password reset.
- **RBAC:** full-policy editing and effective authorization checks.
- **API tokens:** one-time-secret creation, metadata and revocation.
- **SSO:** OIDC issuer/client/scopes/groups settings.
- **Monitoring:** collection status and Prometheus integration guidance; the actual scrape URL is the installation-root `/metrics`.

Global **Events** and **Operations** pages aggregate authorized activity across clusters. Group-based roles, complete CSV policy replacement and token ownership are explained in [RBAC/auth](rbac-and-auth.md). Settings pages do not grant access to Zerops project tokens or the operator master key.

**Settings > Orphaned volumes** is a read-only inventory of Local Storage volumes that Zerops refused to delete after their member runtimes were removed. A warning banner links to it while visible orphan records remain. The inventory follows the original cluster's read permissions; administrators see all records, including those for deleted clusters. The control-plane janitor retries deletion automatically, but the volumes still consume disk and may be billed for until Zerops deletes them. See [operations](../operations.md#orphaned-platform-volumes) for retry timing, events and manual cleanup.

## Errors and live updates

Read error messages, cluster conditions and the failed Operation before resubmitting. A failed request can differ from a failed asynchronous job. Events can replay on SSE reconnect; loss of the live connection does not erase durable operations. If a view looks stale, refresh and inspect [CLI/API](cli.md) status before assuming the mutation was lost. Do not repeatedly initiate backup/restore after a response-loss error; PBM receipts may deliberately report an ambiguous outcome.

Proxies can **buffer small SSE event frames**. Karakoram sends approximately 16-KiB random-base64 connection comments and similarly padded fifteen-second heartbeats, but view invalidation can still wait until the next heartbeat. Each open stream adds approximately 16 KiB at startup and 16 KiB per heartbeat (~1.1 KiB/s before wire compression). Configure a nonbuffering custom-domain/proxy path where possible. Refresh/read durable status if a view appears stale; do not resubmit a completed mutation. See [API transport details](api.md#pagination-and-events) and [limitations](../limitations.md).

If the origin serves “UI not built”, build with `make ui` followed by `make build` (or `make build-all`) and deploy the resulting tagged binary. A Go-only server remains usable by API/CLI but does not contain the SPA. For development tooling see [ui/README.md](../../ui/README.md); for health, logs and safe troubleshooting see [operations](../operations.md).
