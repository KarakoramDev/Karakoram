<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Web UI

The embedded Web UI manages authorized resources through the same API as `karactl`; open the installation's HTTPS origin, without a separate frontend deployment or browser infrastructure credential.

## Sign in

Use your local account or the configured SSO button. Bootstrap and reset-password accounts must complete **Change password**, then sign in again. Later, use **Change password** at the foot of the sidebar. The browser uses HttpOnly session cookies; refresh and CSRF handling are automatic. See [access control](access-control.md) for accounts, SSO and permissions, and [security](security.md) for session protections.

Missing or disabled controls can reflect RBAC restrictions. Without create permission, an empty cluster list says **No accessible clusters** rather than offering creation. Delete actions require deletion permission; backup-policy controls require cluster-update permission. Typing a resource URL does not bypass server authorization.

## Namespace selection

The app-shell switcher offers **All namespaces** and readable namespaces with token/relay health indicators. Selection persists in `?namespace=` and a non-secret local preference. Clusters, backups, events, operations and storages follow that selection. Cluster-detail links retain namespace to distinguish same-named clusters; switching namespace from a detail page returns to the scoped list. Choose an explicit namespace for creation.

**Not in your namespace list** means the selection is absent from your readable list, not necessarily deleted. An API-confirmed missing namespace offers **Back to all namespaces**. See [namespaces and relays](namespaces.md).

## Clusters and creation

**Clusters** lists authorized clusters and their phases. **Create cluster** opens the catalog-driven wizard:

| Step | What to choose or inspect |
| --- | --- |
| Basics | Namespace, unique name, engine, exact catalog version, topology and optional preset |
| Topology | Provider-supported members, components and configuration |
| Resources | Per-component CPU mode and CPU/RAM/disk bounds |
| Backups | Namespace-local storage, supported schedules and optional recovery source |
| Access & TLS | Supported TLS/certificate options, private/public exposure, allowlist and monitoring |
| Review | Complete request, aggregate runtime/resource footprint, volumes and placement limitations |

Use **Continue** or **Back**; validation keeps you on the affected step. Names are 2–12 lowercase letters/digits, begin with a letter and must be unique in the namespace. Schedule retain counts must be positive integers.

Engine-specific fields, presets, minimum resources and connection procedures belong in the [MongoDB](mongodb/README.md), [RabbitMQ](rabbitmq/README.md) and [Elasticsearch](elasticsearch/README.md) guides. Review aggregate capacity across every component against project capacity before submission. Footprint bounds are not initial allocations, a price quote or a host-separation guarantee; backup object storage is additional. Read [platform limitations](limitations.md).

[PostgreSQL](postgres/README.md) and [Supabase](supabase/README.md) add single/HA database topology, proxy resources, synchronous policy and engine-specific options. Supabase component versions are release-locked; the [current artifact inventory](supabase/components.md) can reject a catalog preset or component even when its configuration is otherwise valid.

OpenBao adds engine options in the Basics step: unseal share count and threshold (5/3 by default), the seal (Shamir, transit or AWS KMS auto-unseal, with the transit token and AWS keys entered as write-only secrets that are stored encrypted and never shown again), Raft or PostgreSQL storage and the log level. Unseal shares, seal and storage are fixed once the cluster is initialized; **Topology** shows them read-only and only the log level remains editable. When seeding a new OpenBao cluster from a Raft snapshot whose source cluster was deleted, the Backups step accepts the source's unseal shares pasted from your offline copy; they travel once in the create request and are never cached.

PostgreSQL storage is selectable only when the OpenBao provider advertises `postgresql-storage`, which requires the control plane's shared PostgreSQL binding service (present in the standard server); merely having PostgreSQL clusters in the namespace is not sufficient. Without that service the wizard disables the option and explains that integrated Raft must be used. Pasted restore shares are discarded when disabled and after submission, including a failed request; retrying requires pasting them again.

Creation acceptance starts asynchronous work. Follow cluster health and desired-generation convergence, not just the success notification; see [operations and status](clusters.md#operations-and-status).

## Cluster detail tabs

Tabs and actions are gated by engine capabilities and your permissions.

| Tab | What it shows | Engines where it appears |
| --- | --- | --- |
| Overview | Phase, conditions, password-free connection summary and explicit credential reveal | All |
| Topology | Members, health, resource/membership changes, placement, replacement and logs | All |
| Upgrades | Catalog-based version changes and rolling restart | All; additional controls vary by engine |
| Backups | Supported backup inventory, on-demand backup, storage and schedules | All; recovery capabilities vary |
| Restores | In-place restore or recovery into a new cluster, progress and errors | All |
| Users | Engine identities and roles | MongoDB |
| Access | Engine-native access management | RabbitMQ |
| Elastic Access | Engine-native access management | Elasticsearch |
| Postgres access | PostgreSQL roles, databases and grants | PostgreSQL and Supabase |
| Components | Supabase service topology/resources | Supabase |
| Functions & SMTP | Source deployment/deletion and write-only SMTP password | Supabase |
| OpenBao | Seal/initialization state per member and Raft voter observations, explicit unseal-key reveal, database credential connections | OpenBao |
| Monitoring | Per-member time series and status context | All |
| Events | Cluster activity and diagnostic messages with actor attribution | All |
| Operations | Durable asynchronous work, progress, failure and initiating actor | All |
| Delete | Deletion or retained-volume/backup choices | All, with deletion permission |

In **Topology → Review & apply**, inspect **What will change** and expand **Merge patch to be sent** for the exact request. CPU-only changes do not move data, but platform resource changes may still restart a member. **Leaving** members remain visible during removal; active-member health ratios exclude them and show their count separately. **Upgrades** distinguishes observed running/member versions from the desired version; **not yet observed** is not deployment confirmation.

PostgreSQL major upgrades (16→17 and 17→18) require **offline maintenance**, not a rolling restart: all data members stop for `pg_upgrade --link`, and database access remains unavailable through replica rebuild. Both the topology change review and upgrade confirmation describe this outage and require a recent successful backup. The in-progress warning retains the offline guidance while data members report mixed source/target majors during replica rebuild. Minor upgrades retain replica-first rolling maintenance (or downtime for a single member).

**Reveal credentials** requires `cluster-credentials:read` and is audited. Revealed credentials are the engine's administrative identity, **not an application identity**. Use scoped application access: [MongoDB](mongodb/access-and-tls.md), [RabbitMQ](rabbitmq/access-and-tls.md), [Elasticsearch](elasticsearch/access-and-security.md). The response is not retained in the query cache. Keep copied passwords/URIs out of logs. Reveal also provides the public CA with copy/download controls; if clipboard access fails, use manual copy or download without losing PEM line breaks.

Supabase adds a separately authorized API/dashboard credential reveal; do not expose service-role/secret keys to application browsers. [PostgreSQL access](postgres/access-and-tls.md) describes database identities and ProxySQL limits; [Supabase operations](supabase/operations.md) covers source visibility and key rotation. Functions source controls do not supply an absent runtime artifact.

The **OpenBao** tab reveals the unseal (or recovery) shares and the initial root token only after typing the cluster name; it requires `openbao-unseal-keys:reveal` (administrators by default), is audited, and keeps the response in the open panel only — hide it or leave the page and it is gone. Store the shares offline immediately. Database connections list each target, mount, phase and the credential paths of its roles; creating one needs `clusters:update` on the OpenBao cluster and `database-users:create` on the target, deleting one `database-users:delete` on the target. Only PostgreSQL, MongoDB replica-set and RabbitMQ clusters in the same namespace are offered as targets. In-place restores of an OpenBao cluster from another cluster's snapshot accept pasted unseal shares the same way as the creation wizard.

Changing either the cluster name or namespace discards the revealed material and confirmation state immediately. Pending reveal requests are aborted when their panel is removed, and late responses cannot populate a different cluster's panel.

JWT rotation is not a **Functions & SMTP** UI action. Use `karactl -n NS supabase rotate-jwt-secret CLUSTER --yes` or POST `/api/v1/clusters/{cluster}/supabase/rotate-jwt-secret`; follow the [CLI/API rotation procedure](supabase/operations.md#credentials-and-rotation) and coordinate application key replacement.

**Cluster → Backups → Backup now** requires a registered storage. Follow the operation and inventory to completion; acceptance does not mean a successful backup. Restore/clone dialogs can select a source namespace; see [backups and recovery](backups.md) for permissions and storage selection. For destructive retention choices, see [delete or suspend](clusters.md#delete-or-suspend).

## Data MCP

Supported clusters show a **Data MCP** card on **Overview**, separate from database connection credentials. Inspect the phase, backend/upstream version, cluster-level access and diagnostic message. Copy the per-cluster endpoint or the `karactl mcp config --cluster <namespace>/<cluster>` command; the CLI, not the browser, mints the restricted token. A write command requests token scope but does not bypass owner RBAC or the installation write switch.

Users with cluster update permission can enable/disable data MCP, select read-only/read-write access, set engine-specific scope, deny tools, require write confirmations and opt into recording query text in audit. Save applies `spec.dataMcp` as a cluster merge patch; disabling asks for confirmation. Other users see settings read-only. Enabling data access and requesting writes are security decisions: review the [data MCP guide](data-mcp.md), particularly [all four write switches](data-mcp.md#four-switches-for-writes), before sharing a client configuration.

## Installation settings

| Settings page | Purpose |
| --- | --- |
| Backup storages | Register S3 credentials, inspect transport options and latest verification result; see [backups](backups.md) |
| Namespaces | Add/validate projects, inspect relay health, rotate tokens and remove unused namespaces; see [namespaces](namespaces.md) |
| Accounts, RBAC, API tokens, SSO | Account lifecycle, policy/effective-permission checks, one-time token creation and OIDC configuration; see [access control](access-control.md) |
| Monitoring | Collection status and Prometheus guidance; scrape URL is installation-root `/metrics` |
| Orphaned volumes | Read-only inventory of volumes awaiting platform cleanup; see [operations](operations.md#orphaned-platform-volumes) |

The sidebar **API specification** opens `/api/v1/openapi.yaml`. Settings never expose stored backend infrastructure credentials or the operator master key.

The API-token creation dialog blocks double submission and reuses its retry key for an unchanged body. After creation, the one-time-secret panel provides `KARAKORAM_SERVER` / `KARAKORAM_TOKEN` CLI commands and the running OpenAPI link. Store the secret before closing; prefer secret-manager injection over pasting secret-bearing commands into history or shared logs.

## Events history and live rows

Global **Events** and cluster **Events** initially load the newest **200** authorized persisted events, not every history page. Matching SSE events merge by ID, newest-first; the live buffer holds up to **500** rows. For older history, use [API pagination](api.md#pagination-and-events) or `karactl events list` with the returned cursor.

Apply **Type**, global **Cluster**, and authorized **Actor / Resource** filters with **Apply filters**. Applied filters persist in the URL; draft edits do not apply automatically. Values are exact and case-sensitive. Type is `Normal`, `Warning` or `Audit`; `OperationFailed` is a reason, not a type. Audit visibility requires the relevant `audit:read` permission. See [audit attribution and durability](security.md#audit-attribution-and-durability).

Expand **Event details** for complete JSON. Cluster names and operation IDs link to their detail pages; authorization still applies to shared links.

## Operation details and cancellation

Acceptance toasts offer **View operation** when the response identifies one. Events and global/cluster **Operations** link to the same detail page. It shows phase, namespace/cluster, target, step, initiating actor, reported progress, message/error, cancellation state and exact UTC timestamps. Active operations poll every three seconds; duration starts at `startedAt`, or `createdAt` if absent.

**Cancel operation…** is available for Pending/Running work with cluster-update permission unless already requested. Confirmation requests cancellation, not rollback; unsafe checkpoints can return **409**, and Running work may remain Running during cleanup. Follow the terminal result. See [API cancellation mechanics](api.md) and the [status model](clusters.md#operations-and-status).

## Monitoring ranges

| Range | Query step |
| --- | --- |
| 15m | 15s |
| 1h (default) | 30s |
| 6h | 2m |
| 24h | 10m |

Selection persists as `range` in the URL; there is no seven-day UI query. Charts retain the full window with sparse samples. Recognized state series have labels/legends; fractional state values represent averaging across sampled states, not extra engine states. Use [external monitoring](monitoring.md) for longer retention.

## Errors and live updates

Read the request error, cluster conditions and existing operation before retrying. Forms preserve server error context; unchanged supported submissions reuse retry keys as described in [safe API retries](api.md#safe-retries-with-idempotency-key).

**Live**, **reconnecting — data may be stale** and **connection lost — data may be stale** describe delivery state. Failed background reads can mark data stale even with SSE connected. Retained values during outages are last-known data, not fresh health confirmation. Refresh and inspect durable CLI/API status before resubmitting; see [SSE reconnect and proxy buffering](api.md#pagination-and-events).

**The page could not be displayed → Reload page** reloads without resubmitting an action. If the origin reports **UI not built**, deploy a binary with built frontend assets; see [contributor build instructions](../CONTRIBUTING.md) and [operations](operations.md).
