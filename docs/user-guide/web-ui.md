<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Web UI

The React SPA is embedded in the Karakoram server when production assets are built. Open the installation's HTTPS origin; no separate frontend deployment or browser Zerops token is needed. The UI calls the same REST API as `karactl` and listens to durable event SSE for live view invalidation.

## Sign in

Use local username/password or the configured SSO button. First bootstrap and reset-password users are directed to **Change password** before ordinary pages. After a local password change, sign in again. To change your own password later, choose **Change password** at the foot of the sidebar. Browser credentials are HttpOnly cookies, not local-storage bearer tokens; refresh and CSRF handling are built into the client. [Authentication](rbac-and-auth.md) explains local/OIDC setup and authorization.

A missing/disabled control can be an RBAC restriction, not missing functionality. Viewers without deletion permission have no cluster **Delete** tab/action; backup-policy controls without cluster-update permission are read-only. Without create permission, an empty cluster list says **No accessible clusters**, asks you to contact an administrator, and does not offer **Create your first cluster**. The server enforces permissions regardless of what the UI displays. Unauthorized resources do not become visible just by typing a URL.

## Namespace selection

The app-shell switcher is always visible, even with only one namespace or
collapsed navigation. It offers **All namespaces** and readable namespace names
with token/relay health indicators. Selection is persisted in `?namespace=` and
a non-secret localStorage preference. Clusters, backups, events, operations and
storages follow it; cluster detail routes carry the namespace so same-named
clusters remain distinct. Switching namespace from a cluster detail page returns
to the scoped cluster list. Choose an explicit namespace for creation.

A selection absent from your readable namespace list is marked **Not in your
namespace list**, not silently replaced with All namespaces. If the API reports
that the namespace does not exist, the view names it and offers **Back to all
namespaces**. A namespace missing from your readable list is not by itself proof
that it was deleted; authorization still applies. See [namespaces](namespaces.md).

## Clusters and creation

**Clusters** lists authorized clusters and their current phases. **Create cluster** opens the catalog-driven wizard at `/clusters/new`. Select a name, exact catalog version, topology, component replicas/resources and the relevant backup, recovery, TLS, exposure and monitoring settings. Review the resource footprint and placement limitations before submitting; resource choices are not a binding price quote.

The wizard uses provider metadata/presets; it does not promise a “production” preset supplies physical anti-affinity. Name, topology, backup dependencies and unsafe transitions are validated by the server. Creation acceptance starts reconciliation; keep observing until member health and desired-generation convergence are reported. [Replica sets](replica-sets.md), [sharding](sharding.md) and [limitations](../limitations.md) describe the constraints.

Percona member resources require at least **0.5 GiB RAM** and **5 GiB data disk**. The resource editor applies these limits before submission; an initial cluster may not yet have connection hosts, in which case Overview shows the initializing message rather than a connection URI.

### Creation wizard walkthrough

The wizard has six screens, in this order. Choose **Continue** after completing
each screen, or **Back** to revise a previous screen. A double-click advances
or returns only one screen; double-clicking Continue cannot also submit Review.
Invalid input keeps you on the affected step with field errors; review errors
must be resolved before creation.

Cluster names must be 2–12 lowercase letters/digits beginning with a letter and
unique in the target namespace; invalid names are rejected rather than silently
lowercased. Schedule **Retain (count)** must be an integer of at least one.
Entering zero keeps the value visible with an inline error instead of changing
it to one.

| Screen | Fields and choices |
| --- | --- |
| **Basics** | Target namespace (selected automatically when only one is available), cluster name, engine, exact version, topology and optional preset. Unavailable catalog versions cannot be selected. |
| **Topology** | Replica-set member count; for sharding, shard count, members per shard, config-server members and routers. MongoDB replica-set/config-server choices are 1/3/5/7. Optional engine configuration overrides are YAML and remain subject to provider-owned-setting restrictions. |
| **Resources** | CPU mode, minimum/maximum CPU cores, optional start CPU and minimum/maximum RAM in GiB for each component. Data-bearing components also have minimum/maximum disk in GiB. Sharded clusters have separate shard-member, config-server and router resource editors; routers have no durable data-disk editor. |
| **Backups** | Primary namespace-local storage or None; provider-supported backup schedules (name, five-field UTC cron, type, storage, positive retain count, enabled); PITR and oplog slice interval where supported; optional seed data from a backup or recovery time, including source namespace/cluster. |
| **Access & TLS** | TLS mode, managed or supplied CA, certificate inputs, private/public exposure, IP family and client CIDR allowlist, plus per-member monitoring. A supplied CA needs its certificate and signing private key; supplied server certificate/key are an optional matching pair. |
| **Review** | Namespace/name, engine/version, complete topology, each component's resource bounds, backup storage/schedules/PITR, optional seed source, TLS/CA source, exposure/IP family/allowlist and monitoring. Review also summarizes total runtime instances and CPU/RAM, data-volume count/capacity and additional runtime disks separately, plus engine configuration and placement limitations. Its Zerops pricing link is not a live monetary quote or host-separation guarantee. |

The footprint totals multiply per-shard engine members by shard count and
include config servers and routers in runtime CPU/RAM totals. Routers do not
add data volumes; their runtime disks are counted separately. These are
aggregate autoscaling bounds, not initial allocations or a billing quote, and
backup object storage is additional.

The current MongoDB catalog offers **Custom** (no preset), **dev** (one member,
1 GiB RAM, no database redundancy), **prod** (three voting members, without a
host-separation guarantee), and **sharded** (two three-member shards, three config
servers and one router). A preset replaces topology/resource/backup defaults,
but you can edit them afterward; it is not a capacity or production-readiness
certification. The catalog served by your installation is authoritative.

For a first replica set, choose **dev** for disposable development or **prod**
for three voting members, keep private access and `requireTLS`, choose an existing
S3 storage if you need backups, then inspect Review before **Create cluster**.
For a sharded deployment, also check the resource totals across *all* shards and
config servers. Creation starts asynchronous work; follow the resulting cluster
and operation until the desired generation is observed and members are healthy.
For literal CLI examples and readiness checks, see [replica sets](replica-sets.md)
and [sharding](sharding.md).


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
| Events | Cluster activity and diagnostic messages, including the actor |
| Operations | Durable asynchronous operation progress/failure and initiating actor |
| Delete | Destructive deletion or retained-volume/backups choices |

Use operation and resource status together. Accepted mutations are not completed provisioning/upgrades/restores, and local TCP readiness does not prove replica-set health. Physical restore intentionally stops database processes; do not use UI restart buttons to undo a restore's maintenance holds.

During membership removal, members reported as **Leaving** remain visible with
in-progress health styling, not as unhealthy members. Overview and cluster-list
health ratios count active members only and show the leaving count separately.
Member tables retain a previously observed role while available in the current
view; without one, the role cell also says **Leaving** rather than **unknown**.

In **Topology → Review & apply**, **What will change** describes only the patch
being submitted: membership addition/removal, resource changes or CPU-only
updates, and any other changed settings. Expand **Merge patch to be sent** to
inspect the exact `{spec: ...}` request. CPU-only updates do not move data, but
platform resource changes may still restart a member. See
[scaling](scaling-and-upgrades.md#resource-and-membership-scaling).

**Upgrades** separates the observed **Running version** and per-member versions
from **Desired version**; **not yet observed** is not confirmation of deployment.
Upgrade and restart confirmations warn of downtime for a singleton and possible
brief write pauses during primary failover for a multi-member MongoDB replica
set. FCV remains separate from binary version: raising it enables new on-disk
features, cannot be undone through Karakoram, and never implies supported binary
rollback. A backup requirement shown by the UI is also
[enforced by the server](scaling-and-upgrades.md#required-pre-upgrade-backups).

Restore/clone dialogs support selecting a source namespace; cross-namespace
recovery checks source `backups/export` as well as backup read and target
mutation. Source storage is used during recovery; target backup settings must
refer to target-local storage. See [cross-namespace recovery](backups-and-restore.md#cross-namespace-recovery).

**Reveal credentials** is an explicit, audited action requiring `cluster-credentials:read`. Its response is not stored in the query cache; treat displayed/copied URI/password material according to [users/TLS/access](users-tls-access.md). Reveal includes the public cluster **CA certificate**, with copy and `ca.pem` download controls. If browser clipboard access is denied, use the visible error/manual-copy alternative or download rather than losing the PEM line breaks. Normal Overview data does not reveal application/system passwords. The revealed administrative account is not a workload identity: follow the scoped application-user link and the [mongosh/PyMongo TLS recipe](users-tls-access.md#connect-an-application-user-with-tls). Public replica-set endpoints still require correct discovery of private advertised members; a public seed is not by itself an external failover solution.

After revealing MongoDB credentials, expand **How to connect (MongoDB)** for a
copyable `mongosh` command with `--tlsCAFile ca.pem` and a driver URI with
`tlsCAFile=ca.pem`. Download the CA first, run private-endpoint clients inside
the database project network, and replace the administrative credentials with
your application's scoped identity. URI-encode its credentials; the CA path is
relative to the client process. Do not disable certificate verification.

For **MongoDB application users** under **Users → Add user**, an empty password is intentionally omitted from the create request: the server generates a password and the success dialog displays it exactly once. Copy it into a secret manager before closing, or supply your own password of at least eight characters. In **Edit roles / password**, an empty password instead retains the existing password; it does not generate a replacement. Application authentication databases and role references are separate from Karakoram accounts. See the [scoped application-user and TLS recipe](users-tls-access.md#connect-an-application-user-with-tls).

Custom-role deletion requires removing its user assignments and inherited-role
references first. The deletion dialog lists known dependents; the server returns
**409** with their identities if references remain, rather than stripping them.
The system-user table shows **Last rotated** (`lastRotatedAt`), advanced only by
a successful rotation. See [users and roles](users-tls-access.md).

## Installation settings

**Settings** contains:

- **Backup storages:** S3 destination/credential registration and verification. Transport options show HTTP/HTTPS and certificate verification separately from the latest in-session probe result, its failure category/message and check time.
- **Namespaces:** project/token/relay health, Validate and add, detail/description, token rotation, re-validation and guarded removal. Tokens are write-only; default-token rotation requires a service-secret update/restart.
- **Accounts:** local account lifecycle, enable/disable and password reset.
- **RBAC:** full-policy editing and effective authorization checks.
- **API tokens:** one-time-secret creation, metadata and revocation.
- **SSO:** OIDC issuer/client/scopes/groups settings.
- **Monitoring:** collection status and Prometheus integration guidance; the actual scrape URL is the installation-root `/metrics`.

The sidebar **API specification** link opens the installation's authoritative specification at
`/api/v1/openapi.yaml`; see [REST API](api.md) for authentication and examples.

Global **Events** and **Operations** pages aggregate authorized activity across clusters and show **Actor**, as do their cluster-detail tabs. Actors identify local account names, OIDC account names with an `(oidc: issuerURL)` suffix, API-token owners with token name/ID, or system work such as `reconciler`, `scheduler`, `system:startup` and the orphan-volume janitor's `system`. Desired-state reconciliation preserves the actor recorded with the generation; asynchronous operations retain their initiating actor rather than attributing their results to whichever user last opened the page.

With **`audit:read`** for the affected objects, the global **Events** page also includes authorized audit activity and **Resource**/**Target** columns. In **Event filters**, set **Actor / Resource**, then **Apply filters**, to match exact, case-sensitive values; leave a field blank to include all values. These filters compose with the namespace switcher and apply to both the durable list and matching live updates. Resource examples include `accounts`, `rbac`, `settings`, `api-tokens`, `auth`, `backup-storages`, `namespaces`, `clusters` and `orphaned-volumes`. Noncluster audit records have no Cluster: namespace-level records require namespace-scoped audit access, while global installation records require global audit access. Cluster records keep their cluster name and require `events:read` or `audit:read` on that namespace-qualified cluster. Clearing both fields and applying restores the view for the selected namespace, not every namespace.

Every successful mutation and credential reveal is audited without logging request/response bodies or secrets. Authentication records also cover login/OIDC success/failure and logout. Audit events use the same SQLite event history as cluster events, currently retained indefinitely without automatic age/count pruning; this is not a tamper-proof external archive. See [security](../security.md#audit-attribution-and-durability) for identity and durability limits.

### Events history, live rows and investigation

Global **Events** and the cluster **Events** tab load the newest **200**
authorized persisted events first (`order=desc`). This is one history page, not
an automatic fetch of every `nextCursor` page and not the complete audit archive.
Matching live/replayed SSE events are merged with that page, deduplicated by ID
and sorted newest-first; the live buffer keeps up to **500** rows. This can show
more than 200 rows without the user requesting another history page. To inspect
older history deterministically, use the [API's cursor pagination](api.md#pagination-and-events)
or `karactl events list` with the same filters and returned cursor.

Applied **Type** and global **Cluster** filters, plus **Actor / Resource** where
audit access is available, are kept in the URL query alongside namespace.
Refreshing or sharing that URL preserves the investigation scope; editing a
draft filter does not apply it until **Apply filters**. Values are exact and
case-sensitive. Event **Type** is the stored event type, such as `Normal`,
`Warning` or `Audit`; the lifecycle action is the **reason**, such as
`OperationFailed`, not a `type=operation.updated` filter.

Expand **Event details** in a row to inspect its complete event JSON, including
metadata and diagnostic references when present. Cluster names link to their
cluster and operation IDs link to `/operations/<id>`. Use these durable records
even if live delivery is disconnected or buffered. Authorization still applies
to shared URLs and operation links.

### Operation details and cancellation

Accepted asynchronous mutations show **View operation** in their success toast
when the response identifies the operation. Global and cluster **Operations**
lists, and **Events** rows carrying an operation ID, link to the same detail
page at `/operations/:id`.
The link opens the operation in the current app without reloading the page.
A notification confirms acceptance, not that the operation has finished.

The detail page shows phase, namespace/cluster, target, step, initiating actor,
progress when reported, message/error and cancellation-request status. Created,
started and completed times are exact UTC timestamps, not just relative ages.
Duration runs from `startedAt` (or `createdAt` if no start was recorded) to
completion; active work shows elapsed time. The page polls Pending/Running
operations every three seconds.

**Cancel operation…** is offered for Pending/Running work when you have
cluster-update permission and cancellation has not already been requested.
Confirming requests cancellation; the server still decides whether the current
checkpoint is safe and may return **409**. Pending work is cancelled immediately;
Running work may stay Running during cooperative cleanup. Cancellation does not
undo completed work or accepted desired state. Follow the phase/message to a
terminal result; see [API cancellation semantics](api.md#cancel-an-operation).

### Monitoring ranges and state series

**Monitoring** offers **15m**, **1h**, **6h** and **24h** ranges, kept in the URL
as `range`; the default is **1h**. There is no seven-day UI query. Their query
steps are **15s**, **30s**, **2m** and **10m**, respectively.
Charts keep the complete query window even when only one recent sample exists.
Time-axis ticks use evenly spaced, rounded times and include the calendar date
when the window crosses midnight; repeated local hours include their UTC offset.

Charts identify member/series lines and provide state-code labels and legends
for recognized state series, including MongoDB replica member states and
Up/Down health. Values are averaged over the query step: a fractional state
code represents a mixture of sampled states, not an additional database state.
Use [external Prometheus](monitoring.md#prometheus-scraping) for longer retention.


Group-based roles, complete CSV policy replacement and token ownership are explained in [RBAC/auth](rbac-and-auth.md). Settings pages do not grant access to Zerops project tokens or the operator master key.

**Settings > Orphaned volumes** is a read-only inventory of Local Storage volumes that Zerops refused to delete after their member runtimes were removed. A warning banner links to it while visible orphan records remain. The inventory follows the original cluster's read permissions; administrators see all records, including those for deleted clusters. The control-plane janitor retries deletion automatically, but the volumes still consume disk and may be billed for until Zerops deletes them. See [operations](../operations.md#orphaned-platform-volumes) for retry timing, events and manual cleanup.

## Errors and live updates

Read error messages, cluster conditions and the failed Operation before resubmitting. A failed request can differ from a failed asynchronous job. Events can replay on SSE reconnect; loss of the live connection does not erase durable operations. If a view looks stale, refresh and inspect [CLI/API](cli.md) status before assuming the mutation was lost. Do not repeatedly initiate backup/restore after a response-loss error; PBM receipts may deliberately report an ambiguous outcome.

If an unexpected display error prevents the app from rendering, it shows
**The page could not be displayed** with a **Reload page** button instead of
a blank page. Reloading does not resubmit the action. Inspect existing
operations and resource state before submitting another mutation.

The sidebar and Events views distinguish **live**, **reconnecting — data may be
stale** and **connection lost — data may be stale**. Failed background reads can
mark the sidebar stale even while SSE is connected. During a server/network
outage, any retained values are last-known data, not a fresh health confirmation.
Restore connectivity and re-read durable status before acting on them.

Mutation dialogs keep the server's human error message and field/resource
context visible without discarding the form. Submit controls become busy
immediately to prevent duplicate requests. Cluster create/edit, restart, resume,
TLS/system-user rotation, FCV raise, member replacement, backup/restore creation,
database-user/role create/edit, backup-storage create/edit and API-token creation
reuse their idempotency key when retrying an unchanged submission. The server
honours these keys for 24 hours after success; other actions are not covered.
See [safe API retries](api.md#safe-retries-with-idempotency-key). Read the outcome
and existing operation before starting another submission.

Proxies can **buffer small SSE event frames**. Karakoram sends approximately 16-KiB random-base64 connection comments and similarly padded fifteen-second heartbeats, but view invalidation can still wait until the next heartbeat. Each open stream adds approximately 16 KiB at startup and 16 KiB per heartbeat (~1.1 KiB/s before wire compression). Configure a nonbuffering custom-domain/proxy path where possible. Refresh/read durable status if a view appears stale; do not resubmit a completed mutation. See [API transport details](api.md#pagination-and-events) and [limitations](../limitations.md).

If the origin serves “UI not built”, build with `make ui` followed by `make build` (or `make build-all`) and deploy the resulting tagged binary. A Go-only server remains usable by API/CLI but does not contain the SPA. For development tooling see [ui/README.md](../../ui/README.md); for health, logs and safe troubleshooting see [operations](../operations.md).
