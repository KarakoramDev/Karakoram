<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# REST API and SDK

The public contract is [api/openapi.yaml](../api/openapi.yaml), [OpenAPI](https://www.openapis.org/) 3.0.3; use the specification embedded in your running server at **`GET /api/v1/openapi.yaml`** for deployment-matched schemas and clients.

Most routes are under `/api/v1`. `/healthz` and authenticated `/metrics` are installation-root exceptions. The specification download is public, served as `application/yaml` and cacheable for five minutes; there is no bundled Swagger UI. Relay and agent protocols are not public management APIs.

Agents can use the embedded [MCP server](mcp.md) at `/mcp`, with the same REST enforcement and API tokens.

## Authentication

Scripts use a Karakoram API token, **not a provider infrastructure credential** (such as a [Zerops](https://zerops.io/) integration token):

```http
Authorization: Bearer <karakoram-token>
```

Bearer tokens use their owner's current permissions and need no CSRF header. Local `POST /auth/login` takes `{username, password}`, sets session cookies and returns `{account, csrfToken, expiresAt}`. Cookie-authenticated unsafe methods require **`X-Karakoram-CSRF`** matching the session token/readable CSRF cookie; login itself does not. `GET /auth/me` returns the current cookie session's CSRF token, including after [OIDC](https://openid.net/developers/how-connect-works/). `POST /auth/refresh` rotates refresh credentials; reusing an older refresh token revokes that family. Complete mandatory password changes and reauthenticate before creating tokens or ordinary work.

OIDC uses `/auth/oidc/login` and `/auth/oidc/callback` with redirects/state/PKCE, not device-code login. Successful OIDC redirects to the installation root; `returnTo` currently does not change that. Account/password rules, token expiry/revocation and SSO setup live in [access control](access-control.md); cookie protections live in [security](security.md).

`POST /api-tokens` accepts optional immutable `restrictions`: `readOnly`,
`namespaces`, `clusters` (`namespace/name`), `permissions` (`resource:action`,
action may be `*`) and `allowCredentialReveal`. Token metadata returns the same
fields. Omission is unrestricted; an explicit object defaults credential reveal
to false. Updates accept only name/expiry. Restrictions intersect owner RBAC on
every route and returned object, including replay and streams; permission
discovery reports effective authority. See [restricted API tokens](access-control.md#restricted-api-tokens)
for the subset issuance rule and OIDC group limitations.

The UI bounds ordinary API requests to **30 seconds**, excluding SSE. Network/5xx failures preserve the session and offer retry. A 401 after a successful refresh, or a new 401 within **ten seconds** of that refresh, ends the session rather than repeatedly rotating credentials.

### Curl bootstrap

Requires [Bash](https://cgit.git.savannah.gnu.org/cgit/bash.git/), [curl](https://curl.se/), [jq](https://jqlang.org/) and an enabled local account whose mandatory password change is complete. Replace the origin without a trailing slash. The protected temporary cookie jar and CSRF header are both needed for token creation:

```bash
set -euo pipefail
set +x
umask 077
SERVER='https://example.com'
COOKIE_DIR=$(mktemp -d)
trap 'rm -rf "$COOKIE_DIR"' EXIT
COOKIE_JAR="$COOKIE_DIR/cookies"
read -r -p 'Karakoram username: ' USERNAME
read -r -s -p 'Karakoram password: ' PASSWORD
printf '\n'
LOGIN=$(jq -n --arg username "$USERNAME" --arg password "$PASSWORD" \
  '{username: $username, password: $password}' \
  | curl --fail-with-body --silent --show-error \
      -c "$COOKIE_JAR" -H 'Content-Type: application/json' \
      --data-binary @- "$SERVER/api/v1/auth/login")
unset PASSWORD
if [ "$(printf '%s' "$LOGIN" | jq -r '.account.mustChangePassword')" = true ]; then
  printf 'Complete the required password change and sign in again.\n' >&2
  exit 1
fi
CSRF=$(printf '%s' "$LOGIN" | jq -er .csrfToken)
unset LOGIN
CREATED=$(curl --fail-with-body --silent --show-error \
  -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
  -H 'Content-Type: application/json' -H "X-Karakoram-CSRF: $CSRF" \
  --data '{"name":"automation"}' "$SERVER/api/v1/api-tokens")
KARAKORAM_TOKEN=$(printf '%s' "$CREATED" | jq -er .secret)
TOKEN_ID=$(printf '%s' "$CREATED" | jq -er .token.id)
unset CREATED
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $KARAKORAM_TOKEN" "$SERVER/api/v1/auth/me"
curl --fail-with-body --silent --show-error \
  -b "$COOKIE_JAR" -H "X-Karakoram-CSRF: $CSRF" \
  -X POST "$SERVER/api/v1/auth/logout"
unset CSRF
```

Save the one-time `KARAKORAM_TOKEN` securely before closing the shell. `TOKEN_ID` identifies metadata/revocation, not the bearer secret. Logout closes the temporary session without revoking the API token. Revoke it with `DELETE /api-tokens/{id}` when no longer needed. Keep credentials out of shell tracing, logs and browser local storage.

## Authorization and credential reveal

[OpenAPI](https://www.openapis.org/)'s `x-karakoram-rbac` declares each operation's resource/action/object. Lists, streams and metrics filter individual authorized objects; collection access does not reveal all resources. See [access control](access-control.md).

`GET /clusters/{cluster}/connection` is an explicit, audited reveal requiring `cluster-credentials:read`, with `Cache-Control: no-store`. It returns **engine driver-defined administrative credentials** and public CA, not an application identity. Ordinary cluster/status reads are password-free. Follow engine access procedures: [MongoDB](mongodb/access-and-tls.md), [RabbitMQ](rabbitmq/access-and-tls.md), [Elasticsearch](elasticsearch/access-and-security.md). Storage keys/private PEM inputs are write-only; generated application passwords and API-token secrets are creation responses, not later GET results.

[PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) use [native PostgreSQL access](postgres/access-and-tls.md), not `/database-users` or `/database-roles`. Supabase's separate GET `/clusters/{cluster}/supabase/credentials` requires `cluster-credentials:read` and returns API/Studio URLs and sensitive service/dashboard keys with no-store; do not confuse this with a normal cluster read.

## Operation safety metadata

Every [OpenAPI](https://www.openapis.org/) operation declares `x-karakoram-agent` immediately after its
`x-karakoram-rbac` block. `go generate ./api/...` validates this inventory and
emits `internal/api/agentmap.gen.go`; CI's `make openapi-check` rejects missing,
invalid or stale metadata.

| Field | Contract |
| --- | --- |
| `tool` | Unique resource/verb snake_case name, 1–64 characters, starting with a lowercase letter. PUT uses `replace`; PATCH uses `update`. |
| `exposed`, `reason` | Exposure defaults to true. Browser-session, streaming and API-description operations use `exposed: false`, a nonempty reason and no tool. |
| `risk`, `confirm` | `read` and `write` require `none`. `destructive` requires `confirm` or `name` (type the target's name). |
| `secret` | Response can reveal credentials; defaults to false. Write-only inputs, namespace token status and password-free connection summaries do not count. |
| `async` | `none`, `operation` (poll an Operation), `generation` (observe cluster convergence), or `record` (poll a backup/restore record). |
| `idempotent` | Repeating identical arguments has no additional effect; defaults to false. This is not proof of side-effect freedom. |
| `untrusted` | Response carries engine or end-user content; defaults to false. Treat it as data, not instructions. |

A non-GET `read` requires `idempotent: true` and a reviewed
operation-ID/method/path entry in `agentgen`'s side-effect-free allowlist.
The allowlist covers permission checks and the four validation-only dry-run
routes. Authentication observability is not a domain mutation.

Status, member, operation, event and log responses are untrusted, including
nested engine messages in cluster and namespace responses. Restore and storage
verification messages also receive this classification. Backup deletion uses
`async: operation` because its asynchronous response is an Operation, while
backup and restore creation use `record`. Namespace deletion returns no
Operation or record, so it uses `none`; observe its lifecycle separately.

[OpenBao](https://openbao.org/)'s connection response is an exception to the administrative-credential set: it contains HTTPS endpoints and CA only. Initialization material has a separate confirmed reveal route and permission; see [OpenBao access and TLS](openbao/access-and-tls.md).

The `openbao_unseal_keys_reveal` management tool is secret and requires explicit
confirmation, as do creation and deletion of managed database connections.
Restricted tokens need both `allowCredentialReveal` and the independent
`openbao-unseal-keys:reveal` permission to reveal initialization material; ordinary
`cluster-credentials:read` does not grant it. Connection list/get tools return
password-free summaries and are idempotent reads.

## Resource families

Paths below are relative to `/api/v1`; cluster suffixes follow `/clusters/{cluster}`. **All** remains subject to engine driver capabilities and RBAC. Use [OpenAPI](https://www.openapis.org/) for exact paths, payloads and permissions.

| Family | Routes or cluster suffixes | Engines |
| --- | --- | --- |
| Identity/policy/settings | `/auth/*`, `/accounts`, `/api-tokens`, `/permissions`, `/rbac/policy`, `/rbac/can`, `/settings` | All |
| Discovery/health | `/providers`, `/providers/{provider}`, `/backends`, `/version`; root `/healthz` | All |
| Namespaces | `/namespaces`, `/namespaces/{namespace}`, `/token`, `/validate` suffixes | All |
| Cluster state/connections | `/clusters`, `/clusters/{cluster}`; `/status`, `/members`, `/placement`, `/connection` | All |
| Lifecycle | `/restart`, `/resume`, `/retry-deployments`, `/rotate-tls`, `/members/{member}/replace`, `/system-users`, `/system-users/{user}/rotate` | Engine driver-supported capabilities |
| Compatibility/balancing | `/raise-fcv`, `/balancer` | MongoDB only |
| Database identities | `/database-users`, `/database-roles` and identity paths | MongoDB only |
| Native access | `/rabbitmq/vhosts`, `/rabbitmq/users`, `/rabbitmq/permissions`, `/rabbitmq/topic-permissions`, `/rabbitmq/policies`, `/rabbitmq/operator-policies` | RabbitMQ only |
| Native access | `/elasticsearch/users`, `/elasticsearch/roles`, `/elasticsearch/api-keys` | Elasticsearch only |
| Native PostgreSQL access | `/postgres/roles`, `/postgres/roles/{role}`, `/postgres/roles/{role}/password`, `/postgres/databases`, `/postgres/databases/{database}`, `/postgres/grants` | PostgreSQL and Supabase |
| PostgreSQL consumer bindings | `/postgres/bindings`, `/postgres/bindings/{binding}/rotate` | PostgreSQL source; metadata only, never credentials |
| Native MySQL access | `/mysql/users`, `/mysql/users/{user}`, `/mysql/users/{user}/password`, `/mysql/users/{user}/password/discard-old`, `/mysql/databases`, `/mysql/databases/{database}`, `/mysql/grants` | MySQL and PXC |
| PXC minority recovery | `/mysql/recover-quorum` | PXC only; explicit confirmation and provider fencing |
| Native Neo4j access | `/neo4j/users`, `/neo4j/users/{user}`, `/neo4j/users/{user}/password` | Neo4j Community only; all users have administrative privileges |
| Native Weaviate access | `/weaviate/users`, `/weaviate/users/{user}`, `/weaviate/users/{user}/rotate-key`, `/weaviate/roles`, `/weaviate/roles/{role}` | Weaviate |
| Supabase services | `/supabase/credentials`, `/supabase/smtp-password`, `/supabase/functions`, `/supabase/functions/{name}`, `/supabase/rotate-jwt-secret` | Supabase |
| OpenBao | `/openbao/unseal-keys/reveal`, `/openbao/database-connections`, `/openbao/database-connections/{connection}` | Confirmed initialization-material reveal; list/create/get/delete managed target connections |
| Temporal certificates | `POST /temporal/client-certificates` (issue once); `GET /temporal/client-certificates` (metadata) | Temporal |
| Temporal namespaces | `GET /temporal/namespaces`; `DELETE /temporal/namespaces/{temporalNamespace}` (asynchronous) | Temporal |
| Temporal UI secret | `PUT /temporal/ui/oidc-client-secret` (write-only) | Temporal |
| Recovery | `/backup-storages`; cluster `/backups`, `/restores` | All |
| Continuous recovery window | `/pitr` | MongoDB, PostgreSQL, Supabase, MySQL and PXC |
| Activity/cleanup | `/operations`, `/operations/{operation}`, `/operations/{operation}/cancel`, `/events`, `/events/stream`, `/orphaned-volumes`; cluster `/operations` | All |
| Observability | Cluster `/metrics`, `/members/{member}/logs`; root `/metrics` | All |
| Fleet operations | Cluster `/upgrade-agents`, `/members/{member}/move`; `/settings/agent-rollout` | Engine driver-supported rolling upgrades and relocation |
| Control-plane recovery | `/control-plane/backup-policy`, `/control-plane/backups` | Installation-wide |
| Notifications | `/notification-channels`, `/notification-channels/{channel}`, `/notification-channels/{channel}/test` | Installation-wide |
| Backup verification | Cluster `/backup-drills`, `/backup-drills/{drill}` | Engines with eligible successful backups |

[PostgreSQL](https://www.postgresql.org/) role PUT replaces attributes; PATCH changes only supplied fields. Database routes expose creation/read/deletion, not update; grants are bounded database/schema privileges. Generated login passwords are one-time no-store responses, with sealed idempotent replay. Native access uses `database-users` RBAC, with extra `clusters:update` authorization for privileged role/membership/password changes. See [payloads and limits](postgres/access-and-tls.md).
[Neo4j](https://neo4j.com/) native-user reads use `database-users:read`; create/delete/password changes additionally require `clusters:update` alongside the corresponding `database-users` permission. Password responses are no-store. The managed `neo4j` administrator is hidden from lists and cannot be mutated here. See [Neo4j access and TLS](neo4j/access-and-tls.md).

See [consumer bindings](postgres/bindings.md) for rotation and deletion/upgrade guards. In-place restores of a bound PostgreSQL source require `acknowledgeBoundConsumers: true`; otherwise HTTP 409 `bound_consumers` lists affected consumer identities.

Distributed PostgreSQL exposes `POST /clusters/{cluster}/actions/rebalance` with `{strategy?: string}` and returns 202 Operation (`type: rebalance`). It uses `clusters:update`, CSRF and idempotency conventions. Members expose a read-only `group` (0 coordinator); `CitusTopology` conditions report metadata drift. See [distributed PostgreSQL](postgres/distributed.md).
[MySQL](https://www.oracle.com/mysql/) native access uses `database-users` RBAC for application users at the fixed `%` host, databases and bounded database privileges. User PATCH changes only supplied connection-limit and lock fields. Create and password rotation return generated passwords only once, with `Cache-Control: no-store` and sealed replay when an idempotency key is supplied. Rotation accepts `retainCurrent` for dual passwords; discard-old removes the retained password. Database creation defaults to `utf8mb4` / `utf8mb4_0900_ai_ci`; deletion requires `force=true` for a nonempty database. Grant listing requires at least one `user` or `database` filter. Global privileges and `GRANT OPTION` are not exposed.

The topology contract accepts MySQL Group Replication `group` and [PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster) `cluster` alongside `single`; engine driver catalogs identify which topologies each engine supports. Backup type enums include `incremental` for MySQL/PXC, separately from PostgreSQL's `incr` spelling.

MySQL and PXC share the native access routes, not engine driver identity or interchangeable backups. See [access and TLS](mysql/access-and-tls.md), [GTID-based recovery](mysql/backups.md) and [explicit PXC minority recovery](mysql/operations.md#explicit-pxc-minority-recovery). Quorum recovery requires cluster update permission, returns an accepted Operation and is not arbitrary SQL or a fencing bypass.

[Weaviate](https://weaviate.io/) uses [native users/roles](weaviate/access-and-tls.md), not [MongoDB](https://www.mongodb.com/) identity routes. List/get requires `database-users/read`; user create/update/delete uses the corresponding action. Assigning any role other than `viewer` or `read-only` additionally requires `clusters/update`, including roles already present on user update. Key rotation requires `database-users/update` and, if the user's current roles include any role beyond `viewer` or `read-only`, `clusters/update`. User deletion requires only `database-users/delete`, even for privileged users. Every role create/replace/delete requires both `database-users/update` and `clusters/update`. System/environment identities and built-in roles remain protected. Creation/key rotation returns no-store generated API keys; GET cannot retrieve them, while matching idempotency retries replay sealed results for 24 hours. Administrative reveal separately requires `cluster-credentials:read`.

[Supabase](https://supabase.com/) function GETs use `clusters:read`, mutations `clusters:update`; SMTP password is write-only. Function PUT replaces the relative-path/source map (100 functions maximum, 1 MiB each, nonempty `index.ts`). JWT rotation returns a durable operation. Source storage does not establish runnable Functions availability; enabled service artifacts must exist in the release lock. See [Supabase operations](supabase/operations.md).

[Temporal](https://temporal.io/) certificate issuance requires `cluster-credentials:read`; certificate/namespace listing requires `clusters:read`; namespace deletion and UI [OIDC](https://openid.net/developers/how-connect-works/) secret writes require `clusters:update`, all scoped to the cluster. Issuance returns private material once with `no-store`; secret writes return no value. See [Temporal access/TLS](temporal/access-and-tls.md) and [namespace/recovery operations](temporal/operations.md).

Fleet disruption requests and cluster updates support `ignoreMaintenanceWindow=true` where declared in the specification. Without an override, accepted operations can remain Pending until the next configured window; acceptance is not permission to interrupt an in-progress member step. See [maintenance and relocation](operations.md) and [agent rollout](upgrade.md).

Installation-wide agent rollout, control-plane backup and notification routes use `settings:read` or `settings:update`, not cluster permissions. Notification destinations and credentials are write-only; omitted secrets on replacement are retained only for an unchanged destination identity. Backup-drill creation uses `restores:create` on the source cluster; report reads use `restores:read`. Drill and control-plane backup responses are durable reports rather than cluster `Operation` resources: poll their respective inventories. See [notifications](notifications.md), [state recovery](control-plane-backup.md) and [restore drills](backup-drills.md).

## Namespace parameters and lifecycle

Object routes and creation default to **`default`**. Omitting `namespace` on cross-cluster collections (`/clusters`, `/operations`, `/events`, `/events/stream`, `/orphaned-volumes`, `/backup-storages`, root `/metrics`) includes all authorized namespaces; pass `namespace=NAME` to filter. Omit it rather than sending an empty value. Namespace is part of resource identity: retain it in caches and links.

For an explicit nonexistent namespace, authorized callers receive **404** `not_found` with `details.resource: namespace` and `details.name`. Without a permission there, collections remain empty and object requests retain their normal **403**, regardless of existence. Public/authentication routes do not resolve this parameter.

**`GET /namespaces` paginates normally**, after authorization filtering. Namespace creation accepts required `name`, `projectId` (provider target ID) and write-only `token`, plus optional `description`, `backend` (default `zerops`) and `config` (non-secret string settings). `GET /backends` requires `namespaces/read` on global `*` and lists registered kinds, capabilities, relay requirements and config keys; namespace responses include `backend` and enabled `capabilities`. Registered providers are `zerops`, `dokploy`, and `nomad`; unknown config keys return **400**. [Zerops](https://zerops.io/) accepts no config keys; its API URL is operator-only (`KARAKORAM_ZEROPS_API_URL`) and applies equally to legacy and new namespaces. [Dokploy](https://dokploy.com/)'s `url` and [Nomad](https://developer.hashicorp.com/nomad)'s `address` settings are restricted to operator-approved origins ([Dokploy target settings](dokploy.md#target-settings), [Nomad target settings](nomad.md#target-settings)).

Creation returns **201** after synchronous validation, then deploys a relay asynchronously only if the provider requires one (as Zerops does). **`DELETE /namespaces/{namespace}` returns 202 without a body or `Karakoram-Operation-Id`**: poll namespace lifecycle/health and events until removal, not `/operations`. Registration, token validation/rotation and guarded deletion belong in [namespaces](namespaces.md). Recovery source namespace fields and source-export permissions belong in [backups](backups.md).

## Requests and asynchronous work

Create accepts `{name, spec}`; PUT replaces `{spec}`; PATCH accepts only `{spec: {...}}` as a JSON merge patch. Arrays replace entirely. Name/status/generation/timestamps are server-owned. Unknown fields, wrong types, invalid enums/bounds and trailing JSON produce **400** with `details.field` and `details.reason`. Explicit zero is not a request for defaults: resources and required member counts must satisfy their bounds, while optional component `replicas: 0` explicitly disables that component where the engine permits it. `spec.provider` is required; omitted version/topology/components/resources use that catalog's defaults. [Go](https://go.dev/) input fields use pointers: nil requests defaults, a pointer to zero does not.

Cluster create, PUT and PATCH apply engine defaults and validation, then check the namespace provider's static capabilities before saving. A spec requiring an unavailable capability returns **400 `invalid_request`** naming the provider and missing capabilities (for example `publicTcp`); admission does not make a platform API request.

`spec.dataMcp` opts into per-cluster [data MCP](data-mcp.md#enable-and-scope-access): `enabled`, `access`, engine-specific `scope`, `tools`, `auditQueryText` and companion `resources`; observed fields live in `status.dataMcp`. The MCP transport is **outside `/api/v1`**, at `/mcp/clusters/{namespace}/{cluster}`, with restricted bearer-token authentication and its own [request, result and rate limits](data-mcp.md#confirmation-results-and-limits), including a 1 MiB request-body cap. REST session cookies and unrestricted API tokens do not authorize data MCP.

Cluster PUT/PATCH and queued actions commit against the cluster incarnation and desired generation inspected during admission. Concurrent changes return **409 conflict** rather than overwriting a newer spec or attaching work to a recreated cluster with the same name. Reload current state and retry the intended change. Suspended clusters still accept resume, full deletion, and backup deletion; ordinary runtime actions such as restart, backup, and restore require resume first.

`POST /clusters/{cluster}/retry-deployments?namespace=NS` requires `clusters/update` and returns `202` with the cluster after advancing its server-managed retry epoch and generation. It re-arms deployments in `DeploymentRetriesExhausted`; when none remain exhausted in the current epoch it returns `409 conflict` without a mutation. An `Idempotency-Key` replay returns the original result, not another retry budget. See [deployment recovery](clusters.md#retry-exhausted-platform-deployments).

Send the declared Content-Type: usually `application/json`, but cluster PATCH uses `application/merge-patch+json` (`application/json` is also accepted there for older clients). The request-body limit is **2 MiB**, including chunked requests. Responses echo `X-Request-ID`; optionally supply 1–128 characters from `[A-Za-z0-9._:-]`. Invalid/repeated IDs are replaced. Include the ID, never credentials, in error reports.

Accepted cluster mutations have durable Operations. Cluster/backup/restore responses expose `operationId`; operation-bearing responses also set **`Karakoram-Operation-Id`**. Imperative work commonly returns **202**; cluster creation returns **201**, not completed provisioning. Cluster DELETE returns **202 with an Operation body**. Poll `/operations/{id}` and resource status/inventory. HTTP acceptance, terminal Operation phase and cluster health are distinct; see [operations and status](clusters.md#operations-and-status). A wait timeout/disconnect does not cancel work.

Cancellation uses **`POST /operations/{operation}/cancel`**, requiring `clusters/update` on the operation's persisted namespace/cluster; no namespace override is needed. Acceptance returns **202 Operation** plus the operation header. Pending work becomes Cancelled immediately. Supported Running checkpoints set `cancelRequested: true` and can remain Running during cleanup. Terminal/unsafe states return **409 conflict**, `details.reason: invalid_state`. Poll the same ID; cancellation is neither rollback nor reversal of accepted desired state. Engine guides define safe checkpoints.

## Server-side dry-run

Preview cluster creation with `POST /dry-run/clusters`, replacement or merge
patches with `PUT` / `PATCH /dry-run/clusters/{cluster}`, and restores with
`POST /dry-run/clusters/{cluster}/restores` (all relative to `/api/v1`).
Use the corresponding mutation's body, content type and `namespace` query.
The same authentication, cookie CSRF, RBAC (including recovery-source
permissions), defaults, engine driver validation and read-only admission checks apply.

A successful preview returns **200 DryRunResult**: `operation` is `create`,
`replace`, `patch` or `restore`; `cluster` contains the normalized candidate
(the current target for restore), and `restore` is the prospective record when
applicable. `changes` contains dot paths under `spec` with before/after values;
create and restore return an empty changes list. Sensitive diff values are
masked. `effects` describes generic version, member, resource, exposure, TLS,
backup and recovery consequences; it does not predict engine-specific restarts.
`warnings` contains applicable admission caveats.
Placement warnings describe observed member/container placement, not a predicted
node allocation. A create preview has no provisioned members and does not
forecast co-location from the current Ready-node count; empty `warnings` and
an empty `status.placement.warning` do not certify future host separation.
After provisioning, inspect `/clusters/{cluster}/placement` and the cluster's
placement status. See [backend placement limits](limitations.md).

Dry-run does not persist clusters, secrets, operations, restores or audit events,
publish events, trigger reconciliation, or reserve/replay `Idempotency-Key`.
Normal authentication may still update token last-used metadata. No operation
ID is allocated, and an existing cluster's generation is unchanged. Admission
is a point-in-time observation, not a reservation or guarantee that a later
mutation or restore will succeed. `karactl` exposes these endpoints with
[`--dry-run`](cli.md).

## Fleet operations and resilience contract

These API groups implement fleet operations and installation resilience:

| Group | Routes | Success contract and permission |
| --- | --- | --- |
| Agent rollout | `POST /clusters/{cluster}/upgrade-agents` | Optional `{includeCurrent}`; **202 Operation**, `clusters:update` on the cluster; **409** when agents are already current. |
| Agent rollout settings | `GET/PUT /settings/agent-rollout` | `AgentRolloutSettings` with `automatic`, `maxConcurrentClusters` (1–5) and read-only `currentBuild`; installation `settings:read/update`. |
| Member move | `POST /clusters/{cluster}/members/{member}/move` | **202 Operation**, `clusters:update`; placement guarantees depend on provider capabilities. |
| Control-plane backups | `GET/PUT /control-plane/backup-policy`, `GET/POST /control-plane/backups` | Installation `settings:read/update`; POST returns **202 ControlPlaneBackup**, not a cluster Operation. |
| Notifications | `GET/POST /notification-channels`, `GET/PUT/DELETE /notification-channels/{channel}`, `POST /notification-channels/{channel}/test` | Installation `settings:read/update`; test returns **200 NotificationTestResult**. |
| Backup drills | `GET/POST /clusters/{cluster}/backup-drills`, `GET /clusters/{cluster}/backup-drills/{drill}` | `restores:read/create` on the source cluster; POST accepts optional `{backup}` and returns **202 BackupDrill**. |

Lists use `items` and optional `nextCursor`. New mutations declare CSRF and optional `Idempotency-Key` headers. Notification destination URLs, signing secrets and SMTP passwords are **write-only**; responses expose only configured flags and secret-free status.

Cluster desired state adds `spec.maintenance{timeZone, windows[{schedule, duration}]}` and `spec.backup.drill{enabled, schedule, backupSchedule, timeout}`. Agent build identity is exposed alongside existing member agent connectivity as `agent.version`, `agent.commit` and `agent.build`; `status.agents` summarizes outdated/unknown members. `status.maintenance` reports the computed open window. `managedBy` is server-owned. Placement exposes provider-neutral `source` and `guaranteedAntiAffinity`.

The boolean query `ignoreMaintenanceWindow` is declared on cluster PUT/PATCH, restart, TLS rotation, member replacement/move, agent upgrade and FCV raise. It is an explicit per-request override, not a persisted desired-state policy.

## Safe retries with Idempotency-Key

Supply the header only on operations declaring `IdempotencyKey` in [OpenAPI](https://www.openapis.org/): supported resource creation/update and lifecycle/recovery actions, not every mutation. Use a unique random key per intended mutation, **1–255 visible ASCII characters without spaces**, and reuse the same key/body for network retries.

- Scope includes account creation identity/[OIDC](https://openid.net/developers/how-connect-works/) issuer or the specific bearer token, method, route, namespace and path parameters. Omitted namespace and explicit `default` share a scope. Authentication, CSRF and authorization are checked again on replay.
- Successful **2xx** status/body/relevant headers are replayable for **24 hours**, including one-time secrets and operation IDs. Replay sets `Idempotent-Replayed: true`. It is the original response snapshot; query current state separately.
- JSON whitespace/key order do not affect matching; array order, values, omitted fields, explicit null and numeric spellings do. A changed body returns **409 conflict**, `details.field: Idempotency-Key`, `details.reason: conflict`. Concurrent in-flight duplicates return **409** with reason `invalid_state`; retry the same key/body after completion.
- **4xx/5xx are not cached**. Operation-backed mutations atomically save a recovery response with their accepted operation. If the server stops or the final response cannot be saved after acceptance, the same key replays the accepted resource/Operation snapshot and its original `Karakoram-Operation-Id`, rather than creating another action. The recovery response keeps the endpoint's normal success status and response shape; once the ordinary successful response is saved, replay uses its exact status/body/headers. Non-operation-backed mutations can still have an ambiguous remote or response boundary: inspect their resources before retrying. Expiry permits new execution; a fresh key is not a recovery shortcut or an exactly-once guarantee.

`karactl` does not send this header and has no flag for it. Raw HTTP clients set `Idempotency-Key`; [Go](https://go.dev/) callers can pass a per-call `client.RequestEditorFn` that sets the header, or configure request editing with `client.WithRequestEditorFn`. Retain each mutation's key across retries; never reuse one fixed key for unrelated writes or log replayed secrets.

A cluster PATCH or PUT whose normalized desired spec and supplied secret values
are unchanged returns HTTP 200 without creating an operation or cancelling
in-flight work. This no-op behavior does not require an `Idempotency-Key`.
Changed desired state still creates a superseding operation; keyed retries
replay their original response as described above.
An explicit `ignoreMaintenanceWindow=true` still releases the accepted generation
when the spec is unchanged: it persists that generation's override and triggers
reconciliation without replacing its in-flight operation. Repeating an already
applied override does not trigger more work.

## Pagination and events

Lists return `{items: [...], nextCursor?: "..."}`. `limit` defaults to **100**, range **1–500**. Follow the opaque cursor with the same filters until absent; authorization precedes pagination. Multiple pages are not a transactional snapshot. Specialized status/log responses use their own schemas. Member logs are bounded tails: `process`/`lines` work, but `cursor`/`since` currently do not, and no next cursor is returned.

Member responses and cluster status members include `agent.connected`, optional
`agent.configVersion` and `agent.lastSeenAt`, and an optional `agent.message`
diagnostic. The message is sanitized using the same credential and base64
redaction rules as other diagnostic text and is omitted when empty.

`/events` and `/events/stream` share namespace/cluster/type/operationId/actor/resource filters. Actor/resource matches are exact, case-sensitive AND intersections. Types are **Normal, Warning, Audit**; lifecycle names such as `OperationFailed` are reasons. Unknown types/resources return 400; explicitly unreadable resource categories return 403, while unfiltered results omit unauthorized records. Cluster events require `events:read` or `audit:read`; namespace/global audit requires the corresponding namespace/global audit permission. Lists default oldest-first; `order=desc` pages newest-to-oldest.

Events may include optional `details`. For `reason: "ClusterSpecChangeRequested"`, it contains `changes?: EventChange[]` and `truncated?: boolean`. Each change has required `path: string`, optional JSON-valued `before`/`after`, and optional boolean `sensitive`/`truncated`:

```json
{
  "details": {
    "changes": [
      {"path": "components.engine.resources.maxCpu", "before": 1, "after": 2}
    ]
  }
}
```

Paths are dotted JSON field paths. Schedules and other named arrays use entry names or IDs rather than positions, for example `backup.schedules[nightly].retainCount`. An absent `before` or `after` normally means the field was absent; explicit JSON `null` remains `null`. Sensitive changes use the [redaction markers and rules](security.md#audit-attribution-and-durability).

| Bound / flag | Meaning |
| --- | --- |
| Value exceeds 1 KiB of encoded JSON | After redaction, replaced by `(N bytes)` and the change marked `truncated: true` |
| Details exceed 32 KiB | Values may be omitted with per-change and top-level `truncated: true`; if paths alone exceed the cap, only an ordered prefix remains; individual oversized paths may be shortened |
| Change `truncated: true` | A missing `before`/`after` means omitted content, **not unset** |
| `details.truncated: true` | The comparison may lack values or paths |
| Human-readable summary | At most 512 bytes; individual displayed values at most 64 bytes; [summary format and UI workflow](operations.md#reviewing-audit-activity) |

Both list and stream omit the entire `details` object without [structured audit access](access-control.md#policy-format). Clients should use `details.changes`, not parse the message, and check truncation flags before treating a comparison as complete. Older events may lack `details`. To distinguish a requested change from convergence, check [cluster status and Operations](clusters.md#operations-and-status).

SSE uses `text/event-stream`, numeric IDs and stored types as `event:` names. Reconnect with **`Last-Event-ID`**; an explicit `cursor` query takes precedence. Preserve filters and the last dispatched ID. Parse complete blank-line-delimited frames, join `data:` lines, and ignore `:` comments; comments never advance the cursor. Authentication/permissions are revalidated during delivery and heartbeats.

The stream sends an immediately flushed connected comment and **15-second heartbeats**, each approximately **16 KiB** padded: budget roughly **1.1 KiB/s per connection** before compression, plus startup. Proxies must preserve `Cache-Control: no-cache, no-transform` and `X-Accel-Buffering: no`; configure nonbuffering delivery (`proxy_buffering off` where applicable). Small events can still wait until a heartbeat. Use durable lists/status when live delivery is stale, not repeated mutations.

## Errors

Errors are `{code, message, details?}`. Branch on code/field/reason, not English messages. An HTTP-successful poll can describe a failed Operation.

Cluster create, PUT and PATCH return `400 invalid_request` when fresh provider information positively establishes that public `spec.exposure.ipType` is unavailable. [Zerops](https://zerops.io/) checks current project IPv4/IPv6 addresses, not cached namespace identity. Select a provisioned family, provision the requested public address, or disable public exposure. A failed lookup or provider without authoritative family information does not reject the request: reconciliation reports `ExposureSupported=Unknown/PublicIPSupportUnknown` while attempting routing normally. [Dokploy](https://dokploy.com/) and [Nomad](https://developer.hashicorp.com/nomad) accept both family selections for native published ports; configured public addresses advertise endpoints, not an authoritative inventory of host ingress families.

Updates that preserve public/private mode, IP family and allowlist keep unchanged legacy exposure; an unrelated update is not rejected solely because that existing exposure is unavailable. New exposure choices or routing changes still undergo the availability check.

For already accepted requests, a definitive unsupported-family result sets `ExposureSupported=False/PublicIPTypeUnsupported`, `Ready=False`, and phase `Degraded`; the current generation's convergence operation fails rather than remaining Running. Desired state is retained and reconciled, so correcting exposure (or provisioning the address) can recover it without recreating the database. Existing `400` response schemas are unchanged.

| HTTP | Meaning |
| --- | --- |
| 400 | Invalid body/query/cursor/configuration/transition |
| 401 | Missing/expired/invalid authentication; invalid OIDC callback |
| 403 | RBAC/ownership/CSRF denial or `password_change_required` |
| 404 | Missing/not-visible resource or route; namespace rules above apply |
| 405 | Unsupported method on known route; inspect `Allow` |
| 409 | Conflicting state, unsafe cancellation, unsupported engine action or retry-key conflict |
| 413 / 415 | Body over 2 MiB / missing or unsupported Content-Type |
| 422 | Namespace token/project/relay-hostname validation failure |
| 429 | Login backoff; honor `Retry-After` and `details.retryAfterSeconds` |
| 500 | Generic unexpected failure, or sanitized dependency failure such as temporary namespace platform unavailability |
| 503 | Route-specific store/monitoring unavailability or disabled/unavailable OIDC issuer |

Actions and native credential/security mutations blocked by an unfinished initial
restore return `409 conflict` with `details.resource: operation`,
`details.name: <owning operation id>` and `details.reason: invalid_state`.
This restore-owner check precedes capability, member-state and native-engine
prechecks, so those checks cannot hide the actionable owner. Read operations,
target deletion, own backup policy and the narrow pre-claim component amendment
rules retain their separate admission behavior.

Outside that fence, an unsupported engine action returns `409 conflict` with
`details.resource: operation`, `details.name: <requested action>` and
`details.reason: unsupported`; the message identifies the engine. [PostgreSQL](https://www.postgresql.org/) and
[Supabase](https://supabase.com/) do not support the generic system-user rotation action. Password
rotation for **non-reserved PostgreSQL login roles** uses
`POST /clusters/{cluster}/postgres/roles/{role}/password`; managed roles such as
`postgres` remain engine-owned and are not made rotatable by that hint.

## Clients/SDK

Install the public [Go](https://go.dev/) client at a release/commit matching the server:

```sh
go get github.com/KarakoramDev/Karakoram/pkg/client@<version>
```

Use base URL **`https://example.com/api/v1`**, including the suffix, and the release's required Go version. One-page example:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/KarakoramDev/Karakoram/pkg/client"
)

func main() {
    c, err := client.NewClientWithResponses("https://example.com/api/v1",
        client.WithBearerToken(os.Getenv("KARAKORAM_TOKEN")),
        client.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}))
    if err != nil { log.Fatal(err) }
    r, err := c.ListClustersWithResponse(context.Background(), nil)
    if err != nil { log.Fatal(err) }
    if r.StatusCode() != http.StatusOK || r.JSON200 == nil {
        log.Fatalf("list clusters: HTTP %d", r.StatusCode())
    }
    for _, cluster := range r.JSON200.Items { fmt.Println(cluster.Name) }
    // Follow r.JSON200.NextCursor with the same filters for subsequent pages.
}
```

Transport/decoding errors and HTTP errors are separate: check status and the expected `JSON200`/`JSON201`/`JSON202` pointer; decode error bodies as `client.Error` when needed. Optional fields are pointers; check before dereferencing. Keep one-time secrets out of logs. Poll returned operation IDs, not resource names.

`StreamEvents` returns raw `*http.Response`, not decoded events. Close its body and use a streaming SSE parser, not `io.ReadAll` or a single JSON decoder. Inspect raw frames with:

```sh
curl --no-buffer --fail-with-body \
  -H "Authorization: Bearer $KARAKORAM_TOKEN" \
  -H 'Accept: text/event-stream' \
  'https://example.com/api/v1/events/stream?namespace=default'
```

On reconnect send `Last-Event-ID`; follow the parsing/buffering rules above. Generate other-language clients from the deployment's spec:

```sh
curl --fail-with-body https://example.com/api/v1/openapi.yaml -o openapi.yaml
openapi-generator-cli generate -i openapi.yaml -g python -o karakoram-python-client
python -m pip install ./karakoram-python-client
npx openapi-typescript openapi.yaml -o karakoram-api.d.ts
npm install openapi-fetch
```

Configure `/api/v1` base URL and bearer authentication. [TypeScript](https://www.typescriptlang.org/) generation produces types; pair with `openapi-fetch`, or generate a runtime client with `-g typescript-fetch`. Generated clients still need error handling, polling and SSE parsing. Repository regeneration belongs in [CONTRIBUTING](../CONTRIBUTING.md).

## OpenBao operations

POST `/clusters/{cluster}/openbao/unseal-keys/reveal` accepts `{"confirm":"<cluster name>"}`, requires `openbao-unseal-keys:reveal`, returns no-store initialization material and audits disclosure without response secrets. Database-connection GET/list uses `clusters:read`; POST uses `clusters:update` plus target `database-users:create`; DELETE uses `clusters:update` plus target `database-users:delete`. Connections target Ready, TLS-enabled [PostgreSQL](https://www.postgresql.org/), PSMDB replica sets or [RabbitMQ](https://www.rabbitmq.com/) in the same namespace.

POST `/clusters/{cluster}/openbao/database-connections` example:

```json
{"name":"orders","targetCluster":"ordersdb","roles":[{"name":"reader","access":"readonly","defaultTtlSeconds":3600,"maxTtlSeconds":86400}]}
```

The returned secret-free record can be `pending`; use its final `credentialsPath` with a native [OpenBao](https://openbao.org/) token once `ready`. OpenBao issues credentials, not Karakoram. Shared restart/replace-member/rotate-tls routes and catalog-bound version patches drive maintenance. Shared backup/restore routes use `raft-snapshot`; cluster-create and restore requests accept top-level write-only `openbaoUnsealShares` for deleted-source recovery. See [operations](openbao/operations.md) and [recovery request examples](openbao/backups.md).
