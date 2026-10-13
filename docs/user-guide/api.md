<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# REST API

The public contract is [api/openapi.yaml](../../api/openapi.yaml), **OpenAPI 3.0.3**. Most routes are rooted at `/api/v1`. `GET /healthz` and authenticated `GET /metrics` are installation-root exceptions. The root WebSocket `/relay/v1/connect` is **not in OpenAPI** and authenticates inside the tunnel with relay mTLS, not API tokens/cookies. The private `/internal/agent/v1` and node `/v1` protocols are not public management APIs.

The UI and `karactl` use this contract. Karakoram does not currently ship an interactive Swagger UI or promise the YAML is served at a guessed HTTP URL; use the checked-in specification and generated client.

## Authentication

For scripts use a Karakoram API token:

```http
Authorization: Bearer kk_...
```

The abbreviated value is only illustrative. Load a real token from a secret manager; never a Zerops integration token. Tokens act as the owner's current RBAC permissions and optional expiry, and do **not** require browser CSRF. See [token creation/revocation](rbac-and-auth.md#api-tokens-and-browser-sessions).

Local browser login uses `POST /api/v1/auth/login` with `{username, password}`. The response sets cookies and returns `{account, csrfToken, expiresAt}`, not a browser-readable JWT. Runtime cookie names are **`karakoram_access`**, `karakoram_refresh` and readable `karakoram_csrf`; the access cookie contains an EdDSA JWT. Access/refresh are HttpOnly and SameSite=Strict, and Secure for an HTTPS public origin. Default access life is five minutes; rotating refresh families last thirty days.

For cookie-authenticated unsafe methods (anything except GET/HEAD/OPTIONS), send **`X-Karakoram-CSRF`** matching the readable CSRF cookie and persisted session token. This includes refresh/logout/password changes. Pre-authentication login does not require that header. `GET /api/v1/auth/me` can discover the current session's `csrfToken`, including after OIDC callback; bearer responses omit it. Do not put session/API credentials into browser local storage.

`POST /auth/refresh` rotates the refresh secret; reusing an older refresh token revokes that family. Forced password-change sessions permit current-user discovery, password change and logout, not normal operations or token creation. After password change reauthenticate. OIDC uses `/auth/oidc/login` and `/auth/oidc/callback` under `/api/v1`, with browser redirect/state/PKCE rather than an invented device-code API.

## Authorization and credential reveal

Every authenticated operation is classified by `x-karakoram-rbac` in OpenAPI. Resources/actions/objects, deny-overrides semantics and default roles are described in [RBAC](rbac-and-auth.md). List/stream/metrics filtering checks each namespace-qualified object, not just a collection-wide allow. Reading clusters does not grant `cluster-credentials:read`.

`GET /api/v1/clusters/{cluster}/connection` is an explicit sensitive response with `Cache-Control: no-store`; it currently reveals databaseAdmin connection credentials and the public CA. Ordinary cluster/status reads contain password-free URIs. Storage keys and PEM private inputs are write-only. Application passwords generated at creation and API token secrets are one-time responses, not retrievable later.

## Resource families

| Family | Principal routes under `/api/v1` |
| --- | --- |
| Auth/identity | `/auth/*`, `/accounts`, `/api-tokens`, `/permissions` |
| Policy/settings | `/rbac/policy`, `/rbac/can`, `/settings` |
| Provider catalog | `/providers`, `/providers/{provider}` |
| Namespaces | `/namespaces`, `/namespaces/{namespace}`, `/namespaces/{namespace}/token`, `/namespaces/{namespace}/validate` |
| Cluster desired/observed state | `/clusters`, `/clusters/{cluster}`, `/status`, `/members`, `/placement`, `/balancer`, `/connection` (suffixes on a cluster) |
| Cluster operations | `/restart`, `/resume`, `/raise-fcv`, `/rotate-tls`, `/members/{member}/replace`, `/system-users/{user}/rotate` (cluster suffixes) |
| Database identities | Cluster `/database-users` and `/database-roles` |
| Recovery | `/backup-storages`; cluster `/backups`, `/restores`, `/pitr` |
| Activity | `/operations`, `/operations/{operation}`, cluster `/operations`, `/events`, `/events/stream` |
| Member observability | Cluster `/metrics`, `/members/{member}/logs`; root `/metrics` |

Use the exact path parameters, request schema and operation-specific permission in OpenAPI rather than treating the abbreviations above as standalone paths.

## Namespace parameters and lifecycle

Cluster-scoped and backup-storage routes accept optional query
`namespace=NAME`. Single-object routes and creation default to **`default`**.
Collection `/clusters`, `/operations`, `/events`, `/events/stream`,
`/orphaned-volumes`, `/backup-storages` and root `/metrics` include all
authorized namespaces when omitted; a value filters to that namespace.
Responses identify their namespace. A bare cluster name is not an
installation-wide identity; keep namespace in caches and links.

`spec.dataSource.namespace` selects a cluster-create recovery source;
in-place restore uses `sourceNamespace`. Both default to the target namespace.
Cross-namespace recovery additionally checks source `backups/export`.

| Method/path under `/api/v1` | Behavior |
| --- | --- |
| `GET /namespaces` | Read-filtered namespace collection |
| `POST /namespaces` | `{name, projectId, token, description?}`; validates synchronously, returns 201 then deploys relay asynchronously |
| `GET /namespaces/{namespace}` | Secret-safe project/token/relay health |
| `PATCH /namespaces/{namespace}` | `{description}` only; name/project immutable |
| `PUT /namespaces/{namespace}/token` | `{token}`; validates same project before swapping; default returns 409 |
| `POST /namespaces/{namespace}/validate` | Returns updated health, including invalid-token state |
| `DELETE /namespaces/{namespace}` | 202 asynchronous owned-relay teardown; 409 for default or retained resources |

Tokens are write-only. Invalid token/project, duplicate project and occupied
relay hostname return **422** `invalid_namespace`; duplicate name returns
**409**. Deletion is guarded by clusters, storages, retained backup records and
orphaned-volume records, and does not remove policy rules.
See [namespace lifecycle](namespaces.md).

Namespace registration/validation checks project identity and service-list
access, not every Full access mutation capability. A readable but insufficiently
privileged token can fail later during relay import/deployment; inspect health.
Currently the namespace collection returns all readable items without applying
its advertised `limit`/`cursor` parameters.

## Declarative updates and asynchronous work

Cluster create accepts `{name, spec}`; PUT replaces full desired `{spec}`. PATCH accepts a JSON merge patch containing **only** `{spec: {...}}`. Name, status, timestamps and generation are server-owned. Merge-patch arrays replace the old arrays in full. Mutations persist intent and trigger reconciliation; a returned cluster or HTTP 201 is not a completed deployment.

Imperative operations return durable asynchronous work, commonly HTTP 202. Poll `/operations/{id}` until `Succeeded` or `Failed`, also inspect Backup/Restore and cluster status. Operation phases are `Pending`, `Running`, `Succeeded`, `Failed`. Requested engine version and observed deployed version/FCV differ during rollout. Read observedGeneration before concluding a desired change has converged.

Network disconnect or client wait timeout does not cancel server work. Do not submit a second restore simply because a reply was lost: node receipts may intentionally report an ambiguous initiation requiring observation/operator recovery. No cancel-every-operation endpoint or universal exactly-once mutation guarantee is supplied.

## Pagination and events

Paginated lists return `{items: [...], nextCursor?: "..."}`. `limit` defaults to **100** and lists accept **1–500**. Pass the returned opaque `cursor` to the same list with the same filters. Authorization filtering happens before pagination. Stop when `nextCursor` is absent. Lists are not a snapshot transaction across multiple requests; concurrent changes can affect pagination. Providers and specialized status/window/log responses have their own schemas.

`GET /api/v1/events/stream` uses `text/event-stream`, numeric event IDs and named event types such as `cluster.updated`, `backup.updated`, `restore.updated`, `operation.updated` and `member.updated`. Clients must tolerate new dot-qualified types. Reconnect with `Last-Event-ID`, or an explicit `cursor` query (which takes precedence), to replay authorized durable events. Filters include cluster/type/operationId. SSE live delivery can reconnect; it does not replace persisted operation/status queries.

SSE also sends **comment frames**: an immediately flushed `: connected` comment with approximately **16 KiB of random-base64 padding**, followed by similarly padded `: heartbeat` comments every **fifteen seconds**. Three precomputed heartbeat variants are cycled to resist gzip dictionary compression without generating fresh padding for each heartbeat. A correct SSE parser ignores comments; they carry no event ID/data and must **not** advance the replay cursor or become application events. The stream sets `Cache-Control: no-cache, no-transform` and `X-Accel-Buffering: no`, rather than the ordinary sensitive-JSON `no-store` policy. Preserve these headers through additional proxies. Authentication/permissions are revalidated while draining durable events and on heartbeats; a closed/expired session stream should reauthenticate and reconnect using its last received event ID.

**Transport behavior and cost:** small event frames can remain proxy-buffered until a padded fifteen-second heartbeat. Budget approximately **16 KiB at connection start plus 16 KiB per heartbeat (~1.1 KiB/s per open stream, before wire compression)**; this is not a zero-latency or zero-overhead push guarantee. Configure a nonbuffering proxy path (including `proxy_buffering off` where applicable). Durable event listing and operation/status reads remain available; do not discard replay cursors or repeatedly replay all history to evade buffering.

Member logs are bounded process-log tails and are not an unlimited archive. Use `process`/`lines` and the exact response schema; see [operations](../operations.md#health-and-logs).

## Errors

Structured errors are `{code, message, details?}`; do not rely on English message text for branching. Typical responses:

| HTTP | Meaning |
| --- | --- |
| 400 | Invalid body/query/cursor, provider configuration or unsafe/unsupported desired transition |
| 401 | Missing/invalid/expired authentication; login errors intentionally hide account existence |
| 403 | RBAC/ownership denial, failed CSRF, or `password_change_required` |
| 404 | Missing/not-visible resource or API route |
| 409 | Conflicting resource state/operation or unsupported action reported as conflict |
| 413 | Request exceeds the current 2 MiB body limit |
| 422 | Namespace token/project/name or relay-hostname validation failed |
| 500 | Unexpected server error, deliberately generic public response |
| 503 | Unavailable store/monitoring subsystem for the relevant endpoint |

A failed asynchronous Operation is not necessarily a failed HTTP request. Read its persisted error/message and cluster conditions. Never embed secrets in request names or expect diagnostic responses to reveal infrastructure credentials. Public auth errors may be intentionally less specific than administrative observations.

## Regeneration

From the repository root:

```sh
make generate
make openapi-check
# TypeScript generation is separate:
(cd ui && npm ci && npm run gen:api)
```

Go generation uses pinned oapi-codegen 2.4.1 for `internal/api/gen.go` and `internal/apiclient/client.gen.go`, then the owned RBAC generator for `internal/api/rbacmap.gen.go`. TypeScript generation uses the lockfile's openapi-typescript and writes `ui/src/api/schema.d.ts`. Do not hand-edit generated artifacts.

`make openapi-check` regenerates/diffs the Go API/client/RBAC paths; it is not a TypeScript-drift or live behavior test. Update schema, all callers/tests and docs together. See [CONTRIBUTING](../../CONTRIBUTING.md) for full build/test guidance and [CLI reference](cli.md) for API-shaped file examples.
