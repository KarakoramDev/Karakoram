<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# REST API

The public contract is [api/openapi.yaml](../../api/openapi.yaml), **OpenAPI 3.0.3**. Most routes are rooted at `/api/v1`. `GET /healthz` and authenticated `GET /metrics` are installation-root exceptions. The root WebSocket `/relay/v1/connect` is **not in OpenAPI** and authenticates inside the tunnel with relay mTLS, not API tokens/cookies. The private `/internal/agent/v1` and node `/v1` protocols are not public management APIs.

The UI and `karactl` use this contract. Download the exact specification embedded in your running server at **`GET /api/v1/openapi.yaml`** (public, no authentication, `Content-Type: application/yaml`, cacheable for five minutes). Karakoram does not ship an interactive Swagger UI. Use the running server's document when generating a client for that deployment.

## Authentication

For scripts use a Karakoram API token:

```http
Authorization: Bearer kk_...
```

The abbreviated value is only illustrative. Load a real token from a secret manager; never a Zerops integration token. Tokens act as the owner's current RBAC permissions and optional expiry, and do **not** require browser CSRF. See [token creation/revocation](rbac-and-auth.md#api-tokens-and-browser-sessions).

Local browser login uses `POST /api/v1/auth/login` with `{username, password}`. The response sets cookies and returns `{account, csrfToken, expiresAt}`, not a browser-readable JWT. Runtime cookie names are **`karakoram_access`**, `karakoram_refresh` and readable `karakoram_csrf`; the access cookie contains an EdDSA JWT. Access/refresh are HttpOnly and SameSite=Strict, and Secure for an HTTPS public origin. Default access life is five minutes; rotating refresh families last thirty days.

Login applies account/source exponential backoff after five failed attempts. While throttled, correct and incorrect passwords alike return 429 `rate_limited` with a `Retry-After` seconds header and `details.retryAfterSeconds`. Wait that long before retrying; ordinary 401 responses intentionally do not reveal account existence.

For cookie-authenticated unsafe methods (anything except GET/HEAD/OPTIONS), send **`X-Karakoram-CSRF`** matching the readable CSRF cookie and persisted session token. This includes refresh/logout/password changes. Pre-authentication login does not require that header. `GET /api/v1/auth/me` can discover the current session's `csrfToken`, including after OIDC callback; bearer responses omit it. Do not put session/API credentials into browser local storage.

`POST /auth/refresh` rotates the refresh secret; reusing an older refresh token revokes that family. Forced password-change sessions permit current-user discovery, password change and logout, not normal operations or token creation. After password change reauthenticate. OIDC uses `/auth/oidc/login` and `/auth/oidc/callback` under `/api/v1`, with browser redirect/state/PKCE rather than an invented device-code API.

Local account creation/reset and `newPassword` require at least **12 Unicode characters**, at most **1024 UTF-8 bytes**, and changes/resets must differ from the current password. A rejected change returns 400 `invalid_request` with `details.field` and `details.reason`: `currentPassword`/`invalid`, `newPassword`/`reused` or `newPassword`/`out_of_range`. Rejection preserves the mandatory-change restriction. Account names start with a letter, contain only letters/digits/`_ . @ -`, and are 1–64 characters; duplicates return 409 `already_exists` naming the account. Account `updatedAt` advances on metadata updates as well as password changes.

API-token names are trimmed, non-blank and 1–128 characters; duplicate names are allowed and IDs distinguish them. Creation and update both require future RFC 3339 `expiresAt`, with 400 field-specific `invalid` or `out_of_range` errors. Null on update removes expiry. The UI prevents concurrent duplicate submissions and reuses an `Idempotency-Key` when retrying an unchanged token-creation request.

The UI restores an access-expired session through refresh, preserves identity during 5xx/network failures and offers automatic/manual retry. Ordinary UI API requests are bounded to 30 seconds (SSE is excluded). Persistent 401 after a successful refresh ends the session instead of causing repeated refreshes.

### Copyable curl bootstrap

Use this Bash recipe with `curl` and `jq` installed. Start with an enabled local
account whose required first-login password change is already complete (see
[first sign-in](../install.md#first-sign-in-and-cli-connection)). Replace the
HTTPS installation origin below, without a trailing slash. The login response
supplies `csrfToken`; curl's cookie jar preserves the matching
`karakoram_access`, `karakoram_refresh` and `karakoram_csrf` cookies.

```bash
set -euo pipefail
set +x
umask 077
SERVER='https://YOUR-SUBDOMAIN'
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
  printf 'Complete the required password change and sign in again before creating a token.\n' >&2
  exit 1
fi
CSRF=$(printf '%s' "$LOGIN" | jq -er .csrfToken)
unset LOGIN

# Cookie-authenticated token creation needs BOTH the cookie jar and CSRF header.
CREATED=$(curl --fail-with-body --silent --show-error \
  -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
  -H 'Content-Type: application/json' -H "X-Karakoram-CSRF: $CSRF" \
  --data '{"name":"automation"}' "$SERVER/api/v1/api-tokens")
KARAKORAM_TOKEN=$(printf '%s' "$CREATED" | jq -er .secret)
TOKEN_ID=$(printf '%s' "$CREATED" | jq -er .token.id)
unset CREATED

# Bearer requests use the one-time secret, not the token ID or a Zerops token.
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $KARAKORAM_TOKEN" "$SERVER/api/v1/auth/me"
printf '\nCreated token ID: %s\n' "$TOKEN_ID"

# Close the temporary browser-style session; this does not revoke the API token.
curl --fail-with-body --silent --show-error \
  -b "$COOKIE_JAR" -H "X-Karakoram-CSRF: $CSRF" \
  -X POST "$SERVER/api/v1/auth/logout"
unset CSRF
```

Save `KARAKORAM_TOKEN` securely before closing the shell; `.secret` is returned
only at creation, while `.token.id` is the metadata/revocation identifier.
Bearer calls need neither cookies nor CSRF. All curl calls above report server
error bodies and fail on HTTP errors. Use an account with the resource permissions
needed by your automation; a token does not bypass its owner's RBAC. Revoke it
when no longer needed with `DELETE /api/v1/api-tokens/{id}` and bearer
authentication.

Root **`GET /metrics`** accepts both bearer API tokens and signed-in browser cookie sessions, with identical per-cluster `monitoring:read` filtering. Use a dedicated, least-privilege bearer token for unattended scrapers; browser sessions expire and rotate and are not suitable scraper credentials.

OIDC login/callback failures return the same JSON `{code, message}` envelope as other API errors. A disabled or temporarily unavailable issuer returns 503; invalid callback state or credentials returns 401.

The current OIDC flow redirects successful authentication to the installation
root; the accepted `returnTo` hint does not currently change that redirect.

## Authorization and credential reveal

Every authenticated operation is classified by `x-karakoram-rbac` in OpenAPI. Resources/actions/objects, deny-overrides semantics and default roles are described in [RBAC](rbac-and-auth.md). List/stream/metrics filtering checks each namespace-qualified object, not just a collection-wide allow. Reading clusters does not grant `cluster-credentials:read`.

`GET /api/v1/clusters/{cluster}/connection` is an explicit, audited sensitive response with `Cache-Control: no-store`; it currently reveals databaseAdmin connection credentials and the public CA. Ordinary cluster/status reads contain password-free URIs. Storage keys and PEM private inputs are write-only. Application passwords generated at creation and API token secrets are one-time responses, not retrievable later.

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
An explicit nonexistent namespace returns **404** `not_found` with
`details.resource: namespace` and `details.name` on lists, details and sub-resources;
omit the parameter rather than passing an empty value for all namespaces.

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
relay hostname return **422** `invalid_namespace`; missing fields and invalid name
syntax return **400** naming the field/rule. Duplicate name and reserved `default`
return **409**, reason `already_exists`. Deletion is guarded by clusters, storages, retained backup records and
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

Each Operation exposes its initiating `actor`. Desired-state writes persist attribution with the cluster generation so subsequent reconciliation and resulting operations/events retain that generation's actor. Local actors use persisted account names; OIDC actors add `(oidc: issuerURL)` from the verified account issuer. Bearer actors identify owner plus token name and ID, never its secret. Autonomous work uses system actors such as `reconciler`, `scheduler`, `system:startup` or `system` (orphan-volume janitor). See [audit attribution](../security.md#audit-attribution-and-durability).

Network disconnect or client wait timeout does not cancel server work. Do not submit a second restore simply because a reply was lost: node receipts may intentionally report an ambiguous initiation requiring observation/operator recovery. No cancel-every-operation endpoint or universal exactly-once mutation guarantee is supplied.

## Safe retries with Idempotency-Key

Send an optional `Idempotency-Key` header for API-token creation, cluster creation,
cluster PUT/PATCH, backup and restore creation. Account, namespace, backup-storage,
database-user and database-role creation also support it. Use a random, unique key
for each intended mutation (for example a UUID); reuse that same key and body for
network retries or double submissions. A key must contain **1–255 visible ASCII
characters without spaces**. Requests without the header retain their usual behavior.

Keys are scoped to the authenticated account (including its OIDC issuer), or the
specific bearer token identity, plus method, route, namespace and path parameters.
Omitting `namespace` and sending `namespace=default` have the same scope. Another
account/token or another route cannot retrieve your stored response. Authentication,
CSRF and route authorization are checked again before every replay.

After a successful **2xx** response, the server retains its exact status and body,
plus relevant headers such as `Content-Type`, `Cache-Control`, `Location`, `ETag`
and `Karakoram-Operation-Id` when supplied, for **24 hours**. A matching retry
returns that response with `Idempotent-Replayed: true`, without creating another
resource or queuing another operation. JSON whitespace and object-key order do
not affect matching; array order, values, omitted fields and explicit `null` do.
Numeric spellings are retained, so reuse the same numeric representations.
The response is the original snapshot, not the resource's latest state; poll the
resource or its returned operation to observe asynchronous completion.

* Reusing a key with a different body returns **409**, `code: conflict`,
  `details: {field: "Idempotency-Key", reason: "conflict"}`. Use a fresh key only
  when a genuinely different mutation is intended.
* A concurrent request with the same key returns **409** with reason
  `invalid_state` while the first request is in flight. Retry the same key/body
  after that request finishes.
* **4xx/5xx are not cached**. Once a failed request finishes, its key can be retried
  with corrected input. Do not interpret a network timeout as a server rejection.

One-time API-token secrets and generated database passwords are replayed too.
Treat the replay as sensitive: responses remain `Cache-Control: no-store` where
applicable, and the complete stored response is encrypted using the installation's
master key. Expired records are unusable immediately and are purged on server
startup and hourly (so ciphertext removal can lag expiry by up to one hour).
After expiry the key is a new request; existing resource names may then conflict.

This is bounded retry protection, not an atomic, universal exactly-once guarantee:
an interrupted server between committing a mutation and saving its response leaves
an in-flight reservation until expiry. A response-persistence failure likewise
keeps the reservation instead of executing a duplicate. Inspect the resource and
operation before deciding what to do; changing keys is not a recovery shortcut.

## Pagination and events

Paginated lists return `{items: [...], nextCursor?: "..."}`. `limit` defaults to **100** and lists accept **1–500**. Pass the returned opaque `cursor` to the same list with the same filters. Authorization filtering happens before pagination. Stop when `nextCursor` is absent. Lists are not a snapshot transaction across multiple requests; concurrent changes can affect pagination. Providers and specialized status/window/log responses have their own schemas.

Successful responses follow the published schemas: collections and present array fields are `[]`, never `null`; optional identifiers are omitted when absent. Cluster fields that do not apply, such as an unconfigured PITR span, are omitted rather than reported as zero. Handler tests validate HTTP responses against this document under ordinary `go test ./...`.

Backup schedules may omit `storage` to inherit the cluster's primary
`spec.backup.storage`; successful responses omit that optional override when it
was not configured rather than returning `storage: ""`.

`GET /api/v1/events` accepts optional string filters **`actor`** and **`resource`**, alongside `namespace`, `cluster`, `type`, `operationId`, `limit` and `cursor`. Actor/resource filters are exact, case-sensitive matches, combined with each other and all other filters (AND); omitted/empty values do not restrict that field. For example, `/api/v1/events?namespace=dev&actor=alice&resource=namespaces` selects that actor's namespace-management activity in dev. URL-encode actor strings containing spaces or token metadata. Filters never grant permission: cluster events require **`events:read` or `audit:read`** on `<namespace>/<cluster>`; namespace-level audit requires **`audit:read`** on `<namespace>`; global installation audit requires it on `*`. Keep the same filters when advancing a cursor.

Event list/stream `type` accepts **Normal, Warning, Audit**; lifecycle action names such as `OperationFailed` are `reason`, not `type`. Unknown types/resources return **400** and list accepted values. The `cluster` filter on events and operations must be 2–12 lowercase letters/digits starting with a letter. An explicit `resource=auth` (or another unreadable resource category) returns **403** naming the missing `audit/read` permission. Unfiltered queries continue to silently omit unauthorized records, so a successful empty list does not prove there are no installation-wide audit records.

Events are listed oldest-first by default, so following `nextCursor` replays history forward. Pass **`order=desc`** to start from the newest event instead; its `nextCursor` then continues to older events. The Events pages in the web UI use `order=desc`.

Events expose `actor`, optional string **`resource`**/**`target`**, and `namespace` for namespace-scoped activity (omitted for global installation activity). Cluster-management events use `resource: "clusters"`, `target` equal to the cluster name, and retain `cluster`. Orphan-volume janitor events retain the original namespace/cluster but use resource `orphaned-volumes` and target equal to the volume hostname. Namespace-management and relay lifecycle records omit `cluster`, retain `namespace` and use resource `namespaces`. Other audit records use the affected resource (for example `accounts`, `rbac`, `settings`, `api-tokens`, `auth` or `backup-storages`) and target. Older events may omit attribution/resource/target. Every successful mutation and credential reveal is audited using safe action metadata, not request/response bodies or secrets. Login/OIDC success/failure and logout have authentication-owned events. All audit activity shares the durable event history and indefinite retention described in [security](../security.md#audit-attribution-and-durability).

`GET /api/v1/events/stream` uses `text/event-stream`, numeric event IDs and the **stored event type verbatim** as the frame's `event:` field. Current severity/audit categories are **`Normal`**, **`Warning`** and **`Audit`**; lifecycle detail is in **`reason`** (for example `ClusterReady`, `OperationFailed`, `LoginSucceeded`). The list and stream **`type` filter matches these stored values**, not `cluster.updated` or `operation.updated`. Clients must tolerate additional stored types. Reconnect with `Last-Event-ID`, or an explicit `cursor` query (which takes precedence), to replay authorized durable events. Filters include namespace/cluster/type/operationId and the same exact, case-sensitive actor/resource intersections as the list; preserve all filters on reconnect. SSE live delivery does not replace persisted operation/status queries.

```text
id: 42
event: Warning
data: {"id":42,"namespace":"default","cluster":"demo","type":"Warning","reason":"OperationFailed","message":"Backup failed","createdAt":"2026-01-01T00:00:00Z"}

```

For warning history use `/api/v1/events?type=Warning`; for a live warning stream use `/api/v1/events/stream?type=Warning`. Global audit events omit `cluster` (and omit `namespace` when installation-wide).

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
| 422 | Namespace token/project or relay-hostname validation failed |
| 429 | `rate_limited` login backoff; retry after the header/body delay |
| 500 | Unexpected server error, deliberately generic public response |
| 503 | Unavailable store/monitoring subsystem for the relevant endpoint |

A failed asynchronous Operation is not necessarily a failed HTTP request. Read its persisted error/message and cluster conditions. Never embed secrets in request names or expect diagnostic responses to reveal infrastructure credentials. Public auth errors may be intentionally less specific than administrative observations.

## Clients/SDK

### Install the Go client

The first-party Go package is public and importable from external applications:

```sh
go get github.com/KarakoramDev/Karakoram/pkg/client@<version>
```

Replace `<version>` with a release tag or commit matching your server (the angle brackets are placeholders, not shell syntax). Import `github.com/KarakoramDev/Karakoram/pkg/client`; its package name is `client`. The client uses the same generated contract as `karactl` and requires the Go version declared by that release's `go.mod`.

Use **`https://<host>/api/v1`** as the management client base URL, including `/api/v1`; do not pass only the origin or append an individual resource path. `client.WithBearerToken(token)` takes the raw Karakoram token, adds the `Authorization: Bearer …` header and needs no CSRF cookie/header. Keep HTTPS certificate verification enabled.

This example lists the first cluster page, handles the server's structured `Error` (`code`, `message`, optional `details`), then polls an existing operation. Replace the example host and supply `KARAKORAM_TOKEN` securely. Set `KARAKORAM_OPERATION_ID` to the ID returned by an accepted mutation: a resource's `operationId`, the `Karakoram-Operation-Id` response header, or a returned Operation's `id`. Do not use a cluster/backup/restore name as the operation ID.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/KarakoramDev/Karakoram/pkg/client"
)

func main() {
    c, err := client.NewClientWithResponses("https://karakoram.example/api/v1",
        client.WithBearerToken(os.Getenv("KARAKORAM_TOKEN")),
        client.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}))
    if err != nil {
        log.Fatal(err)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()
    if err := listAndWait(ctx, c, os.Getenv("KARAKORAM_OPERATION_ID")); err != nil {
        log.Fatal(err)
    }
}

func listAndWait(ctx context.Context, c *client.ClientWithResponses, operationID string) error {
    response, err := c.ListClustersWithResponse(ctx, nil)
    if err != nil {
        return err // Transport, context or response-decoding failure.
    }
    if err := responseError(response.StatusCode(), response.Body); err != nil {
        return err // An HTTP error is separate from a transport error.
    }
    if response.JSON200 == nil {
        return fmt.Errorf("unexpected cluster response: %s", response.Status())
    }
    for _, cluster := range response.JSON200.Items {
        fmt.Println(cluster.Name)
    }
    // Lists are paginated; follow JSON200.NextCursor to fetch subsequent pages.
    if operationID == "" {
        return fmt.Errorf("supply the operation ID returned by an accepted mutation")
    }
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    for {
        response, err := c.GetOperationWithResponse(ctx, operationID)
        if err != nil {
            return err
        }
        if err := responseError(response.StatusCode(), response.Body); err != nil {
            return err
        }
        if response.JSON200 == nil {
            return fmt.Errorf("unexpected operation response: %s", response.Status())
        }
        op := response.JSON200
        switch string(op.Phase) {
        case "Succeeded":
            return nil
        case "Failed", "Cancelled":
            message := ""
            if op.Message != nil {
                message = *op.Message
            }
            if op.Error != nil {
                return fmt.Errorf("operation %s %s: %s: %s", op.Id, op.Phase, op.Error.Code, op.Error.Message)
            }
            return fmt.Errorf("operation %s %s: %s", op.Id, op.Phase, message)
        }
        select {
        case <-ctx.Done():
            return ctx.Err() // Does not cancel server work; resume polling the same ID.
        case <-ticker.C:
        }
    }
}

func responseError(status int, body []byte) error {
    if status >= 200 && status < 300 {
        return nil
    }
    var apiError client.Error
    if err := json.Unmarshal(body, &apiError); err != nil || apiError.Code == "" {
        return fmt.Errorf("HTTP %d: %s", status, http.StatusText(status))
    }
    var details map[string]interface{}
    if apiError.Details != nil {
        details = *apiError.Details
    }
    // Branch on code and details["field"]/details["reason"], not message text.
    return fmt.Errorf("HTTP %d: %s: %s (details: %v)", status, apiError.Code, apiError.Message, details)
}
```

The same compiled example lives in [`pkg/client/example_test.go`](../../pkg/client/example_test.go). `WithResponse` methods return a transport/decoding error separately from HTTP status: always check the status and the expected `JSON200`/`JSON201`/`JSON202` field. Operation `Failed`/`Cancelled` is a failure even when polling returns HTTP 200. A local timeout stops waiting, not the operation; retain its ID and resume polling instead of repeating the mutation.

Optional OpenAPI properties use pointers. For example, successful `CreateAPITokenWithResponse` returns `JSON201.Secret` as `*string`: check both `JSON201` and `Secret` for nil before dereferencing, store the one-time secret securely, and never print it in ordinary logs. Later token reads expose metadata, not that secret. For all clusters in a namespace pass `ListClustersParams.Namespace`; continue pagination using `NextCursor` with the same filters.

### Consume SSE

The generated client's `StreamEvents` returns a raw `*http.Response`; it does not decode a sequence of SSE events into JSON objects. Close the body when finished and use a streaming SSE parser (not JSON decoding or `io.ReadAll` of an unending body). For raw frame inspection:

```sh
curl --no-buffer --fail-with-body \
  -H "Authorization: Bearer ${KARAKORAM_TOKEN}" \
  -H 'Accept: text/event-stream' \
  'https://YOUR-HOST/api/v1/events/stream?namespace=default'
# On reconnect, also pass: -H 'Last-Event-ID: <last-dispatched-event-id>'
```

Each blank line ends a frame. An SSE parser ignores `:` comment lines (including padded heartbeats), joins multiple `data:` lines with newlines, and dispatches `event:`/`id:`/`data:` fields only once the frame is complete. Dispatch all event names, including `Normal` and `Warning`, rather than filtering only dot-qualified names. Save the dispatched event's ID; preserve filters and send `Last-Event-ID` when reconnecting. Comments never advance that cursor. SSE is a notification channel, not proof that asynchronous work succeeded; continue querying operation/resource state. See [pagination and events](#pagination-and-events) for replay and buffering behavior.

### Generate clients for other languages

Download the spec matching the running server, or use `api/openapi.yaml` from the matching repository revision:

```sh
curl --fail-with-body 'https://YOUR-HOST/api/v1/openapi.yaml' -o openapi.yaml
# Python (with OpenAPI Generator's CLI installed):
openapi-generator-cli generate -i openapi.yaml -g python -o karakoram-python-client
python -m pip install ./karakoram-python-client
# TypeScript types + a typed fetch client:
npx openapi-typescript openapi.yaml -o karakoram-api.d.ts
npm install openapi-fetch
```

`openapi-typescript` generates types, not a runtime SDK. Import its `paths` type into `openapi-fetch` and set `baseUrl` to `https://YOUR-HOST/api/v1` and an `Authorization` bearer header. For a full TypeScript runtime client, OpenAPI Generator also supports `-g typescript-fetch`. Configure the generated Python client's host with the same `/api/v1` suffix and its `bearerAuth` security scheme with the raw token. Follow the generated package's authentication instructions; generator versions can expose different configuration names. Generated clients still need explicit HTTP-error handling, operation polling and raw SSE consumption.

## Regeneration

From the repository root:

```sh
make generate
make openapi-check
# TypeScript generation is separate:
(cd ui && npm ci && npm run gen:api)
```

Go generation uses pinned oapi-codegen 2.4.1 for `internal/api/gen.go` and `pkg/client/client.gen.go`, then the owned RBAC generator for `internal/api/rbacmap.gen.go`. TypeScript generation uses the lockfile's openapi-typescript and writes `ui/src/api/schema.d.ts`. Do not hand-edit generated artifacts.

`make openapi-check` regenerates/diffs the Go API/client/RBAC paths; it is not a TypeScript-drift or live behavior test. Update schema, all callers/tests and docs together. See [CONTRIBUTING](../../CONTRIBUTING.md) for full build/test guidance and [CLI reference](cli.md) for API-shaped file examples.

For consumers whose YAML parser limits alias expansion, maintainers can normalize
the source without changing its OpenAPI meaning:

```sh
go run ./internal/tools/specnoalias
go run ./internal/tools/specnoalias -check
go generate ./api/...
(cd ui && npm run gen:api)
```

The tool preserves existing `#/components/responses/...` references rather than
copying error schemas. `-check` exits nonzero if any YAML anchor or alias remains,
so it can guard a normalized specification in CI.

