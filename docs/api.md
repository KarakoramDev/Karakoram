<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# REST API and SDK

The public contract is [api/openapi.yaml](../api/openapi.yaml), OpenAPI 3.0.3; use the specification embedded in your running server at **`GET /api/v1/openapi.yaml`** for deployment-matched schemas and clients.

Most routes are under `/api/v1`. `/healthz` and authenticated `/metrics` are installation-root exceptions. The specification download is public, served as `application/yaml` and cacheable for five minutes; there is no bundled Swagger UI. Relay and agent protocols are not public management APIs.

## Authentication

Scripts use a Karakoram API token, **not a Zerops integration token**:

```http
Authorization: Bearer <karakoram-token>
```

Bearer tokens use their owner's current permissions and need no CSRF header. Local `POST /auth/login` takes `{username, password}`, sets session cookies and returns `{account, csrfToken, expiresAt}`. Cookie-authenticated unsafe methods require **`X-Karakoram-CSRF`** matching the session token/readable CSRF cookie; login itself does not. `GET /auth/me` returns the current cookie session's CSRF token, including after OIDC. `POST /auth/refresh` rotates refresh credentials; reusing an older refresh token revokes that family. Complete mandatory password changes and reauthenticate before creating tokens or ordinary work.

OIDC uses `/auth/oidc/login` and `/auth/oidc/callback` with redirects/state/PKCE, not device-code login. Successful OIDC redirects to the installation root; `returnTo` currently does not change that. Account/password rules, token expiry/revocation and SSO setup live in [access control](access-control.md); cookie protections live in [security](security.md).

The UI bounds ordinary API requests to **30 seconds**, excluding SSE. Network/5xx failures preserve the session and offer retry. A 401 after a successful refresh, or a new 401 within **ten seconds** of that refresh, ends the session rather than repeatedly rotating credentials.

### Curl bootstrap

Requires Bash, curl, jq and an enabled local account whose mandatory password change is complete. Replace the origin without a trailing slash. The protected temporary cookie jar and CSRF header are both needed for token creation:

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

OpenAPI's `x-karakoram-rbac` declares each operation's resource/action/object. Lists, streams and metrics filter individual authorized objects; collection access does not reveal all resources. See [access control](access-control.md).

`GET /clusters/{cluster}/connection` is an explicit, audited reveal requiring `cluster-credentials:read`, with `Cache-Control: no-store`. It returns **provider-defined administrative credentials** and public CA, not an application identity. Ordinary cluster/status reads are password-free. Follow engine access procedures: [MongoDB](mongodb/access-and-tls.md), [RabbitMQ](rabbitmq/access-and-tls.md), [Elasticsearch](elasticsearch/access-and-security.md). Storage keys/private PEM inputs are write-only; generated application passwords and API-token secrets are creation responses, not later GET results.

## Resource families

Paths below are relative to `/api/v1`; cluster suffixes follow `/clusters/{cluster}`. **All** remains subject to provider capabilities and RBAC. Use OpenAPI for exact paths, payloads and permissions.

| Family | Routes or cluster suffixes | Engines |
| --- | --- | --- |
| Identity/policy/settings | `/auth/*`, `/accounts`, `/api-tokens`, `/permissions`, `/rbac/policy`, `/rbac/can`, `/settings` | All |
| Discovery/health | `/providers`, `/providers/{provider}`, `/backends`, `/version`; root `/healthz` | All |
| Namespaces | `/namespaces`, `/namespaces/{namespace}`, `/token`, `/validate` suffixes | All |
| Cluster state/connections | `/clusters`, `/clusters/{cluster}`; `/status`, `/members`, `/placement`, `/connection` | All |
| Lifecycle | `/restart`, `/resume`, `/rotate-tls`, `/members/{member}/replace`, `/system-users`, `/system-users/{user}/rotate` | Provider-supported capabilities |
| Compatibility/balancing | `/raise-fcv`, `/balancer` | MongoDB only |
| Database identities | `/database-users`, `/database-roles` and identity paths | MongoDB only |
| Native access | `/rabbitmq/vhosts`, `/rabbitmq/users`, `/rabbitmq/permissions`, `/rabbitmq/topic-permissions`, `/rabbitmq/policies`, `/rabbitmq/operator-policies` | RabbitMQ only |
| Native access | `/elasticsearch/users`, `/elasticsearch/roles`, `/elasticsearch/api-keys` | Elasticsearch only |
| Recovery | `/backup-storages`; cluster `/backups`, `/restores` | All |
| Continuous recovery window | `/pitr` | MongoDB only |
| Activity/cleanup | `/operations`, `/operations/{operation}`, `/operations/{operation}/cancel`, `/events`, `/events/stream`, `/orphaned-volumes`; cluster `/operations` | All |
| Observability | Cluster `/metrics`, `/members/{member}/logs`; root `/metrics` | All |

## Namespace parameters and lifecycle

Object routes and creation default to **`default`**. Omitting `namespace` on cross-cluster collections (`/clusters`, `/operations`, `/events`, `/events/stream`, `/orphaned-volumes`, `/backup-storages`, root `/metrics`) includes all authorized namespaces; pass `namespace=NAME` to filter. Omit it rather than sending an empty value. Namespace is part of resource identity: retain it in caches and links.

For an explicit nonexistent namespace, authorized callers receive **404** `not_found` with `details.resource: namespace` and `details.name`. Without a permission there, collections remain empty and object requests retain their normal **403**, regardless of existence. Public/authentication routes do not resolve this parameter.

**`GET /namespaces` paginates normally**, after authorization filtering. Namespace creation accepts required `name`, `projectId` (backend target ID) and write-only `token`, plus optional `description`, `backend` (default `zerops`) and `config` (non-secret string settings). `GET /backends` requires `namespaces/read` on global `*` and lists registered kinds, capabilities, relay requirements and config keys; namespace responses include `backend` and enabled `capabilities`. Registered backends are `zerops` and the experimental `dokploy`; unknown config keys return **400**. Zerops accepts no config keys; its API URL is operator-only (`KARAKORAM_ZEROPS_API_URL`) and applies equally to legacy and new namespaces. Dokploy's `url` setting is restricted to operator-approved origins ([Dokploy backend](dokploy.md#target-settings)). Backend/target identity is immutable.

Creation returns **201** after synchronous validation, then deploys a relay asynchronously only if the backend requires one (as Zerops does). **`DELETE /namespaces/{namespace}` returns 202 without a body or `Karakoram-Operation-Id`**: poll namespace lifecycle/health and events until removal, not `/operations`. Registration, token validation/rotation and guarded deletion belong in [namespaces](namespaces.md). Recovery source namespace fields and source-export permissions belong in [backups](backups.md).

## Requests and asynchronous work

Create accepts `{name, spec}`; PUT replaces `{spec}`; PATCH accepts only `{spec: {...}}` as a JSON merge patch. Arrays replace entirely. Name/status/generation/timestamps are server-owned. Unknown fields, wrong types, invalid enums/bounds and trailing JSON produce **400** with `details.field` and `details.reason`. Explicit zero counts/resources are invalid, not defaults. `spec.provider` is required; omitted version/topology/components/resources use that catalog's defaults. Go input fields use pointers: nil requests defaults, a pointer to zero does not.

Cluster create, PUT and PATCH apply engine defaults and validation, then check the namespace backend's static capabilities before saving. A spec requiring an unavailable capability returns **400 `invalid_request`** naming the backend and missing capabilities (for example `publicTcp`); admission does not make a platform API request.

Send the declared Content-Type: usually `application/json`, but cluster PATCH uses `application/merge-patch+json` (`application/json` is also accepted there for older clients). The request-body limit is **2 MiB**, including chunked requests. Responses echo `X-Request-ID`; optionally supply 1–128 characters from `[A-Za-z0-9._:-]`. Invalid/repeated IDs are replaced. Include the ID, never credentials, in error reports.

Accepted cluster mutations have durable Operations. Cluster/backup/restore responses expose `operationId`; operation-bearing responses also set **`Karakoram-Operation-Id`**. Imperative work commonly returns **202**; cluster creation returns **201**, not completed provisioning. Cluster DELETE returns **202 with an Operation body**. Poll `/operations/{id}` and resource status/inventory. HTTP acceptance, terminal Operation phase and cluster health are distinct; see [operations and status](clusters.md#operations-and-status). A wait timeout/disconnect does not cancel work.

Cancellation uses **`POST /operations/{operation}/cancel`**, requiring `clusters/update` on the operation's persisted namespace/cluster; no namespace override is needed. Acceptance returns **202 Operation** plus the operation header. Pending work becomes Cancelled immediately. Supported Running checkpoints set `cancelRequested: true` and can remain Running during cleanup. Terminal/unsafe states return **409 conflict**, `details.reason: invalid_state`. Poll the same ID; cancellation is neither rollback nor reversal of accepted desired state. Engine guides define safe checkpoints.

## Safe retries with Idempotency-Key

Supply the header only on operations declaring `IdempotencyKey` in OpenAPI: supported resource creation/update and lifecycle/recovery actions, not every mutation. Use a unique random key per intended mutation, **1–255 visible ASCII characters without spaces**, and reuse the same key/body for network retries.

- Scope includes account creation identity/OIDC issuer or the specific bearer token, method, route, namespace and path parameters. Omitted namespace and explicit `default` share a scope. Authentication, CSRF and authorization are checked again on replay.
- Successful **2xx** status/body/relevant headers are replayable for **24 hours**, including one-time secrets and operation IDs. Replay sets `Idempotent-Replayed: true`. It is the original response snapshot; query current state separately.
- JSON whitespace/key order do not affect matching; array order, values, omitted fields, explicit null and numeric spellings do. A changed body returns **409 conflict**, `details.field: Idempotency-Key`, `details.reason: conflict`. Concurrent in-flight duplicates return **409** with reason `invalid_state`; retry the same key/body after completion.
- **4xx/5xx are not cached**. Expiry permits new execution. Ambiguous timeouts or server interruption can occur after a mutation committed but before a replayable response was saved: inspect resources/operations before retrying. A fresh key is not a recovery shortcut or an exactly-once guarantee.

`karactl` does not send this header and has no flag for it. Raw HTTP clients set `Idempotency-Key`; Go callers can pass a per-call `client.RequestEditorFn` that sets the header, or configure request editing with `client.WithRequestEditorFn`. Retain each mutation's key across retries; never reuse one fixed key for unrelated writes or log replayed secrets.

## Pagination and events

Lists return `{items: [...], nextCursor?: "..."}`. `limit` defaults to **100**, range **1–500**. Follow the opaque cursor with the same filters until absent; authorization precedes pagination. Multiple pages are not a transactional snapshot. Specialized status/log responses use their own schemas. Member logs are bounded tails: `process`/`lines` work, but `cursor`/`since` currently do not, and no next cursor is returned.

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

| HTTP | Meaning |
| --- | --- |
| 400 | Invalid body/query/cursor/configuration/transition |
| 401 | Missing/expired/invalid authentication; invalid OIDC callback |
| 403 | RBAC/ownership/CSRF denial or `password_change_required` |
| 404 | Missing/not-visible resource or route; namespace rules above apply |
| 405 | Unsupported method on known route; inspect `Allow` |
| 409 | Conflicting state, unsafe cancellation or retry-key conflict |
| 413 / 415 | Body over 2 MiB / missing or unsupported Content-Type |
| 422 | Namespace token/project/relay-hostname validation failure |
| 429 | Login backoff; honor `Retry-After` and `details.retryAfterSeconds` |
| 500 | Generic unexpected failure, or sanitized dependency failure such as temporary namespace platform unavailability |
| 503 | Route-specific store/monitoring unavailability or disabled/unavailable OIDC issuer |

## Clients/SDK

Install the public Go client at a release/commit matching the server:

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

Configure `/api/v1` base URL and bearer authentication. TypeScript generation produces types; pair with `openapi-fetch`, or generate a runtime client with `-g typescript-fetch`. Generated clients still need error handling, polling and SSE parsing. Repository regeneration belongs in [CONTRIBUTING](../CONTRIBUTING.md).
