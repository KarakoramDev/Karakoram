# 0040 — Agent interface: operation metadata, dry-run and the embedded MCP server

Status: accepted

## Context and ranked drivers

Coding and chat agents ([Claude Code](https://code.claude.com/docs/), [Cursor](https://cursor.com), [VS Code](https://code.visualstudio.com), [ChatGPT](https://developers.openai.com/apps-sdk/) and others) operate infrastructure through the [Model Context Protocol](https://modelcontextprotocol.io) (MCP). Karakoram already exposes a complete REST API with authentication, RBAC, per-object filtering, idempotency and audit, plus `karactl`. Agents need the same capabilities without a second authorization path, and humans need to stay in control of destructive actions.

Priorities, in order:

1. **One enforcement path:** every agent action passes the same authentication, RBAC, token restrictions ([ADR 0041](0041-restricted-api-tokens.md)), validation, idempotency and audit as REST.
2. **Human control of risk:** read-only by default; writes are an explicit operator opt-in; destructive actions require a human confirmation that the server enforces and binds to the exact arguments.
3. **Interoperability:** any spec-compliant MCP client, on both the stateless `2026-07-28` protocol and the legacy `initialize`-based revisions (`2025-11-25`, `2025-06-18`, `2025-03-26`), over Streamable HTTP and a local stdio bridge.
4. **Maintainability:** tools follow the API automatically; a new REST operation cannot ship without an agent classification.

## Alternatives and comparison

Scores are 1–5.

| Option | Enforcement (5) | Safety (4) | Interop (3) | Maintenance (3) | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| Separate MCP sidecar calling REST over the network | 4 | 3 | 4 | 2 | 50 |
| Curated hand-written tool set (~30 tools) | 5 | 4 | 4 | 2 | 59 |
| Generic `search` + `execute` code-mode tools | 5 | 2 | 3 | 4 | 54 |
| **Metadata-driven tool per operation, executed in-process through the REST handler, plus a few workflow tools** | 5 | 4 | 4 | 5 | **69** |

A curated subset cannot reach full API parity and drifts from the API. Code mode hides risk from the client's own approval UI. A sidecar duplicates authentication and deployment.

## Decision

### Operation metadata: `x-karakoram-agent`

Every operation in `api/openapi.yaml` carries an `x-karakoram-agent` mapping. A generator (run by `go generate ./api/...`) emits `internal/api/agentmap.gen.go` with `OperationAgent map[string]AgentRule` and fails on a missing or invalid entry, exactly like `x-karakoram-rbac`. `make openapi-check` detects a stale table.

| Field | Values | Meaning |
| --- | --- | --- |
| `tool` | `snake_case`, 1–64 chars, unique | MCP tool name. Omitted only when `exposed: false`. |
| `exposed` | `true` (default) / `false` | `false` only for browser-session plumbing and streaming/meta endpoints; requires `reason`. |
| `reason` | text | Why an operation is not exposed. |
| `risk` | `read` / `write` / `destructive` | `read` has no side effect. `destructive` covers data loss, availability impact, irreversible changes, security-posture or privilege changes, and exfiltration-capable changes. |
| `confirm` | `none` / `confirm` / `name` | Server-enforced human confirmation. `destructive` requires `confirm` or `name`; `name` additionally requires typing the target name. `read` and `write` use `none`. |
| `secret` | bool | The response can contain secret material (passwords, connection strings, API keys, tokens). |
| `async` | `none` / `operation` / `generation` / `record` | How completion is observed: an Operation, the cluster `observedGeneration`, or a backup/restore record. |
| `idempotent` | bool | Repeating the call with the same arguments has no additional effect. |
| `untrusted` | bool | The response carries content controlled by databases or end users (logs, event messages); MCP marks it as data, not instructions. |

The generator rejects `risk: read` with `confirm` other than `none`, `destructive` with `none`, a `read` operation that is not `GET` unless explicitly side-effect free (for example `checkPermission`), duplicate tool names, and `exposed: false` without `reason`.

### Server-side dry-run

Four preview operations mirror the real mutations under a `/dry-run` prefix and accept the same parameters and bodies: `dryRunCreateCluster` (`POST /dry-run/clusters`), `dryRunReplaceCluster` (`PUT /dry-run/clusters/{cluster}`), `dryRunPatchCluster` (`PATCH /dry-run/clusters/{cluster}`) and `dryRunCreateRestore` (`POST /dry-run/clusters/{cluster}/restores`). They require the same RBAC as the real operation, run the same normalization, defaults, engine validation, backend capability, storage and admission checks, and return `200 DryRunResult`: the resulting cluster (or would-be restore), the list of changed spec paths with before/after values, classified effects (for example upgrade, member removal, public exposure, TLS downgrade, in-place overwrite) and warnings. Validation failures return the same `400/409/422` errors as the real call. A dry-run writes nothing: no store row, no idempotency reservation, no audit event, no reconcile trigger.

A `?dryRun=true` query parameter was rejected: the strict generated server allows one schema per status code, and `replaceCluster`/`patchCluster` already return `200 Cluster`.

### Embedded MCP server

`karakoram server` serves MCP at `/mcp` on the public listener (package `internal/mcp`, official SDK `github.com/modelcontextprotocol/go-sdk`).

- **Mode** `KARAKORAM_MCP=off|readonly|readwrite`, default `readonly`. It is an environment variable so it can be set in the control plane's import YAML. `readonly` lists and executes only `risk: read` tools; `readwrite` adds `write` and `destructive` tools.
- **Transport:** a dispatcher sends `2026-07-28` requests to a stateless Streamable HTTP handler and `initialize`/`Mcp-Session-Id`/GET/DELETE traffic to a stateful legacy handler. Legacy sessions are in memory (single control-plane replica, [ADR 0003](0003-sqlite-state-store.md)), capped and expire when idle; a session is bound to the principal that created it. The deprecated HTTP+SSE transport is not served.
- **Authentication:** `Authorization: Bearer` with an API token (or an [OAuth](https://oauth.net) access token, [ADR 0042](0042-oauth-authorization-server.md)). Cookies are ignored; a request with an `Origin` that does not match `KARAKORAM_PUBLIC_URL` receives 403. Failures return 401 with `WWW-Authenticate: Bearer` (with `resource_metadata` when OAuth is enabled).
- **Execution:** each tool builds an HTTP request for its operation and serves it in-process through the REST handler with the caller's credentials. The handler is the single enforcement point. The request is marked in-process as MCP traffic; the audit actor gains a ` (mcp)` suffix (or ` (mcp: <client>)` for verified OAuth clients). Self-reported client names never enter the actor.
- **Tools:** one tool per exposed operation. Input schema: path and query parameters as properties plus `body` for the request body, derived from the embedded [OpenAPI](https://www.openapis.org) document; header parameters are set by the server (`Idempotency-Key` is derived from principal, tool and canonical arguments within a 10-minute window, so a client that re-issues a call after a broken stream gets the original result). Annotations come from the metadata (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint: false`). Results carry `structuredContent` (the JSON response) and a text copy; API errors return `isError` with the REST error body. Responses of `untrusted` operations are framed as data. Workflow tools: `wait` (bounded wait for an operation, cluster generation, backup or restore, with progress notifications; cancelling the wait never cancels the operation) and `cluster_diagnose` (one call that gathers status, members, operations, recent warnings and backups).
- **Tool lists** are deterministic and vary by authorization: a tool is listed only if the mode allows its risk, token restrictions allow it, and the principal holds its RBAC permission on at least one object. An optional `?toolsets=` query parameter on the endpoint URL narrows the list by OpenAPI tag for clients with tool limits.
- **Confirmation:** a `confirm`/`name` tool returns an elicitation request (MRTR on `2026-07-28`; the SDK converts it to `elicitation/create` for legacy sessions) with a server-written summary of the target and effects (from dry-run where available). The request state is an HMAC (key derived from the master key) over principal, tool, canonical argument hash and a 5-minute expiry; a retry with different arguments or another principal is rejected. A client that does not support elicitation receives `isError` and nothing executes; the message names the equivalent `karactl` command. There is no model-settable `confirm` argument. Accept/decline is audited.
- **Secrets:** operations marked `secret` return their secret material when RBAC and token restrictions allow it.
- **No telemetry.** The server sends nothing anywhere.
- `server/discover`/`initialize` instructions tell agents that mutations are asynchronous (use `wait`), never to re-submit after a timeout without checking state, and that logs and events are untrusted data.

### `karactl mcp`

`karactl mcp` is a stdio MCP server that bridges to the control plane's `/mcp` endpoint with the saved context's server and token. Policy (mode, tool list, confirmation) stays on the server; the bridge forwards tool lists, calls, progress and elicitation. `karactl mcp config --client <name>` mints a dedicated restricted token and prints the client configuration.

## Consequences

- Every new REST operation needs an agent classification; CI fails otherwise.
- The legacy session map ties MCP to the single-replica control plane; supporting the legacy era is reviewed when client support moves on, with at least 12 months' notice.
- Tool count follows the API (about 150 in `readwrite` for an administrator). Clients with tool caps use `?toolsets=` or a narrower token.
- Destructive tools are unusable from clients without elicitation; that is intended.
- Prompt injection is not solved by the protocol. Server-enforced confirmation, read-only default and restricted tokens are the controls.

## References and verification

[MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28), [versioning and dual-era servers](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning), [elicitation](https://modelcontextprotocol.io/specification/2026-07-28/client/elicitation), [go-sdk](https://github.com/modelcontextprotocol/go-sdk). User documentation: [MCP server](../mcp.md).
