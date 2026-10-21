<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# MCP server

Karakoram embeds an MCP server at `https://YOUR-CONTROL-PLANE/mcp`. It executes management requests in-process through the same REST authentication, authorization, validation, idempotency and audit path. It never sends telemetry.

## Enable and authenticate

`KARAKORAM_MCP=readonly` is the default: only read-risk tools are available. Set `readwrite` to expose mutations and destructive tools, or `off` to make `/mcp` return 404. Invalid values prevent startup. Environment changes require a restart. For an import YAML, add the setting to the Karakoram service's environment variables (retain existing variables):

```yaml
services:
  - hostname: karakoram
    envVariables:
      KARAKORAM_MCP: readonly
```

Use a dedicated Karakoram API token in the HTTP `Authorization: Bearer …` header, not a Zerops project token. Keep tokens in your client's secret store, never in a checked-in project configuration. Cookies are ignored. Token owner permissions are evaluated again for each REST operation; revocation stops subsequent calls. Prefer restricted, read-only tokens scoped to the namespaces and clusters the agent needs. Credential reveal requires its own permission and token restriction allowance.

The endpoint supports stateless MCP `2026-07-28` and legacy initialize-based Streamable HTTP (`2025-11-25`, `2025-06-18`, `2025-03-26`). It does not serve deprecated HTTP+SSE. Legacy sessions are memory-only, principal-bound and expire after 30 idle minutes; at most 256 are retained.

## Client configuration

Claude Code supports a remote HTTP server:

```sh
claude mcp add --transport http karakoram https://YOUR-CONTROL-PLANE/mcp --header 'Authorization: Bearer YOUR_API_TOKEN'
```

Cursor (`.cursor/mcp.json`) and clients accepting `mcpServers` use this generic shape (substitute the token through the client's supported secret mechanism):

```json
{
  "mcpServers": {
    "karakoram": {
      "url": "https://YOUR-CONTROL-PLANE/mcp",
      "headers": {"Authorization": "Bearer YOUR_API_TOKEN"}
    }
  }
}
```

VS Code (`.vscode/mcp.json`) uses `servers` and supports prompted secret inputs:

```json
{
  "inputs": [{"id": "karakoram-token", "type": "promptString", "description": "Karakoram API token", "password": true}],
  "servers": {
    "karakoram": {
      "type": "http",
      "url": "https://YOUR-CONTROL-PLANE/mcp",
      "headers": {"Authorization": "Bearer ${input:karakoram-token}"}
    }
  }
}
```

Client configuration formats change; use the client's remote Streamable HTTP option, endpoint URL and bearer header if its format differs.

## Tools, completion and confirmation

Tool names are stable snake_case names from the OpenAPI agent metadata. Path and query parameters are top-level arguments; JSON request bodies are under `body`. Nullable fields accept JSON `null`, including merge-patch removals and removing an API token's expiry. Lists are sorted and filtered by server mode, current RBAC and token restrictions. Narrow large lists using OpenAPI tags, for example `/mcp?toolsets=Clusters,Backups`; names are case-insensitive. Include `Workflows` to retain workflow tools when narrowing.

`wait` observes an operation ID, a cluster generation, a backup or restore, with a default 120-second and maximum 600-second wait. Cancelling the client wait never cancels the underlying operation. `cluster_diagnose` gathers status, members, recent operations, warning events and backups; unavailable or unauthorized sections are omitted with notes.

Mutations are asynchronous even when REST returns a cluster instead of HTTP 202. Results include `operationId` when the REST response supplies it. Use `wait`; after any timeout or broken stream, check state before resubmitting. Identical arguments derive the same idempotency key within a ten-minute time bucket; crossing a bucket is not a replay guarantee.

Destructive tools require a human elicitation response. The server writes the approval summary, including dry-run changes/effects where available, and binds approval to the principal, tool and exact arguments for five minutes. Summaries identify the namespace and full nested resource path where applicable. Some operations additionally require typing the actual resource's name: for example, deleting a backup requires the backup name, not its parent cluster name. Decline, cancel, missing or expired approval, altered arguments, or a mismatched name execute nothing. There is no model-settable confirmation argument. Clients without elicitation must use `karactl` or the web UI instead. Acceptance and rejection are audited; successful management actions carry an ` (mcp)` actor suffix.

## Security

Readonly mode classifies side effects, not confidentiality. Secret-bearing operations return actual material unchanged when authorized: do not allow agents to reveal credentials unless necessary. Treat all returned logs, events and database-controlled strings as untrusted data, not instructions. Tool annotations are guidance; REST authorization and server-side confirmation are enforcement. Self-reported client names do not enter audit identity.

The public URL must match the control plane origin. Requests with a different browser `Origin` are rejected, and authentication failures return a Bearer challenge. Run behind HTTPS. The MCP endpoint inherits the public listener's security headers and request-body limits. Store the master key securely: independently derived HMAC keys protect confirmation state and retry identities.

## Connect with OAuth

Set `KARAKORAM_OAUTH=on` and restart the control plane. OAuth is off by default.
Configure the connector with `https://<your-control-plane>/mcp`. Its 401 challenge
links to protected-resource metadata, which advertises the embedded authorization
server. Use the same public HTTPS origin as `KARAKORAM_PUBLIC_URL`.

Clients may use an HTTPS client ID metadata document. Its `client_id` must equal
the document URL; `client_name` and `redirect_uris` are required. Optionally set
`KARAKORAM_OAUTH_CLIENT_HOSTS=client.example,other.example` to restrict metadata
hosts. Private/loopback/link-local destinations and HTTP redirects are never
accepted, even for allowlisted hosts. Metadata is cached briefly; failed fetches
are cached for five seconds. Slow metadata retrieval does not block existing
connections' token authentication, refresh or revocation.

For clients requiring dynamic registration, additionally enable
`KARAKORAM_OAUTH_DCR=on`; registration is otherwise unavailable. Only public
clients (`token_endpoint_auth_method: none`) are supported. Client redirects must
match exactly; HTTP loopback IP redirects may vary only in port.

Sign in with your ordinary Karakoram browser account (local or SSO); SSO returns
to the consent flow after login. Every authorization shows
a new consent page with the client, verified metadata host, redirect and scopes.
Review these before approving; optionally enter a comma-separated namespace
limit. Declining sends `access_denied` to the validated client redirect.
Authorization uses single-use codes and mandatory S256 PKCE.

| Scope | Authority (always intersected with your current RBAC) |
| --- | --- |
| `karakoram:read` | Default; read-only, without credential reveal. |
| `karakoram:write` | Allows writes permitted by your account and MCP mode. |
| `karakoram:credentials` | Allows credential reveal permitted by your account. |

Access tokens last 10 minutes and work only at `/mcp`, not the REST API. Refresh
tokens rotate on use; replay disconnects the entire grant. Grants last at most
30 days. Account lifecycle changes are enforced immediately.
Where scopes and RBAC permit API-token management, a token created or extended
by an OAuth connection cannot outlive that connection's grant.

Open **Settings → Connected apps** to inspect clients, scopes, namespace limits,
creation and last-use times, and revoke a connection. API equivalents are
`listOAuthGrants` and `deleteOAuthGrant`. Administrators with `api-tokens`
permissions may inspect/revoke all accounts' grants. Grant issuance, revocation
and refresh-token reuse are audited without recording credentials.

Authorization responses identify this installation with the `iss` parameter;
clients should compare it with discovered issuer metadata before exchanging a
code. Discovery and authorization follow [RFC 9728](https://www.rfc-editor.org/rfc/rfc9728),
[RFC 8414](https://www.rfc-editor.org/rfc/rfc8414) and the
[MCP authorization profile](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization).

## karactl bridge and configuration

For clients requiring stdio, run `karactl --context production mcp`. The bridge connects to the selected control plane's `/mcp` with its API bearer token, without an `Origin` header. Server URL and token use the normal CLI flag/environment/context precedence. Standard output is exclusively MCP JSON-RPC; diagnostics use stderr. Instructions, tool discovery, calls, progress, cancellation, ping and confirmation round trips are forwarded. Both native multi-round-trip input requests and legacy elicitation are supported; the bridge never approves an action itself. Server mode, permissions, token restrictions and confirmation remain authoritative.

Use `--toolsets Clusters,Backups,Workflows` to narrow discovery on the server. This does not grant permission or change server mode.

`karactl mcp config --client claude-code|cursor|vscode|codex|json` creates a dedicated restricted API token and prints remote HTTP client configuration. The default client is `json`, token name is `mcp-CLIENT-DATE`, and expiry is 90 days. Repeat `--namespace NAME` to restrict its scope; `--write` removes the token's read-only restriction but cannot override owner RBAC or server readonly mode. Override the name with `--token-name` or lifetime with `--expires-in 30d` (Go durations such as `720h` are also accepted).

```sh
karactl --context production mcp config --client cursor --namespace production
karactl --context production mcp config --client vscode --expires-in 30d --output /secure/path/mcp.json
karactl --context production mcp config --client codex --stdio
```

Claude Code output is a shell command, Codex output is TOML, and other clients receive JSON. Remote configurations include the endpoint and `Authorization: Bearer` header. Protect this output: the newly created secret is printed once with a warning. `--stdio` prints a command using the selected saved context, never embeds a token in the snippet, and reveals the new dedicated token separately on stderr for provisioning. It does not modify the context; save that token into a dedicated context if the bridge should use its restrictions.

No files are written unless `--output FILE` is set, which writes the snippet with mode 0600. Do not commit generated configurations containing credentials. See the [CLI guide](cli.md#mcp-clients) for context and output details.
