# 0042 — Embedded OAuth 2.1 authorization server for MCP

Status: accepted

## Context and ranked drivers

Remote [MCP](https://modelcontextprotocol.io) connectors ([claude.ai](https://www.anthropic.com/claude), [ChatGPT](https://developers.openai.com/apps-sdk/), [VS Code](https://code.visualstudio.com) and others) discover authorization through the MCP [OAuth](https://oauth.net) profile instead of asking for a pasted token. Karakoram already authenticates people (local accounts and [OIDC](https://openid.net/developers/how-connect-works/)) and has restricted API tokens ([ADR 0041](0041-restricted-api-tokens.md)), but issues no OAuth tokens.

Priorities, in order:

1. **No token passthrough, no confused deputy:** tokens accepted at `/mcp` are issued by Karakoram for `/mcp` only, after a person approved that specific client.
2. **Least authority:** OAuth grants map onto token restrictions and never exceed the person's RBAC.
3. **Small, auditable surface:** authorization code + PKCE, refresh rotation, revocation; nothing optional is on by default.

## Alternatives and comparison

| Option | Safety (5) | Client reach (3) | Operability (3) | Total |
| --- | ---: | ---: | ---: | ---: |
| Delegate to the customer's identity provider | 3 | 2 | 2 | 27 |
| API tokens only | 4 | 2 | 4 | 38 |
| **Embedded authorization server using Karakoram login + consent** | 4 | 5 | 4 | **47** |

Identity providers rarely support client metadata documents, dynamic registration or `/mcp` audiences, and their access tokens would be passed through.

## Decision

Enabled with `KARAKORAM_OAUTH=on` (default `off`). Dynamic client registration additionally needs `KARAKORAM_OAUTH_DCR=on`. `KARAKORAM_OAUTH_CLIENT_HOSTS` optionally restricts which client-metadata hosts may register (comma-separated host names).

Endpoints on the public listener:

- `GET /.well-known/oauth-protected-resource` and `/.well-known/oauth-protected-resource/mcp` — RFC 9728 metadata: `resource` = `<public URL>/mcp`, `authorization_servers` = `[<public URL>]`, `scopes_supported`, `bearer_methods_supported: [header]`. `/mcp` 401 responses name it in `WWW-Authenticate: Bearer resource_metadata=…`.
- `GET /.well-known/oauth-authorization-server` — RFC 8414 metadata: authorization code only, PKCE `S256` only, public clients (`token_endpoint_auth_methods_supported: [none]`), `client_id_metadata_document_supported: true`, registration endpoint only with DCR on.
- `GET/POST /oauth/authorize` — validates the client, exact `redirect_uri` (loopback redirect URIs may differ only in port, RFC 8252), `response_type=code`, `code_challenge_method=S256`, `resource` (must be the `/mcp` URL when present) and scopes. Without a browser session it redirects to the UI login and returns afterwards. It then always renders a consent page — client name, client ID host, redirect URI, requested scopes and an optional namespace limit — protected by the session CSRF token, `frame-ancestors 'none'` and a same-origin form. There is no remembered consent: every authorization is approved by the person.
- `POST /oauth/token` — `authorization_code` (single-use code, 60-second lifetime, bound to client, redirect URI, PKCE challenge, account and approved restrictions) and `refresh_token` (rotated on every use; reuse of a consumed refresh token revokes the whole grant).
- `POST /oauth/revoke` — RFC 7009.
- `POST /oauth/register` — RFC 7591 public-client registration, only with DCR on.

Clients are identified by a client ID metadata document (an `https` URL fetched with the outbound safeguards used for backup endpoints: resolve once and pin, no private or loopback addresses, no redirects, size and time limits, short cache) or by a registered client ID.

Access tokens are opaque (`kko_` prefix), stored hashed, valid 10 minutes, audience-bound to `/mcp` and rejected by the REST API. Refresh tokens (`kkr_`) last at most 30 days per grant. The grant stores the account, client, scopes and restrictions; a token principal carries those restrictions, re-checked against the person's current RBAC and account state on every request.

| Scope | Restriction |
| --- | --- |
| `karakoram:read` (default) | `readOnly: true` |
| `karakoram:write` | not read-only |
| `karakoram:credentials` | `allowCredentialReveal: true` |

The consent page's namespace limit becomes the `namespaces` restriction. People list and revoke their grants in the UI (Settings → Connected apps) and through the API (`listOAuthGrants`, `deleteOAuthGrant`); administrators with `api-tokens` permissions see all grants. Grant issue, refresh-token reuse and revocation are audited; the audit actor names the verified client host.

## Consequences

- Remote connectors work without pasted secrets; access is still bounded by restrictions and RBAC.
- Karakoram is security-critical OAuth code: changes need review against RFC 9700 and the MCP authorization specification.
- Grants are stored in the control-plane database; a database restore restores grants.
