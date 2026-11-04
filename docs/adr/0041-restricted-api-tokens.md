# 0041 — Restricted API tokens

Status: accepted

## Context and ranked drivers

An API token exercises its owner's full current RBAC. The only way to give an agent, CI job or integration less authority than a person is a separate account with its own policy. Agents in particular should get the least authority that works.

Priorities, in order:

1. **Never widen:** a restriction can only remove authority from the owner's RBAC, now and after later policy changes.
2. **Complete coverage:** restrictions apply to every authorization point — route checks, list filtering, nested checks, event streams, permission discovery and token issuance.
3. **Simple model:** a handful of fields a person can review on the token page.

## Alternatives and comparison

| Option | Never widen (5) | Coverage (4) | Simplicity (3) | Total |
| --- | ---: | ---: | ---: | ---: |
| Per-token casbin policy rules | 3 | 4 | 2 | 37 |
| Service accounts only (documented workaround) | 5 | 5 | 2 | 51 |
| **Deny overlay evaluated as an intersection with owner RBAC** | 5 | 5 | 4 | **57** |

Per-token policies can grant as well as deny and need policy review skills. Service accounts remain available but are heavy for one agent.

## Decision

`domain.ApiToken` gains `Restrictions *TokenRestrictions`; `nil` means unrestricted (all existing tokens). A token principal carries the restrictions in `auth.Principal.Restrictions`.

| Field | Effect |
| --- | --- |
| `readOnly` | Denies every action except `read`. |
| `namespaces` | Allowlist of namespaces. Cluster and storage objects (`namespace/name`) and namespace objects must be in the list. Global objects (`*`) are denied except for collection operations, whose results are filtered per object. |
| `clusters` | Allowlist of `namespace/cluster`, matched exactly only for cluster-scoped resources. Namespace objects and other namespaced objects (such as backup storages) are allowed in the listed clusters' namespaces, independently of the storage/object name. If `namespaces` is also set, both namespace constraints must hold. |
| `permissions` | Allowlist of `resource:action` pairs (`action` may be `*`). Empty means every permission of the owner. |
| `allowCredentialReveal` | Default `false`: denies `cluster-credentials` and every operation whose response is marked `secret` in [ADR 0040](0040-agent-interface-mcp.md) metadata. |

Enforcement is one predicate, "owner RBAC allows **and** restrictions allow", used by route authorization (before self-service and collection shortcuts and before idempotency replay), per-object list filtering, nested handler checks, event delivery (REST and SSE) and permission discovery (`getPermissions` and `checkPermission` answer for the effective authority). Restrictions are read from the token record on every request, so SSE streams observe a revoked token immediately.

Token management rules:

- Restrictions are fixed at creation; `replaceAPIToken` changes only name and expiry.
- A restricted principal can create a token only with restrictions that are a subset of its own (`readOnly` kept, allowlists narrowed, permissions narrowed, reveal only if it has reveal). A restricted principal cannot create an unrestricted token.
- If a restricted caller's token expires, every child must have an expiry no later than the caller's. Updating any token requires its restrictions to be a subset of the caller's and its resulting expiry to obey the same bound; removing expiry is not allowed for an expiring restricted caller. These checks also prevent a restricted token from extending a broader sibling token.
- Browser sessions and unrestricted tokens may create any restrictions.

The per-cluster data-plane gateway adds RBAC resource `cluster-data` (actions `read`, `write`). It is not part of the built-in viewer or operator roles; administrators have it through `*`. The gateway additionally requires an explicit `cluster-data` entry in a token's `permissions`, so unrestricted tokens never reach cluster data.

Tokens authenticate with the owner's `user:` and `role:` subjects only. [OIDC](https://openid.net/developers/how-connect-works/) group grants apply to browser sessions, not tokens; grant agent accounts explicit `user:` or role bindings.

`karactl tokens create` gains `--name`, `--expires`, `--read-only`, `--scope-namespace`, `--scope-cluster`, `--permission` and `--allow-credential-reveal`. The UI token dialog edits the same fields at creation and shows them on the token list.

## Consequences

- Least-privilege agent and CI tokens without extra accounts.
- Every new authorization point must use the shared predicate; tests cover denial through each path.
- A restriction cannot be loosened later; mint a new token instead.
