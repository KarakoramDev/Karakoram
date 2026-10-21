<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Access control

Karakoram accounts, roles and tokens authorize control-plane actions, independently of backend infrastructure credentials and engine-native users.

The server enforces policy; hidden UI controls are not an authorization boundary. Engine identities belong in [MongoDB](mongodb/access-and-tls.md), [RabbitMQ](rabbitmq/access-and-tls.md) and [Elasticsearch](elasticsearch/access-and-security.md).

## Connected OAuth applications

When [OAuth for MCP](mcp.md#connect-with-oauth) is enabled, every connection requires
interactive consent. Grants intersect the account's current RBAC with approved
scopes and optional namespace limits; they do not inherit browser OIDC group
bindings. **Settings → Connected apps** lists and revokes your grants.
`listOAuthGrants` and `deleteOAuthGrant` provide the same self-service API;
`api-tokens:read` / `api-tokens:delete` allow administrators to manage others'
grants. Revocation invalidates every access and refresh token in the grant.


## Local accounts and first login

An empty account store creates `admin` from `KARAKORAM_ADMIN_PASSWORD`. Set it through a protected service secret; the [security model](security.md) describes the generated-password logging fallback. The initial account must change its password: sign in at the HTTPS UI, change it, then sign in again before privileged work/token creation. Changing the bootstrap environment variable later does not reset an existing account.

| Rule | Value/behavior |
| --- | --- |
| Account name | 1–64 characters; starts with a letter; letters/digits/`_ . @ -` only |
| Password | At least 12 Unicode characters; at most 1024 UTF-8 bytes; Argon2id hash |
| Self-service change | Requires current password and a different new password |
| Admin reset | May reuse current value; revokes prior authentication and forces change |
| Login backoff | After five account/source failures: 1 second, doubling to 15 minutes; both correct/incorrect attempts receive 429 during delay |

Respect `Retry-After`/`details.retryAfterSeconds`. Password errors identify `currentPassword: invalid`, `newPassword: reused` or `newPassword: out_of_range`; reuse does not clear forced change. Duplicate accounts return 409 `already_exists`. Enabled/forced-change updates refresh `updatedAt` independently of password changes.

Use **Settings → Accounts** or a private request file:

```yaml
name: analyst
password: <temporary-password>
enabled: true
mustChangePassword: true
```

```sh
karactl accounts create -f account.yaml
karactl accounts list
karactl accounts get analyst
karactl accounts set analyst -f account-update.yaml
karactl accounts reset-password analyst -f password-reset.yaml
karactl accounts delete analyst
```

Assign roles through policy, not account fields. Self-service change is **User menu → Change password**, `/change-password`, or `karactl accounts change-password -f FILE` with `currentPassword`/`newPassword`. Forced-change users cannot finish CLI login's token creation first. See [local administrator recovery](operations.md#recovering-local-admin-access).

## Default roles

| Role | Grants |
| --- | --- |
| `role:admin` | All resources/actions/objects; bootstrap `user:admin` is bound here |
| `role:operator` | All actions on `clusters`, `backups` (including export), `restores`, `database-users` (including roles), `operations`, `events`, `monitoring`; read `backup-storages`, `providers`, `namespaces` |
| `role:viewer` | Read operational resources, namespace metadata, storages and providers; no backup export, credentials, audit, accounts, tokens, RBAC or settings |

Operator has no namespace administration, credential reveal, audit or account/token/RBAC/settings administration. It can still disrupt workloads, restore data and change engine identities.

Defaults apply to **fresh installations**; upgrades retain stored policy. Operator/viewer namespace visibility requires these additions if absent from the existing complete policy:

```csv
p, role:operator, namespaces, read, **
p, role:viewer, namespaces, read, **
```

Without namespace-read grants the UI shows only `default`, even with other cluster access. Narrower grants are valid. New accounts have no automatic role; permissions accumulate through direct rules/inheritance, subject to deny overrides.

## Policy format

```text
p, SUBJECT, RESOURCE, ACTION, OBJECT[, allow|deny]
g, SUBJECT, role:ROLE
```

CSV `p` rules default to allow. Subjects are `user:NAME`, `group:OIDC_GROUP`, `role:NAME`; `g` binds users/groups/roles to roles. Comments start with `#`. Resource/action/object use glob matching: `*` does not cross `/`, while `dev/**` matches `dev` and its objects. No allow means deny; **any matching deny overrides allows across all subjects/roles**.

| Resource | Actions / object |
| --- | --- |
| `namespaces` | create on global `*`; read/update/delete on bare namespace; validation/rotation use update |
| `clusters`, `database-users` | CRUD as exposed by route; `<namespace>/<cluster>`; database roles share `database-users` |
| `cluster-credentials` | read reveals credentials; `<namespace>/<cluster>` |
| `backups` | create/read/delete/export; `<namespace>/<cluster>` |
| `restores`, `operations` | Route-defined actions; `<namespace>/<cluster>` |
| `backup-storages` | CRUD; `<namespace>/<storage>` |
| `events`, `monitoring` | read; `<namespace>/<cluster>` |
| `audit` | read; cluster object, bare namespace or global `*`, according to record scope |
| `accounts`, `api-tokens`, `rbac`, `settings`, `providers` | Route-defined actions; global `*`; token self-service exceptions below |

[OpenAPI](../api/openapi.yaml) defines route mappings. Lists, events/streams and `/metrics` filter/check actual objects; filters never widen access. Namespace metadata does not grant audit access. Cross-namespace source export and target permissions are documented in [backups](backups.md); scoped examples are in [namespaces](namespaces.md).

`events:read` or `audit:read` on `<namespace>/<cluster>` permits reading a cluster event, but only **`audit:read`** returns its optional `details` object in lists and streams; viewer/operator event access alone does not grant structured comparisons. Audit readers still receive masked sensitive values, never secrets; see [audit redaction and durability](security.md#audit-attribution-and-durability).

Normalization at startup/submission changes object `*` to `**`; legacy bare cluster/storage/audit patterns become `default/<pattern>`. Bare namespace objects remain unchanged. Use `dev/**`, not bare `dev`, for namespace-wide audit; inspect the canonical policy after updates.

```csv
g, user:analyst, role:viewer
g, group:db-operators, role:production-operator
p, role:production-operator, clusters, *, prod/**
p, role:production-operator, clusters, delete, prod/critical, deny
p, role:production-operator, namespaces, read, prod
```

```sh
karactl rbac policy get
karactl rbac policy set -f complete-policy.csv
karactl rbac can --resource clusters --action delete --object prod/critical
karactl permissions
```

**Warning:** `policy set` consumes raw CSV and replaces the entire policy; examples above are additions, not replacements. Keep a protected copy and unrestricted administrator access.

The UI warns on missing admin assignments; confirmation does not bypass server validation. Replacement must leave a verifiable enabled account or currently verified OIDC-group identity with unrestricted administration and no denies. Invalid CSV identifies its line; rejected replacement preserves the previous policy. Keep a local break-glass administrator. A 403 identifies resource/action/object; check denies before retrying.

## OIDC setup and groups

1. Register a confidential client with exact callback `https://example.com/api/v1/auth/oidc/callback`.
2. Set `KARAKORAM_PUBLIC_URL` to that HTTPS UI origin. Callback is derived; returned `redirectURL` is informative, not an override.
3. Emit valid `preferred_username` and a groups claim containing a JSON string array, such as `["db-operators"]`, in the **ID token**. Access-token-only groups do not suffice.
4. Configure **Settings → SSO**, or submit:

```yaml
oidc:
  enabled: true
  issuerURL: https://identity.example.com/realms/production
  clientId: karakoram
  clientSecret: <oidc-client-secret>
  scopes: [openid, profile, email, groups]
  groupsClaim: groups
```

```sh
karactl settings set-oidc -f oidc.yaml
karactl settings get
```

5. Add `g, group:db-operators, role:operator` or narrower bindings to the complete policy. Test SSO with low-privilege and operator identities while retaining local admin recovery.

Issuer URLs require HTTPS except loopback development. Client secrets are sealed/write-only; omitted/empty secrets retain the existing value only for unchanged issuer/client identity. Public settings expose `groupsClaim`/`scopes`, not `usernameClaim`, `allowedGroups` or an auto-creation toggle. Enabling OIDC enables account creation, **not automatic roles**.

Issuer/subject pinning prevents adopting existing local accounts through matching names. Session groups become `group:<value>` subjects; sign in again after identity-provider group changes. Current Karakoram policy changes apply independently. See [protocol protections](security.md).

Test-only issuer instructions live in the [end-to-end suite guide](../test/e2e/README.md), not production SSO setup.

## API tokens and browser sessions

Tokens exercise their **owner's current permissions**, optionally narrowed by immutable restrictions. Use a dedicated least-privilege automation account. Tokens begin `kk_`, are returned once and stored as SHA-256 hashes; disabled/deleted owners and forced password changes cannot retain ordinary token authority.

Password-current authenticated users may create/list/read/update/revoke their own tokens without `api-tokens` grants. Global read/update/delete grants permit the corresponding actions on other owners' tokens. Creation always binds the caller; lists otherwise show only owned tokens.

`name` is required, trimmed, nonblank, 1–128 characters; duplicates are allowed, so revoke by unique ID. Optional `expiresAt` must be RFC3339 and strictly future (`invalid`/`out_of_range` errors). Creation without expiry lasts until revocation/owner invalidation; update omission preserves expiry, null removes it.

```yaml
name: nightly-automation
expiresAt: "2026-12-31T00:00:00Z"
```

```sh
karactl tokens create -f token.yaml
karactl tokens list
karactl tokens delete '<token-id>' --yes
```

Securely capture creation's one-time `secret`; later reads expose metadata only. Inject `KARAKORAM_TOKEN` from a secret manager, not command arguments/history. CLI login saves its own token; logout revokes it unless `--local` only removes local credentials. Bearers bypass CSRF, not RBAC.

`lastUsedAt` is best-effort, asynchronous usage metadata, not an authentication
or audit guarantee. At most one usage update runs at a time with a one-second
write budget; concurrent updates may be skipped when the writer is busy.
Credential validity and owner permissions are still checked from persisted
state on each request. Public version discovery reads its settings snapshot
without waiting for the identity mutation mutex.

Browsers use rotating cookie sessions, not automation tokens; [cookie/CSRF guarantees](security.md) are separate from token permissions. The UI refreshes expired access automatically; network/5xx errors preserve the session and offer retry. Repeated authentication rejection after refresh signs out instead of looping. See [CLI](cli.md) and [API](api.md).

## Restricted API tokens

Set `restrictions` when creating a token through the API, CLI flags or the UI's
**Restrictions** section. Omitted restrictions preserve the owner's full authority;
an explicit empty object is restricted and does not permit credential reveal.
Restrictions only remove authority: owner RBAC and token restrictions must both
allow an action, including after policy changes.

| Field | Effect |
| --- | --- |
| `readOnly` | Only `read` actions are allowed. |
| `namespaces` | Allowlisted namespaces for namespace, cluster and storage objects. |
| `clusters` | Exact allowlist of `namespace/cluster` for cluster-scoped resources only. Namespace metadata and other namespaced objects, such as backup storages, are visible in the listed clusters' namespaces regardless of their own names. Combined namespace and cluster namespace constraints intersect. |
| `permissions` | Allowlisted `resource:action` pairs; action may be `*`. Empty means all owner permissions. Names and actions are validated against the generated RBAC vocabulary. |
| `allowCredentialReveal` | Defaults to false; blocks `cluster-credentials` and operations marked as returning secrets, including token creation. |

Namespace/cluster-scoped tokens cannot access global objects. Collection routes
remain available when their action is allowed, but filter each returned object.
The same restrictions apply to events, SSE, metrics, operations, permission
discovery and idempotent request replay. A read-only token may still be denied a
read that returns credentials.

Restrictions cannot be changed with token update; mint a replacement instead.
A restricted caller can mint only a subset of its own restrictions, never an
unrestricted token. Token creation also requires credential-reveal permission and
the route's action/scope. Browser sessions and unrestricted tokens may choose any
restrictions.
A restricted caller with an expiring token must give every child token an expiry
at or before its own; omitting/removing expiry is denied. Updating a token also
requires its restrictions to be a subset of the caller's and the resulting expiry
to obey the same bound. A restricted caller therefore cannot rename or extend a
broader sibling token. Browser sessions and unrestricted tokens retain the
ordinary name/expiry update rules.

```sh
karactl tokens create --name inventory --read-only \
  --scope-namespace prod --permission clusters:read
```

OIDC group grants apply to browser sessions, not tokens: token authentication uses
the owner's user/role subjects. Give automation accounts explicit user or role
bindings. See [ADR 0041](adr/0041-restricted-api-tokens.md).
