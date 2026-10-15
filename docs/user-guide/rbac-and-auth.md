<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Authentication and RBAC

Karakoram identities are distinct from Zerops integration tokens and MongoDB database users. A control-plane user authenticates locally or through OIDC, then the server evaluates its Casbin policy subjects for each resource/action/object. UI controls are convenience checks; the server is authoritative.

## Local accounts and first login

On an empty account store Karakoram creates `admin` from `KARAKORAM_ADMIN_PASSWORD`; if no bootstrap password is supplied it generates one and logs it once. The installer supplies a generated secret, avoiding that logging path. The initial account is marked **must change password**. Sign in at the HTTPS UI, change the password, then sign in again before using ordinary privileged workflows or creating tokens. Changing the environment's bootstrap password later does not reset an existing account.

All new/reset passwords require **at least 12 Unicode characters and at most 1024 UTF-8 bytes**. A self-service change must use a password different from the current one. Reuse returns HTTP 400 with `details.field: newPassword` and `details.reason: reused`, and does not clear a required-change flag. An incorrect current password identifies `currentPassword` with reason `invalid`; a short new password identifies `newPassword` with reason `out_of_range`. Administrative resets do not compare against the current password: resetting to the same value succeeds, invalidates previous authentication and forces a password change.

After five failed logins from an account/source pair, exponential backoff starts at one second and doubles up to fifteen minutes. During that delay, both correct and incorrect passwords receive HTTP **429 `rate_limited`**, with `Retry-After` (seconds) and `details.retryAfterSeconds`. Wait before trying again; the sign-in page displays the delay. This does not disclose whether a submitted password is correct.

Passwords are Argon2id hashes. Password changes/reset, account disabling/deletion and token revocation invalidate authority through persisted state, not only eventual JWT expiry. New/reset local users can require password change. Use **Settings → Accounts**, or:

```yaml
name: analyst
password: A_PRIVATE_TEMPORARY_PASSWORD
enabled: true
mustChangePassword: true
```

```sh
karactl accounts create -f /private/path/account.yaml
karactl accounts list
karactl accounts get analyst
```

Use a private request file/secret manager; the password above is only a placeholder. `accounts set NAME -f FILE` updates enabled/forced-change state, `accounts reset-password NAME -f FILE` resets a local password, and `accounts delete NAME` removes the account. Grant roles through policy bindings, not by adding undocumented fields to the account API.

Account names are 1–64 characters, start with a letter, and contain only letters, digits, `_`, `.`, `@` or `-`. Create errors identify `name` or `password`; duplicate account names return 409 `already_exists` naming the existing account. Every enabled/disabled or forced-change update returns a fresh `updatedAt`, independently of the last password change.

Self-service password changes are available from **the sidebar user menu → Change password**, or directly at **`/change-password`**. The API is `/api/v1/auth/change-password` (`currentPassword`, `newPassword`); `karactl accounts change-password -f FILE` submits that same shape. Successful changes return to sign-in with confirmation to use the new password. A forced-change account cannot complete normal CLI login's token creation until the password is changed. Lost-admin recovery supports a startup service-secret update or the stopped-writer offline `karakoram admin reset-password ACCOUNT` procedure; see [operations](../operations.md#recovering-local-admin-access).

## Default roles

| Role | Default authority |
| --- | --- |
| `role:admin` | All resources/actions/objects; bootstrap `user:admin` is bound to it |
| `role:operator` | All actions on clusters, backups (including export), restores, database-users (including roles), operations, events and monitoring; read backup-storages/providers and namespace metadata; no namespace administration, credential reveal, audit or account/token/RBAC/settings administration |
| `role:viewer` | Read operational resources, namespace metadata, storages and providers; no backup export, cluster credentials, audit, accounts, api-tokens, rbac or settings |

These are **fresh-install defaults**. In particular, operator/viewer namespace visibility comes from `namespaces, read, **`. Upgraded installations keep their stored policy; startup normalization does not add these grants. To give those roles namespace visibility, add the following to the existing complete policy, retaining administrator access:

```csv
p, role:operator, namespaces, read, **
p, role:viewer, namespaces, read, **
```

Without namespace-read grants, the UI shows only `default`, even when cluster permissions allow access elsewhere. Narrower namespace-read rules may be used instead; see [upgrade guidance](../upgrade.md#existing-single-project-installations).

New accounts are not automatically bound to viewer/operator. Permissions accumulate via direct rules and role inheritance, subject to **deny overrides**. Give automation a dedicated account with only necessary rules; a token is not a second independently scoped role.

## Policy format

Policy is CSV:

```text
p, SUBJECT, RESOURCE, ACTION, OBJECT[, allow|deny]
g, SUBJECT, role:ROLE
```

The five-field `p` form defaults to allow; the optional sixth field is effect. Subjects are `user:NAME`, `group:OIDC_GROUP` or `role:NAME`. `g` binds a user/group/role to a role. Resource/action/object use glob matching. Cluster-scoped objects are **`<namespace>/<cluster>`**, storage objects **`<namespace>/<storage>`**, and `namespaces` objects are bare namespace names. Global resources use `*`. `*` does not cross `/`; `dev/**` matches `dev` and its objects. No match means deny, and any matching deny defeats a matching allow across the identity's subjects/roles. Comments beginning with `#` are supported.

Resources: `namespaces`, `clusters`, `cluster-credentials`, `database-users`, `backups`, `restores`, `backup-storages`, `operations`, `events`, `audit`, `monitoring`, `accounts`, `api-tokens`, `rbac`, `settings`, `providers`. Database role CRUD shares `database-users`; there is no separate `database-roles` policy resource. Actions are `create`, `read`, `update`, `delete`, source backup **`export`**, or a glob such as `*`, as appropriate for each operation. Credential reveal is **`cluster-credentials, read`**, not `read-credentials` on clusters. Namespace token rotation/validation use `namespaces/update`. Audit reads use the affected `<namespace>/<cluster>`, bare namespace for namespace-level records, and global `*` for installation records. Actor/resource/namespace filters do not widen visibility. The annotations in [OpenAPI](../../api/openapi.yaml) define the route mapping.

Example additions to the existing policy:

```csv
g, user:analyst, role:viewer
g, group:db-operators, role:production-operator
p, role:production-operator, clusters, *, prod/**, allow
p, role:production-operator, backups, *, prod/**, allow
p, role:production-operator, monitoring, read, prod/**, allow
p, role:production-operator, namespaces, read, prod, allow
p, role:production-operator, clusters, delete, prod/critical, deny
p, user:breakglass, cluster-credentials, read, prod/orders, allow
# Audit and namespace metadata are separate permissions.
p, user:alice, audit, read, dev/**
p, user:alice, namespaces, read, dev
```

These are additions, **not a complete replacement policy**: retain the bootstrap/admin binding and appropriate existing defaults. Root `/metrics` filters each `<namespace>/<cluster>` by monitoring read; a global grant is not required. Namespace list similarly filters per namespace; creating a namespace requires `namespaces/create` on global `*`.

Normalization applies at startup and to every submitted policy: object `*` becomes `**`; a legacy bare cluster pattern such as `orders*` becomes `default/orders*`, never an implicit grant in another project. Bare audit patterns follow this same legacy normalization: use `dev/**`, not bare `dev`, to read namespace-level and cluster audit records in dev. Bare objects on the `namespaces` resource remain namespace names. Built-in roles use `**`; inspect the canonical returned policy after updates.

Cross-namespace clone/restore requires source `backups/read` **and `backups/export`**, plus target `clusters/create` or `restores/create`. Same-namespace recovery needs no export grant. See [dev/stage/prod examples](namespaces.md#rbac-examples) for explicit credential denies and separate namespace metadata permissions.

```sh
karactl rbac policy get
karactl rbac policy set -f complete-policy.csv
karactl rbac can --resource clusters --action delete --object prod/critical
karactl permissions
```

`policy set` reads raw CSV, not a YAML wrapper; it replaces the **entire** policy after validation. Retain a protected copy and a tested administrator rule to avoid lockout. `rbac can` checks effective authorization; do not interpret a returned allow rule in isolation from denies. List responses, logs/events/streams and monitoring filter/check the actual cluster object, not just the route. Some current-self/owner token operations have ownership rules in addition to general policy; inspect the API contract rather than assuming viewer has token-administration permission.

The policy editor warns and asks for explicit confirmation when a draft has no user/group assignment to `role:admin` (including inherited assignments). Confirmation is **not** a lockout override: the server independently rejects a policy unless it can verify an enabled account or currently verified OIDC group identity retains unrestricted administrative rights without denies. Retain a local break-glass administrator when OIDC is used. Malformed policy errors identify `policy` and the CSV line; a rejected replacement leaves the previous policy intact.

Authorization denials identify the requested resource/action/object and advise contacting an administrator for a grant. A 403 does not indicate a missing account/resource; inspect the requested permission and any explicit denies before retrying.

## OIDC setup and groups

1. Register a confidential OIDC client with your identity provider. Use the exact redirect URL `https://YOUR-ORIGIN/api/v1/auth/oidc/callback`.
2. Set `KARAKORAM_PUBLIC_URL` to the HTTPS UI origin (the Zerops pipeline derives it by default). The server derives the callback; the API's returned `redirectURL` is informative, not an independently honored callback override.
3. Ensure ID tokens include a valid `preferred_username` and a groups claim containing a **JSON array of strings**, e.g. `["db-operators"]`. Group membership in an access token alone is insufficient. Configure any provider-specific scope/mapper needed to emit groups.
4. In **Settings → SSO**, configure issuer, client ID, secret, scopes and groups claim; or:

   ```yaml
   oidc:
     enabled: true
     issuerURL: https://identity.example.com/realms/production
     clientId: karakoram
     clientSecret: YOUR_PRIVATE_OIDC_CLIENT_SECRET
     scopes: [openid, profile, email, groups]
     groupsClaim: groups
   ```

   ```sh
   karactl settings set-oidc -f /private/path/oidc.yaml
   karactl settings get
   ```

5. Add `g, group:db-operators, role:operator` (or narrower custom roles) to the complete policy before testing sign-in. Group admission is not automatic administrative permission.
6. Sign in using the UI's SSO button. Keep a local administrator recovery path and verify a low-privilege user as well as an operator.

Issuer URLs must be HTTPS except loopback development issuers. The flow is server-side authorization code with PKCE, nonce/state and ID-token signature/issuer/audience/expiry verification. Client secrets are write-only and sealed in SQLite; omission/empty secret retains the current value when issuer/client identity is unchanged. Do not accidentally switch issuer/client while assuming the old secret follows it.

The public settings API currently exposes **groupsClaim/scopes**, not `usernameClaim`, `allowedGroups` or an account auto-creation toggle. Runtime defaults use `preferred_username`; enabling the public OIDC setting enables account auto-creation. New OIDC accounts receive no automatic role. Their issuer/subject identity is pinned: an OIDC username cannot adopt an existing local account, even if names match. Groups become subjects `group:<value>` in the session. Refresh a sign-in when identity-provider groups change; Karakoram policy changes are evaluated from current policy independently.

### TEST ONLY live issuer

`test/oidc-issuer` is a standalone auto-approve issuer for acceptance testing, **not a production identity provider**. It authenticates every visitor as `oidctest` with group `karakoram-admins`; bind that group only to a low-privilege test role. Its discovery/JWKS/code/token endpoints enforce the configured callback, one-minute single-use codes and S256 PKCE, and sign five-minute ID tokens with an ephemeral RSA key. Restarting the issuer invalidates pending codes and rotates that test signing key.

Build with `go build ./test/oidc-issuer`. Supply HTTPS `OIDC_ISSUER_URL` and `OIDC_CALLBACK_URL` for your deployment; the fixed client ID is `karakoram-e2e`. The dedicated `test/oidc-issuer/zerops.yaml` setup builds from the repository root. Create a disposable `oidcissuer` runtime service, push with `zcli service push oidcissuer --setup oidcissuer --zerops-yaml-path test/oidc-issuer/zerops.yaml`, then enable its HTTP subdomain. Set the issuer origin to that subdomain and the callback to your Karakoram origin followed by `/api/v1/auth/oidc/callback`. Configure Karakoram's issuer to the service's public HTTPS origin and `groupsClaim: groups`. Delete the issuer and remove its SSO settings/group binding when testing finishes.

## API tokens and browser sessions

Browser sessions use HttpOnly access/refresh cookies, SameSite=Strict and CSRF on unsafe methods. The short-lived access JWT expires after five minutes by default; refresh families last thirty days and rotate on refresh. The UI performs refresh and sends `X-Karakoram-CSRF`; do not copy its cookies into normal automation.

Reloading with an expired/missing access cookie restores the session using the valid refresh cookie and CSRF token. Network failures and 5xx responses do **not** clear the session: the UI reports temporary unavailability, offers Retry, and retries identity loading automatically. Ordinary UI API requests time out after 30 seconds; long-lived SSE streams are excluded. If authentication is still rejected after one successful refresh, or a new request receives 401 within ten seconds of that refresh, the UI signs out rather than repeatedly rotating and flooding audit history.

API tokens have prefix `kk_`, are returned only once and are stored as SHA-256 hashes. Optional `expiresAt` is RFC3339. They act as their owner's current permissions; disabled/deleted accounts and forced password changes cannot keep ordinary token authority. Every authenticated, password-current account can create, list, read, update and revoke its **own** tokens without an `api-tokens` policy grant. Global `api-tokens` read/update/delete grants additionally permit the corresponding administration of other owners' tokens; self-service lists otherwise contain only the caller's tokens. Creation always binds the token to the caller, never an arbitrary owner.

Token `name` is required, trimmed, non-blank and limited to **1–128 characters**. Duplicate names are intentionally allowed; use the unique token ID (shown in the UI table) to select a revoke target. Long names wrap instead of expanding the table. On both creation and update, `expiresAt` must be a valid RFC 3339 timestamp **strictly in the future**; errors identify `expiresAt` with `invalid` for malformed timestamps or `out_of_range` for past/current times. Updating with null removes expiry.

The token-creation dialog sends an `Idempotency-Key` for each submission and reuses it when retrying the same body, while synchronously preventing a double click from submitting twice. After creation, its one-time-secret panel provides working `KARAKORAM_SERVER`/`KARAKORAM_TOKEN` CLI commands and links to [the running API reference](/api/v1/openapi.yaml). Avoid pasting secret-bearing commands into shared logs or shell history; prefer injecting the token from a secret manager.

```yaml
name: nightly-automation
expiresAt: "2026-12-31T00:00:00Z"
```

```sh
karactl tokens create -f token.yaml
karactl tokens list
karactl tokens delete TOKEN_ID
```

Capture creation's one-time `secret` securely; lists/gets expose metadata only. A bearer token bypasses browser CSRF requirements, not RBAC. Supply it through `KARAKORAM_TOKEN` from a secure runtime environment, avoiding `--token` in shell history. CLI `login` creates/saves its own token and `logout` normally revokes that saved token; `logout --local` removes only local credentials. See [CLI](cli.md), [API](api.md) and [security](../security.md).
