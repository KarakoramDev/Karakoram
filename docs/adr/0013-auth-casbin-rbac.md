# 0013 — Casbin tuples, local/OIDC identity and protected sessions

Status: Accepted

## Context

Multi-user administration needs permissions per resource, action and cluster, including a separate credential-reveal boundary. OpenEverest's Casbin tuple is useful without importing Kubernetes RBAC. Browser sessions and CLI automation have different credential-handling requirements.

## Decision

Use Casbin with request/policy tuple `sub, res, act, obj`, role inheritance `g = _, _` and glob matching on resource, action and object. Name subjects `user:<name>`, `group:<oidcGroup>` and `role:<name>`. Authorize cluster-scoped objects by cluster-name glob and global objects by `*`; filter lists, events, metrics and streams per authorized object. Evaluate allow rules with explicit-deny support: a matching deny is not canceled by a broad allow.

Provide built-in admin, operator and viewer roles. Admin has all permissions. Operator manages clusters, backups, restores, database users and operations, reads events/monitoring and storages/providers, but cannot reveal credentials or administer accounts/RBAC/settings. Viewer is read-only without credentials. Credential reveal requires `read` on the separate `cluster-credentials` resource; operational read access does not imply it.

Support local accounts with Argon2id password hashes, and server-side OIDC authorization-code flow with PKCE and group-claim mapping. First-run bootstrap creates an admin account marked for password change from the installer-supplied secret. UI access JWTs use EdDSA and an HttpOnly, Secure-in-HTTPS, SameSite=Strict cookie; enforce CSRF header checks on unsafe cookie-authenticated requests and rotate refresh sessions. Do not store access credentials in browser local storage.

Use owner-scoped API tokens for `karactl`/automation, with `kk_` prefix, hashed storage, revocation and optional expiry. Tokens exercise their owner's current RBAC rather than granting a parallel platform role. Public auth routes are explicitly classified, not accidentally exempted through missing metadata.

## Consequences

Identity and authorization remain independent of Zerops's project token. UI hiding is not enforcement; every server operation and streamed/listed object is checked. OIDC group naming avoids collisions with local accounts. Policy updates and account/token lifecycle must take effect without letting a revoked identity retain management authority. This is tuple compatibility with OpenEverest, not drop-in policy/API compatibility.

## Alternatives considered

- **Three hardcoded roles only:** simple but cannot express per-cluster policies or OIDC groups.
- **Zerops RBAC only:** protects the shared infrastructure principal, not Karakoram users/resources.
- **Browser bearer tokens in local storage:** simplifies transport but increases script-readable credential exposure.
