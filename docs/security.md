<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Security model

Karakoram is self-hosted administration software with authority over its configured Zerops project and managed databases. It is not a hostile multi-tenant isolation layer or a managed service. Read [limitations](limitations.md) and [operations/recovery](operations.md) alongside this model.

## Threat model and trust boundaries

Protected assets include database data, administrative/application credentials, object-store recovery chains, control-plane desired state/checkpoints, local/OIDC identities, session/token authority, signing/CA keys and the infrastructure integration token.

The design protects against unauthorized public API use, accidental credential disclosure through normal resource reads, cross-object RBAC mistakes, unauthenticated node commands, untrusted agent/server certificates and a copied SQLite database without its master key. It reduces unsafe workflow replay using persisted state/receipts. It does **not** protect against a compromised control-plane process/root user, an administrator who can read its environment plus database, a malicious Zerops project/platform administrator, physical failure-domain co-location, or deliberate use of an authorized powerful database credential.

- **Browser/CLI → control plane:** HTTPS public origin, local/OIDC identity or bearer API token, RBAC and browser CSRF.
- **Agent → control plane:** private TLS listener 9443, pinned installation internal CA, member-specific token identifying exactly the assigned member's configuration.
- **Control plane → agent:** pinned per-cluster CA/server hostname and that member's bearer token; bounded authenticated commands, not remote shell.
- **Database clients/members:** MongoDB authentication, cluster keyfile and configured database TLS, distinct from agent transport.
- **Control plane → Zerops:** server-held project-scoped Full access integration token. The public UI/CLI never receives it.
- **PBM → object storage:** S3 credentials and endpoint TLS verification, plus cluster-isolated prefixes.

The public service listener is HTTP 8080 behind Zerops's HTTPS L7 termination. Traffic within the project to that HTTP listener is not end-to-end browser TLS; treat the project private network and platform as trusted infrastructure. Do not bind it directly to an untrusted public plaintext route. Agent traffic uses end-to-end TLS independently of the L7 path.

## Secrets at rest

`internal/secrets` derives a 32-byte AES key using **HKDF-SHA-256**, salt `karakoram/secrets/v1` and info `AES-256-GCM`, from the operator-held `KARAKORAM_MASTER_KEY`. AES-256-GCM uses a cryptographically random nonce per ciphertext, version prefix `kk:v1:` and authenticated data binding ciphertext to its storage identity. This is authenticated value encryption, **not** full SQLite/disk encryption or automatic MongoDB data-file encryption.

| Location | Contents and implications |
| --- | --- |
| Control-plane secret environment | `KARAKORAM_MASTER_KEY`, `KARAKORAM_ZEROPS_TOKEN`, first-run `KARAKORAM_ADMIN_PASSWORD`; preserve/restrict these on the control-plane service |
| Control-plane nonsecret configuration | `KARAKORAM_DATA_DIR`, public/private listener/origins, project ID/API URL; names and defaults in `internal/server/config.go` |
| SQLite sealed values | Cluster passwords/keyfile, CA/private-key/member material, recoverable agent tokens, S3 credentials, auth signing key, OIDC client secret and installation internal PKI |
| SQLite verifiers | Argon2id local password hashes; SHA-256 API/refresh-token hashes and agent-token lookup hashes |
| SQLite plaintext metadata | Specs/status, member ownership/process IDs, checkpoints/events, policy/account/token metadata, settings and metric samples; encryption does not hide all metadata |
| Member secret environment | Its own `KARAKORAM_AGENT_TOKEN`; not the infrastructure token or master key |
| Member config/files | Owner-only cached MemberConfig and rendered key/certificate files, containing plaintext secrets needed by local processes |
| CLI context | Plain bearer token in owner-only `~/.config/karakoram/config.yaml`; filesystem permissions are its protection |

Write-only TLS/storage inputs are removed from ordinary cluster/storage response models and retained sealed separately. Agents receive locally needed backup/monitor credentials, not general system-account disclosure. PBM's connection URI is placed in its process environment; local process/root access can expose it. Member config cache is **0600**, keyfile **0400**, controls/receipts **0600**, protected directories normally **0700**. Volume/runtime administrators can read these files: state encryption does not extend to node-cache plaintext.

Use a strong random master key (installer generates 48 characters); minimum input length is 32. Keep it outside state snapshots in a separate secret manager. Loss means encrypted state is unusable. Replacing the variable is **not** rotation: no master-key re-encryption command exists. Back up the original key and consistent complete state independently, encrypted off-platform. Do not assume a live `.db` copy or Local Storage tarball is transactionally consistent.

## PKI, database TLS and authentication

Karakoram persists an installation internal CA for its agent-facing server and a **separate per-cluster CA** for member/agent certificates. Generated CAs use ECDSA P-256; user-provided CAs must use RSA **at least 2048 bits** or ECDSA **P-256/P-384/P-521**, match the private key, and carry valid CA/certificate-signing constraints. Private roots/signing material remain sealed in the control-plane store. Agents pin the installation CA; controller outbound agent connections pin the cluster CA. Transport uses server certificate validation plus bearer tokens, **not a claim of mandatory mutual-TLS client certificates**. The internal listener renews its leaf during TLS handshakes when within thirty days of expiry, persisting the new leaf atomically under the same CA; renewal does not require a control-plane restart or replace the trust root.

Database TLS defaults to **requireTLS** in the current implementation. Rendered MongoDB TLS sets `allowConnectionsWithoutCertificates: true`: a client with username/password and trusted server CA can connect without a client cert. This does not permit unauthenticated access. `preferTLS` permits plaintext clients; `disabled` removes database encryption. Neither choice disables independently protected agent TLS. Do not describe `preferTLS` as encrypted-everywhere.

The cluster keyfile authenticates members internally; application passwords authenticate workload identities. Generated member certificates include hostname/localhost and known public-IP SANs. User-provided PKI requires a CA **signing private key**, not only a public CA. An optional provided leaf must cover all used member hostnames, localhost IP 127.0.0.1 and selected public IP, and verify for serverAuth/clientAuth under that CA. Use a dedicated cluster CA rather than giving Karakoram your organization root. Both CA source and existing cluster CA certificate/key material are immutable: PUT/PATCH with different CA material returns **409**, while resubmitting the identical pair is allowed. Leaf rotation is explicit and rolling, not a general CA/master-key rotation workflow. See [users/TLS/access](user-guide/users-tls-access.md).

### S3 verifier outbound policy

Backup-storage verification is a bounded, signed put/get/delete probe, not unrestricted outbound HTTP. The endpoint must be an HTTP(S) origin without credentials, non-root path, query or fragment. Unapproved endpoints may use public global-unicast destinations only on ports **80/443**. Private destinations or nonstandard ports require the operator-held `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST`, a comma-separated list of exact `host` or `host:port` entries; a host-only entry permits 80/443, and a host:port entry permits that exact port. `storage-prg1.zerops.io` is trusted by default on 80/443 for Zerops storage's private resolution.

The verifier resolves and checks every address on each new connection, then dials the checked numeric address to avoid DNS-rebinding races. Loopback, link-local/metadata-style, unspecified/non-global addresses and shared address space **100.64.0.0/10** remain blocked even for allowlisted hosts. It disables proxy dialing, rejects redirects and applies a **30-second** probe deadline. Do not allowlist an untrusted service: verification sends signed S3 requests with the supplied credentials. Keep TLS verification enabled and use HTTPS; allowing a host is not certificate validation. This restriction describes the control-plane verifier, not a claim that every PBM/engine outbound request shares that egress sandbox.

## System users and PBM authority

Karakoram separates `clusterAdmin`, `userAdmin`, `clusterMonitor`, `backup` and `databaseAdmin` with independent passwords and purpose-specific roles. Monitor uses clusterMonitor@admin/read@local; userAdmin manages users/roles rather than using root for every action. System rotations are durable per-set operations with staged secrets and member-specific credential activation.

**PBM's backup identity is broadly privileged:** it has backup, restore, clusterMonitor, readWrite and custom **pbmAnyAction**, which grants **anyAction on anyResource**. This is required for PBM's recovery mechanics and must not be advertised as narrow read-only access. DatabaseAdmin also has broad database/admin/recovery rights. User administration can change grants and therefore carries escalation potential. Separation reduces routine credential reuse, not the consequences of every system-user compromise.

Application users/custom roles are managed separately and should be least-privileged. Their generated password is returned once at creation and cannot be retrieved from subsequent public reads. Restores copy application credentials/roles and reset target system users. Logical recovery retains PBM's usable restore account; physical recovery uses a protected snapshot userAdmin identity stored independently of the source cluster. Before restoring a legacy physical backup without that identity, Karakoram protects the still-existing source's userAdmin credential; this requires unchanged source credentials since the snapshot. If the source or its credential is unavailable, the restore is rejected before modifying the target.

## RBAC and credential reveal

Persisted Casbin request tuples are `subject, resource, action, object` with role inheritance and glob matching. Subjects include local accounts, roles and OIDC groups. Deny **overrides** allow; lack of a matching allow denies. Route metadata is generated from OpenAPI, while lists/events/streams/metrics also check each actual object. Do not grant all-cluster access just to expose a UI page.

Credential reveal requires **read on cluster-credentials**, separate from clusters read. Ordinary URIs omit passwords. The connection endpoint returns databaseAdmin credentials/CA with `Cache-Control: no-store`; UI requests it explicitly and does not cache it in query state. Authorized users can still copy displayed credentials; RBAC cannot recall an already disclosed database password. Rotate when exposure requires it. API/storage/TLS secret inputs and token one-time creation responses must never be logged.

Admin role manages policy/accounts/settings and broad orchestration. Operator can disrupt workloads, restore data and manage application identities, despite not receiving cluster-credentials read by default; this is operational trust, not a read-only boundary. Full-policy replacement can lock out administrators. [RBAC/auth](user-guide/rbac-and-auth.md) lists exact defaults/resources and safe policy examples.

## Audit attribution and durability

Every successful management mutation and credential reveal appends a durable event. Login/OIDC success and failure and logout are recorded by the authentication flow. Records contain actor, action/reason, safe message and affected resource/target; **request and response bodies, passwords, API/session/refresh tokens, storage secrets and private keys are not logged**. Do not put secrets into user-controlled resource or token names: those names are public metadata.

- **Local sessions:** the persisted account name, for example `admin`, not a transient cookie/session ID.
- **OIDC sessions:** the persisted Karakoram account name plus its verified issuer, for example `sso-user (oidc: https://idp.example.com)`, not a mutable display name or group list. The account is pinned to issuer/subject; the issuer suffix prevents confusing this actor with a plain local username. OIDC and local account identities cannot silently adopt each other.
- **API tokens:** owner plus public token metadata, for example `admin (token: deploy [TOKEN_ID])`. The ID distinguishes tokens even if renamed or given identical names; events preserve the metadata at action time, never the bearer secret.
- **System work:** `reconciler` for uninitiated reconciliation, `scheduler` for scheduled backups/retention, `system:startup` for startup actions and `system` for automatic orphan-volume janitor work. User-triggered asynchronous work retains its initiating actor. Desired-state writes store the actor with the generation so later reconciliation does not lose attribution.

Cluster-management events retain `cluster`, with resource `clusters` and target equal to the cluster name, and continue to use per-cluster event authorization. Orphan-volume janitor events retain the original cluster scope but use resource `orphaned-volumes` and target equal to the volume hostname. Installation audit events leave `cluster` empty and require **`audit:read` on `*`** for both list and stream access. Administrative wildcard grants include audit access; built-in viewer/operator event access does not expose noncluster account, RBAC, settings, token or authentication activity. Optional actor/resource list and stream filters are exact, case-sensitive intersections, not authorization bypasses.

Audit records are append-only through the event API, stored in the same SQLite WAL history and retained **indefinitely** like existing cluster events; there is no separate audit retention interval or automatic age/count pruning. This provides restart durability, not cryptographic integrity, external immutability or a complete forensic archive against control-plane/root compromise. Protect and consistently back up the state database, and monitor its growth.

A remote mutation and its local event append cannot be one atomic transaction. A crash or persistence failure after a remote action can leave an action without its audit record; an audit failure must not be read as proof that no remote change occurred. SQLite durability also depends on the storage and consistent backups. Use operation/resource status and external evidence to resolve ambiguous outcomes; do not retry a destructive mutation merely because an event is absent.

## Sessions, cookies and CSRF

The auth package contract currently lives in `internal/auth/auth.go` and `session.go` (there is no separate `doc.go`). It specifies:

- Ed25519/EdDSA access JWT, default **five-minute** access TTL.
- Default **thirty-day** refresh family, rotating secrets stored as SHA-256 hashes. Replay revokes the family, not just one old cookie.
- `karakoram_access`: HttpOnly, path `/`; `karakoram_refresh`: HttpOnly, path `/api/v1/auth`; `karakoram_csrf`: script-readable, path `/`.
- All ordinary session cookies are **SameSite=Strict**; Secure is enabled when the configured public origin is HTTPS. Production must use HTTPS and the correct origin.
- Unsafe cookie-authenticated methods require `X-Karakoram-CSRF` and readable cookie to match the persisted session token. GET/HEAD/OPTIONS are exempt. Pre-authentication login is exempt; refresh/logout/password changes are not.
- Persisted account/session state is checked on requests, so disabled/deleted accounts, password changes and revocation do not wait solely for JWT expiry. Forced-change sessions are restricted to the change/discovery/logout path.

Auth `Options` supports lifetime configuration for embedded callers, but no TTL environment variables are exposed by the normal server wiring. Do not invent `KARAKORAM_ACCESS_TTL` settings. API tokens (`kk_`) are hashed, owner-scoped, optionally expiring bearer secrets; they bypass browser CSRF only, not RBAC/owner-state checks. CLI contexts contain them and require protection.

API-token lifecycle is also owner self-service: password-current authenticated users can create and manage only their own tokens without global token-administration grants. Owner lists are filtered, and read/update/delete of another owner's token requires the corresponding `api-tokens` grant. Token creation always uses the authenticated owner. Cookie-based creation and mutation still require CSRF.

Local login uses bounded Argon2id password verification and in-process login backoff. Peer address comes from `RemoteAddr`, not blindly trusted forwarded headers; behind L7, do not assume perfect per-end-user rate isolation or distributed rate limiting. [Local administrator recovery](operations.md#recovering-local-admin-access) supports startup-only `KARAKORAM_RECOVERY_PASSWORD` (optional `KARAKORAM_RECOVERY_ACCOUNT`, default `admin`) as a Zerops service secret, as well as the stopped-writer offline command. Startup recovery creates/re-enables a local administrator, repairs its admin authorization, revokes its sessions/refresh/API tokens and forces a password change; only a master-key-derived HMAC fingerprint is retained as its once-per-value receipt, and the recovery password is never logged or put in events. Anyone with Zerops project service-environment write access is already inside the trusted operator boundary: they control the service holding the master key and can take over its runtime/state; this adds no new trust boundary or public unauthenticated reset endpoint. Remove the recovery secret and restart after use. The installer creates a forced-change account; do not leave the bootstrap password active.

## OIDC

SSO uses server-side authorization code + PKCE/S256, signed state/nonce and ID-token signature/issuer/audience/expiry checks. Identity is pinned to issuer/subject; a matching username cannot adopt a local account. Group claim arrays map to `group:<name>` policy subjects. New OIDC accounts have no automatic role; explicit bindings grant authority. IDP group membership captured in a session may require re-login to reflect a changed IDP claim, while policy changes are evaluated against current policy.

The temporary `karakoram_oidc_state` cookie is HttpOnly, SameSite=Lax, path `/api/v1/auth/oidc`, ten minutes; Lax allows the top-level cross-site callback. Ordinary access/refresh/CSRF cookies remain Strict. Callback derives from `KARAKORAM_PUBLIC_URL`; use exact registered HTTPS redirect. Client secrets are sealed/write-only. Public settings expose groupsClaim/scopes but not the auth package's internal allowedGroups/usernameClaim controls. See [OIDC setup](user-guide/rbac-and-auth.md#oidc-setup-and-groups).

## Public exposure and Zerops token scope

MongoDB is private by default. Opt-in public TCP requires explicit CIDR allowlists plus project IPv6/dedicated IPv4. Use TLS and strong credentials; an IP allowlist is defense in depth, not a substitute for authentication. Keep `requireTLS` and the smallest intended ranges, and verify allowed and denied access in your deployment. The HTTP subdomain/shared IPv4 are not database TCP endpoints. Never publish agent port 9443. A public seed does not fix normal RS discovery of private hosts/ports: no automatic external split-horizon DNS/SNI solution is shipped. Direct-connection diagnostic reachability is not public failover verification.

The control-plane integration token needs **Custom access per project → Full access** for the selected existing project: import/deploy, secret updates, resource/volume/lifecycle/routing and log inspection. Organization-wide Owner/Admin access and project-creation rights are unnecessary. Full project scope can read other services' sensitive environment entries. Treat the token and Karakoram runtime as trusted project infrastructure authority, and keep service environment isolation enabled. Zerops custom user variables cannot use its reserved `ZEROPS_` prefix; use **KARAKORAM_ZEROPS_TOKEN/PROJECT_ID/API_URL**.

Changing an environment secret does not update an already running process; coordinate restart/redeploy without violating database quorum. Never write tokens into pipeline runtime envVariables (which override service secrets), source, artifacts, issue reports or shell traces. Signed upload/log URLs are credential-bearing too. Karakoram API tokens and database passwords are not substitutes for the infrastructure token.

## Reporting vulnerabilities

Report vulnerabilities privately to **security@karakoram.dev**. Do **not** post credentials, private keys, unredacted state archives or weaponized reproduction details in a public issue.

Provide affected revision/deployment, impact/trust boundary, prerequisites and minimal redacted reproduction privately. Distinguish confirmed observation from inference. For accidental credential publication, revoke/rotate immediately and preserve only redacted evidence; removing a message alone does not revoke a secret.
