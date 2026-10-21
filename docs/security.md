<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Security

Karakoram holds infrastructure and engine-management authority across enrolled backend targets; this page defines its trust boundaries and operator responsibilities. Zerops is the reference backend; [Dokploy](dokploy.md) and [Nomad](nomad.md) have additional infrastructure trust requirements.

## Threat model and trust boundaries

Protected assets include engine data, application/administrative credentials, backup recovery chains, desired state, identities, session/token authority, CA/signing keys and infrastructure tokens.

The model protects normal API reads from accidental secret disclosure, applies object-scoped authorization, authenticates agent commands and certificates, and seals recoverable secrets against a copied state database without its master key. It does **not** protect against control-plane/root compromise, an administrator holding both state and key, malicious project/platform administrators, shared physical failure domains or deliberate abuse of authorized engine credentials. Namespaces are management/RBAC scopes, not hostile-tenant isolation.

| Boundary | Protection / trust |
| --- | --- |
| Browser/CLI → control plane | HTTPS origin, local/OIDC identity or API token, server RBAC; browser CSRF |
| Agent → control plane | Private TLS 9443, pinned installation CA, assigned-member token |
| Control plane → agent | Pinned cluster CA/hostname, member token; authenticated commands, not remote shell |
| Engine clients/members | Engine-native authentication and TLS, separate from agent transport |
| Control plane → infrastructure backend | Server-held infrastructure credential per namespace; never returned to UI/CLI |
| Backup path → object storage | Engine-specific credentials/transport; [backup guides](backups.md) define recovery and transfer paths |

On the **Zerops backend**, public UI/API HTTPS terminates at Zerops L7 and forwards to HTTP 8080. The project-private hop is **not end-to-end browser TLS**; trust the platform/private network and never expose the plaintext listener directly to an untrusted network. Dokploy uses its configured public ingress; Nomad needs an operator-managed HTTPS proxy. Agent transport has independent end-to-end TLS on every backend.

## OAuth MCP authorization

The optional embedded authorization server issues opaque, hashed-at-rest tokens
for `/mcp` only; they are not REST API credentials. Authorization codes require
S256 PKCE, expire in 60 seconds and can be consumed once. Access tokens expire in
10 minutes. Refresh tokens rotate; reuse revokes the whole grant. Grants expire
within 30 days, and account disablement, password changes and revocation are
checked against persistent state on each request.

Consent is always interactive, browser-session and CSRF protected, and cannot be
framed. Client metadata retrieval pins resolved public addresses, rejects
redirects, and bounds response size and duration. DCR is separately opt-in.
Operators should restrict client metadata hosts where possible and review
connected applications regularly. Database restores restore grants as well;
revoke restored grants if their former credentials must no longer work.


## Relay threat model

Additional projects connect outbound to `/relay/v1/connect` over verified HTTPS with **inner mTLS 1.3**, preserving tunnel confidentiality across L7 termination. A dedicated relay CA, namespace identity and enrolled certificate serial authorize admission; WebSocket Upgrade alone grants no usable session. Default-project routing remains direct.

| Threat | Control and operator consequence |
| --- | --- |
| Impersonated/stale relay | Enrolled identity/serial checks; new session replaces old; deletion revokes active sessions and pending admissions |
| General-purpose cross-project proxy | Complete provider-defined target/port policy, bare hostnames, reserved-host exclusion, all-address DNS checks and checked numeric dialing; never broaden policy to repair connectivity |
| Passive inspection of agent configuration | End-to-end agent TLS, pinned CA/hostname and member token remain inside the tunnel |
| State/enrollment leakage | Namespace tokens/client keys and installation relay keys are sealed; enrollment delivered through service secrets, not pipeline variables |
| Foreign relay adoption/deletion | Recorded service ID or nonsecret ownership marker required |
| Public admission exhaustion | Bounded handshakes, frames and streams reduce resource use, not public DoS risk |

Target policy refreshes on changes/reconnect and every thirty seconds. Relay certificates renew during runtime before expiry; [health fields and operations](namespaces.md#relay-health) explain connectivity and renewal.

**Control-plane compromise grants authority across every namespace.** A compromised relay host exposes its enrolled credential, can disrupt management and can reach locally available project services: target policy is not a sandbox for a compromised process. Project DNS/network and administrators remain trusted. End-to-end agent TLS does not protect compromised endpoints; engine traffic still needs its own TLS. Keep `requireTLS`.

A relay is a management failure point with a shared ordered carrier, not a replication path or application VPN. Its outage can interrupt observations, configuration and backup control while engines continue running. Backup data paths differ by engine; do not assume a universal member-to-S3 route.

## Secrets at rest

Recoverable values use **HKDF-SHA-256-derived AES-256-GCM** from `KARAKORAM_MASTER_KEY`: a 32-byte key, salt `karakoram/secrets/v1`, info `AES-256-GCM`, fresh random nonce per value and authenticated storage-identity binding. This is direct secret-value encryption, not wrapped per-value keys. It encrypts neither the SQLite file as a whole nor engine data files.

| Location | Security consequence |
| --- | --- |
| Control-plane service secrets | Master key, default infrastructure token, bootstrap password; restrict service administration |
| Sealed state values | Engine credentials, CA/private keys, recoverable agent tokens, namespace tokens/relay keys, S3 credentials, auth signing key, OIDC secret and internal PKI |
| Stored verifiers | Argon2id local passwords; SHA-256 API/refresh-token and agent lookup hashes |
| Plaintext state metadata | Specs/status, checkpoints/events, policy/account/token metadata, settings and metrics remain visible |
| Member/relay environments | Member's own agent token or relay enrollment secrets; not master key/infrastructure token |
| Member configuration/files | Locally required plaintext credentials; owner-only files/cache do not protect against runtime/root administrators |
| CLI context | Plain bearer token in owner-only `~/.config/karakoram/config.yaml`; protect filesystem and backups |

Write-only TLS/storage inputs are omitted from ordinary responses. Config caches/keys use restrictive permissions, but local processes need plaintext secrets; inspect engine guides for process-environment/file exposure.

Use a strong random master key of at least 32 characters; installation generates 48. Preserve it separately from consistent state backups in a secret manager. Loss makes encrypted state unusable. Changing the variable is **not rotation**: no master-key re-encryption command exists. See [state backup/recovery](operations.md#control-plane-state-backup-and-recovery).

**Warning:** If `KARAKORAM_ADMIN_PASSWORD` is unset on initial bootstrap, the generated admin password is logged once. Set it through a protected service secret to avoid that fallback; the installer does this. Audit-event redaction does not mean all application logs are secret-free. Never put secrets in names, shell traces, request logs or support archives.

## Service ownership

Karakoram requires recorded service/volume IDs, its own import evidence, a matching nonsecret ownership marker or readable legacy identity evidence before adoption/deletion. A hostname alone never proves ownership. Member `KARAKORAM_OWNER` is service-bound and derived from the agent token without disclosing it; relay `KARAKORAM_RELAY_OWNER` is a random namespace marker. Neither grants authentication. Agent tokens, certificates and keys remain sensitive.

Verified runtime-to-volume bindings can establish ownership; conflicting/unverified volumes remain protected. Preserve recorded state and markers rather than manually adopting similarly named services. Legacy recovery belongs in [upgrade guidance](upgrade.md); inventory/cleanup belongs in [orphaned volumes](operations.md#orphaned-platform-volumes).

## Exposure and TLS defaults

MongoDB and RabbitMQ default to `requireTLS`; their optional `preferTLS` permits plaintext and `disabled` removes engine encryption. Elasticsearch accepts **only `requireTLS`** for Elasticsearch HTTP/transport, not blanket TLS on every stack input.

| Surface | Default | Network restriction | Authentication |
| --- | --- | --- | --- |
| Control-plane UI/API | Installer enables public HTTPS subdomain → private HTTP 8080 | No generated source-IP restriction; review ingress policy | Karakoram local/OIDC session or API token |
| Agent endpoint 9443 | Private TLS | **Never publish**; assigned management paths only | Pinned CA and member-specific token |
| MongoDB TCP | Private; `requireTLS` | Opt-in public TCP requires CIDR allowlist | MongoDB users |
| RabbitMQ AMQPS | Private; `requireTLS` | Opt-in public TCP requires CIDR allowlist | RabbitMQ users |
| RabbitMQ management HTTPS subdomain | Off; default private management HTTPS | **No CIDR allowlist** on public subdomain; only accepted with `preferTLS`/`disabled`, forwarding private HTTP | RabbitMQ accounts, not Karakoram RBAC |
| Elasticsearch HTTPS TCP | Private; `requireTLS` only | Opt-in public TCP requires CIDR allowlist; transport remains private | Elasticsearch users/API keys |
| Kibana HTTPS subdomain | Off; private HTTP | Public opt-in has **no source-IP restriction**; L7 forwards HTTP | Elasticsearch/Kibana authentication |
| APM HTTPS subdomain | Off; private HTTP | Public opt-in has **no source-IP restriction**; L7 forwards HTTP | APM secret token or authorized API key |
| Logstash inputs | Private Beats 5044 / HTTP 8080 | No automatic public routes or input TLS/auth | User pipeline must configure needed protection |

TCP CIDR allowlists do not protect component HTTPS subdomains. Private HTTP/input listeners still require a trusted network; a public route is not proof of application readiness, CA validation or external discovery. Keep agent/internal ports private.

Engine CA/SAN rules, application users, system credentials and exposure procedures: [MongoDB](mongodb/access-and-tls.md), [RabbitMQ](rabbitmq/access-and-tls.md), [Elasticsearch](elasticsearch/access-and-security.md).

### Public exposure allowlists

Public TCP requires a nonempty `spec.exposure.allowlist` whose CIDRs match `ipType`. If their union covers the entire selected IP family (`0.0.0.0/0`, `::/0` or equivalent split ranges), include `spec.exposure.allowAllAddresses: true` in the create request or any request changing public exposure, IP family or allowlist. Missing acknowledgement returns **400** with reason `required`; overlapping ranges count once and noncanonical CIDRs are normalized for coverage.

Acknowledgement must be fresh in that request, never inherited from PATCH state. Ordinary reads report whether saved public exposure covers a whole family; this is not a reusable acknowledgement. Private/restricted saves clear that flag. Unchanged exposure, including legacy routing, is preserved on unrelated updates; exposure edits must satisfy current rules. Disabling exposure needs no acknowledgement. Keep TLS/authentication and verify allowed and denied clients independently.

## RBAC and credential reveal

Credential reveal requires **`cluster-credentials/read`** on `<namespace>/<cluster>`, separately from cluster read. Ordinary connection data omits passwords. Explicit reveal returns the provider-defined credential set and CA with `Cache-Control: no-store`: MongoDB `databaseAdmin`, RabbitMQ `admin`, Elasticsearch `elastic` and `apm-secret-token`. These are powerful management/intake credentials, not automatically suitable application identities; use engine-specific least-privilege access.

The UI requests reveal explicitly without query-state caching. Authorized users can copy credentials; RBAC cannot recall disclosure. Rotate exposed credentials. Secret inputs and one-time-token responses must not enter shared logs.

Operator is an operational-trust role: disruptive lifecycle, restore and identity-management authority remains powerful without reveal. Full-policy replacement risks lockout; retain unrestricted local break-glass access. See [roles and policy rules](access-control.md#default-roles).

## Cluster data MCP boundary

[Data MCP](data-mcp.md) is off per cluster by default and serves one endpoint per cluster on the existing public listener. It rejects cookies, unrestricted tokens and mismatched origins; explicit token data permission and current owner RBAC are both required. `cluster-data` is not a viewer/operator grant. Writes need [four independent switches](data-mcp.md#four-switches-for-writes), with fail-closed gateway confirmation for destructive tools and optionally all writes.

Managed read/write database identities, scope and reviewed tool manifests constrain access; upstream connection switching and credential tools are unavailable. Companion upstreams remain on loopback behind the private authenticated agent listener, not a new public service. Results are bounded untrusted data, **not protection against prompt injection**. Queries can still load production. Data tool calls have their own `cluster-data` audit events, including denials and argument hashes; `auditQueryText` deliberately adds potentially sensitive arguments. See [limits, identities and audit behavior](data-mcp.md#confirmation-results-and-limits).

## Audit attribution and durability

Action history records successful management mutations and credential reveal with actor/resource/target metadata, not request/response bodies or secrets. Authentication records successful logins, logout and session refresh; local login failures use validated attempted names. OIDC failures are recorded **only after a verified issuer/subject matches a persisted account**. Malformed local identities, login backoff/admission denials, earlier/unmatched OIDC failures and **failed REST actions, including authorization denials, are not recorded**. This is not a complete intrusion log.

Actors identify persisted local accounts, OIDC accounts plus verified issuer, or API-token owner plus public token name/ID. User-triggered asynchronous work preserves its initiating actor; background work uses system actors. User-controlled names are metadata: never embed secrets.

Cluster records require `events/read` or `audit/read` on the cluster object; namespace-only/global records require `audit/read` on bare namespace/global `*`. Filters do not widen scope. See [reviewing audit activity](operations.md#reviewing-audit-activity).

`ClusterSpecChangeRequested` comparisons in `details.changes` require [structured audit access](access-control.md#policy-format). Secret-bearing changes are marked `sensitive: true`; both comparisons and summary messages replace secret values with the literal strings `(changed)`, `(set)` or `(unset)`, never persisting the secret values. Pipeline configuration/source strings are always masked as sensitive, even for audit readers; sensitive value lengths are never exposed.

Value and payload bounds apply after redaction. Truncation indicates incomplete content, not additional authorization: missing values in a truncated change must not be interpreted as unset. See [API schema and limits](api.md#pagination-and-events) and [Events view indicators](operations.md#reviewing-audit-activity).

Events are append-only through the API and retained indefinitely in SQLite, with no automatic age/count pruning. This provides ordinary restart durability, **not cryptographic integrity or external immutability**. Remote actions and event appends are not atomic: a crash/append failure can leave a completed action without a record. Inspect operation/resource state before retrying destructive actions; protect, back up and monitor state growth.

## Sessions, cookies and CSRF

| Item | Contract |
| --- | --- |
| Access | EdDSA JWT; five-minute default; HttpOnly `karakoram_access`, path `/` |
| Refresh | Thirty-day family; rotating SHA-256-hashed secrets; replay revokes family; HttpOnly `karakoram_refresh`, path `/api/v1/auth` |
| CSRF | Script-readable `karakoram_csrf`, path `/`; unsafe cookie requests require matching persisted token and `X-Karakoram-CSRF` |
| Ordinary cookies | SameSite=Strict; Secure when public origin is HTTPS |
| Exemptions | GET/HEAD/OPTIONS and pre-auth login; refresh/logout/password change still require CSRF |
| Revocation | Persisted account/session checks invalidate disabled/deleted accounts, password changes and revocation without waiting only for JWT expiry |

Forced-change sessions have restricted discovery/change/logout access. Bearer API tokens bypass CSRF only. Use the correct HTTPS public origin; protect CLI contexts rather than copying browser cookies into automation. Login backoff is process-local and uses peer address, not blindly trusted forwarded headers; L7 may reduce end-user isolation.

## OIDC

Server-side authorization code with PKCE/S256, signed state/nonce and ID-token signature/issuer/audience/expiry checks protects sign-in. Issuer/subject pinning prevents local-account adoption. The temporary HttpOnly `karakoram_oidc_state` cookie uses SameSite=Lax, path `/api/v1/auth/oidc`, ten-minute lifetime for the cross-site callback; ordinary cookies stay Strict. See [setup, groups and explicit role bindings](access-control.md#oidc-setup-and-groups).

## Zerops token scope

Each namespace needs **Custom access per project → Full access** for its selected existing project: imports/deploys, secret updates, resources/volumes, lifecycle/routing and logs. Organization-wide authority/project creation are unnecessary. **A project-scoped Full access token without sudo mode cannot read sensitive environment values. Sudo mode adds that read access and is not needed: leave it disabled.** Full access still confers broad infrastructure mutation authority; trust the runtime/project administrators and retain service environment isolation.

Keep integration tokens in service secrets, never source, pipeline runtime `envVariables`, artifacts or shell traces. Signed upload/log URLs are credential-bearing too. Secret updates require process restart/redeploy to take effect; coordinate safely. Default token configuration uses `KARAKORAM_ZEROPS_TOKEN/PROJECT_ID/API_URL`, not the reserved `ZEROPS_` prefix. [Namespace lifecycle](namespaces.md#add-validate-and-rotate) owns stored/default token rotation.

## Reporting vulnerabilities

Report privately to **security@karakoram.dev**. Do not post credentials, private keys, unredacted state archives or weaponized reproduction details in public issues.

Include affected revision/deployment, impact/trust boundary, prerequisites and a minimal redacted reproduction. Distinguish observations from inference. Revoke/rotate accidentally published credentials immediately; deleting a message does not revoke them.
