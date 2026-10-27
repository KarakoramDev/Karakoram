<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Security

Karakoram holds infrastructure and engine-management authority across enrolled backend targets; this page defines its trust boundaries and operator responsibilities. [Zerops](https://zerops.io/) is the reference backend; [Dokploy](dokploy.md) and [Nomad](nomad.md) have additional infrastructure trust requirements.

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

On the **[Zerops](https://zerops.io/) backend**, public UI/API HTTPS terminates at Zerops L7 and forwards to HTTP 8080. The project-private hop is **not end-to-end browser TLS**; trust the platform/private network and never expose the plaintext listener directly to an untrusted network. [Dokploy](https://dokploy.com/) uses its configured public ingress; [Nomad](https://developer.hashicorp.com/nomad) needs an operator-managed HTTPS proxy. Agent transport has independent end-to-end TLS on every backend.

## Agent access (MCP) security model

The [management MCP server](mcp.md) and the per-cluster [data MCP gateway](data-mcp.md) are additional clients of the same control plane, not a second authorization system. Their controls are credentials, server-side enforcement and human confirmation; the list of what they do not protect against is as important as the list of what they do.

### Credentials

| Credential | Properties |
| --- | --- |
| Restricted API token | Recommended for agents. `karactl mcp config` mints one per client: read-only unless `--write`, optionally limited to namespaces, and for data access limited to one cluster with explicit `cluster-data` permissions. Restrictions are an immutable deny overlay on the owner's current RBAC; they never add authority and are re-evaluated on every call ([restricted API tokens](access-control.md#restricted-api-tokens)). |
| OAuth access token | Issued only when `KARAKORAM_OAUTH=on` by the embedded authorization server after interactive consent. Opaque, hashed at rest, audience-bound to `/mcp` and rejected by the REST API. Scopes map onto token restrictions (`karakoram:read` is read-only; `karakoram:write` and `karakoram:credentials` widen within the account's RBAC) plus an optional namespace limit chosen on the consent page. Authorization codes require S256 PKCE, expire in 60 seconds and are single-use; access tokens last 10 minutes; refresh tokens rotate and reuse revokes the whole grant; grants last at most 30 days. OAuth connections cannot create or change API tokens; mint dedicated tokens with `karactl` or the web UI. Client metadata retrieval pins resolved public addresses, rejects redirects and private destinations, and bounds response size and duration; dynamic client registration is separately opt-in. Account disablement, password changes and revocation are checked against persistent state on each request. |
| Browser cookies, unrestricted or legacy tokens | Never accepted by the data gateway. The management endpoint ignores cookies and accepts unrestricted API tokens, but prefer restricted ones. |

Operators should restrict client metadata hosts with `KARAKORAM_OAUTH_CLIENT_HOSTS` where possible and review **Settings → Connected apps** regularly. Database restores restore grants as well; revoke restored grants if their former credentials must no longer work.

### Enforcement

| Control | What it does |
| --- | --- |
| REST RBAC and token restriction overlay | Every management tool call executes in-process through the REST handler with the caller's credentials: authentication, RBAC, per-object filtering, validation, idempotency and audit are the same as for `karactl`. Tool annotations are guidance; the handler is the enforcement point. |
| Server mode | `KARAKORAM_MCP` (`readonly` by default) classifies side effects for the whole installation: `readonly` lists and executes only read-risk tools; `readwrite` adds write and destructive tools; `off` disables both endpoints. The mode is not an authorization grant: a write still needs an unrestricted-write token and owner RBAC. |
| Confirmation binding | Destructive management tools return a server-written summary and execute only after a human accepts it in the client. The approval is an HMAC over principal, tool, canonical argument hash and a five-minute expiry, derived from the master key; changed arguments, another principal, expiry, decline or a client without elicitation execute nothing. Some tools also require typing the resource's actual name. There is no model-settable confirmation argument; acceptance and rejection are audited. |
| Data gateway four switches | Data writes require cluster `spec.dataMcp.access: readWrite`, a token that lists `cluster-data:write` and is not read-only, owner RBAC `cluster-data:write`, and `KARAKORAM_MCP=readwrite`; missing any one hides write tools ([four switches](data-mcp.md#four-switches-for-writes)). Destructive data tools, and all writes with `tools.confirmWrites`, need a gateway-run confirmation bound to cluster, token, tool, class and argument hash that fails closed. `cluster-data` is not a viewer or operator grant. |
| Per-engine read/write identities | Each enabled cluster gets Karakoram-managed, sealed database identities: a read-only one always, a read-write one only with `readWrite`. Read tools always execute through the read identity, grants follow `spec.dataMcp.scope`, reviewed manifests deny unknown upstream tools, and upstream connection switching and credential tools are unavailable. Companion upstreams stay on loopback behind the private authenticated agent listener ([managed identities](data-mcp.md#managed-identities-and-rotation)). |
| Origin, transport and limits | Both endpoints reject requests whose browser `Origin` differs from the public origin, inherit the public listener's security headers and body limits, and are meant to run behind HTTPS; the management endpoint returns a Bearer challenge on authentication failure. The data gateway additionally rate-limits per token, bounds concurrency per cluster and truncates results. |
| Audit | Management actions carry an ` (mcp)` actor suffix; self-reported client names never enter audit identity. Every data tool call, including denials, records a `cluster-data` audit event with an argument hash; `auditQueryText` deliberately adds potentially sensitive arguments. |

### Not protected

- **Secrets are returned when authorized.** Read-only mode classifies side effects, not confidentiality. Operations marked as secret-bearing return actual credentials when RBAC and the token's `allowCredentialReveal` permit it; `karactl mcp config` tokens do not permit it. Do not allow agents to reveal credentials unless necessary.
- **Prompt injection through returned data.** Logs, events, database-controlled strings and data tool results are framed as untrusted data, but neither framing nor confirmation prevents a model from following malicious instructions it reads. Restricted tokens, read-only defaults, narrow scopes and a client that shows meaningful confirmations are the controls; never put production authority behind unattended blanket approvals.
- **Query cost.** Result, rate and concurrency bounds limit exposure, not the CPU, memory, I/O or locks a query consumes. Confirmations are not transactions or rollback.
- **Single-process data MCP state.** Data gateway sessions, confirmation signing keys and consumed challenges, and rate/concurrency counters live in one control-plane process; management MCP legacy sessions are memory-only too. Clients need instance affinity, and a restart ends sessions and pending confirmations.
- **[PostgreSQL](https://www.postgresql.org/) PUBLIC grants.** Scope is not an isolation boundary for objects already accessible to PUBLIC; reconciliation does not revoke application privileges.

### Backup repositories and continuous-log writers

Repository write authority is trusted for continuous PostgreSQL WAL,
PSMDB/[PBM](https://docs.percona.com/percona-backup-mongodb/latest/) oplog and [MySQL](https://www.oracle.com/mysql/)/[PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster) binary-log archives before initial recovery
preflight. Engine checksums and repository-side sidecars detect corruption;
they do not authenticate a log against a writer able to replace both the object
and its metadata. There is no control-plane-sealed per-segment archive index
that removes this trust boundary. Restrict repository writers and use immutable
retention/object-lock controls where available.

MySQL physical snapshots separately bind source-generated object manifests to
sealed recovery provenance. Their prepared physical/replay plan is frozen and
revalidated before STOP/installation. These controls detect physical snapshot
substitution and post-preflight changes; they do not make forged continuous
archives supplied before preflight trustworthy. See [MySQL backup integrity](mysql/backups.md#integrity-and-repository-write-access).

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

A relay is a management failure point with a shared ordered carrier, not a replication path or application VPN. Its outage can interrupt observations, configuration and backup control while engines continue running. Backup data paths differ by engine; do not assume a universal member-to-[S3](https://aws.amazon.com/s3/) route.

## Secrets at rest

Recoverable values use **HKDF-SHA-256-derived AES-256-GCM** from `KARAKORAM_MASTER_KEY`: a 32-byte key, salt `karakoram/secrets/v1`, info `AES-256-GCM`, fresh random nonce per value and authenticated storage-identity binding. This is direct secret-value encryption, not wrapped per-value keys. It encrypts neither the [SQLite](https://sqlite.org/) file as a whole nor engine data files.

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

### OpenBao initialization and unseal custody

Karakoram is the custodian of [OpenBao](https://openbao.org/) unseal/recovery shares, the initial root token and its management token. These are sealed in the control-plane secret store under the installation master key. The initialization owner retains only an encrypted receipt until acknowledgement; unseal shares and management/root tokens do not enter member configuration or runtime environment. Auto-unseal credentials are different: they are write-only in the API but must be delivered to OpenBao members. Protect their volumes and configuration.

Loss of consistent control-plane state or the original master key can prevent automatic Shamir re-unseal. Back up both separately and [reveal shares for secure offline custody](openbao/access-and-tls.md#reveal-and-store-recovery-material-offline). Recovery shares do not replace the external transit/KMS key. OpenBao connection details publish endpoints and CA only; they never expose a management token.

Explicit key reveal requires `openbao-unseal-keys:reveal` on the namespace-qualified cluster, administrator-only by default, and exact cluster-name confirmation. It returns `Cache-Control: no-store` and records `OpenBaoUnsealKeysRevealed` without response secrets. Do not capture the output in shared logs, screenshots or support reports.

## Service ownership

Karakoram requires recorded service/volume IDs, its own import evidence, a matching nonsecret ownership marker or readable legacy identity evidence before adoption/deletion. A hostname alone never proves ownership. Member `KARAKORAM_OWNER` is service-bound and derived from the agent token without disclosing it; relay `KARAKORAM_RELAY_OWNER` is a random namespace marker. Neither grants authentication. Agent tokens, certificates and keys remain sensitive.

Verified runtime-to-volume bindings can establish ownership; conflicting/unverified volumes remain protected. Preserve recorded state and markers rather than manually adopting similarly named services. Legacy recovery belongs in [upgrade guidance](upgrade.md); inventory/cleanup belongs in [orphaned volumes](operations.md#orphaned-platform-volumes).

## Exposure and TLS defaults

[MongoDB](https://www.mongodb.com/) and [RabbitMQ](https://www.rabbitmq.com/) default to `requireTLS`; their optional `preferTLS` permits plaintext and `disabled` removes engine encryption. [Elasticsearch](https://www.elastic.co/elasticsearch) accepts **only `requireTLS`** for Elasticsearch HTTP/transport, not blanket TLS on every stack input.
[Neo4j](https://neo4j.com/) Community accepts only `requireTLS`: Bolt 7687 and private HTTPS 7473 use the cluster CA; plain HTTP is disabled. Every native user is an administrator. See [Neo4j access and trust](neo4j/access-and-tls.md).

[Weaviate](https://weaviate.io/) defaults to `requireTLS` for HTTPS-only REST and client gRPC; `disabled` is private-only and `preferTLS` is unsupported. Internal gossip, data and Raft transports remain plaintext upstream; require an isolated private/encrypted network. Native metrics port 2112 is wildcard-bound and must remain private. Certificate changes require rolling restarts, not hot PEM replacement.

Weaviate namespace relay/control paths allow exact member targets on REST 8080, authenticated internal Raft-removal HTTP 7947 and agent HTTPS 9443 only, not client gRPC. Native REST/gossip bind `0.0.0.0` on IPv4 backends and `::` otherwise; wildcard binding is not isolation. Rolling recovery observations alone may verify the exact pinned active expired leaf at an in-validity time, retaining CA/hostname/server-auth checks; native administrative mutations keep current-time verification.

| Surface | Default | Network restriction | Authentication |
| --- | --- | --- | --- |
| Control-plane UI/API | Installer enables public HTTPS subdomain → private HTTP 8080 | No generated source-IP restriction; review ingress policy | Karakoram local/OIDC session or API token |
| Agent endpoint 9443 | Private TLS | **Never publish**; assigned management paths only | Pinned CA and member-specific token |
| MongoDB TCP | Private; `requireTLS` | Opt-in public TCP requires CIDR allowlist | MongoDB users |
| RabbitMQ AMQPS | Private; `requireTLS` | Opt-in public TCP requires CIDR allowlist | RabbitMQ users |
| RabbitMQ management HTTPS subdomain | Off; default private management HTTPS | **No CIDR allowlist** on public subdomain; only accepted with `preferTLS`/`disabled`, forwarding private HTTP | RabbitMQ accounts, not Karakoram RBAC |
| Elasticsearch HTTPS TCP | Private; `requireTLS` only | Opt-in public TCP requires CIDR allowlist; transport remains private | Elasticsearch users/API keys |
| MySQL / PXC application TCP | Private; `requireTLS` only | Opt-in public 3306 requires CIDR allowlist; private reader/replication channels remain separate | Scoped TLS-only MySQL users |
| PXC Galera/IST and SST (4567, 4568, 4444) | Explicit channel TLS and peer verification | Never published; member-only reachability is **not enforced** by any registered backend—use a dedicated network/project ([per backend](mysql/access-and-tls.md#replication-ports-and-network-restriction)) | Shared cluster-traffic certificate/key, separate from per-member SQL leaves |
| Neo4j Bolt | Private; TLS required | Opt-in public TCP requires allowlist policy; HTTPS remains private | Native users; all administrators in Community |
| Weaviate REST/gRPC | Private; `requireTLS` default | Opt-in public TLS TCP requires CIDR allowlist; internal transports remain private/plaintext | API keys and native RBAC; anonymous off |
| Kibana HTTPS subdomain | Off; private HTTP | Public opt-in has **no source-IP restriction**; L7 forwards HTTP | Elasticsearch/Kibana authentication |
| APM HTTPS subdomain | Off; private HTTP | Public opt-in has **no source-IP restriction**; L7 forwards HTTP | APM secret token or authorized API key |
| Logstash inputs | Private Beats 5044 / HTTP 8080 | No automatic public routes or input TLS/auth | User pipeline must configure needed protection |

TCP CIDR allowlists do not protect component HTTPS subdomains. Private HTTP/input listeners still require a trusted network; a public route is not proof of application readiness, CA validation or external discovery. Keep agent/internal ports private.

Engine CA/SAN rules, application users, system credentials and exposure procedures: [MongoDB](mongodb/access-and-tls.md), [RabbitMQ](rabbitmq/access-and-tls.md), [Elasticsearch](elasticsearch/access-and-security.md).

### PXC shared traffic identity

[MySQL](https://www.oracle.com/mysql/) and [PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster) require TLS for application connections. PXC additionally configures [Galera](https://galeracluster.com/)/IST with `socket.ssl=true` and SST with `encrypt=4`; `pxc_encrypt_cluster_traffic=OFF` disables automatic certificate rewriting, **not** encryption. Every replication/state-transfer channel must remain encrypted and reject an untrusted CA. See [manual-channel rationale and pinned upstream source](mysql/configuration.md#pxc-only).

Galera and SST share one cluster-traffic private key under a dedicated traffic CA, separate from SQL and agent trust. SQL/agent leaves cannot authenticate replication, but compromise of the shared traffic key still affects the cluster-wide replication boundary. Managed rotation replaces **both traffic root and shared leaf/key** under full-stop/evidence-based recovery, so reformed members reject the old traffic credential; new leaf issuance under an unchanged CA would not provide that revocation. Fence compromised secret/network access before distributing replacements. This remains cluster-wide rollover, not independent per-member revocation. Rotation causes database downtime; ordinary membership addition does not rotate traffic identity or implicitly impose that outage. [Detailed trust and revocation caveats](mysql/access-and-tls.md#pxc-cluster-traffic-trust-boundary) describe the operator obligations.

Server-side FILE import/export is unavailable for MySQL/PXC: managed configuration sets `secure_file_priv=NULL` and the administrative account has no `FILE` privilege. This limits SQL access to runtime files and shared traffic keys; it does not change native [XtraBackup](https://www.percona.com/mysql/software/percona-xtrabackup)'s backup mechanism.


### Public exposure allowlists

Public TCP requires a nonempty `spec.exposure.allowlist` whose CIDRs match `ipType`. If their union covers the entire selected IP family (`0.0.0.0/0`, `::/0` or equivalent split ranges), include `spec.exposure.allowAllAddresses: true` in the create request or any request changing public exposure, IP family or allowlist. Missing acknowledgement returns **400** with reason `required`; overlapping ranges count once and noncanonical CIDRs are normalized for coverage.

Acknowledgement must be fresh in that request, never inherited from PATCH state. Ordinary reads report whether saved public exposure covers a whole family; this is not a reusable acknowledgement. Private/restricted saves clear that flag. Unchanged exposure, including legacy routing, is preserved on unrelated updates; exposure edits must satisfy current rules. Disabling exposure needs no acknowledgement. Keep TLS/authentication and verify allowed and denied clients independently.

## RBAC and credential reveal

Credential reveal requires **`cluster-credentials/read`** on `<namespace>/<cluster>`, separately from cluster read. Ordinary connection data omits passwords. Explicit reveal returns the provider-defined credential set and CA with `Cache-Control: no-store`: [MongoDB](https://www.mongodb.com/) `databaseAdmin`, [RabbitMQ](https://www.rabbitmq.com/) `admin`, [Elasticsearch](https://www.elastic.co/elasticsearch) `elastic` and `apm-secret-token`. These are powerful management/intake credentials, not automatically suitable application identities; use engine-specific least-privilege access.

[Weaviate](https://weaviate.io/) reveal returns the protected `karakoram-admin` root API key. Use [dynamic database users and roles](weaviate/access-and-tls.md) for applications; extra `clusters/update` is required for non-read-only role assignments, key rotation when current roles exceed `viewer`/`read-only`, and every role mutation. User deletion requires only `database-users/delete`, even for privileged users; protected identities remain excluded. Official binaries are mixed-license, with enterprise features forcibly disabled and no license key supplied; see the [license boundary](weaviate/README.md#licensing).

The UI requests reveal explicitly without query-state caching. Authorized users can copy credentials; RBAC cannot recall disclosure. Rotate exposed credentials. Secret inputs and one-time-token responses must not enter shared logs.

Operator is an operational-trust role: disruptive lifecycle, restore and identity-management authority remains powerful without reveal. Full-policy replacement risks lockout; retain unrestricted local break-glass access. See [roles and policy rules](access-control.md#default-roles).

## Audit attribution and durability

Action history records successful management mutations and credential reveal with actor/resource/target metadata, not request/response bodies or secrets. Authentication records successful logins, logout and session refresh; local login failures use validated attempted names. [OIDC](https://openid.net/developers/how-connect-works/) failures are recorded **only after a verified issuer/subject matches a persisted account**. Malformed local identities, login backoff/admission denials, earlier/unmatched OIDC failures and **failed REST actions, including authorization denials, are not recorded**. This is not a complete intrusion log.

Actors identify persisted local accounts, OIDC accounts plus verified issuer, or API-token owner plus public token name/ID. User-triggered asynchronous work preserves its initiating actor; background work uses system actors. User-controlled names are metadata: never embed secrets.

Cluster records require `events/read` or `audit/read` on the cluster object; namespace-only/global records require `audit/read` on bare namespace/global `*`. Filters do not widen scope. See [reviewing audit activity](operations.md#reviewing-audit-activity).

`ClusterSpecChangeRequested` comparisons in `details.changes` require [structured audit access](access-control.md#policy-format). Secret-bearing changes are marked `sensitive: true`; both comparisons and summary messages replace secret values with the literal strings `(changed)`, `(set)` or `(unset)`, never persisting the secret values. Pipeline configuration/source strings are always masked as sensitive, even for audit readers; sensitive value lengths are never exposed.

Value and payload bounds apply after redaction. Truncation indicates incomplete content, not additional authorization: missing values in a truncated change must not be interpreted as unset. See [API schema and limits](api.md#pagination-and-events) and [Events view indicators](operations.md#reviewing-audit-activity).

Events are append-only through the API and retained indefinitely in [SQLite](https://sqlite.org/), with no automatic age/count pruning. This provides ordinary restart durability, **not cryptographic integrity or external immutability**. Remote actions and event appends are not atomic: a crash/append failure can leave a completed action without a record. Inspect operation/resource state before retrying destructive actions; protect, back up and monitor state growth.

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
