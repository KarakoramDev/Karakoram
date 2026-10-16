<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Security model

Karakoram is self-hosted administration software with authority over its enrolled Zerops projects and managed databases. Namespaces scope credentials, routing and RBAC; they are not a hostile multi-tenant isolation layer or a managed service. Read [limitations](limitations.md) and [operations/recovery](operations.md) alongside this model.

## Threat model and trust boundaries

Protected assets include database data, administrative/application credentials, object-store recovery chains, control-plane desired state/checkpoints, local/OIDC identities, session/token authority, signing/CA keys and the infrastructure integration token.

The design protects against unauthorized public API use, accidental credential disclosure through normal resource reads, cross-object RBAC mistakes, unauthenticated node commands, untrusted agent/server certificates and a copied SQLite database without its master key. It reduces unsafe workflow replay using persisted state/receipts. It does **not** protect against a compromised control-plane process/root user, an administrator who can read its environment plus database, a malicious Zerops project/platform administrator, physical failure-domain co-location, or deliberate use of an authorized powerful database credential.

- **Browser/CLI → control plane:** HTTPS public origin, local/OIDC identity or bearer API token, RBAC and browser CSRF.
- **Agent → control plane:** private TLS listener 9443, pinned installation internal CA, member-specific token identifying exactly the assigned member's configuration.
- **Control plane → agent:** pinned per-cluster CA/server hostname and that member's bearer token; bounded authenticated commands, not remote shell.
- **Database clients/members:** MongoDB authentication, cluster keyfile and configured database TLS, distinct from agent transport.
- **Control plane → Zerops:** a server-held project-scoped Full access token per namespace. The public UI/CLI never receives tokens.
  Sudo mode is not required and should be left disabled: a sudo-mode token can
  read every sensitive variable in the project, including database and Karakoram
  secrets. Karakoram never needs to read those values.
- **PBM → object storage:** S3 credentials and endpoint TLS verification, plus cluster-isolated prefixes.

The public service listener is HTTP 8080 behind Zerops's HTTPS L7 termination. Traffic within the project to that HTTP listener is not end-to-end browser TLS; treat the project private network and platform as trusted infrastructure. Do not bind it directly to an untrusted public plaintext route. Agent traffic uses end-to-end TLS independently of the L7 path.

## Relay threat model

Additional assets are sealed namespace tokens, relay CA/hub private keys,
namespace enrollment keys and the management/configuration bytes carried by the
tunnel. Trust boundaries include the public L7 carrier, control-plane hub,
namespace project relay host, namespace private DNS/targets and the independently
authenticated agent protocol. Default namespace traffic remains direct.

| Boundary or threat | Mitigation |
| --- | --- |
| L7 termination/HTTP forwarding exposes tunnel plaintext | Outer verified HTTPS plus **inner mTLS 1.3**; dedicated relay CA, pinned hub server name and enrolled namespace certificate serial binding |
| An unrelated relay claims a namespace | CA verification, namespace URI identity and enrolled serial check; previous renewal serial accepted only until its expiry/current-certificate transition; absent/default/deleting namespaces rejected at authorization |
| Relay becomes a general cross-project dial proxy | Relay-enforced complete target policy, only provider-published member/router targets on a fixed engine port set (never cluster-internal ports), bare hostnames, no `core` or self; all resolved addresses checked and checked numeric IP dialed to prevent DNS rebinding |
| Carrier/relay reads agent configuration | Agent config pulls preserve **end-to-end TLS** to the internal listener, pinned internal CA/relay hostname SAN and member-specific bearer authentication |
| Public unauthenticated connections exhaust admission | At most thirty-two pending handshakes; ten-second TLS/introduction deadline and a separately bounded policy-acknowledgment interval, not a ten-second total slot lifetime; bounded protocol/control frames and policies |
| State copy discloses enrolled project authority | Per-namespace token/client-key sealing and sealed installation relay PKI; no tokens, fingerprints, client keys or serials in public health responses |
| Pipeline/artifact enrollment leakage or foreign service deletion | Certificate/key/CA delivery through service env secrets; recorded service ID or non-sensitive ownership marker checked before adopting/deleting a relay |

The control plane pushes current target policy on change/reconnect and every
thirty seconds. A new authenticated namespace session replaces its old session;
relay enrollment is independent of cookie/API-token authorization. The public
root `/relay/v1/connect` bypasses browser session auth only because admission is
inside the mTLS tunnel; WebSocket Upgrade alone does not grant a usable session.

Residual risks: **control-plane compromise grants authority over all namespaces**.
A compromised relay host exposes its own enrolled credential and can disrupt its
namespace's management or access locally reachable project services; target policy
does not sandbox a compromised host/process. End-to-end agent TLS protects
forwarded configuration from passive relay inspection, not from control-plane or
member compromise. Project administrators and Zerops remain trusted. A public
endpoint DoS can consume shared listener/network resources despite handshake
bounds. A relay is one management failure point, and all streams share its
ordered carrier; it does not carry replication or member-to-S3 backup data.

Project DNS/network remain trusted: checked addresses do not cryptographically
prove project membership. Inner relay TLS protects the L7 carrier, but database
TLS is separate; `tls.mode: disabled` lets a relay read MongoDB traffic on its
project-side connection. Keep database `requireTLS`.

Resource controls bound pending admission, protocol/control frames,
WebSocket-message reads (**1 MiB per message**) and active data streams
(**256 per relay session**, control stream excluded). The byte-stream adapter's
message limit is restored after construction; it does not bound total tunneled
payload. Production smux uses its default 4-MiB shared receive budget and 64-KiB
stream window, not the larger test buffers. Bounds do not eliminate public DoS.

Hub and namespace client leaves renew during runtime within thirty days of expiry. Enrollment
is checked again/fenced at session publication, preventing a pending connection
from undoing revocation or replacing a newer enrolled session. Namespace
deletion revokes its hub session and pending admissions as well as removing
stored enrollment, so stale sessions cannot route a reused name into the old
project.

Public request identifiers are validated before namespace-qualified record
lookup. Non-default storage credentials use a distinct authenticated-data domain
from legacy default storage credentials. Existing default secrets keep their
legacy AAD; namespace-aware encoding must not be confused with user-supplied
names. These controls preserve object isolation without claiming adversarial
tenant isolation against project/root administrators.

See [ADR 0018](adr/0018-multi-project-namespaces-relay.md) and
[namespace operations](operations.md#namespace-and-relay-operations).

## Secrets at rest

`internal/secrets` derives a 32-byte AES key using **HKDF-SHA-256**, salt `karakoram/secrets/v1` and info `AES-256-GCM`, from the operator-held `KARAKORAM_MASTER_KEY`. AES-256-GCM uses a cryptographically random nonce per ciphertext, version prefix `kk:v1:` and authenticated data binding ciphertext to its storage identity. This is authenticated value encryption, **not** full SQLite/disk encryption or automatic MongoDB data-file encryption.

| Location | Contents and implications |
| --- | --- |
| Control-plane secret environment | `KARAKORAM_MASTER_KEY`, `KARAKORAM_ZEROPS_TOKEN`, first-run `KARAKORAM_ADMIN_PASSWORD`; preserve/restrict these on the control-plane service |
| Control-plane nonsecret configuration | `KARAKORAM_DATA_DIR`, public/private listener/origins, project ID/API URL; names and defaults in `internal/server/config.go` |
| SQLite sealed values | Cluster passwords/keyfile, CA/private-key/member material, recoverable agent tokens, namespace project tokens/relay client keys, dedicated relay CA/hub keys, S3 credentials, auth signing key, OIDC client secret and installation internal PKI |
| SQLite verifiers | Argon2id local password hashes; SHA-256 API/refresh-token hashes and agent-token lookup hashes |
| SQLite plaintext metadata | Specs/status, member ownership/process IDs, checkpoints/events, policy/account/token metadata, settings and metric samples; encryption does not hide all metadata |
| Member secret environment | Its own `KARAKORAM_AGENT_TOKEN`; not the infrastructure token or master key |
| Member nonsecret environment | `KARAKORAM_OWNER`, a service-bound ownership marker, on each member service and volume |
| Member config/files | Owner-only cached MemberConfig and rendered key/certificate files, containing plaintext secrets needed by local processes |
| Relay secret environment | Its enrolled client certificate/key and relay CA; not the namespace infrastructure token or master key |
| Relay nonsecret environment | `KARAKORAM_RELAY_OWNER`, a random per-namespace ownership marker, not a credential |
| CLI context | Plain bearer token in owner-only `~/.config/karakoram/config.yaml`; filesystem permissions are its protection |

Write-only TLS/storage inputs are removed from ordinary cluster/storage response models and retained sealed separately. Agents receive locally needed backup/monitor credentials, not general system-account disclosure. PBM's connection URI is placed in its process environment; local process/root access can expose it. Member config cache is **0600**, keyfile **0400**, controls/receipts **0600**, protected directories normally **0700**. Volume/runtime administrators can read these files: state encryption does not extend to node-cache plaintext.

Use a strong random master key (installer generates 48 characters); minimum input length is 32. Keep it outside state snapshots in a separate secret manager. Loss means encrypted state is unusable. Replacing the variable is **not** rotation: no master-key re-encryption command exists. Back up the original key and consistent complete state independently, encrypted off-platform. Do not assume a live `.db` copy or Local Storage tarball is transactionally consistent.

## Service ownership

Karakoram records the service IDs returned by its own imports and writes a
**non-sensitive** `KARAKORAM_OWNER` variable on each member service and volume.
The marker is an HMAC-SHA256 keyed by that member's random agent token over the
cluster, member and service hostname. It cannot be reversed to recover the
token or produced for another service without that token. Anyone who can read
the project's variables can see the marker; it reveals no secret and grants no
access. `KARAKORAM_AGENT_TOKEN` and other credentials remain sensitive.

Ownership evidence is checked in this order:

1. Previously recorded service or volume IDs, including retained-volume IDs.
2. Service IDs from Karakoram's own recorded import, either its import response
   or the import's process records while Zerops still retains them.
3. A matching `KARAKORAM_OWNER` marker.
4. Legacy cluster/member/agent-token environment values, only when readable.

A volume can also be verified through a runtime proven by its marker or legacy
environment when the runtime's readable `KARAKORAM_DATA_DIR` is exactly
`/mnt/<volume hostname>`. This variable becomes non-sensitive once the member is
deployed; a sudo-mode token can also read it while sensitive. Verified IDs are
persisted before subsequent cleanup, allowing deletion after the runtime is
gone. A recorded volume-ID mismatch remains a conflict for MongoDB. RabbitMQ
and Elasticsearch accept a replacement volume only with that volume's own
ownership evidence: its marker or readable legacy variables. A matching
hostname or member stage alone is never ownership evidence, and a volume with
no verified evidence remains a conflict.

The namespace relay similarly uses its recorded service ID or
`KARAKORAM_RELAY_OWNER`, a random per-namespace, non-sensitive marker that is not
a credential. Its certificate and key remain sensitive.

Sensitive variables are masked for tokens without sudo mode, so ownership does
not depend on reading credentials back from Zerops. Existing verified services
receive the non-sensitive marker on their next reconcile. For clusters stuck
with earlier versions, see [ownership recovery](operations.md#unverified-cluster-ownership).

## PKI, database TLS and authentication

Karakoram persists an installation internal CA for its agent-facing server and a **separate per-cluster CA** for member/agent certificates. Generated CAs use ECDSA P-256; user-provided CAs must use RSA **at least 2048 bits** or ECDSA **P-256/P-384/P-521**, match the private key, and carry valid CA/certificate-signing constraints. Private roots/signing material remain sealed in the control-plane store. Agents pin the installation CA; controller outbound agent connections pin the cluster CA. Transport uses server certificate validation plus bearer tokens, **not a claim of mandatory mutual-TLS client certificates**. The internal listener renews its leaf during TLS handshakes when within thirty days of expiry, persisting the new leaf atomically under the same CA; renewal does not require a control-plane restart or replace the trust root.

Database TLS defaults to **requireTLS** in the current implementation. Rendered MongoDB TLS sets `allowConnectionsWithoutCertificates: true`: a client with username/password and trusted server CA can connect without a client cert. This does not permit unauthenticated access. `preferTLS` permits plaintext clients; `disabled` removes database encryption. Neither choice disables independently protected agent TLS. Do not describe `preferTLS` as encrypted-everywhere.

The cluster keyfile authenticates members internally; application passwords authenticate workload identities. Generated member certificates include hostname/localhost and known public-IP SANs. User-provided PKI requires a CA **signing private key**, not only a public CA. An optional provided leaf must cover all used member hostnames, localhost IP 127.0.0.1 and selected public IP, and verify for serverAuth/clientAuth under that CA. Use a dedicated cluster CA rather than giving Karakoram your organization root. Both CA source and existing cluster CA certificate/key material are immutable: PUT/PATCH with different CA material returns **409**, while resubmitting the identical pair is allowed. Leaf rotation is explicit and rolling, not a general CA/master-key rotation workflow. See [users/TLS/access](user-guide/users-tls-access.md).

### S3 verifier outbound policy

Backup-storage verification is a bounded, signed put/get/delete probe, not unrestricted outbound HTTP. The endpoint must be an HTTP(S) origin without credentials, non-root path, query or fragment. Unapproved endpoints may use public global-unicast destinations only on ports **80/443**. Private destinations or nonstandard ports require the operator-held `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST`, a comma-separated list of exact `host` or `host:port` entries; a host-only entry permits 80/443, and a host:port entry permits that exact port. `storage-prg1.zerops.io` is trusted by default on 80/443 for Zerops storage's private resolution. Registration saves configuration, not proof of access or transport security.

The verifier resolves and checks every address on each new connection, then dials the checked numeric address to avoid DNS-rebinding races. Loopback, link-local/metadata-style, unspecified/non-global addresses and shared address space **100.64.0.0/10** remain blocked even for allowlisted hosts. It disables proxy dialing, rejects redirects and applies a **30-second** probe deadline. Do not allowlist an untrusted service: verification sends signed S3 requests with the supplied credentials. Keep TLS verification enabled and use HTTPS; allowing a host is not certificate validation. This restriction describes the control-plane verifier, not a claim that every PBM/engine outbound request shares that egress sandbox.

## System users and PBM authority

Karakoram separates `clusterAdmin`, `userAdmin`, `clusterMonitor`, `backup` and `databaseAdmin` with independent passwords and purpose-specific roles. Monitor uses clusterMonitor@admin/read@local; userAdmin manages users/roles rather than using root for every action. System rotations are durable per-set operations with staged secrets and member-specific credential activation.

**PBM's backup identity is broadly privileged:** it has backup, restore, clusterMonitor, readWrite and custom **pbmAnyAction**, which grants **anyAction on anyResource**. This is required for PBM's recovery mechanics and must not be advertised as narrow read-only access. DatabaseAdmin also has broad database/admin/recovery rights. User administration can change grants and therefore carries escalation potential. Separation reduces routine credential reuse, not the consequences of every system-user compromise.

Application users/custom roles are managed separately and should be least-privileged. Their generated password is returned once at creation and cannot be retrieved from subsequent public reads. Restores copy application credentials/roles and reset target system users. Logical recovery retains PBM's usable restore account; physical recovery uses a protected snapshot userAdmin identity stored independently of the source cluster. Before restoring a legacy physical backup without that identity, Karakoram protects the still-existing source's userAdmin credential; this requires unchanged source credentials since the snapshot. If the source or its credential is unavailable, the restore is rejected before modifying the target.

## RBAC and credential reveal

Persisted Casbin request tuples are `subject, resource, action, object` with role inheritance and glob matching. Subjects include local accounts, roles and OIDC groups. Deny **overrides** allow; lack of a matching allow denies. Route metadata is generated from OpenAPI, while lists/events/streams/metrics also check each actual object. Do not grant all-cluster access just to expose a UI page.

Cluster-scoped objects are `<namespace>/<cluster>`; storage objects are
`<namespace>/<storage>`. The `namespaces` resource uses the bare namespace name
(create requires global `*`; list checks each namespace). Global settings,
accounts, RBAC, API tokens and providers remain global `*`. Policy object `*`
normalizes to `**`, legacy bare cluster patterns to `default/<pattern>`;
namespace-name rules remain bare. Explicit denies apply across all subjects.
Cross-namespace restore/clone additionally requires source `backups/export`,
source backup read and target mutation permission, preventing read-only users
from exporting production data into a namespace they can operate.


Credential reveal requires **read on cluster-credentials**, separate from clusters read. Ordinary URIs omit passwords. The connection endpoint returns databaseAdmin credentials/CA with `Cache-Control: no-store`; UI requests it explicitly and does not cache it in query state. Authorized users can still copy displayed credentials; RBAC cannot recall an already disclosed database password. Rotate when exposure requires it. API/storage/TLS secret inputs and token one-time creation responses must never be logged.

Admin role manages policy/accounts/settings and broad orchestration. Operator can disrupt workloads, restore data and manage application identities, despite not receiving cluster-credentials read by default; this is operational trust, not a read-only boundary. Full-policy replacement can lock out administrators. [RBAC/auth](user-guide/rbac-and-auth.md) lists exact defaults/resources and safe policy examples.

## Audit attribution and durability

Every successful management mutation and credential reveal appends a durable event. Login/OIDC success and failure and logout are recorded by the authentication flow. Records contain actor, action/reason, safe message and affected resource/target; **request and response bodies, passwords, API/session/refresh tokens, storage secrets and private keys are not logged**. Do not put secrets into user-controlled resource or token names: those names are public metadata.

- **Local sessions:** the persisted account name, for example `admin`, not a transient cookie/session ID.
- **OIDC sessions:** the persisted Karakoram account name plus its verified issuer, for example `sso-user (oidc: https://idp.example.com)`, not a mutable display name or group list. The account is pinned to issuer/subject; the issuer suffix prevents confusing this actor with a plain local username. OIDC and local account identities cannot silently adopt each other.
- **API tokens:** owner plus public token metadata, for example `admin (token: deploy [TOKEN_ID])`. The ID distinguishes tokens even if renamed or given identical names; events preserve the metadata at action time, never the bearer secret.
- **System work:** `reconciler` for uninitiated reconciliation, `scheduler` for scheduled backups/retention, `system:startup` for startup actions and `system` for automatic orphan-volume janitor and namespace controller work. User-triggered asynchronous work retains its initiating actor. Desired-state writes store the actor with the generation in every namespace so later reconciliation does not lose attribution.

Cluster-management events retain `namespace` and `cluster`, with resource `clusters` and target equal to the cluster name; list/stream access requires `events:read` or `audit:read` on `<namespace>/<cluster>`. Orphan-volume janitor events retain the original namespace/cluster scope but use resource `orphaned-volumes` and target equal to the volume hostname. Namespace mutations and relay lifecycle events leave `cluster` empty, retain the namespace and require **`audit:read` on `<namespace>`**. Global installation audit leaves both namespace and cluster empty and requires **`audit:read` on `*`**. Thus `p, user:alice, audit, read, dev/**` grants namespace-level and cluster audit in dev, not global audit or other namespaces. Administrative wildcard grants include audit access; built-in viewer/operator event access does not expose noncluster account, RBAC, settings, token, authentication or namespace audit. Optional namespace/actor/resource filters are intersections, not authorization bypasses.

`ClusterSpecChangeRequested` records include a field-level comparison in optional
`details.changes`. The API removes the entire `details` object from list and
stream responses unless the reader has `audit:read` on the event's scope;
`events:read` can expose the cluster event and its summary without granting the
structured comparison. Secret-bearing changes are marked `sensitive: true` and
their values are replaced by the literal strings `(changed)`, `(set)` or
`(unset)` in both the comparison and message, never persisted as secret values.
Pipeline configuration/source strings are always masked as sensitive content.
Missing `before`/`after` properties normally indicate an absent field, not JSON
`null`; when a change has `truncated: true`, missing values mean omitted content
and must not be interpreted as unset. Large nonsecret values and oversized detail
payloads are bounded after redaction; sensitive value lengths are never exposed.
The global and cluster Events views mark sensitive and truncated rows explicitly
and warn when `details.truncated` indicates an incomplete comparison.

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

Each namespace's integration token needs **Custom access per project → Full access** for its selected existing project: import/deploy, secret updates, resource/volume/lifecycle/routing and log inspection. Organization-wide Owner/Admin access and project-creation rights are unnecessary. Full project scope can read other services' sensitive environment entries. Treat each token and the Karakoram runtime as trusted project infrastructure authority, and keep service environment isolation enabled. Zerops custom user variables cannot use its reserved `ZEROPS_` prefix; use **KARAKORAM_ZEROPS_TOKEN/PROJECT_ID/API_URL** for the default namespace.

Changing an environment secret does not update an already running process; coordinate restart/redeploy without violating database quorum. Never write tokens into pipeline runtime envVariables (which override service secrets), source, artifacts, issue reports or shell traces. Signed upload/log URLs are credential-bearing too. Karakoram API tokens and database passwords are not substitutes for the infrastructure token.

Stored namespace token rotation validates project access before replacing the
sealed value and live Backend token, without restarting its relay. Default-token
rotation is environment-managed and requires service restart. Removing a
namespace does not remove policy grants; review stale authority before reusing
a name. See [namespace lifecycle](user-guide/namespaces.md).

## RabbitMQ security

RabbitMQ has separate trust paths: per-member authenticated node control, broker Management API credentials, AMQP application credentials and shared Erlang distribution authentication. One random cluster cookie is sealed in SecretStore and rendered 0400; it grants node/CLI authority, not application login. Do not export it, put it in command arguments or expose distribution/epmd ports publicly.

Bootstrap creates `karakoram` for control-plane Management API administration and `admin` for customer administration, each with administrator tags and all grants on `/`, and removes `guest`. `monitor` is reserved with a stored credential but not bootstrap-created; Prometheus collection uses a loopback listener rather than this account. Access routes protect those reserved usernames and grants. Only `karakoram`/`admin` have exposed system-password rotation operations. Ordinary application reads never return passwords/hashes; generated passwords are returned once. Use distinct application users and least-privilege vhost/topic grants. RabbitMQ tags grant management authority and are not messaging permissions.

`requireTLS` (default) enables AMQPS, HTTPS private administration and peer-verified IPv6 Erlang distribution TLS. AMQP clients may authenticate with passwords without client certificates; they must verify the broker certificate/CA. `preferTLS` also permits plaintext clients/management and leaves distribution plaintext; `disabled` leaves broker traffic plaintext. The inter-node transport and CA source are creation-time choices: disabled/preferTLS can roll between each other, but crossing requireTLS requires a new cluster. Broker leaf rotation does not rotate the Erlang cookie, CA or control-plane master key.

**Temporary OTP TLS-client restriction:** the catalog pins Erlang 27.3.4.17, affected by the TLS 1.3 client certificate-verification bypass [CVE-2026-89422 / GHSA-rgxr-4g4w-j875](https://github.com/erlang/otp/security/advisories/GHSA-rgxr-4g4w-j875). Patched 27.3.4.18 is not yet available in the configured Team RabbitMQ Ubuntu noble repositories. Every generated Erlang TLS client uses `{versions, ['tlsv1.2']}`: node and CLI distribution share the restricted client optfile, and Osiris stream replication constrains both client and server. Peer verification stays enabled. Public AMQPS, stream and HTTPS server listener version settings are unchanged. The exact Erlang package is a single catalog value shared by the curated broker versions; upgrade it to a packaged fix ≥27.3.4.18 before considering removal of this mitigation. User-supplied Shovel/Federation runtime parameters are not generated client configurations; their TLS settings remain the administrator's responsibility.

Public AMQP routes require explicit CIDR allowlists and project IPv6/dedicated IPv4, targeting AMQPS except when TLS is disabled. Public native management is a separate opt-in HTTPS subdomain forwarding to HTTP 15672 in the private project, available only with `preferTLS`/`disabled`. `requireTLS` rejects `managementUI.public: true` and never enables that plaintext listener: infrastructure does not offer certificate-verified HTTPS backend routing. Native public management exposes UI **and API**, uses RabbitMQ authentication rather than Karakoram sessions/RBAC, and is not covered by the AMQP allowlist. Treat native management administrators and project-private/platform access as trusted infrastructure authority. Never expose 9443, Prometheus, epmd or distribution publicly.

RabbitMQ 4.3.5 is deprecated and accepted explicitly through the API only as an upgrade source, not selectable in the new-cluster wizard. Its creation response and warning event identify stream authorization disclosures [GHSA-6jvc-jx8w-m757](https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-6jvc-jx8w-m757) and [GHSA-342v-cj47-c9g8](https://github.com/rabbitmq/rabbitmq-server/security/advisories/GHSA-342v-cj47-c9g8), fixed in the recommended/default 4.3.6. Use 4.3.6 for new workloads.

Definitions backups contain application-user **password hashes** and potentially sensitive runtime parameters, despite containing no messages. Gzip is not encryption. Restrict S3 credentials/bucket access, use appropriate storage encryption and never publish exports in issues/logs. Export excludes `karakoram`, `admin`, `monitor`, `guest` and their vhost/topic grants before compression/upload. Restore repeats that filter for older backups to preserve target authority, but other definitions remain trusted administrative input and merge into existing state. Retained objects require the original state/key and storage configuration; retained volumes also preserve broker secrets. See [ADR 0022](adr/0022-rabbitmq-access-tls-backups.md) and the [RabbitMQ guide](user-guide/rabbitmq.md).

## Reporting vulnerabilities

Report vulnerabilities privately to **security@karakoram.dev**. Do **not** post credentials, private keys, unredacted state archives or weaponized reproduction details in a public issue.

Provide affected revision/deployment, impact/trust boundary, prerequisites and minimal redacted reproduction privately. Distinguish confirmed observation from inference. For accidental credential publication, revoke/rotate immediately and preserve only redacted evidence; removing a message alone does not revoke a secret.
