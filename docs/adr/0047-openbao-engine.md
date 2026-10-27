# 0047 — OpenBao engine: Raft membership, Karakoram-held unseal material, snapshots and database credentials

Status: Accepted

## Context

[OpenBao](https://openbao.org) is an MPL-2.0 secrets manager. Unlike the existing database engines, a freshly started OpenBao server is neither usable nor safe to reason about until it is initialized once per cluster and unsealed after every process start. Initialization returns the only copies of the Shamir unseal shares (or recovery shares with auto-unseal) and the initial root token; losing that response makes the cluster unrecoverable. Integrated Raft storage gives OpenBao its own quorum, peer set and snapshot API, so Karakoram must coordinate Raft membership the way it coordinates [RabbitMQ](https://www.rabbitmq.com) metadata membership (ADR 0021): add before remove, never shrink below a live majority, never let two seeds initialize.

The member agent caches its full `MemberConfig` in plaintext on the member volume (`DATA_DIR/karakoram/config.json`). Unseal shares therefore must not enter `EngineSpec`, operation checkpoints, `Cluster.Internal`, status, runtime environment, logs or receipts.

Ranked drivers: (1) no loss of initialization material and no plaintext unseal material on members; (2) Raft safety on scale, replacement, upgrade and deletion; (3) automatic recovery after process restarts (re-unseal) without operator action; (4) reuse of the shared runtime/ownership/backpressure machinery (ADRs 0030, 0031, 0036, 0037); (5) opt-in integrations that do not make the secrets store depend on the systems it issues credentials for.

## Decision

### Engine, artifacts and runtime

- Provider name `openbao`, topology `cluster`, single `engine` component with 1, 3 or 5 replicas. Catalog version `2.7.1` (default). Forward-only version changes; downgrades are rejected.
- Binaries come from the native artifact lock (ADR 0036): the official `openbao_<v>_linux_amd64.tar.gz` (installed under `/opt/karakoram/openbao-<v>/bao`) and the official `openbao-plugins` releases (made executable, installed under one plugin directory): `openbao-plugin-database-mongodb` on every member, so that the first [MongoDB](https://www.mongodb.com) connection needs no rolling redeploy, and `openbao-plugin-kms-aws` only for the `awskms` seal, which is fixed at creation. No apt repository, no OCI plugin download at runtime.
- Each member runs one supervised process `bao server` as the unprivileged runtime user. Data lives in `DATA_DIR/openbao` (Raft `raft.db`/`vault.db`), which the agent's native-volume recognizer treats as initialized native data (ADR 0037).
- Listeners: public API `:8200` (TLS, cluster CA leaf with member hostname and client service SANs, TLS 1.2+), Raft/forwarding `:8201` (OpenBao-managed cluster TLS), and a loopback-only `127.0.0.1:8210` TLS listener with unauthenticated [Prometheus](https://prometheus.io) metrics for the agent. TLS is mandatory (`requireTLS`); `disabled` is rejected. Certificate rotation rewrites files and sends `SIGHUP` (OpenBao reloads listener certificates) instead of restarting a sealed-on-start process.
- Provider connection details contain HTTPS endpoints and the cluster CA only: no management or root credential is published as a system user. Member configuration contains listener TLS material but never initialization shares or management/root tokens. Native status maps active members to `primary`, unsealed standbys to `secondary`, and all other observations to `unknown`. [Nomad](https://developer.hashicorp.com/nomad) retains its conservative 3072 MiB disposable runtime-disk budget for the verified OpenBao installation; Raft data lives on the separate persistent volume.
- `node_id` is the member hostname. Every member's `storage "raft"` stanza lists `retry_join` blocks for every other desired member (`https://<member>:8200`, cluster CA file). Changing only the join list never restarts a running member.
- OpenBao 2.x no longer locks memory (the `disable_mlock` setting is gone), so no memory-locking privilege is needed; swap on the host is a documented operator consideration.

### Initialization (single owner, crash-safe receipt)

- Sync persists `InitOwner = <first member>` in engine state before any initialization call. Only that member is ever initialized; other members are never initialized and only become cluster members by being unsealed with the cluster's keys, which triggers `retry_join`.
- The control plane generates an ephemeral X25519 key pair per initialization attempt, stores the private key as a cluster secret, and calls the agent action `POST /v1/openbao/init` with the public key, share count and threshold. The agent calls the local `sys/init`, encrypts the complete response (X25519 ECDH → HKDF-SHA256 → AES-256-GCM, cluster/member bound AAD) and writes only that ciphertext to `DATA_DIR/karakoram/openbao/init-receipt.enc` (0600) before returning it. A retried request with the same receipt identity returns the stored ciphertext; a node that is initialized without a receipt fails closed (`InitializationOutcomeLost`).
- The control plane decrypts the receipt, stores the material sealed in the Karakoram secret store (`openbao/unseal`: shares, threshold, seal type, initial root token), creates a `karakoram-manager` policy and a periodic orphan management token (`openbao/management-token`), then acknowledges (`POST /v1/openbao/init-ack`), which deletes the receipt and the ephemeral private key. Defaults: 5 shares, threshold 3 (`engineOptions.unseal.shares/threshold`, immutable after creation).
- Recovery of control-plane access: when the management token is missing, expired or revoked (for example after restoring an older snapshot), the control plane runs `sys/generate-root` with its stored shares (or recovery shares), creates a new management token and revokes the generated root token.
- Shares-only recovery must also work when every native token has expired or disappeared in a snapshot. OpenBao 2.7.1 disables legacy unauthenticated `sys/generate-root/*` by default; its replacement `sys/generate-root-token/*` requires an existing native token and cannot satisfy that recovery contract. Karakoram therefore keeps the deprecated workflow available **only on a dedicated private TLS listener at 8202**, requiring a control-plane client certificate signed by an independent, per-destination-cluster recovery CA. The API and metrics listeners explicitly keep legacy generation disabled; legacy rekey is not enabled. The control plane verifies the ordinary server cluster CA and hostname as well as presenting its recovery identity. CA signing material and the client certificate/private key remain sealed in the control-plane SecretStore; members receive only the public recovery CA. Ordinary member certificates and another cluster's recovery identity cannot authenticate. The namespace relay may carry this opaque mTLS connection through an exact private target allowlist, but 8202 is never a public route, customer connection endpoint or generic service-proxy target. There is no fallback to 8200. This listener is permanent so unattended expiry recovery remains possible: compromise of its control-plane identity permits unauthenticated attempt initiation/cancellation (including cancellation denial of service), while generation still requires the native share threshold. Protect installation secrets and private management connectivity accordingly. See [upstream deprecation](https://openbao.org/community/deprecation/unauthed-generate-root) and [listener configuration](https://openbao.org/docs/configuration/listener/tcp/).

### Unseal

- Default seal is Shamir. The control plane reads the stored shares and unseals sealed members directly over the member API (`PUT sys/unseal` through the namespace dialer and the cluster CA), threshold shares only; the agent never receives shares. Sync detects sealed members via `sys/seal-status` every pass (30 s when converged), so a crashed and restarted member is re-unsealed without operator action. Status publishes `Sealed` and `Initialized` per member and a `Sealed` cluster condition.
- Optional auto-unseal, chosen at creation and immutable: `transit` (built-in; address, mount, key name, optional CA; token supplied write-only) or `awskms` (KMS plugin; region, key ID, optional endpoint; access key ID and secret access key supplied write-only). Write-only seal credentials are extracted from `engineOptions` by an optional provider hook before the cluster is persisted and are stored as cluster secrets, never echoed. With auto-unseal, initialization uses recovery shares; Karakoram stores them like unseal shares and performs no unseal calls. Seal migration is not supported.
- Explicit reveal: `POST /clusters/{cluster}/openbao/unseal-keys/reveal` with `{"confirm": "<cluster name>"}` returns shares, threshold, seal type and the initial root token with `Cache-Control: no-store`. RBAC resource `openbao-unseal-keys`, action `reveal` (administrators only by default), always audited as `OpenBaoUnsealKeysRevealed` without the response body. CLI `karactl openbao unseal-keys reveal`; UI reveal fetches on click and keeps nothing in the query cache.

### Raft membership

- Formation: deploy members (shared admission/backpressure), initialize the owner, unseal it, wait for it to be active, then unseal each further member one at a time and wait until autopilot reports it as a healthy voter before the next.
- Scale-up: allocate, deploy, unseal, wait for healthy voter. Scale-down (5→3→1): highest index first; step down if it is the leader; `remove-peer` through the active node; verify the Raft configuration no longer lists it; only then stop and delete runtime and volume. A removal that would leave fewer live voters than a majority of the remaining configuration is refused with a `QuorumBlocked` condition.
- Replace member: join the new member as a voter first, then remove the old peer (dead peers are removable by `node_id` while quorum holds), then delete. Lost-quorum recovery (`peers.json`) stays manual and documented.
- Autopilot `cleanup_dead_servers` stays disabled: Karakoram is the single writer of membership.

### Upgrades, restarts, TLS

Version changes, resource changes, restarts and restart-requiring configuration changes (process-generation bumps, for example new database-connection trust anchors) are rolling: standbys first, one at a time (redeploy for a new binary or resize, re-unseal, wait healthy voter and autopilot healthy), then `sys/step-down` on the active node and the same steps for it. At most one member is disrupted at a time. TLS certificate rotation also proceeds one member at a time but needs no restart: the member reloads its listener certificates on `SIGHUP`, so it stays unsealed.

### Snapshots and restore

Backup type `raft-snapshot`: the control plane streams `GET sys/storage/raft/snapshot` from a healthy member to [S3](https://aws.amazon.com/s3/) at `<prefix>/<clusterUID>/raft/<backup>.snap` (bounded, checkpointed, HEAD recovery as for other engines) and supports schedules, retention and retained-backup deletion. Restore into the same cluster posts the snapshot to the active node (`sys/storage/raft/snapshot`); unseal material is unchanged, and the management token is regenerated if the snapshot predates it. Restore or clone from another Karakoram OpenBao cluster requires that source's stored unseal material: the control plane copies it to the target before `snapshot-force` and re-unseals every member. Snapshots of deleted clusters can be restored only by supplying the unseal shares in the restore request; Karakoram never infers keys. No point-in-time recovery.

### Opt-in PostgreSQL storage

`engineOptions.storage.type = "postgresql"` with a reference to a Karakoram [PostgreSQL](https://www.postgresql.org) cluster uses the Postgres binding seam (`internal/pgbinding`, ADR 0045): one database and a least-privilege role; OpenBao runs `storage "postgresql"` with `ha_enabled`. Members are then stateless HA peers (no Raft membership work, no Raft snapshots; backups belong to the bound PostgreSQL cluster and `raft-snapshot` schedules are rejected). The binding is released on deletion (`keepData` follows `keepVolumes`). A build without a binding service rejects the option at admission.

Server provider composition accepts this dependency as `internal/server.engineOptions.Bindings` and passes it unchanged to `openbao.ProviderOptions.Bindings`; the provider never instantiates or copies the concrete binding manager. The standard server constructs one PostgreSQL binding manager and shares that instance with the bindings API, [Keycloak](https://www.keycloak.org), OpenBao and [Temporal](https://temporal.io). `Describe().Capabilities` includes `postgresql-storage` only when the dependency is present, which gates the UI storage choice; a composition without it rejects PostgreSQL storage.

### Opt-in database credentials

`/clusters/{cluster}/openbao/database-connections` creates, lists and deletes connections from this OpenBao cluster to a Karakoram PostgreSQL, MongoDB ([PSMDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) replica set) or RabbitMQ cluster in the same namespace (members must reach the target's private endpoints). Admission requires `database-users:create` on the target in addition to `clusters:update` on the OpenBao cluster. Karakoram creates a dedicated administrative identity on the target through the target engine's own access API (PostgreSQL: `LOGIN CREATEROLE` with admin option on `pg_read_all_data`/`pg_write_all_data`; MongoDB: `userAdminAnyDatabase`; RabbitMQ: `administrator` tag), stores it sealed, mounts and configures `database/` (PostgreSQL plugin, MongoDB plugin) or `rabbitmq-<name>/` (RabbitMQ secrets engine), writes the requested roles (`readonly`/`readwrite` templates with TTLs), and rotates the root credential where the plugin supports it (PostgreSQL, MongoDB) so that only OpenBao knows the live password. The target CA is published to members and added to the OpenBao process trust (`SSL_CERT_DIR`). Deleting a connection removes the OpenBao configuration, then the target identity; deleting the target cluster is refused while connections exist.

The dedicated PostgreSQL identity uses the unreserved `karakoram_openbao_` prefix
(the PostgreSQL access API reserves `kk_`), `LOGIN CREATEROLE`, and membership
**with ADMIN OPTION** in `pg_read_all_data` and `pg_write_all_data`. It receives
no superuser, database-creation, replication or row-security-bypass privilege.
Dynamic readonly roles inherit `pg_read_all_data`; readwrite roles inherit both
predefined roles. MongoDB uses `userAdminAnyDatabase` on `admin`; RabbitMQ uses
the `administrator` tag and full permissions only on the requested vhosts.
Pending connections publish target CAs before configuring plugins, and wait for
the serialized process-trust rolling restart to finish. Connection credentials
remain sealed separately from the secret-free index; PostgreSQL/MongoDB root
rotation leaves the retained bootstrap password stale by design.
RabbitMQ's native secrets engine configures lease TTLs per mount rather than per
role. Every role in a RabbitMQ connection must therefore request the same default
and maximum TTL pair; differing pairs are rejected before any side effects.


### Observability

The agent scrapes the loopback metrics listener and republishes a fixed vocabulary (seal state, active/standby, Raft commit/apply indexes and leader contact, autopilot health and failure tolerance, request/lease counts, runtime memory); the provider exposes the matching `MetricsCatalog`. Native status per member: initialized, sealed, active, Raft voter, leader address, version.

## Consequences

- The Karakoram secret store becomes the custodian of unseal and management material; installation master-key loss (ADR 0012) also loses the ability to unseal. Operators are told to reveal and store the shares offline.
- A sealed cluster cannot serve until the control plane is reachable (Shamir); auto-unseal removes that dependency at the cost of an external KMS or transit dependency.
- Initialization needs a new agent action and an encrypted on-member receipt; everything else talks to the OpenBao API from the control plane, so members never hold shares.
- Lost-quorum recovery, seal migration, performance/DR replication, namespaces and HSM seals are out of scope.

## Alternatives rejected

- Initialize from the control plane directly over the API: a lost response (relay drop, control-plane crash) permanently loses the shares; the encrypted agent receipt closes that window.
- Ship unseal shares in `MemberConfig` and let agents unseal locally: the agent's config cache would hold them in plaintext.
- Autopilot dead-server cleanup: competes with Karakoram's membership decisions and can shrink quorum during platform outages.
- PostgreSQL storage by default: couples the secrets store to the database it issues credentials for.
