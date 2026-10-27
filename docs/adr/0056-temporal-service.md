# 0056 — Temporal as an application service on bound PostgreSQL

Status: Accepted

## Context

[Temporal](https://temporal.io) is a workflow service, not a database. Its server is four stateless roles (frontend, history, matching, internal worker) whose durable state lives entirely in a persistence store and a visibility store. Running it well needs the database lifecycle Karakoram already has for [PostgreSQL](https://www.postgresql.org) (HA, [pgBackRest](https://pgbackrest.org) backups, PITR, restore, upgrades) plus Temporal-specific concerns: versioned schema migrations, an immutable history shard count, internode and client TLS, namespaces, a Web UI, independent role scaling and strictly sequential server upgrades. Duplicating PostgreSQL lifecycle inside a Temporal provider would create a second, weaker database implementation.

## Decision

### Provider, persistence and versions

Add a `temporal` provider. `spec.version` is the Temporal server version. Every admitted version is a locked [native artifact](0036-native-artifacts.md) (official `temporal_<version>_linux_amd64.tar.gz`, which also contains `temporal-sql-tool` with embedded PostgreSQL schemas). The Web UI uses the locked official `ui-server` release; its version follows the Karakoram release lock.

Persistence and visibility always live in a Karakoram PostgreSQL cluster referenced through the generic PostgreSQL binding (ADR 0045): `engineOptions.persistence.postgres` names the source cluster (namespace optional, defaults to the Temporal cluster's namespace). The binding creates two databases on the **same** source (persistence and SQL visibility, default names `temporal_<cluster>` and `temporal_<cluster>_visibility`) and one least-privilege login role owning both. One source cluster means one backup or point in time covers both stores consistently. Temporal uses the `postgres12_pgx` plugin with TLS `verify-full` against the binding's CA and the binding's stable writer endpoint. The binding admits PostgreSQL major ≥ 12. There is no embedded database mode: a provider-owned PostgreSQL would either duplicate HA/backup/upgrade logic or require providers to create clusters, which the provider contract does not allow. If the build has no binding service, admission rejects Temporal clusters with an explicit reason.

The standard server constructs one PostgreSQL binding manager and shares that same service with the bindings API, [Keycloak](https://www.keycloak.org), [OpenBao](https://openbao.org) and Temporal. Embedders that omit the service still receive admission rejection; consumers use the single contract in `internal/pgbinding`.

Bound credentials are never copied into options or status. The control plane resolves the password in-process when publishing member configuration. A change of the binding's credential generation re-renders configuration and rolls every role.

### Roles as components

Temporal roles are stateless [components](0031-engine-component-model.md) in `spec.components.extra`, ordered as `history`, `matching`, `frontend`, `worker`, `server`, `ui`.

- Topology `combined`: `server` (1–5) runs all four roles in one process per member; role components must be absent or zero.
- Topology `split`: `history` (1–9), `matching` (1–5), `frontend` (1–5) and `worker` (1–3) each run one role; `server` must be zero.
- `ui` (0–3, default 1) runs the Web UI in both topologies.

Each component is one backend service; replicas are containers scaled through backend horizontal scaling. Roles find each other through Temporal's membership ([ringpop](https://github.com/uber/ringpop-go)) via the persistence store; each instance advertises the private IPv4 address it uses to reach the bound PostgreSQL writer, not its shared service DNS identity, and fixed per-role ports ([gRPC](https://grpc.io)/membership: frontend 7233/6933 plus HTTP API 7243, history 7234/6934, matching 7235/6935, worker 7239/6939). The internal worker reaches frontends through membership, not a load balancer. Clients and the UI reach the frontend component's private service address.

`engineOptions.historyShards` (default 512, 1–16384) is fixed at creation and immutable: changing it would orphan existing workflow state. The Temporal cluster name (`clusterMetadata.currentClusterName`) is the Karakoram cluster name and never changes. Multi-cluster replication and global namespaces are out of scope.

### Schema

Every server member ensures the schema for its own binary before starting the server: under a PostgreSQL advisory lock on the persistence database it runs `temporal-sql-tool setup-schema -v 0.0` and `update-schema --schema-name postgresql/v12/{temporal,visibility}` against both databases as the bound owner role. Both commands are idempotent, so concurrent or repeated starts converge; the lock prevents concurrent migrations. The server's own startup compatibility check refuses a schema newer than it supports. Members report observed schema versions; status shows them.

### TLS

The provider issues a cluster CA (`internal/pki`). Internode traffic is always mutual TLS with per-component certificates and a fixed internode server name. The frontend always serves TLS; `engineOptions.frontend.requireClientAuth` (default **true**) requires client certificates. Client certificates for workers/SDK clients are issued by a dedicated API (`POST /clusters/{cluster}/temporal/client-certificates`) that returns key and certificate once; Karakoram keeps only the serial, subject, fingerprint and expiry. Temporal has no CRL support: revocation is a CA rotation (`rotate-tls`), which invalidates all issued client certificates. Server authorization stays Temporal's no-op authorizer (`--allow-no-auth`): a valid client certificate is full access. This is documented, not hidden.

CA rotation is a checkpointed three-phase trust roll across all enabled components in roll order: first distribute old+new trust with old identities, then issue new identities while trusting both roots, then prune the old root. Old client certificates cease to authenticate after pruning; clients must receive replacement certificates and trust. Per-certificate revocation and per-namespace authorization are not supported.

### Namespaces

`engineOptions.namespaces` declares namespaces (`name`, `retention`, optional `description`); `default` (72h retention) is always ensured. Sync creates missing namespaces and updates retention/nonempty descriptions through a frontend member's local HTTPS API using the internode client identity. Temporal treats an empty description update as unchanged, so descriptions cannot be cleared through this API. Removing a namespace from options does not delete it. Deletion is an explicit API call (`DELETE /clusters/{cluster}/temporal/namespaces/{temporalNamespace}`), delegated to the frontend gRPC operator API because the HTTP gateway has no delete binding. Temporal deletes workflows asynchronously. Deletion refuses `default`, `temporal-system` and namespaces still declared in options.

### Web UI

The `ui` component runs the native `ui-server` against the frontend with a client certificate. It is private by default, without UI login authentication. Public HTTPS (`engineOptions.ui.publicHttps`) requires enabled UI replicas, backend subdomain support and [OIDC](https://openid.net/developers/how-connect-works/) configuration (`engineOptions.ui.oidc` issuer/client ID, client secret write-only and sealed). OIDC is supported only together with public HTTPS, which supplies the `/auth/sso/callback` origin. Options reject public exposure without OIDC; configuration publication requires the sealed secret before public routing is enabled. The frontend remains private regardless of `requireClientAuth`.

The `karactl temporal` commands expose this access surface:

- `client-certificate issue <cluster> --common-name worker [--ttl 720h] (--out-dir DIR | --stdout)` issues a certificate. Lifetimes are [Go](https://go.dev) durations rounded to the nearest whole hour. Directory output creates `worker.crt` and `worker.key` with mode 0600 and `ca.crt` with mode 0644, creating missing directories with mode 0700. All three paths are checked before writing; existing files are never overwritten. `--stdout` emits the certificate, private key and CA PEM bundle; protect redirected output. Serial, expiry, frontend address and TLS server name go to stderr.
- `client-certificate list <cluster>` lists certificate metadata, shortening the CA fingerprint only in table output. `-o json` and `-o yaml` retain the complete response.
- `namespace list <cluster>` lists workflow namespaces; `namespace delete <cluster> <name> [--yes]` requires confirmation because it deletes workflows.
- `ui-oidc-secret set <cluster> (--from-file F | --from-stdin)` reads the write-only secret without accepting an argv secret value, removes one trailing LF or CRLF, and rejects an empty result. It never prints the secret.

All commands accept the usual project `--namespace` flag; it is distinct from a Temporal workflow namespace.

### Upgrades and restarts

Upgrades follow `UpgradePaths`: any patch within a minor, or exactly one minor forward; skipping a minor and downgrades are rejected. The upgrade is a checkpointed operation: verify every role is Ready at the current version, then roll components one at a time in the order history → matching → frontend → worker → server (combined) → ui. The first upgraded member migrates the schema before its server starts. After each component the operation waits for every instance to be Ready at the new version and, for history, a settle period for shard ownership to move. Rolling restart uses the same order. Scale and resource changes are per-component and do not touch other roles.

The current locked catalog is 1.31.4 and 1.32.1 (default), with 1.31.4 → 1.32.1 as the advertised path; both embed persistence schema 1.19 and visibility schema 1.14. The Web UI is locked at 2.56.0. Topology, persistence options and history shard count are immutable after creation.

### Backup and restore

Temporal has no snapshot tool of its own; a Temporal backup is the source PostgreSQL backup plus a configuration manifest. The `backup` operation stores a non-secret manifest (Temporal version, schema versions, history shard count, cluster name, namespace options, source/database names and CA fingerprint) in Temporal cluster state, keyed by Temporal backup name, then requests a source backup through the binding's optional backup extension and waits for it. The binding backup ID is the generic backup record's `location`; backup records have no engine-metadata field. `delete-backup` removes only the Temporal inventory record and manifest, not source PostgreSQL archives, whose retention remains source-owned.

`restore` requires a complete manifest, the same shard count and cluster name, the original PostgreSQL source and database names, and the same server version or a supported forward upgrade path from the recorded version. Timestamp PITR is not accepted. A shared source is refused unless the restore request explicitly sets `acknowledgeSharedSource: true`; an in-place source restore rolls back every consumer. The operation stops all roles, restores through the binding, then starts components in upgrade order, with schema ensured before each server starts and namespaces synchronized afterward. Workflow replay compatibility of user workers remains the user's responsibility. Preserve Temporal cluster state as well as source archives: the manifest is not stored inside the archive.

### Deletion

Delete removes roles in reverse order, then releases the binding: plain delete drops the bound databases and role on the source; deletion that retains volumes keeps the databases and disables the role. A bound source cannot be deleted while the Temporal cluster exists.

## Consequences

Temporal gets PostgreSQL HA, backups, PITR and upgrades from the existing engine without a second implementation. A Temporal cluster always needs a PostgreSQL cluster first. Shared sources save resources but couple restores and major upgrades; status and UI show the consumer count. The client-certificate model is simple but coarse (no per-namespace authorization, revocation by CA rotation).

## Alternatives considered

- **Embed the PostgreSQL core (as [Supabase](https://supabase.com) does).** Rejected: the core reads `spec.version` as the PostgreSQL version throughout, while Temporal needs `spec.version` for its own sequential upgrades.
- **Provider-owned child PostgreSQL cluster.** Rejected for now: providers have read-only cluster access; cross-cluster creation belongs to the binding layer if ever needed.
- **Single embedded PostgreSQL member without HA/backups.** Rejected: a second, weaker database lifecycle that cannot meet the backup contract.
- **[Cassandra](https://cassandra.apache.org) or [Elasticsearch](https://www.elastic.co/elasticsearch) visibility.** Rejected: new dependencies; SQL visibility on PostgreSQL 12+ provides advanced visibility.
- **Control-plane-driven schema step on a dedicated member.** Rejected: old members lack the new tool during upgrades; per-member idempotent migration under an advisory lock is simpler and ordered by the rolling upgrade.

## References

- Temporal server deployment and upgrade guides: <https://docs.temporal.io/self-hosted-guide>, <https://docs.temporal.io/self-hosted-guide/upgrade-server>
- Temporal license (MIT): <https://github.com/temporalio/temporal/blob/main/LICENSE>
