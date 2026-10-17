<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL access and TLS

This interface also applies to Supabase's PostgreSQL core. Use namespace-qualified requests and protect credential-bearing files/output. API paths below are relative to `/api/v1/clusters/{cluster}`.

## Connections

```sh
karactl -n NS clusters connection appdb
karactl -n NS clusters ca appdb > cluster-ca.pem
```

The ordinary connection summary is password-free. `clusters connection appdb --reveal` explicitly retrieves administrative credentials and CA and requires `cluster-credentials:read`; do not use the administrative identity in applications. Create a restricted login role and use the advertised proxy hostname with the CA and `sslmode=verify-full`. Backend public TCP endpoints can have a different port/address from the private listener: use returned connection details rather than inventing a route.

Public exposure is opt-in. Zerops can enforce a CIDR allowlist; experimental backends without `tcpAllowlist` require explicit allow-all acknowledgement and an external firewall policy. Only the selected 5432 client service is managed publicly. Disabling the proxy is allowed only in single topology, where that sole data member becomes the client service. Management relay connectivity is not application ingress.

Public endpoint removals and allowlist restrictions are applied independently of database-member health or rolling convergence. New endpoints and allowlist expansions wait for readiness. A mixed allowlist edit first retains only the intersection of the old and desired networks; if that intersection is empty the endpoint is removed until the cluster is ready.

## Roles

`/postgres/roles` supports GET/POST; `/postgres/roles/{role}` GET/PUT/PATCH/DELETE; POST `/postgres/roles/{role}/password` generates a replacement password. CLI examples:

```sh
karactl -n NS postgres roles create appdb -f role.json
karactl -n NS postgres roles list appdb
karactl -n NS postgres roles get appdb appuser
karactl -n NS postgres roles patch appdb appuser -f role-patch.json
karactl -n NS postgres roles rotate-password appdb appuser --yes
```

A non-secret `role.json` can request a generated password:

```json
{"name":"appuser","login":true,"createdb":false,"createrole":false,"connectionLimit":20,"memberOf":[]}
```

Creation without a supplied login password returns `generatedPassword` once; rotation also returns a newly generated password. Responses are `no-store`; authorized idempotent retries replay the sealed result. Role reads do not reveal stored passwords. `validUntil` accepts an RFC3339 timestamp. PUT replaces attributes; PATCH changes only supplied fields. Optional `password` belongs only in a protected request document, never command arguments or source control.

Permissions use the existing `database-users` RBAC resource. Setting `superuser`, `replication` or `bypassrls`, privileged memberships, and modifying an existing privileged role/password additionally require `clusters:update`. Engine system roles are hidden and immutable; Supabase protects its service identities too. Permitted predefined memberships include `pg_read_all_data`, `pg_write_all_data`, `pg_monitor`, `pg_read_all_stats` and `pg_signal_backend`.

Managed login passwords are sealed and loaded into all proxies because ProxySQL needs recoverable passwords. SQL-created roles/password changes are not automatically imported; manage proxy-facing identities through this API. Access mutations use a freshly resolved writable primary, and HA actions require Patroni's leader lock. No current primary returns unavailable rather than directing writes to a replica. This is direct access administration, not continuous reconciliation of arbitrary SQL changes.

Credential DDL carries SCRAM-SHA-256 verifiers, never plaintext passwords. System/Supabase bootstrap derives verifiers in the agent; the application-role API derives them before forwarding to the agent. Dedicated PostgreSQL management connections use `search_path=pg_catalog` and disable statement, duration, sample and error-statement/parameter logging before credential SQL. Database logging options still apply to application sessions.

## Databases and grants

`/postgres/databases` supports GET/POST; `/postgres/databases/{database}` GET/DELETE, with `force=true` for forced deletion. There is no public database-update route. Create fields are `name`, `owner`, `encoding` (default UTF8), `locale` (default C.UTF-8) and `allowConnections` (default true). Encoding/locale are creation-only.

```json
{"name":"app","owner":"appuser","encoding":"UTF8","locale":"C.UTF-8","allowConnections":true}
```

```sh
karactl -n NS postgres databases create appdb -f database.json
karactl -n NS postgres grants grant appdb -f grant.json
karactl -n NS postgres grants list appdb --database app
karactl -n NS postgres databases delete appdb app --force --yes
```

`/postgres/grants` supports GET with `database`, POST to grant, DELETE with a body to revoke. A grant document contains `role`, `database`, `privileges`, and optional `schema`:

```json
{"role":"appuser","database":"app","schema":"public","privileges":["USAGE","CREATE"]}
```

Database privileges are CONNECT/CREATE/TEMP; schema privileges are USAGE/CREATE. This is not table/sequence/function ACL management or an arbitrary SQL endpoint. Role/database deletion, grant revocation and password rotation require CLI confirmation or `--yes`.

Both control plane and agent reject CREATE (and any unsupported write privilege) on PostgreSQL system schemas and engine-managed namespaces: `pg_*`, `information_schema`, `_karakoram*`, Supabase's `auth`, `storage`, `realtime`, `_realtime`, `extensions`, `graphql*`, `supabase_*`, `vault`, `pgsodium`, `pgsodium_masks`, `net`, `cron`, `pgbouncer`, `_supavisor` and `_analytics`. USAGE remains available; revoking historical CREATE grants remains allowed. Use an application-owned schema for object creation.

When upgrading an existing installation, audit schema ACLs and objects in these namespaces in every database, including `postgres`: a previously issued CREATE grant or malicious overload is not removed automatically. Revoke unauthorized CREATE grants (including inherited/PUBLIC grants), inspect ownership and function definitions, and remove unauthorized objects before resuming privileged management. Grant listing includes system-schema ACLs so existing grants remain visible for revocation.

## TLS

The default `requireTLS` requires encrypted client authentication; `preferTLS` allows encrypted and plaintext clients; `disabled` disables proxy frontend TLS but does not disable internal etcd/Patroni/replication protection. PostgreSQL data-member TLS remains available for private/internal use. CA sources are `selfSigned` and `userProvided`; the latter requires the CA certificate/signing key and a suitable server certificate/key through the shared TLS inputs. Supplied certificates must cover required member names, loopback and exposed addresses, and permit server/client authentication. Keep private keys out of options and normal request logs.

Proxy-to-PostgreSQL traffic and ProxySQL's backend monitor require TLS and use the cluster CA through [`pgsql-ssl_p2s_ca`](https://proxysql.com/documentation/global-variables/pgsql-variables/#pgsql-ssl_p2s_ca). Missing or malformed CA material is rejected before applying a proxy configuration; an unrelated backend certificate is rejected by libpq's CA-chain verification. Proxies restart when adopting this trust setting so previously unverified pooled connections are not retained.

**Backend hostname verification limitation:** ProxySQL 3.0.11 uses libpq `sslmode=require` with an explicit root certificate for both [query](https://github.com/sysown/proxysql/blob/v3.0.11/lib/PgSQL_Connection.cpp) and [monitor](https://github.com/sysown/proxysql/blob/v3.0.11/lib/PgSQL_Monitor.cpp) connections. With the CA supplied, this authenticates the certificate chain, but it does **not** compare the certificate's names to the configured backend hostname; it is not `verify-full`. Keep the cluster CA/signing key private, use a dedicated CA per cluster, and restrict private backend network access. Client-to-proxy `sslmode=verify-full` and direct PostgreSQL connections are separate TLS legs.

Generated certificates are staged for renewal within 30 days of expiry and activated member-by-member. If the control plane restarts after staging certificates but before recording their activation state, reconciliation recovers the pending material and publishes a rolling configuration revision before activation. Supplied certificates must be rotated by the operator; expiry warnings do not create replacement user certificates. The supplied CA cannot be replaced in place: no trust-overlap CA rollout is implemented. Reloadable HBA/TLS changes preserve existing SQL sessions; postmaster changes or proxy listener/certificate activation can still interrupt connections. See [operations](operations.md).

## Proxy compatibility

ProxySQL 3.0.11 is a PostgreSQL-aware proxy, not a transparent TCP tunnel. The upstream 3.0 series Stable Tier label is not a PostgreSQL-specific GA or complete protocol guarantee.

- LISTEN/NOTIFY forwarding, named portals, Flush semantics, incremental Execute row limits and extended COPY FROM STDIN are unsupported.
- Mixed result formats per column are unsupported. For pgx, select simple protocol or uniform text results where required.
- Optional read/write splitting uses SELECT-pattern rules (including a FOR UPDATE exception), not full SQL parsing; read-after-write consistency and every read-with-side-effect pattern are not guaranteed.
- Failover changes the backend writer; clients must reconnect/retry according to application transaction semantics.

Supabase internal services bypass these limitations using their local raw TCP leader forwarder. This does not change the public application's proxy protocol contract. See [ADR 0034](../adr/0034-postgres-engine.md).
