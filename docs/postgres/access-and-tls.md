<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL access and TLS

This interface also applies to [Supabase](https://supabase.com/)'s [PostgreSQL](https://www.postgresql.org/) core. Use namespace-qualified requests and protect credential-bearing files/output. API paths below are relative to `/api/v1/clusters/{cluster}`.

## Connections

```sh
karactl -n NS clusters connection appdb
karactl -n NS clusters ca appdb > cluster-ca.pem
```

The ordinary connection summary is password-free. `clusters connection appdb --reveal` explicitly retrieves administrative credentials and CA and requires `cluster-credentials:read`; do not use the administrative identity in applications. Create a restricted login role and use the advertised proxy hostname with the CA and `sslmode=verify-full`. Backend public TCP endpoints can have a different port/address from the private listener: use returned connection details rather than inventing a route.

The managed [Data MCP companion](../data-mcp.md) instead connects directly to non-removing [PostgreSQL](https://www.postgresql.org/) members on private port 5432, bypassing [ProxySQL](https://proxysql.com/) and (for [Supabase](https://supabase.com/)) [Supavisor](https://github.com/supabase/supavisor). Its multi-host URI uses `target_session_attrs=prefer-standby` for reads and `read-write` for the primary-only write identity, with hostname-verified TLS and the cluster CA when TLS is enabled. This avoids proxy extended-protocol limitations and does not require the optional proxy reader port.

Public exposure is opt-in. [Zerops](https://zerops.io/) can enforce a CIDR allowlist; backends without `tcpAllowlist` require explicit allow-all acknowledgement and an external firewall policy. Only the selected 5432 client service is managed publicly. Disabling the proxy is allowed only in single topology, where that sole data member becomes the client service. Management relay connectivity is not application ingress.

On [Dokploy](https://dokploy.com/), the public PostgreSQL port is fixed at **5432/tcp** and Swarm ingress
ports are Swarm-wide, so only one service on that Swarm can publish it. A
conflicting enable fails promptly with `PublicPortInUse`, leaving the private
endpoint intact; disable the other service's publication or keep this cluster
private. Karakoram does not silently allocate another PostgreSQL public port.

Public endpoint removals and allowlist restrictions are applied independently of database-member health or rolling convergence. New endpoints and allowlist expansions wait for readiness. A mixed allowlist edit first retains only the intersection of the old and desired networks; if that intersection is empty the endpoint is removed until the cluster is ready.

### Planned proxy drain and external load balancers

On graceful agent termination (including a backend replica-count reduction),
healthy ProxySQL instances execute `PROXYSQL PAUSE` before their native process
is stopped. This closes the PostgreSQL listeners while leaving established
sessions running. The agent waits at least **10 seconds** for external TCP
health checks to remove the instance, then waits for client sessions to disappear,
up to **25 seconds total**, including preparation and the native PAUSE
acknowledgement. Only the initial writer-health query has a separate three-second
limit: listener removal can wait for native workers longer than that. The
10-second detection wait begins after PAUSE acknowledges, and is truncated by
the total deadline if necessary. Sessions still open at the
deadline are terminated; pool owners must reconnect. Keep transactions shorter
than this window if they must complete across a planned reduction. A graceful
stop of the last proxy still removes the only route; redundancy is required.

This contract requires a **drain-capable agent binary in the allocation**.
Updating the control plane does not replace existing agent binaries, and a
count-only scale-up reuses the artifact registered in the existing runtime job.
A newly created allocation is therefore not proof of a new agent release.
Before relying on the contract or testing it, record the allocation's actual
agent version and binary/artifact digest; deploy the intended artifact through
the normal rollout workflow, or create a fresh cluster with that artifact.
Drain-capable agents log PAUSE acknowledgement, changes in the connected-client
count, skipped-drain reasons, and completion duration/error. A deadline/error
is not successful session exhaustion. The count includes open-transaction and
idle frontend sessions; a transaction COMMIT alone does not close a pooled
client session. Such sessions may keep the drain open until the bound.

An external LB must detect listener failure within **10 seconds**, retry a
**failed TCP connect on a different healthy backend**, and preserve established
sessions when a backend becomes DOWN. TCP checks alone do not prevent errors in
the check-detection race. Do not replay PostgreSQL queries/transactions at the LB
or enable `on-marked-down shutdown-sessions`. For example, an HAProxy TCP backend:

```haproxy
backend postgres
    mode tcp
    timeout connect 1s
    retries 3
    option redispatch 1
    default-server inter 1s fastinter 1s downinter 1s fall 2 rise 2
    server node-a 192.0.2.11:5432 check
    server node-b 192.0.2.12:5432 check
```

Use actual eligible node addresses and retain the deployment's client/server
timeouts. The example retries transport establishment only, never SQL. Clients
must still reconnect after idle/persistent sessions reach the drain deadline or
after an unplanned failure. An already non-listening or writer-unhealthy proxy
does not wait out the drain window. Crashes and SIGKILL bypass the graceful hook;
there is no added failover delay. [Nomad](https://developer.hashicorp.com/nomad) renders a 120-second task kill timeout,
but its client's [`max_kill_timeout`](https://developer.hashicorp.com/nomad/docs/configuration/client#max_kill_timeout)
defaults to **30 seconds** and caps that request. The 25-second proxy hook leaves
five seconds for ordinary native exit under that default; a stuck native process
may still be forcibly killed by the platform. Raising the client limit alone
does not extend the agent's 25-second drain: a longer drain would require both
an agent-budget change and adequate client grace. Dokploy renders a 120-second
Swarm stop grace, but uses the same 25-second agent drain. Zerops uses this hook
only when its platform delivers graceful termination; Karakoram has no Zerops
stop-grace setting or published platform timing guarantee. Operators must
establish at least **30 seconds** of platform grace for the proxy drain plus
ordinary child exit (or more for slow native shutdown). Karakoram cannot extend
a platform's forced-kill deadline; the overall agent shutdown remains bounded
at 60 seconds.


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

Permissions use the existing `database-users` RBAC resource. Setting `superuser`, `replication` or `bypassrls`, privileged memberships, and modifying an existing privileged role/password additionally require `clusters:update`. Engine system roles are hidden and immutable; [Supabase](https://supabase.com/) protects its service identities too. Permitted predefined memberships include `pg_read_all_data`, `pg_write_all_data`, `pg_monitor`, `pg_read_all_stats` and `pg_signal_backend`.

In-process callers of `postgres.Access.CreateRole` / `UpdateRole` may additionally
set `Role.MemberOfAdmin` (`memberOfAdmin,omitempty` on the internal agent/journal
protocol). Every entry must also appear in `MemberOf`; the same target allowlist
applies. This is intentionally absent from the public OpenAPI and public role
responses. [OpenBao](https://openbao.org/)'s dedicated `LOGIN CREATEROLE` account uses both
`pg_read_all_data` and `pg_write_all_data` in both lists so it can delegate those
memberships to dynamic roles. Grants use `WITH ADMIN OPTION`; removing only an
admin entry uses `REVOKE ADMIN OPTION FOR`, preserving ordinary membership.
Admin-option roles (and their effective membership graph) require the same
additional administrative RBAC checks as other privileged roles. Empty admin
lists are omitted, preserving existing journal request bytes and receipt hashes.


Managed login passwords are sealed and loaded into all proxies because [ProxySQL](https://proxysql.com/) needs recoverable passwords. SQL-created roles/password changes are not automatically imported; manage proxy-facing identities through this API. Access mutations use a freshly resolved writable primary, and HA actions require [Patroni](https://patroni.readthedocs.io/)'s leader lock. No current primary returns unavailable rather than directing writes to a replica. This is direct access administration, not continuous reconciliation of arbitrary SQL changes.

**Proxy publication is asynchronous.** A role-create `201`, update/rotation
`200`, or delete `204` confirms the [PostgreSQL](https://www.postgresql.org/) mutation and managed credential
storage, not that every ProxySQL instance has applied it. Immediate proxy logins
can fail with `User not found` or still encounter the preceding password until
publication completes. Deletion likewise propagates asynchronously; it is not
a barrier that terminates existing sessions.

After the mutation returns, poll **`GET /clusters/{cluster}/status`** (with the
namespace query parameter), using this CLI command:

```sh
karactl -n NS clusters status appdb -o json
```

Wait for `conditions[]` with `type: CredentialsPublished` and `status: "True"`.
For automation with `jq` (stop or investigate on CLI errors):

```sh
while :; do
  status=$(karactl -n NS clusters status appdb -o json) || exit
  printf '%s\n' "$status" |
    jq -e 'any(.conditions[]?; .type == "CredentialsPublished" and .status == "True")' >/dev/null && break
  sleep 1
done
```

This condition requires the **current** managed access revision to have entered
the desired configuration, every live proxy instance to acknowledge that
configuration, at least the desired number of instances to be present, and
healthy native writer/user observations on all of them. The status endpoint
also fences cached success against newer access mutations. Missing/False is
not success. Do not substitute cluster `Ready`, absence of `ProxyRollout`, or
`CredentialsConsistent`: those alone can describe the configuration preceding
the mutation. Use the status endpoint above rather than a cached cluster/list
response. With proxies disabled, this publication condition is absent and
PostgreSQL applies the credential mutation directly. This barrier certifies
configuration publication, not session revocation or future availability.
While publication is in progress after a role create or delete, the cluster
briefly reports phase `Updating` with `Ready=False` and reason
`MembersStarting`; existing connections are unaffected, and the cluster returns
to `Ready` once the proxies apply the new configuration.

On receipt-capable agents, managed role writes seal a recovery intent **before**
sending credential SQL. The agent commits the role mutation and an operation
receipt containing the request digest and native role OID in one PostgreSQL
transaction. It reports `applied`, `rejected`, or `unknown`; transport failures
are never treated as proof of rollback. If the SQL response is lost or
password/index/revision persistence fails, reconciliation replays the same
selected intent, observes its receipt, and publishes the matching proxy revision.
`CredentialsConsistent=False` identifies a pending transition; restore database
and sealed-state-store availability rather than changing the password directly
in SQL. Retrying a still-pending rotation completes that intent instead of
selecting another password.

A definitive SQL rejection (for example, a nonexistent membership target)
retires the intent and restores any partially published credential metadata;
the API/recovery error identifies the rejection and ordinary reconciliation can
continue. A same-name native role without this operation's receipt, or a role
whose OID differs from the receipt after a drop/recreate, is refused as a
conflict. Recovery never converts CREATE into an unconditional name-only ALTER
and never adopts that foreign role. Successful receipt replay does not rewrite
subsequent native role edits.

During mixed-version upgrades, old agents retain the earlier direct SQL then
metadata-publication behavior: credential administration remains available, but
ambiguous response/crash recovery is **not** guaranteed for those writes.
The informational `CredentialRecovery=False` / `LegacyAgent` condition recommends
rolling the data-member agents to enable transactional receipts; it does not
make a healthy legacy cluster unready. Pre-receipt pending CREATE recovery never
adopts an existing same-name role. Journaled writes already sent to new agents
remain journaled and are not silently retried through an older agent protocol.
The journal and rollback snapshot contain credentials only in sealed state,
never in cluster status or logs; native receipts contain no password or verifier.

Credential DDL carries SCRAM-SHA-256 verifiers, never plaintext passwords. System/Supabase bootstrap derives verifiers in the agent; the application-role API derives them before forwarding to the agent. Dedicated PostgreSQL management connections use `search_path=pg_catalog` and disable statement, duration, sample and error-statement/parameter logging before credential SQL. Database logging options still apply to application sessions.

## Databases and grants

`/postgres/databases` supports GET/POST; `/postgres/databases/{database}` GET/DELETE, with `force=true` for forced deletion. There is no public database-update route. Create fields are `name`, `owner`, `encoding` (default UTF8), `locale` (default C.UTF-8) and `allowConnections` (default true). Encoding/locale are creation-only.

```json
{"name":"app","owner":"appuser","encoding":"UTF8","locale":"C.UTF-8","allowConnections":true}
```

Optional `extensions` is an ordered list of up to 32 extension identifiers. The
control plane checks the cluster profile's available set (or its conservative
contrib baseline); the primary repeats identifier and allowlist validation.
After database creation the management identity connects to that database and
runs quoted `CREATE EXTENSION IF NOT EXISTS ... CASCADE` statements in order.
A failed extension step can be retried against the same database owner; completed
extensions are left intact. The database must permit management connections.
Database reads omit installed extensions rather than opening every database.
CLI callers can add extensions to the request file with repeatable
`--extension pgcrypto --extension uuid-ossp` flags.
The database form accepts comma-separated extension names, preserving in-progress
input (including trailing commas) while editing. Submission trims names and omits
empty entries.

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

Both control plane and agent reject CREATE (and any unsupported write privilege) on [PostgreSQL](https://www.postgresql.org/) system schemas and engine-managed namespaces: `pg_*`, `information_schema`, `_karakoram*`, [Supabase](https://supabase.com/)'s `auth`, `storage`, `realtime`, `_realtime`, `extensions`, `graphql*`, `supabase_*`, `vault`, `pgsodium`, `pgsodium_masks`, `net`, `cron`, `pgbouncer`, `_supavisor` and `_analytics`. USAGE remains available; revoking historical CREATE grants remains allowed. Use an application-owned schema for object creation.

When upgrading an existing installation, audit schema ACLs and objects in these namespaces in every database, including `postgres`: a previously issued CREATE grant or malicious overload is not removed automatically. Revoke unauthorized CREATE grants (including inherited/PUBLIC grants), inspect ownership and function definitions, and remove unauthorized objects before resuming privileged management. Grant listing includes system-schema ACLs so existing grants remain visible for revocation.

## TLS

The default `requireTLS` requires encrypted client authentication; `preferTLS` allows encrypted and plaintext clients; `disabled` disables proxy frontend TLS but does not disable internal [etcd](https://etcd.io/)/[Patroni](https://patroni.readthedocs.io/)/replication protection. [PostgreSQL](https://www.postgresql.org/) data-member TLS remains available for private/internal use. CA sources are `selfSigned` and `userProvided`; the latter requires the CA certificate/signing key and a suitable server certificate/key through the shared TLS inputs. Supplied certificates must cover required member names, loopback and exposed addresses, and permit server/client authentication. Keep private keys out of options and normal request logs.

Proxy-to-PostgreSQL traffic and [ProxySQL](https://proxysql.com/)'s backend monitor require TLS and use the cluster CA through [`pgsql-ssl_p2s_ca`](https://proxysql.com/documentation/global-variables/pgsql-variables/#pgsql-ssl_p2s_ca). Missing or malformed CA material is rejected before applying a proxy configuration; an unrelated backend certificate is rejected by libpq's CA-chain verification. Proxies restart when adopting this trust setting so previously unverified pooled connections are not retained.

**Backend hostname verification limitation:** ProxySQL 3.0.11 uses libpq `sslmode=require` with an explicit root certificate for both [query](https://github.com/sysown/proxysql/blob/v3.0.11/lib/PgSQL_Connection.cpp) and [monitor](https://github.com/sysown/proxysql/blob/v3.0.11/lib/PgSQL_Monitor.cpp) connections. With the CA supplied, this authenticates the certificate chain, but it does **not** compare the certificate's names to the configured backend hostname; it is not `verify-full`. Keep the cluster CA/signing key private, use a dedicated CA per cluster, and restrict private backend network access. Client-to-proxy `sslmode=verify-full` and direct PostgreSQL connections are separate TLS legs.

Generated database certificates are staged for renewal within 30 days of expiry and activated member-by-member. Agent HTTPS identities are published independently through authenticated configuration pulls, including recovery after leaf expiry or clock correction; this transport renewal does not restart the database. If the control plane restarts after staging database certificates but before recording their activation state, reconciliation recovers the pending material and publishes a rolling configuration revision before activation; unusable pending leaves are reissued. Patroni reload is requested through its locally supervised process before verified HTTPS configuration requests, so its old HTTPS identity does not have to authenticate its own reload. Supplied certificates must be rotated by the operator; expiry warnings do not create replacement user certificates. The supplied CA cannot be replaced in place: no trust-overlap CA rollout is implemented. Reloadable HBA/TLS changes preserve existing SQL sessions; postmaster changes or proxy listener/certificate activation can still interrupt connections. See [operations](operations.md) and [certificate lifecycle limits](../limitations.md#certificate-and-member-credential-lifecycle).

Generated leaf certificates use a one-hour backdated validity start (bounded by
the issuing CA's validity) without extending their expiry. Larger clock jumps
still require clock correction. If an active member leaf is not yet valid or
has expired, the control plane reissues it through the normal publication and
reload path, including during initial formation or a single-to-HA conversion.
Database, Patroni and etcd share the data member's native identity; proxy and
agent identities are also renewed. Native health and a healthy ProxySQL writer
route are not prerequisites for correcting an invalid native identity.

The control plane checks member maintenance and operation/restore ownership,
publishes only the already-applied configuration, and retains a reload
acknowledgement journal across lost responses. `MemberTLSReady` reports the
invalid validity boundary, a pending replacement acknowledgement, or a blocked
legacy-agent recovery. It does not relax TLS verification or replace the CA.
Reload-only capable agents write only identity files and signal the existing
PostgreSQL or Patroni process; they never consume a pending postmaster restart.
That restart remains visible for the ordinary configuration rollout.
After acknowledgement, the accepted certificate revision remains the member's
published configuration floor, including after a control-plane restart. Completing
the certificate journal does not republish an older revision or mark deferred
options, engine-version, or resource changes as applied.

Existing HA agents already support the required Patroni certificate reload, so
an in-flight single-to-HA conversion can recover after a control-plane upgrade
without replacing agents. Older single-node agents use the existing path only
when native observation confirms that no postmaster restart is pending.
Otherwise `CertificateRecoveryBlocked` requests an agent upgrade for safe,
reload-only certificate recovery rather than restarting the writer.
The blocked reason clears when the active identity becomes valid or a
replacement is activated; it is not retained after the validity problem ends.

After a clock rollback, a non-serving ProxySQL instance can retain monitor
observations from the future even after all leaf certificates are valid. If
there is no ONLINE writer, monitor errors are over five minutes ahead of the
control-plane clock, and a healthy primary is independently verified, the
control plane records an instance-specific restart intent before resetting only
that stateless proxy through the existing authenticated process API.
`ProxyClockReady=False/MonitorClockAhead` describes this distinct recovery;
`MemberTLSReady` is absent once certificate validity and reload receipts are
healthy. Maintenance and stop holds are respected. The same evidence never
causes a second restart, and changed evidence is subject to a thirty-minute
cooldown. Healthy writer routes are not restarted because of old monitor rows.
Unavailable inventory, transport, or native observations do not establish a clock
fault and do not block ordinary deployment observation or recovery. Existing
restart receipts are retained until a complete instance observation is available.

## Proxy compatibility

[ProxySQL](https://proxysql.com/) 3.0.11 is a [PostgreSQL](https://www.postgresql.org/)-aware proxy, not a transparent TCP tunnel. The upstream 3.0 series Stable Tier label is not a PostgreSQL-specific GA or complete protocol guarantee.

- LISTEN/NOTIFY forwarding, named portals, Flush semantics, incremental Execute row limits and extended COPY FROM STDIN are unsupported.
- Mixed result formats per column are unsupported. For pgx, select simple protocol or uniform text results where required.
- Optional read/write splitting uses SELECT-pattern rules (including a FOR UPDATE exception), not full SQL parsing; read-after-write consistency and every read-with-side-effect pattern are not guaranteed.
- Failover changes the backend writer; clients must reconnect/retry according to application transaction semantics.

[Supabase](https://supabase.com/) internal services bypass these limitations using their local raw TCP leader forwarder. This does not change the public application's proxy protocol contract. See [ADR 0034](../adr/0034-postgres-engine.md).

## Data MCP identities

[PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) use the `pgedge-postgres-mcp` companion with separate
`karakoram-mcp-ro` / `karakoram-mcp-rw` system identities, mapped to reserved
PostgreSQL LOGIN roles `kk_mcp_ro` / `kk_mcp_rw`. Neither has superuser, database
or role creation, replication, ownership, or RLS bypass privileges. Supabase's
`supabase_read_only_user` and `service_role` are never reused.

Only database and schema scopes are supported. The default database is
`postgres`; the default schemas are all non-system schemas. An entirely unset
scope permits `pg_read_all_data` for the reader; any explicit database or schema
scope instead uses per-database schema USAGE and table SELECT grants. The writer
gets SELECT/INSERT/UPDATE/DELETE, not DDL, plus USAGE/SELECT on scoped sequences
so serial/identity defaults work. Matching default table and sequence privileges
cover application owner roles. API-driven role catalog changes trigger an immediate
ACL refresh on the writable member. Objects and owner roles created directly
through SQL are discovered by an additive refresh every ten minutes while the
member is observed for readiness. Refreshes issue grants/default ACLs only;
they do not regenerate password verifiers or revoke privileges. Failed refreshes
remain retryable at the next observation. All old grants and default ACLs owned by
these reserved identities are removed when narrowing scope or disabling access.
Do not transfer application object ownership to these engine-owned roles.

Roles have a connection limit of 10, a 30-second statement timeout and a
60-second idle-in-transaction timeout. The reader defaults to read-only
transactions, but SELECT-only grants enforce read-only access even if a client
turns that default off. Role defaults are not immutable resource limits.

PostgreSQL privileges inherited from PUBLIC are intentionally unchanged:
CONNECT to other databases and access to PUBLIC-readable objects can remain
possible. PUBLIC CREATE or function EXECUTE privileges also remain inherited;
audit application ACLs when a strict DDL or function boundary is required.
Database scope is not an isolation boundary against existing PUBLIC grants.
The companion connects only to its selected scoped database (the first entry).

Credentials always contain all live data-member DNS names, never the proxy, even
when [ProxySQL](https://proxysql.com/) is configured: PostgreSQL URIs use
`target_session_attrs=prefer-standby` for reads and `read-write` for writes.
TLS-enabled clusters supply their CA and `sslmode=verify-full`; disabled TLS
uses `sslmode=disable`. The credential consumer must install the supplied CA
in its connection trust configuration.

`rotate-system-user` accepts either enabled logical identity. A sealed,
operation-specific staged password is published through member configuration,
and the operation waits for member revision acknowledgements.
