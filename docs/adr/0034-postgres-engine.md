# 0034 — Native PostgreSQL with independent HA coordination and a client proxy

Status: Accepted

## Context

PostgreSQL needs a native runtime, a single-writer authority independent of the control plane, physical recovery, and PostgreSQL-shaped access administration. A replica count alone is not a failure-domain guarantee. Supabase also needs the same database lifecycle without inheriting a proxy's wire-protocol restrictions.

## Decision

### Database and coordination

The `postgres` provider installs catalog-pinned Percona Distribution for PostgreSQL packages on Ubuntu 24.04, with majors 16, 17 and 18; 17.11.1 is the default. Preparation disables distribution cluster creation and package-service autostart. Runtime supervision, ownership and data paths use the existing infrastructure layout rather than a platform-specific filesystem or service manager.

`single` runs one foreground PostgreSQL server, without Patroni or a DCS. `ha` has three or five data members, each with PostgreSQL, Patroni 4 and an etcd v3 voter. PostgreSQL and etcd data persist together on that member's volume. Patroni owns PostgreSQL in HA: the runtime must never start a competing standalone server. etcd uses TLS and client certificates; Patroni REST uses TLS and authenticated mutations. Replication uses the cluster PKI. Patroni leader-lock checks, not a successful SQL connection alone, establish writable-primary authority for management actions.

Co-located etcd avoids an external coordination service and allows failover during control-plane downtime. Quorum loss must fail closed rather than create a second writer. `engineOptions.synchronousMode` is `off` (default), `on`, or `strict`; strict mode may block writes without a qualifying synchronous standby. Rewind, replication slots and rebuilding divergent replicas are engine-managed. Voters still need independent infrastructure failure domains.

### Client and internal connections

A separate stateless `proxy` component runs locked ProxySQL 3.0.11, defaults to one replica and supports up to five. HA requires a proxy; only `single` permits zero replicas and then uses its data-member endpoint. Native `pgsql_replication_hostgroups` monitors `pg_is_in_recovery()` with writer/reader hostgroups 10/20. Port 5432 is writer-only by default. `engineOptions.proxy.readWriteSplit` enables limited SELECT-pattern routing; `readerPort` adds private port 5433. These rules are not a SQL correctness or read-after-write consistency classifier. The administrative listeners remain loopback-only.

ProxySQL PostgreSQL support began in the 3.0 alpha series and was described as beta in 3.0.2; the 3.0.11 series is labelled Stable Tier upstream, not a separate PostgreSQL GA assurance. Document its protocol boundaries: asynchronous LISTEN/NOTIFY forwarding, named portals, Flush, incremental Execute row limits and extended COPY FROM STDIN are unsupported; mixed per-column result formats also require client adaptation. Prepared statements alone do not establish full PostgreSQL compatibility. Applications must reconnect after failover.

ProxySQL requires recoverable login passwords. Application roles managed through the native access API have passwords sealed in the secret store and synchronized to proxy configurations. SQL-created roles are not automatically routable through the proxy. Direct member listeners remain private except the explicit proxy-free single-member exposure case. Public TCP publishes the selected client endpoint, with backend-specific allowlist capability checks.

Internal Supabase services bypass ProxySQL. A local leader-aware raw TCP forwarder preserves PostgreSQL TLS and session/replication protocols, polls authenticated Patroni `/primary` over TLS every second, and closes old sessions when leadership disappears or changes. Single topology forwards to its sole data member. This is internal plumbing, not a general public connection API.

### Recovery and access

pgBackRest writes backups and WAL directly from the current primary to registered S3 storage. Repositories use stanza `kk` and `<storage-prefix>/postgres/<namespace>/<cluster>-<uid>` isolation. Types are `full`, `diff`, and `incr`. Durable operation receipts prevent duplicate backup dispatch from being mistaken for new work. WAL archiving is configured even when PITR restores are disabled; without repository storage it is a no-op, not recoverability.

Karakoram owns scheduled count retention. pgBackRest automatic expiration is disabled; live deletion invokes `expire --set=<label>`. Removing an anchor also removes dependent backups and their inventory records, potentially crossing schedules. Retained repositories use guarded S3 cleanup and native dependency metadata when no live member remains. PITR is opt-in and reports a conservative archive-backed interval, not a precise last-commit guarantee.

In-place restore is destructive and offline. Single recovery stops PostgreSQL, restores and promotes. HA fences all data members, resets only the cluster's DCS scope, restores a seed through Patroni custom bootstrap and rebuilds replicas. Recovery adopts the backup's sealed system credentials after fencing. Clone bootstrap reads the source repository with archiving disabled, then establishes the target repository and a new recovery history. Source and target providers and PostgreSQL majors must match; `postgres` and `supabase` are not interchangeable backup providers. See [recovery details](../postgres/backups.md).

Cross-source restore into an existing target repository appends the restored database system-ID history through stanza-upgrade, retaining prior target backup sets rather than overwriting their inventory with source metadata. Archiving resumes only after target repository configuration is active and bootstrap archive overrides have been removed from all members.

Native `/clusters/{cluster}/postgres/roles`, `/databases` and `/grants` routes model PostgreSQL rather than MongoDB users. Bounded actions execute on the current primary through its local socket; the interface is not arbitrary SQL execution. Roles support login, memberships, expiry and privilege flags, with extra cluster-update authorization for privileged changes. System roles are hidden and immutable. Password generation/rotation returns the generated value once; role-action SQL receives a SCRAM verifier. Database encoding/locale are creation-only; grants cover database and schema privileges, not arbitrary object ACLs.

### Maintenance

Reloadable configuration is applied without a database restart; restart-required settings and resources use replica-first rolling maintenance and a writer switchover. Proxy-only configuration does not restart PostgreSQL. Minor upgrades redeploy catalog-pinned packages in the same order. Adjacent major upgrades require a successful current-version backup and an offline checkpointed workflow: install both majors, stop databases while preserving etcd, `pg_upgrade --check` then `--link` on the primary, reset the Patroni scope, start the upgraded writer, rebuild replicas, upgrade the pgBackRest stanza and queue a full backup. Downgrades/skipped majors are rejected. After link-mode activation, recovery uses the pre-upgrade backup, not the old directory.

Single-to-HA conversion preserves the original volume and adopts its PGDATA under Patroni. HA supports 3↔5 members, not HA-to-single. Membership adds etcd learners, observes catch-up and promotes them; scale-in moves leadership before removal. Replacement adds a monotonically named streaming replica before removing the old identity and retiring its runtime/volume. Orphaned volumes remain tracked for cleanup. Retained-volume suspension preserves PostgreSQL and etcd identity; resume recreates runtimes.

## Consequences

One database core supplies both standalone PostgreSQL and Supabase recovery and lifecycle behavior. The control plane need not be available for established Patroni failover, but scheduling, configuration and management still depend on it. Co-located DCS increases per-member resource cost. ProxySQL provides a stable client service without promising transparent compatibility with every driver/protocol. Secrets, backup dependencies, offline major upgrades and additional replacement capacity are explicit operational obligations.

## Alternatives considered

- **Patroni Raft:** deprecated upstream; unsuitable for a new coordination dependency.
- **External Consul, ZooKeeper or Kubernetes DCS:** viable Patroni integrations, but add infrastructure dependencies not otherwise required here.
- **Karakoram as DCS:** adds consensus/lease semantics and makes failover depend on control-plane availability.
- **Patroni plus one etcd voter for single topology:** adds processes without HA; plain PostgreSQL is simpler.
- **ProxySQL for internal Supabase services:** incompatible with notifications, replication and session features those services need.
- **HAProxy sidecar or client multi-host strings for internal access:** extra executable/configuration or uneven driver support; a protocol-transparent local forwarder is smaller.
- **Generic database-users API:** misrepresents PostgreSQL roles, memberships and database/schema privileges.

## References

[Operating guide](../postgres/README.md), [Patroni DCS configuration](https://patroni.readthedocs.io/en/latest/yaml_configuration.html), [Patroni REST API](https://patroni.readthedocs.io/en/latest/rest_api.html), [ProxySQL PostgreSQL support](https://proxysql.com/documentation/postgresql-support/), [pgBackRest](https://pgbackrest.org/user-guide.html), [native artifacts](0036-native-artifacts.md).
