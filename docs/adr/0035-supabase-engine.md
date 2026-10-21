# 0035 — Modular native Supabase over the PostgreSQL core

Status: Accepted

## Context

Supabase combines a PostgreSQL database with independently sized API and administrative services. Duplicating HA, backups and upgrades would create competing database lifecycle implementations. Reproducing an upstream all-services deployment would also hide substantial optional resource costs and make unavailable native binaries appear supported.

## Decision

### Composition and versions

The `supabase` provider wraps the [PostgreSQL core](0034-postgres-engine.md). Its implied [Supabase profile](0044-postgres-profiles.md) supplies pinned Percona extension packages, forced logical-replication settings, selected preload libraries and curated post-readiness SQL from the shared profile registry; wrapper hooks compose application readiness and relay targets. Data members retain the PostgreSQL runtime and its single-writer authority. Only PostgreSQL **17** is admitted; `spec.version` means the Percona PostgreSQL catalog version, not a Supabase aggregate release. Component versions come from the release's [native artifact lock](0036-native-artifacts.md), not independently chosen application versions.

Use ordered `ComponentSet` definitions and `spec.components.extra.<name>` resources/replicas. Components are gateway, auth, rest, realtime, storage, imgproxy, meta, studio, functions, pooler, analytics and vector, alongside the core's proxy. An omitted entry receives defaults; explicit zero disables it; enabled counts are 1–5, subject to backend horizontal-scaling capability. Imgproxy requires storage, Studio requires meta and gateway, Vector requires analytics. Service runtime identities are separate, avoiding upstream default-port collisions.

The default `minimal` preset enables gateway, Auth and PostgREST plus the database/proxy. `standard` adds Realtime, Storage, postgres-meta and Studio; `full` selects all components. Preset definitions describe desired combinations, not artifact availability. Admission rejects any enabled component without a locked artifact or resolvable artifact URL. The current lock supplies gateway/Auth/REST, Realtime, Storage, imgproxy, meta, analytics/Logflare and Vector, but lacks Studio, Functions and Supavisor; therefore standard/full are not currently deployable as declared. Storage also requires a registered S3 destination.

### Gateway and database connectivity

Use native Envoy, following the pinned upstream routing contract, rather than Kong or an embedded application gateway. Routes are rendered only for enabled upstreams; disabled paths deny access. API-key translation supports legacy JWT and opaque publishable/secret keys. `/pg/` requires a service key. Studio is protected by dashboard basic authentication, with credentials stripped before upstream forwarding; MCP and Realtime tenant-administration routes are denied. Public HTTPS is opt-in through `engineOptions.gateway.publicHttps` and requires backend support. Otherwise the gateway URL is private HTTP on port 8000; a management relay is not public application ingress.

Database-using services connect through a local raw TCP leader forwarder, not ProxySQL. It preserves TLS, LISTEN/NOTIFY, logical replication and session protocol behavior and drops existing sessions at leader change. Reconnection remains the service/client's responsibility.

### Secrets and initialization

Generate JWT signing material, legacy anon/service-role JWTs, opaque `sb_publishable_`/`sb_secret_` keys, asymmetric/JWKS material, dashboard password, service encryption keys, Logflare/Vector tokens, Storage protocol keys and per-service database passwords in the sealed secret store. Never put them in options, ordinary status or operation checkpoints. The dashboard username is `supabase`. `GET /clusters/{cluster}/supabase/credentials` is a separate `cluster-credentials:read` reveal and is not cacheable. SMTP password is write-only through its dedicated PUT route. JWT/API-key rotation is a durable operation; it retains independent dashboard/database/encryption credentials and invalidates old application JWT/key use as configurations roll.

Apply curated SQL on a writable Supabase primary under an advisory lock with the `_karakoram.supabase_migrations` ledger. The plan creates component-specific roles/schemas/helpers and requested extensions; services own their subsequent application migrations. Percona's `postgres` remains superuser. Supabase service identities are protected from the application-role API. Restore epochs cause the bootstrap plan to be reapplied even if options are unchanged.

### Extensions

Use Percona-named PostgreSQL 17 extension packages and contrib. PGDG server/extension packaging is not interchangeable with Percona merely because a package advertises PostgreSQL compatibility. Available selection includes pgcrypto, uuid-ossp, pg_stat_statements, pg_cron, pgaudit, vector, postgis, pg_repack, pg_stat_monitor, set_user, hstore, citext, btree_gist, btree_gin, unaccent and fuzzystrmatch. wal2json is installed as a logical-decoding plugin, not advertised as a `CREATE EXTENSION` selection.

Additional native extensions are built against the selected Percona 17 headers as one `supabase-pg17-extensions` filesystem-root artifact. Its locked `extensions` list supplies pg_net, supabase_vault, pgsodium, pgjwt, pg_graphql, pg_jsonschema, hypopg, index_advisor, plpgsql_check and pg_hashids. Extension capabilities and preload requirements come from the lock's `extensionFlags`: pg_net requires preload; pgsodium is crypto-only, must not be preloaded, and does not offer server-key-management, key-id or transparent-column-encryption (TCE) features. Selecting pg_graphql enables the GraphQL gateway route. Supautils, wrappers, PLV8, PLJava and TimescaleDB remain outside the supplied baseline and are not offered. A build recipe or pinned source alone is not an available extension.

### Storage, functions and logs

Storage uses registered S3 storage, optionally overriding the bucket; credentials remain sealed. Tenant identity `<namespace>-<cluster>` isolates object keys. A local file backend is not offered for stateless service runtimes. PostgreSQL physical backups do **not** include those S3 objects; object protection and coordinated database/object recovery are operator responsibilities.

Function sources use GET/PUT/DELETE `/clusters/{cluster}/supabase/functions/{name}` and a list route. Store at most 100 functions, each at most 1 MiB of source text, sealed and delivered in runtime configuration. Require a nonempty `index.ts`, portable relative paths and a nonreserved name; `main` belongs to the pinned runtime dispatcher. Reads require cluster-read permission, so source encryption at rest is not a reason to embed application secrets. `functions.verifyJwt` controls JWT enforcement. The source API does not make an absent Functions artifact runnable.

When enabled, native supervised service logs are pushed to the authenticated Vector HTTP source and transformed into upstream source identities for Logflare/analytics. Studio logs are enabled only with analytics. No application-container socket is needed. This optional pipeline is not a durable independent audit archive and cannot be enabled until its component/dependency artifacts exist.

## Consequences

The database has one implementation of HA and recovery, while API services have independent capacity and explicit dependency/resource costs. A Karakoram upgrade can change pinned application versions and trigger component configuration/redeployment; PostgreSQL upgrades remain separately cataloged. The current artifact lock intentionally limits usable presets and extensions. Public exposure, service-role keys, SMTP and object recovery need independent operational policies; database readiness alone does not establish a working API or full upstream-product parity.

## Alternatives considered

- **Duplicate PostgreSQL lifecycle in Supabase:** creates divergent fencing, restore and upgrade behavior.
- **Monolithic mandatory stack:** obscures cost and forces optional features/dependencies into minimal installations.
- **Kong gateway:** heavier dependency and now an upstream override rather than the selected baseline.
- **Custom embedded Go gateway:** creates a new security-sensitive routing/authentication implementation to keep upstream-equivalent.
- **PGDG extension substitution or unqualified packages:** risks replacing/conflicting with the selected Percona server; explicit artifact capabilities are safer.
- **Local Storage service file backend:** requires stateful service placement and replication semantics absent from these stateless components.

## References

[Operating guide](../supabase/README.md), [components and current availability](../supabase/components.md), [native artifact tooling](../native-artifacts.md), [upstream self-hosting configuration](https://github.com/supabase/supabase/tree/bc27d2661edd09a5d0b8d76ce33da6d4071a756e/docker), [Envoy documentation](https://www.envoyproxy.io/docs/envoy/latest/).

## Update — 2026-10-17

The artifact-availability statements above describe the initial lock, not the current supplied set. The lock now includes Studio **2026.10.12-bc27d2661edd**, Edge Runtime **1.77.4** and Supavisor **2.9.13**. All components of `standard` and `full` have locked artifacts; those presets still require registered S3 Storage configuration and resolvable artifact URLs. The lock continues to constrain extension availability, but no longer excludes a preset because Studio, Functions or pooler artifacts are missing.

Gateway browser CORS now defaults to all origins without credentials, matching upstream header-based authentication; `gateway.corsOrigins` optionally restricts exact origins. Preflight precedes authentication. PostgREST defaults to `public,graphql_public`, as in the pinned upstream environment, and enabling `pg_graphql` also includes `graphql_public` with custom schema selections. The GraphQL route sets both POST and GET profile headers.

