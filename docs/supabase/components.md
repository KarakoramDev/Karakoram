<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Supabase components

## Selection and availability

Each `spec.components.extra.<name>` has `replicas` and `resources`. An absent entry receives defaults; explicit `replicas: 0` disables it. Enabled counts are 1–5, requiring provider horizontal scaling above one, **except Realtime, which supports exactly one enabled replica**. Its native distribution network is not configured, so create/update admission rejects Realtime counts above one instead of running independent nodes as if they shared broadcast/presence. Other components retain their existing bounds. Defaults per service are shared 1 CPU, 1 GiB RAM and 1 GB runtime disk; review aggregate footprint and application requirements. The database/proxy have [separate defaults](../postgres/README.md).

Enabled analytics requires at least **1 GiB `minRam`** per replica (with `maxRam` at least as large). The native [Logflare](https://logflare.app/) release exceeds a 512 MiB memory limit during startup, including migrations, despite bounded BEAM schedulers. The default already reserves 1 GiB; admission rejects smaller reservations rather than accepting a stack that repeatedly dies before readiness. This is a startup floor, not production workload sizing.

Analytics keeps a temporary logical replication slot (`cainophile_*`, publication `logflare_pub` in the `_supabase` database) for its metadata cache. The shipped build acknowledges the server's WAL position while that publication has no changes, so the slot does not pin the cluster's WAL when the metadata is idle (upstream Logflare's client otherwise retains WAL without bound). If any slot does pin WAL, the cluster reports `ReplicationSlots=False` and `Ready=False` with reason `SlotWALRetention`; see [PostgreSQL operations](../postgres/operations.md) for the optional `max_slot_wal_keep_size` safety limit.

Acknowledging WAL on keepalives is safe only for this ephemeral cache-invalidation consumer on a temporary slot; it is not a durable-consumer acknowledgment policy. Feedback can advance before queued invalidations are processed. Durable consumers must not use this policy: reconnecting to a persistent slot could skip changes acknowledged before they were delivered or processed.

| Component | Native application / current lock version | Private port | Dependency / availability |
| --- | --- | --- | --- |
| `gateway` | Envoy 1.39.6 | 8000 | Default enabled; supplied |
| `auth` | GoTrue 2.197.0 | 9999 | Default enabled; supplied |
| `rest` | PostgREST 14.18 | 3000 | Default enabled; supplied |
| `realtime` | Realtime 2.153.3 | 4000 | Supplied; singleton only (0 disabled / 1 enabled) |
| `storage` | Storage API 1.81.5 (Node 24.22.2) | 5000 | Supplied; requires registered S3 configuration |
| `imgproxy` | imgproxy 4.0.21 | 5001 | Supplied; requires storage |
| `meta` | postgres-meta 0.100.0 (Node 22.23.4) | 8080 | Supplied |
| `studio` | Studio 2026.10.12-bc27d2661edd (Node 22.23.4) | 3002 authenticated proxy | Supplied; requires meta + gateway |
| `functions` | Edge Runtime 1.77.9 | 9000 | Supplied |
| `pooler` | Supavisor 2.9.13 | 5432 session / 6543 transaction | Supplied |
| `analytics` | Logflare 1.50.15 | 4000 | Supplied |
| `vector` | Vector 0.61.0 | 9002 log source | Supplied; requires analytics |

`minimal` enables gateway/auth/rest plus proxy. `standard` adds realtime/storage/meta/studio; `full` selects every component. All three presets have their component artifacts in the current lock. Standard/full also require `storage.backendStorage`. Inspect the embedded [lock](../../internal/engine/nativeartifacts/lock.json) and running engine driver catalog when upgrading. Source-built services and the always-installed extension bundle need an operator mirror in development builds; admission rejects an unresolved bundle even for the minimal preset. Official entries can resolve upstream. No independent component-version override exists.

The imgproxy v4 integration retains the upstream Supabase compose environment
names (`IMGPROXY_BIND`, `IMGPROXY_USE_ETAG`, `IMGPROXY_AUTO_WEBP`,
`IMGPROXY_MAX_SRC_RESOLUTION=16.8`); none requires a v4 rename. Unlike compose's
file-storage setup, Karakoram always uses registered S3 storage and does not
enable `IMGPROXY_LOCAL_FILESYSTEM_ROOT`. Storage's renderer sends
`/public/<options>/plain/<percent-encoded presigned source URL>`, forwards
`Accept` for format negotiation, and retains object metadata for its response.
Native smoke exercises this HTTP-origin contract, PNG resizing, WebP negotiation,
SVG sanitization and loopback-source rejection. It does not replace a live
Storage public/authenticated/signed image-render request against the configured
S3 backend. Keep imgproxy private: the internal transform URL is unsigned;
Storage is responsible for object authorization before handing off the source.
See [upstream compose](https://github.com/supabase/supabase/blob/bc27d2661edd09a5d0b8d76ce33da6d4071a756e/docker/docker-compose.yml)
and the [pinned Storage renderer](https://github.com/supabase/storage/blob/f31599e188d7c9c854b1daa76a11b41c6d179462/src/storage/renderer/image.ts).

Realtime 2.153.3 crosses the security-fix boundary for [GHSA-2rcv-vpjw-5g6c](https://github.com/supabase/realtime/security/advisories/GHSA-2rcv-vpjw-5g6c) (fixed upstream in 2.146.0). The release retains the singleton deployment, `_realtime` metadata migrations and tenant-owned logical-slot behavior. Administration uses a separate private signing secret; anonymous and service-role tenant credentials cannot administer tenants, including through direct member access. Migrations and tenant seeding run before the service becomes ready; preserve existing database metadata and replication slots during upgrade.

#### Realtime 2.143.3 → 2.153.3 configuration parity

The [upstream comparison](https://github.com/supabase/realtime/compare/v2.143.3...v2.153.3)
and each intermediate minor release (2.144 through 2.153) preserve the emitted
configuration below. No emitted variable was renamed or removed, and no new
mandatory variable was introduced. Optional settings not emitted by Karakoram
retain upstream defaults.

| Rendered setting or operation | New-line compatibility |
| --- | --- |
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_AFTER_CONNECT_QUERY` | Same metadata connection and `_realtime` search path; tenant seed uses the same database. |
| `DB_ENC_KEY`, `SECRET_KEY_BASE` | Existing 16-byte legacy database encryption key and Phoenix secret remain supported; optional GCM settings are not required. |
| `DB_SSL`, optional `DB_SSL_CA_CERT` | Same metadata TLS/CA handling; tenant seed still propagates `ssl_enforced`. |
| `API_JWT_SECRET` | Independent administration signing secret, not the tenant key. The tenant credential cannot administer the member API. |
| Optional `API_JWT_JWKS` | Still consumed by tenant seeds, not the administration API. |
| `METRICS_JWT_SECRET` | Remains required and supplied. |
| `PORT`, `REALTIME_IP_VERSION`, `APP_NAME` | Same HTTP listener, explicit IPv4/IPv6 family and release identity. |
| `CLUSTER_STRATEGIES=""`, `GEN_RPC_SOCKET_IP` | Empty strategies still disable discovery; RPC binds loopback. DNS empty-string handling changed in 2.149; Karakoram does not emit `DNS_NODES`. Singleton admission remains unchanged. |
| `ERL_AFLAGS`, `ERL_EPMD_ADDRESS`, `RELEASE_COOKIE` | Same local distribution limits, loopback distribution/epmd and secret-derived cookie. Upstream 2.146 removes Fly-specific release addressing; native local identity remains loopback. |
| `SEED_SELF_HOST`, `RUN_JANITOR`, `DISABLE_HEALTHCHECK_LOGGING`, `RLIMIT_NOFILE` | Same seed, cleanup, logging and release resource settings. |
| `HOME`, `LANG`, `NODE_ENV` | Common process environment remains unchanged; not a new tenant API contract. |
| `bin/migrate`; application start then `Realtime.Release.seeds(Realtime.Repo)` | Metadata migrations and idempotent tenant upsert remain supported. Only the seed's environment is changed to the tenant signing secret after runtime admin configuration has been loaded; the temporary `KARAKORAM_REALTIME_TENANT_JWT_SECRET` is native preparation input. 2.153.3 additionally enables the upstream `broadcast_persistence` seed flag. |
| `Realtime.Tenants.Migrations.run_migrations(tenant)` | Tenant migrations add message expiry/replay indexes, hashed subscription filters and revocation of migration-table grants. The curated bootstrap includes `dashboard_user`, required by the new revocation. Preserve existing migration ledgers. |
| Authenticated `GET /api/tenants/realtime-dev/health` | Same private API route; require `data.healthy=true`, not merely HTTP 200 or HEAD. |
| Websocket join, broadcast, presence and `postgres_changes` | Existing payload contracts remain valid. 2.147 refactors socket tracking; 2.151/2.153 add feature-flagged node/socket join limits, with valid defaults. No renderer override is needed. |

Intermediate review: 2.144 adds optional replica mappings; 2.145 changes
presence/broadcast accounting; 2.146 validates tenant identifiers (the security
fix) and removes Fly-specific addressing; 2.147 refactors sockets; 2.148 adds
request metrics; 2.149 changes authorization/filter indexing, DNS empty handling
and optional SQL-logex cache timing; 2.150 adds replay/expiry
and migration privilege hardening; 2.151 adds optional join-budget settings;
2.152 adjusts HTTP broadcast parsing/limits; 2.153 adds optional socket limits
and migration/connection recovery refinements. None requires a new external
tenant administration call. The product continues using the release seed,
not REST tenant creation.

Native qualification exports the production renderer and curated SQL bootstrap
through `TestRealtimeNativeFixture`, applies them on PostgreSQL 17, runs upstream
metadata and tenant migrations, rejects tenant credentials at the private admin
API, and requires a real websocket join, echoed broadcast and logical-decoded
INSERT. An HTTP-only smoke is not sufficient qualification.

Status collects PostgreSQL, each enabled component, and final gateway upstream readiness independently and concurrently. Component observations have a four-second budget inside the five-second sweep; a hung native application endpoint withdraws that component's health and stack readiness without disconnecting responsive agents or discarding healthy peers' observations. All live instances must acknowledge current configuration and native readiness, including extra rollout instances. A timeout includes the affected instance and directs operators to restore its agent/application observations before serving traffic; it is not evidence that the entire stack is absent.

Database-backed readiness uses read-only serving requests, not merely process
listeners: REST requires both [PostgREST](https://postgrest.org/) admin `/ready` and an authenticated
OpenAPI root request, Auth performs a single indexed user-ID lookup (including
the successful no-such-user result on empty databases), Storage lists buckets,
and metadata lists schemas. Auth readiness never counts the user population.
These probes use existing local service credentials
and do not create application data. A reachable process whose database path or
schema cache cannot serve is unhealthy; its instance-scoped diagnosis withdraws
`ComponentsReady` and stack readiness on the normal observation sweep. Upstream
response bodies and credentials are not copied into status. The existing
three-second native probe budget and runtime recovery hysteresis are unchanged.
Gateway listener readiness remains independent during sequential bootstrap;
final gateway discovery evidence is not an end-to-end database query.
Realtime readiness also requires its tenant health payload to report healthy,
not just HTTP success; a healthy idle tenant need not already have an active
database connection. Pooler and Analytics retain their upstream database-aware
health checks. None of these observations replaces an end-to-end workload probe.

These probe changes require the updated agent on each component instance.
A control-plane-only rollout leaves existing agents' native probes unchanged;
use fresh members or the supported component artifact rollout to install the
updated agent, and verify every live instance's artifact provenance.

Auth and REST member configurations also retain the existing service-role API
credential for these authenticated database probes. Correcting missing probe
credentials requires a control-plane upgrade and normal member configuration
convergence, not another agent artifact rollout if the instance already runs
these probes. Storage already retains this credential; metadata's native
loopback database probe does not require it. Do not bypass authentication or
fall back to process-only readiness when a serving probe returns HTTP 401.

## Configuration

All fields below are inside `engineOptions`; unknown fields, secret/password options and `null` are rejected.

| Field | Meaning |
| --- | --- |
| `siteUrl` | HTTP(S) application URL, without credentials or fragments; defaults from gateway when omitted |
| `additionalRedirectUrls` | Additional HTTP(S) redirect URLs |
| `auth.disableSignup`, `auth.autoconfirm` | Signup and automatic-confirmation policy; default false |
| `auth.jwtExpiry` | Seconds, default 3600, maximum 604800 |
| `auth.smtp.host`, `.port`, `.user`, `.adminEmail`, `.senderName` | Non-secret SMTP settings; port defaults 587; password uses dedicated API |
| `rest.schemas` | Exposed identifier-safe schemas; default `[public, graphql_public]`, matching the pinned upstream `PGRST_DB_SCHEMAS`; enabling `pg_graphql` always includes `graphql_public` even with custom schemas; configure grants/RLS separately |
| `rest.maxRows` | Row-limit input; negative values rejected; zero uses service defaults |
| `storage.backendStorage` | Registered S3 storage in the cluster namespace; required when storage is enabled |
| `storage.bucket` | Optional override of the registered storage's bucket |
| `storage.fileSizeLimit` | Upload-size input; negative values rejected; zero uses service defaults |
| `storage.imageTransformation` | Requires enabled imgproxy and its storage dependency |
| `gateway.publicHttps` | Opt-in provider-managed public gateway HTTPS; default false |
| `gateway.corsOrigins` | Exact HTTP(S) origins without paths, queries, fragments or credentials; omitted/empty (or a sole `*`) allows any browser origin; credentials are never enabled |
| `studio.organization`, `.project` | Dashboard display names |
| `extensions` | Explicit supported PostgreSQL extension selection, below |
| `functions.verifyJwt` | Default true; disabling weakens function invocation authentication |

For example, add Storage to the minimal stack with a registered destination:

```yaml
spec:
  components:
    extra:
      storage:
        replicas: 1
  engineOptions:
    storage:
      backendStorage: objects
      imageTransformation: false
```

A merge patch must retain any other desired options; arrays replace. The [S3](https://aws.amazon.com/s3/) endpoint, credentials, region and path-style settings come from the registered storage. Object tenant identity is `<namespace>-<cluster>`; local file storage is not offered. Physical database backups do not include objects. A bucket override must already exist and be accessible with those credentials.

Storage [1.81.1–1.81.5](https://github.com/supabase/storage/compare/v1.81.0...v1.81.5) retain the existing S3 endpoint, credentials, tenant prefix and database configuration; there are no SQL migration changes in this patch range. Fixes cover zero-byte bucket limits for multipart uploads, quoted multipart ETags, invalid copy metadata directives and idle TUS bodies. TUS now disconnects a request body after 60 seconds without incoming or consumed bytes; this is not a total upload-duration limit. Karakoram retains that upstream default (`TUS_BODY_IDLE_TIMEOUT_MS` is not overridden). Storage's service-owned migrations still run normally; back up both SQL metadata and S3 objects before an application rollout.

postgres-meta and Studio retain their application source selections and frozen dependency trees; both bundle the security-patched [Node 22.23.4](https://nodejs.org/en/blog/release/v22.23.4) runtime. Their metadata encryption and private gateway contracts are unchanged.

Edge Runtime [1.77.5–1.77.9](https://github.com/supabase/edge-runtime/compare/v1.77.4...v1.77.9) fix worker cleanup with in-flight requests, queued requests waiting for a worker to boot and ONNX cache-key hashing, and update rustls-webpki to 0.103.16. The Rust toolchain and linked rusty_v8 pin are unchanged; this is not a claim that every upstream Deno/V8 advisory is fixed. Function directory/import-map layout, JWT enforcement and the 150-second user-worker request-idle timeout are unchanged. Exercise both plain and npm-importing functions after rollout, including concurrent cold-start requests.

## Initialization and extensions

The primary applies curated component roles/schemas/helpers and requested extensions under an advisory lock and `_karakoram.supabase_migrations` ledger. [Percona](https://www.percona.com/) `postgres` stays superuser; services run their own application migrations. Logical replication settings are engine-owned. Selection adds required preload libraries where necessary and may require database rolling restart.

The baseline always creates the empty `graphql_public` API schema with usage grants for the API roles, so the upstream PostgREST schema list works before `pg_graphql` is selected. Only explicit extension selection installs the GraphQL wrapper and enables its gateway route.

Current admitted selections: `pgcrypto`, `uuid-ossp`, `pg_stat_statements`, `pg_cron`, `pgaudit`, `vector`, `postgis`, `pg_repack`, `pg_stat_monitor`, `set_user`, `hstore`, `citext`, `btree_gist`, `btree_gin`, `unaccent`, `fuzzystrmatch`. Percona supplies these packages/contrib except `vector`, which uses the standalone locked `pgvector-pg17` 0.8.7 source bundle. Preparation removes the old vector APT payload without dropping database objects; follow the [binary/SQL migration and index checks](../postgres/profiles.md#pgvector-security-maintenance). Installed `wal2json` is a decoding plugin, not a selectable CREATE EXTENSION name.

The Percona-17-built `supabase-pg17-extensions` bundle additionally supplies `pg_net`, `supabase_vault`, `pgsodium`, `pgjwt`, `pg_graphql`, `pg_jsonschema`, `hypopg`, `index_advisor`, `plpgsql_check`, and `pg_hashids`. Selecting `pg_graphql` enables the GraphQL route. The bundle's `extensionFlags` declare preload and unavailable-feature boundaries: `pg_net` requires preload, while `pgsodium` is **crypto-only and must not be preloaded**. Pgsodium server-key-management, key-id and transparent-column-encryption (TCE) features are unavailable. Supautils, wrappers, PLV8, PLJava and [TimescaleDB](https://www.timescale.com/) are not supplied. Build recipes or upstream package names do not establish availability; only qualified lock capabilities expand the list.

## Gateway and logs

Envoy renders routes for enabled components only. It translates supported opaque/legacy API keys, restricts `/pg/` to service keys, and protects Studio with dashboard Basic authentication. After successful authentication it replaces `apikey` with the cluster service-role credential for Studio's private proxy, which validates and strips that transport credential before forwarding to loopback-only Studio. MCP and Realtime tenant-admin paths are denied. Do not expose individual service/admin ports publicly.

Envoy 1.39.6 retains the generated 1.39 bootstrap: review of the
[1.39.2–1.39.6 patch notes](https://www.envoyproxy.io/docs/envoy/v1.39.6/version_history/v1.39/v1.39)
found no Lua/CORS/RBAC/path/WebSocket configuration migration. The removed
Bullseye packages do not affect the official standalone executable. The fixed
API-key/OAuth2 filters, RDS filter-lifetime races, Mobile trust bundle, Istio
contrib metadata and container CA bundles are not used by this static native
bootstrap; do not infer native host CA remediation from container release notes.

PostgREST application paths such as `/rest/v1/messages` and
`/rest/v1/rpc/list_messages` accept anon/publishable and service-role/secret API
keys; database grants and row-level security still determine application access.
The exact `/rest/v1/` root (including requests with a query string) serves OpenAPI
schema discovery and is **admin-only**, matching
[upstream Kong policy](https://github.com/supabase/supabase/blob/bc27d2661edd09a5d0b8d76ce33da6d4071a756e/docker/volumes/api/kong.yml)
and its [security rationale](https://github.com/orgs/supabase/discussions/42949).
An anon key returning 403 at that root is expected; test an actual table or RPC
path for anon application access. A service key does not replace Studio dashboard
authentication, and anon/publishable keys cannot access `/pg/`.

Realtime routes positively allow only websocket/longpoll, `/api/ping`, `/api/broadcast` (including `/api/broadcast/:topic/events/:event`), and `/healthcheck` beneath `/realtime/v1`. Unknown paths and noncanonical paths containing empty or dot segments, escaped slashes/backslashes, or nested percent encoding are denied. This policy does not normalize Storage paths: raw SigV4 paths remain unchanged. Realtime's private administration API uses a separate sealed signing secret, never the tenant/anon JWT secret; ordinary application and service-role keys cannot administer tenants directly.

Both broadcast endpoints use `POST`, matching the pinned Realtime router. The gateway removes only the `/realtime/v1` prefix, retaining the leading slash in the upstream `/api/broadcast/...` request target.

Gateway, PostgREST, Storage, metadata/Studio private proxies and [Vector](https://vector.dev/) ingestion follow the provider's private address family. IPv6 proxies retain IPv4 loopback health access. Raw Studio stays on `127.0.0.1:3000`; its authenticated private ingress is port `3002`. Vector uses `[::]:9002` on IPv6 providers and `0.0.0.0:9002` on IPv4 providers, with health remaining at `127.0.0.1:9001`.

Realtime, [Supavisor](https://github.com/supabase/supavisor) and Logflare retain their native [Erlang](https://www.erlang.org/) distribution for local runtime use only. Distribution sockets and EPMD bind to IPv4 loopback, and each component uses a distinct cookie derived from existing sealed, random per-cluster credentials, not the cookie bundled in the downloadable release. Do not open EPMD (TCP 4369) or Erlang distribution ports between members; these services do not provide an inter-member BEAM cluster. Apply the updated agent-rendered configuration to existing members before relying on this isolation. Never publish process environments or release cookies. The upstream release launchers pass the cookie as a process argument. With default [Linux](https://www.kernel.org/) procfs permissions, any user who can access a process's `/proc` entry can read its arguments; `hidepid=1` or `hidepid=2` restricts access by other users but does not isolate processes running as the same user. On [Nomad](https://developer.hashicorp.com/nomad) and [Dokploy](https://dokploy.com/), members run in private PID namespaces, so sibling members do not see each other's processes, but member processes remain visible in the node host's PID namespace. Karakoram does not enable `hidepid` for the member's `/proc`, and the member agent and component processes run as the same service user. The cookie is therefore readable by any process inside the member; with default Linux procfs permissions on the node host, it is also readable by any host user. On other providers, assume the same exposure to anyone with access to the member's host. Do not run untrusted users or processes inside a Realtime, Supavisor or Logflare member or on its node host, and do not share the member's process or network namespaces. To restrict access through the node host's `/proc`, mount it with `hidepid=2`; this hides member process entries from other unprivileged host users except members of the group exempted with the procfs `gid=` option. The host's root user and processes running as the member's service user keep access, and the member's own `/proc` mount is unchanged.

Realtime's internal RPC listener is bound to loopback and must never be reachable.
Existing deployments require the updated member agent as well as its rendered
configuration. A control-plane upgrade or an in-place application restart does
not replace that member executable. Follow the
[component artifact roll procedure](operations.md#cluster-restart-and-member-software)
before relying on updated member behavior.

Other native application listeners are distinct from distribution: Supavisor
also opens PostgreSQL proxy port `5412` and internal session/transaction shards
`12100`–`12107`, all using its PostgreSQL credential authentication. Logflare opens
gRPC ingestion port `50051`, protected by API-key/source authorization. These
upstream application listeners default to wildcard binds and do not expose
arbitrary Erlang RPC. Do not publish them: the pinned releases do not expose an
environment option to loopback-bind these listeners individually. `NODE_IP` and
`LOGFLARE_NODE_HOST` name nodes, not application bind addresses.

Browser CORS defaults to upstream allow-any-origin behavior because application authentication uses headers, not browser cookies. Preflight is handled before API-key or dashboard authentication; credentialed CORS is disabled. To restrict browser callers, set `gateway.corsOrigins: [https://app.example.com]`. CORS is a browser policy, not an authorization boundary: non-browser callers still need the appropriate API keys and database grants/RLS. GraphQL requests receive both `Content-Profile` (POST) and `Accept-Profile` (GET) for `graphql_public`.

Optional log shipping sends supervised service logs to authenticated Vector HTTP ingestion and onward to analytics/Logflare with upstream source identities. Enable both `analytics` and `vector`; Studio enables logs only with analytics. Log shipping is not a durable independent security audit archive. See [operations](operations.md).

## Vector upgrade qualification

Vector 0.53.0 → [0.61.0](https://vector.dev/releases/0.61.0/) is a
security-motivated newer-line upgrade, not a patch-only change. The official
latest stable release remains 0.61.0. It includes the 0.57 fixes for
CVE-2026-77619/77620/77621: [Logstash allocation](https://github.com/vectordotdev/vector/security/advisories/GHSA-rrfg-9487-mhp6),
[nested compression](https://github.com/vectordotdev/vector/security/advisories/GHSA-qp6f-fpfx-4gg6)
and [file path traversal](https://github.com/vectordotdev/vector/security/advisories/GHSA-6342-xwvw-c637).
Those Logstash source/file sink components are not enabled here. The HTTP source
also benefits from 0.57's decompressed request-size cap. The 0.61 Mozilla root
store update ships in the binary; its container-only CA update is not evidence
about native host certificates. This does not establish that every bundled
dependency is vulnerability-free.

Every intermediate release was reviewed:

| Release / upstream guide | Managed configuration impact |
| --- | --- |
| [0.54](https://vector.dev/releases/0.54.0/) | No dedicated upgrade guide; release notes change Datadog logs' default compression to zstd. That sink and the changed syslog encoder are unused; no HTTP/VRL migration needed. |
| [0.55](https://vector.dev/highlights/2026-04-20-0-55-0-upgrade-guide/) | Observability moves to gRPC; our loopback HTTP `/health` remains supported. No GraphQL/playground options; sink authentication already uses `request.headers`. Azure is unused. |
| [0.56](https://vector.dev/releases/0.56.0/) | No dedicated upgrade guide; release notes require GreptimeDB v1 for Greptime sinks, which are unused. HTTP retry classification now distinguishes retryable transport errors; retain existing bounded retry settings and default strategy. |
| [0.57](https://vector.dev/highlights/2026-07-14-0-57-0-upgrade-guide/) | Keep environment interpolation disabled and template confinement enabled. Rendered JSON contains literal credentials, static source-name query strings and static headers; no environment expansion, event-derived URI/header templates, or dangerous opt-outs are needed. |
| [0.58](https://vector.dev/highlights/2026-08-26-0-58-0-upgrade-guide/) | HTTP source already uses `decoding.codec=json` and `framing.method=bytes`, not removed `encoding`. Sink authorities are static. Azure/Influx/Mezmo/WebHDFS and removed buffer metrics are unused. |
| [0.59](https://vector.dev/highlights/2026-10-05-0-59-0-upgrade-guide/) | Source already uses `http_server`, not removed `http` alias. Avro, Datadog, Kubernetes, Greptime and Vector-protocol sinks are unused. |
| [0.60](https://vector.dev/highlights/2026-10-19-0-60-0-upgrade-guide/) | Removed Datadog v1 and required Influx version do not affect this pipeline. |
| [0.61](https://github.com/vectordotdev/vector/blob/v0.61.0/website/content/en/highlights/2026-10-29-0-61-0-upgrade-guide.md) | Scheme-less Vector-protocol sinks now default to HTTPS; ours is an HTTP sink with an explicit `http://` private analytics URI. |

Parity was checked against upstream Supabase master at
[`fa7e8b636a432427a380ff6ed9be134cd3e61fcc`](https://github.com/supabase/supabase/blob/fa7e8b636a432427a380ff6ed9be134cd3e61fcc/docker/volumes/logs/vector.yml).
All seven Logflare source identities match, including gateway
`cloudflare.logs.prod`; Auth also retains upstream's structured timestamp.
Native supervision deliberately replaces Docker labels with process/member
identities and authenticated JSON-array HTTP ingestion. Remap keeps member
metadata, service parsing and Realtime health filtering. Gateway diagnostic
logs are retained rather than parsed as Kong nginx access logs: the managed
Envoy bootstrap does not install an access logger. Parsing failures retain raw
messages instead of dropping diagnostic lines. Existing historical analytics
records and backup metadata are not rewritten.

`TestNativeVector` validates the rendered configuration with the official binary
(without `--no-environment`, so sink confinement is checked), rejects invalid
bearer authentication, exercises IPv6 ingestion/loopback health, and checks all
seven source identities, service message parsing and Realtime health filtering
against a local HTTP analytics receiver. Live acceptance must additionally
confirm actual Logflare ingestion and Studio log queries after a rolling update;
local HTTP receiver qualification is not a live Logflare/database smoke.


## Imgproxy v4 configuration parity

The native 3.31.4 → 4.0.21 upgrade retains Storage's unsigned, private-service
HTTP(S) source handoff. Review sources:
[v4 changelog](https://github.com/imgproxy/imgproxy/blob/master/CHANGELOG.md)
and [configuration reference](https://docs.imgproxy.net/configuration/options).
The golden `scripts/native-artifacts/imgproxy-env.json` is checked against the
renderer and consumed unchanged by clean-runner qualification.

| Option / behavior | v3 managed value or default | v4 | Action |
| --- | --- | --- | --- |
| `IMGPROXY_BIND` | `:5001` | Same option | Retain exact listener |
| `IMGPROXY_USE_ETAG` | `true` | Same option; origin-based validators | Retain; accept cache churn |
| `IMGPROXY_AUTO_WEBP` | `true` | Same option | Retain Accept negotiation |
| `IMGPROXY_MAX_SRC_RESOLUTION` | `16.8` MP | Same option | Retain image-bomb cap |
| `IMGPROXY_USE_LAST_MODIFIED` | Unset / false | Defaults true per v4 changelog | Explicit false preserves v3 |
| Loopback / link-local sources | Forbidden | Forbidden | Keep guards enabled |
| Private-network sources | Allowed | Allowed; corrected address classification | Retain private S3 reachability |
| `IMGPROXY_SANITIZE_SVG` | Unset / true | True, with security fixes | Keep sanitizer enabled |
| SVG raster DPI | 72 | 96 | Accept changed output bytes; no object rewrite |
| Presets | None | CLI `--presets` removed | None emitted; no migration |
| Local filesystem / direct S3 sources | Disabled; no root or credentials | Same opt-in source backends | Keep disabled; Storage uses presigned HTTP(S) |
| Source file / result dimension limits | Unset / zero | Unset / zero | No new limit imposed |
| Healthcheck | HTTP `/health` | HTTP `/health` | Exercise real endpoint |

No removed or renamed option is emitted. The first five rows enumerate every
rendered `IMGPROXY_*` variable (Storage separately emits `IMGPROXY_URL` pointing
to this service, not an imgproxy configuration option). Qualification performs
PNG resize, WebP negotiation and SVG sanitization from a local fixture served
over private HTTP, checks Last-Modified stays absent, and rejects loopback
sources. Changed ETag derivation and SVG DPI are v0.4.7 behavior changes:
transient CDN/client cache churn and different SVG raster bytes are expected.

