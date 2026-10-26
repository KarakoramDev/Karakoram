<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Supabase components

## Selection and availability

Each `spec.components.extra.<name>` has `replicas` and `resources`. An absent entry receives defaults; explicit `replicas: 0` disables it. Enabled counts are 1–5, requiring backend horizontal scaling above one, **except Realtime, which supports exactly one enabled replica**. Its native distribution network is not configured, so create/update admission rejects Realtime counts above one instead of running independent nodes as if they shared broadcast/presence. Other components retain their existing bounds. Defaults per service are shared 1 CPU, 1 GiB RAM and 1 GB runtime disk; review aggregate footprint and application requirements. The database/proxy have [separate defaults](../postgres/README.md).

Enabled analytics requires at least **1 GiB `minRam`** per replica (with `maxRam` at least as large). The native Logflare release exceeds a 512 MiB memory limit during startup, including migrations, despite bounded BEAM schedulers. The default already reserves 1 GiB; admission rejects smaller reservations rather than accepting a stack that repeatedly dies before readiness. This is a startup floor, not production workload sizing.

Analytics keeps a temporary logical replication slot (`cainophile_*`, publication `logflare_pub` in the `_supabase` database) for its metadata cache. The shipped build acknowledges the server's WAL position while that publication has no changes, so the slot does not pin the cluster's WAL when the metadata is idle (upstream Logflare's client otherwise retains WAL without bound). If any slot does pin WAL, the cluster reports `ReplicationSlots=False` and `Ready=False` with reason `SlotWALRetention`; see [PostgreSQL operations](../postgres/operations.md) for the optional `max_slot_wal_keep_size` safety limit.

Acknowledging WAL on keepalives is safe only for this ephemeral cache-invalidation consumer on a temporary slot; it is not a durable-consumer acknowledgment policy. Feedback can advance before queued invalidations are processed. Durable consumers must not use this policy: reconnecting to a persistent slot could skip changes acknowledged before they were delivered or processed.

| Component | Native application / current lock version | Private port | Dependency / availability |
| --- | --- | --- | --- |
| `gateway` | Envoy 1.39.1 | 8000 | Default enabled; supplied |
| `auth` | GoTrue 2.197.0 | 9999 | Default enabled; supplied |
| `rest` | PostgREST 14.18 | 3000 | Default enabled; supplied |
| `realtime` | Realtime 2.143.3 | 4000 | Supplied; singleton only (0 disabled / 1 enabled) |
| `storage` | Storage API 1.81.0 | 5000 | Supplied; requires registered S3 configuration |
| `imgproxy` | imgproxy 3.31.4 | 5001 | Supplied; requires storage |
| `meta` | postgres-meta 0.100.0 | 8080 | Supplied |
| `studio` | Studio 2026.10.12-bc27d2661edd | 3002 authenticated proxy | Supplied; requires meta + gateway |
| `functions` | Edge Runtime 1.77.4 | 9000 | Supplied |
| `pooler` | Supavisor 2.9.13 | 5432 session / 6543 transaction | Supplied |
| `analytics` | Logflare 1.50.15 | 4000 | Supplied |
| `vector` | Vector 0.53.0 | 9002 log source | Supplied; requires analytics |

`minimal` enables gateway/auth/rest plus proxy. `standard` adds realtime/storage/meta/studio; `full` selects every component. All three presets have their component artifacts in the current lock. Standard/full also require `storage.backendStorage`. Inspect the embedded [lock](../../internal/engine/nativeartifacts/lock.json) and running provider catalog when upgrading. Source-built services and the always-installed extension bundle need an operator mirror in development builds; admission rejects an unresolved bundle even for the minimal preset. Official entries can resolve upstream. No independent component-version override exists.

Status collects PostgreSQL, each enabled component, and final gateway upstream readiness independently and concurrently. Component observations have a four-second budget inside the five-second sweep; a hung native application endpoint withdraws that component's health and stack readiness without disconnecting responsive agents or discarding healthy peers' observations. All live instances must acknowledge current configuration and native readiness, including extra rollout instances. A timeout includes the affected instance and directs operators to restore its agent/application observations before serving traffic; it is not evidence that the entire stack is absent.

Database-backed readiness uses read-only serving requests, not merely process
listeners: REST requires both PostgREST admin `/ready` and an authenticated
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
| `gateway.publicHttps` | Opt-in backend-managed public gateway HTTPS; default false |
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

A merge patch must retain any other desired options; arrays replace. The S3 endpoint, credentials, region and path-style settings come from the registered storage. Object tenant identity is `<namespace>-<cluster>`; local file storage is not offered. Physical database backups do not include objects. A bucket override must already exist and be accessible with those credentials.

## Initialization and extensions

The primary applies curated component roles/schemas/helpers and requested extensions under an advisory lock and `_karakoram.supabase_migrations` ledger. Percona `postgres` stays superuser; services run their own application migrations. Logical replication settings are engine-owned. Selection adds required preload libraries where necessary and may require database rolling restart.

The baseline always creates the empty `graphql_public` API schema with usage grants for the API roles, so the upstream PostgREST schema list works before `pg_graphql` is selected. Only explicit extension selection installs the GraphQL wrapper and enables its gateway route.

Current admitted selections: `pgcrypto`, `uuid-ossp`, `pg_stat_statements`, `pg_cron`, `pgaudit`, `vector`, `postgis`, `pg_repack`, `pg_stat_monitor`, `set_user`, `hstore`, `citext`, `btree_gist`, `btree_gin`, `unaccent`, `fuzzystrmatch`. Percona supplies these packages/contrib; installed `wal2json` is a decoding plugin, not a selectable CREATE EXTENSION name.

The Percona-17-built `supabase-pg17-extensions` bundle additionally supplies `pg_net`, `supabase_vault`, `pgsodium`, `pgjwt`, `pg_graphql`, `pg_jsonschema`, `hypopg`, `index_advisor`, `plpgsql_check`, and `pg_hashids`. Selecting `pg_graphql` enables the GraphQL route. The bundle's `extensionFlags` declare preload and unavailable-feature boundaries: `pg_net` requires preload, while `pgsodium` is **crypto-only and must not be preloaded**. Pgsodium server-key-management, key-id and transparent-column-encryption (TCE) features are unavailable. Supautils, wrappers, PLV8, PLJava and TimescaleDB are not supplied. Build recipes or upstream package names do not establish availability; only qualified lock capabilities expand the list.

## Gateway and logs

Envoy renders routes for enabled components only. It translates supported opaque/legacy API keys, restricts `/pg/` to service keys, and protects Studio with dashboard Basic authentication. After successful authentication it replaces `apikey` with the cluster service-role credential for Studio's private proxy, which validates and strips that transport credential before forwarding to loopback-only Studio. MCP and Realtime tenant-admin paths are denied. Do not expose individual service/admin ports publicly.

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

Gateway, PostgREST, Storage, metadata/Studio private proxies and Vector ingestion follow the backend's private address family. IPv6 proxies retain IPv4 loopback health access. Raw Studio stays on `127.0.0.1:3000`; its authenticated private ingress is port `3002`. Vector uses `[::]:9002` on IPv6 backends and `0.0.0.0:9002` on IPv4 backends, with health remaining at `127.0.0.1:9001`.

Realtime, Supavisor and Logflare retain their native Erlang distribution for local runtime use only. Distribution sockets and EPMD bind to IPv4 loopback, and each component uses a distinct cookie derived from existing sealed, random per-cluster credentials, not the cookie bundled in the downloadable release. Do not open EPMD (TCP 4369) or Erlang distribution ports between members; these services do not provide an inter-member BEAM cluster. Apply the updated agent-rendered configuration to existing members before relying on this isolation. Never publish process environments or release cookies. The upstream release launchers pass the cookie as a process argument. With default Linux procfs permissions, any user who can access a process's `/proc` entry can read its arguments; `hidepid=1` or `hidepid=2` restricts access by other users but does not isolate processes running as the same user. On Nomad and Dokploy, members run in private PID namespaces, so sibling members do not see each other's processes, but member processes remain visible in the node host's PID namespace. Karakoram does not enable `hidepid` for the member's `/proc`, and the member agent and component processes run as the same service user. The cookie is therefore readable by any process inside the member; with default Linux procfs permissions on the node host, it is also readable by any host user. On other backends, assume the same exposure to anyone with access to the member's host. Do not run untrusted users or processes inside a Realtime, Supavisor or Logflare member or on its node host, and do not share the member's process or network namespaces. To restrict access through the node host's `/proc`, mount it with `hidepid=2`; this hides member process entries from other unprivileged host users except members of the group exempted with the procfs `gid=` option. The host's root user and processes running as the member's service user keep access, and the member's own `/proc` mount is unchanged.

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
