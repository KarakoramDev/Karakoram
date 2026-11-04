<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Cluster data MCP

Data [MCP](https://modelcontextprotocol.io/) gives an MCP client tools for the **data inside one cluster**, not authority to create, restore or administer clusters. It is **experimental** in this release: the interfaces and reviewed tool catalogue can change between minor versions. Each cluster has its own Streamable HTTP endpoint:

```text
https://<karakoram>/mcp/clusters/{namespace}/{cluster}
Authorization: Bearer <restricted-api-token>
```

It uses the existing public listener; no public database port or companion port is needed. [MongoDB](https://www.mongodb.com/)/PSMDB uses `mongodb-mcp-server`, [PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) use `pgedge-postgres-mcp`, and [Elasticsearch](https://www.elastic.co/elasticsearch) and [RabbitMQ](https://www.rabbitmq.com/) use native toolsets. See the [design decision](adr/0043-data-mcp-gateway.md), [agent access security model](security.md#agent-access-mcp-security-model), [access control](access-control.md) and the [MCP quick start](mcp-quickstart.md#5-data-mcp-quick-start).

PostgreSQL extension profiles retain Data MCP support on single and HA topologies. Distributed PostgreSQL ([Citus](https://www.citusdata.com/)) rejects `spec.dataMcp.enabled: true` at admission because its group-aware access model is not supported by the PostgreSQL companion. [Keycloak](https://www.keycloak.org/) does not support Data MCP.

Data endpoints support stateless MCP `2026-07-28` and legacy initialize-based Streamable HTTP (`2025-11-25`, `2025-06-18`, `2025-03-26`), for both companion-backed and native toolsets. Companion connections negotiate an initialize-based revision independently of the client's protocol; they do not forward the client's protocol version.

Tool lists are caller-specific. On MCP `2026-07-28`, `tools/list` returns `cacheScope: "private"` and `ttlMs: 0`; clients must re-fetch to reflect current authorization and cluster tool filtering.

## Enable and scope access

**Off by default:** an absent `spec.dataMcp` or `enabled: false` disables the endpoint and removes its managed data identities and companion when no longer needed. Disabled configurations can retain their settings for later use. Installation policy can also disable all [MCP](https://modelcontextprotocol.io/) access with `KARAKORAM_MCP=off`; the installation default is `readonly`, not permission to enable any cluster automatically.

For example, this cluster merge patch enables read-only access to one [MongoDB](https://www.mongodb.com/) database:

```yaml
spec:
  dataMcp:
    enabled: true
    access: read
    scope:
      databases: [app]
    tools:
      deny: [mongodb-logs]
      confirmWrites: true
    auditQueryText: false
```

Apply it through the existing [cluster create/edit/patch workflow](clusters.md#create-and-change-desired-state). Cluster changes require cluster update permission; that does not itself grant data access.

| `spec.dataMcp` field | Default and meaning |
| --- | --- |
| `enabled` | `false`; explicit opt-in |
| `access` | `read`; only `read` and `readWrite` are accepted |
| `scope.databases` | Empty; MongoDB or PostgreSQL/Supabase database names |
| `scope.schemas` | Empty; PostgreSQL/Supabase application schemas only |
| `scope.indexPatterns` | Empty; Elasticsearch index patterns only |
| `scope.vhosts` | Empty; RabbitMQ vhost names only |
| `tools.deny` | Empty; hides and refuses named tools, including write aliases |
| `tools.confirmWrites` | `false`; require confirmation for ordinary writes as well as destructive tools |
| `auditQueryText` | `false`; opt into argument text in audit details in addition to its hash |
| `resources` | Companion service resources; omitted selects shared CPU, `minCpu: 1`, `maxCpu: 1`, `minRam: 0.25`, `maxRam: 0.5` (RAM in GiB). Does not size the database or native toolsets. |

Each scope list accepts at most **64** nonblank entries of at most **256 bytes** before engine-specific validation. `tools.deny` accepts at most **64 unique**, nonblank names of at most **64 bytes**. Unknown upstream tools are denied regardless of this list. When enabling, the engine driver must support data MCP; fields for a different engine are rejected.

### Engine scope semantics

- **MongoDB/PSMDB:** only `databases`. Names must be valid UTF-8 without surrounding whitespace/control characters, fewer than 64 bytes, and exclude MongoDB's prohibited name characters. `admin`, `local`, `config` and `$external` cannot be scoped. Empty scope grants the corresponding `readAnyDatabase`/`readWriteAnyDatabase` data role; explicit scope grants `read`/`readWrite` on the named application databases.
- **[PostgreSQL](https://www.postgresql.org/)/[Supabase](https://supabase.com/):** only `databases` and `schemas`. Identifiers must be nonempty, at most 63 bytes and contain no NUL. Schemas `information_schema` and names beginning `pg_` are rejected. Named databases and schemas must exist when grants reconcile. Empty database scope selects **`postgres`** for explicit object grants and the companion connection, **not every database**. With neither database nor schema scope, the reader additionally receives `pg_read_all_data`. Empty schema scope selects non-system schemas in the selected database(s). The companion connects only to the **first selected database**; database switching is unavailable. See the PUBLIC caveat below.
- **[Elasticsearch](https://www.elastic.co/elasticsearch):** only `indexPatterns`. Engine driver admission and native gateway routing share one rule: patterns must contain 1–255 bytes of lowercase ASCII letters, digits, `_`, `.`, `-`, `*` or `?`, and cannot begin with `.`, `-` or `_` (including `_all`). Other characters, including `+`, whitespace and non-ASCII letters, are rejected. Use simple patterns such as `app-*`. Empty scope resolves open indices, but hidden/system/dot indices remain inaccessible. Aliases and concrete backing targets are checked, not just the requested pattern.
- **[RabbitMQ](https://www.rabbitmq.com/):** only `vhosts`; names must contain 1–255 UTF-8 bytes without control characters. Empty scope grants existing vhosts, and reconciliation includes newly created vhosts. Every tool call must still name a permitted, nonempty `vhost`.

Prefer explicit, narrow scope even for read-only deployments. Scope controls managed grants and tool routing; it is not a substitute for reviewing pre-existing database privileges.

### Four switches for writes

All four must allow writes; otherwise write and destructive tools are absent from `tools/list` and calls are refused:

1. Cluster `spec.dataMcp.enabled: true` and `access: readWrite`.
2. Restricted token explicitly includes `cluster-data:write` and is **not** `readOnly`.
3. Token owner currently holds RBAC `cluster-data:write` on `<namespace>/<cluster>` (and `cluster-data:read` to connect).
4. Installation sets `KARAKORAM_MCP=readwrite`.

A token never expands its owner's current RBAC. Built-in viewer and operator roles do **not** include `cluster-data`; the administrator's wildcard grant includes it. See [RBAC policy format](access-control.md#policy-format).

## Connect an agent client

Authenticate `karactl` to the intended installation and mint a dedicated restricted token:

```sh
karactl mcp config --cluster production/appdb
karactl mcp config --cluster production/appdb --client claude-code
karactl mcp config --cluster production/appdb --client cursor
karactl mcp config --cluster production/appdb --client vscode
# Requests write scope, but does not bypass the other three switches:
karactl mcp config --cluster production/appdb --write --client json
```

`--cluster` requires `namespace/name`; it does not use the ordinary implicit namespace. Formats are `json` (default: `url` and `headers`), `claude-code` (a `claude mcp add --transport http` command), `cursor` (`mcpServers`) and `vscode` (`servers`, with `type: http`). Each invocation creates a new token scoped to both that namespace and cluster, with `cluster-data:read`; `--write` adds `cluster-data:write` and removes the read-only restriction. The default lifetime is **90 days**; override `--expires-in` with a positive [Go](https://go.dev/) duration or whole days, such as `24h` or `90d`. `--token-name` overrides the default `mcp-data-<cluster>-<hostname>` name.

**Output contains a credential shown only once.** Store it in protected client configuration; do not paste it into tickets, logs or source control. Revoke it through API token management when no longer needed. The bearer is a Karakoram API token, never a database password or infrastructure token.

The gateway rejects browser cookies, unrestricted/legacy tokens and missing explicit data permissions. Tokens must name at least one namespace or cluster; both restriction lists empty is denied, and every nonempty list must match the requested cluster. `Origin` must be absent or exactly match the configured public origin; [MCP](https://modelcontextprotocol.io/) sessions are bound to their token. Use the CLI's cluster-scoped tokens rather than broad manually constructed restrictions.

## Reviewed tool catalogue

These are manifest-approved tools, not a guarantee that every upstream tool is available with every database grant. Tools also need upstream availability, effective access and absence from `tools.deny`. Denied and unrecognized upstream tools are never listed. Read tools always execute through the read identity, even when writes are enabled.

### MongoDB/PSMDB — `mongodb-mcp-server` 3.1.0

| Class | Tools |
| --- | --- |
| Read | `find`, `count`, `explain`, `aggregate`, `aggregate-db`, `collection-indexes`, `collection-schema`, `collection-storage-size`, `db-stats`, `list-collections`, `list-databases`, `mongodb-logs` |
| Write | `create-collection`, `create-index`, `insert-many`, `rename-collection`, `update-many` |
| Destructive | `delete-many`, `drop-collection`, `drop-database`, `drop-index` |

All approved tools force `connectionId: preconfigured`; connection overrides are refused. The companion configures a maximum of **100 documents per query** and **30,000 ms** query time, disables server-side JavaScript, telemetry, request overrides and connection/Atlas/export/knowledge features. Database grants remain authoritative: listing a tool does not confer administrative privileges.

### PostgreSQL and Supabase — `pgedge-postgres-mcp` v1.1.0 manifest

| Class | Tools |
| --- | --- |
| Read | `query_database`, `get_schema_info`, `count_rows`, `execute_explain`, `read_resource` |
| Write | None |
| Destructive | `query_database_write` |

`query_database_write` is a gateway alias for `query_database` using the write identity. **Every call to the alias requires confirmation**, because arbitrary SQL can be destructive; it is not an ordinary write-class tool. Database switching, LLM/embedding/knowledge tools are unavailable. [Supabase](https://supabase.com/) uses the dedicated [PostgreSQL](https://www.postgresql.org/) identity, not `service_role` or an RLS-bypassing application credential.

### Elasticsearch — `native-elasticsearch` manifest 1

| Class | Tools |
| --- | --- |
| Read | `indices_list`, `mapping_get`, `search`, `esql_query`, `count`, `shards_get`, `index_stats` |
| Write | `doc_index`, `doc_update` |
| Destructive | `delete_by_query` |

Search size defaults to **10**, capped at **100**, with a forced **5-second** server timeout and total-hit tracking capped at **10,000**. ES|QL accepts a conservative `FROM`-and-pipeline subset and appends **`LIMIT 500`**. Hidden, system and dot indices are never reachable, including through aliases; **data streams with hidden/dot backing indices are refused**, not transparently queried. Native request/response bodies are capped at **4 MiB**, before the smaller gateway result bound. `delete_by_query` uses a 5-second timeout, scroll size 100 and `max_docs: 500`; this is still a destructive operation.

### RabbitMQ — `native-rabbitmq` manifest 1

| Class | Tools |
| --- | --- |
| Read | `overview`, `queues_list`, `queue_get`, `exchanges_list`, `bindings_list`, `consumers_list` |
| Write | `messages_peek`, `message_publish` |
| Destructive | `queue_purge` |

Every tool requires an explicit permitted **`vhost`**, including `overview`. List tools default to page **1**, `page_size: 100`, maximum **100** per page. Some lists are paginated locally after a bounded Management API response, so page size is not a database-load guarantee.

`overview` returns only the exact requested vhost object, including message statistics, selected locally from the Management API vhost collection. It requests summary columns, but [RabbitMQ](https://www.rabbitmq.com/) 4.3.6 ignores that column selection and fails on paginated vhost collections. The single-vhost endpoint requires administrator privileges; no such privileges are granted to data identities. The tool fails closed if the collection exceeds the **4 MiB** response bound, even when the requested vhost alone would fit.

`messages_peek` **consumes and requeues** messages: it is a write, not a read-only inspection. Count defaults to **5**, capped at **20**. Only `ackmode: ack_requeue_true` and `encoding: auto` are accepted; payloads are truncated at **64 KiB**. `message_publish` accepts a UTF-8 payload of at most **64 KiB**. `queue_purge` permanently removes ready messages and always requires confirmation.

## Confirmation, results and limits

Destructive tools always require gateway-run confirmation; `tools.confirmWrites: true` extends this to ordinary writes. The client must support **form elicitation and multi-round-trip tool calls (MRTR)**: the initial result requests input and supplies signed `requestState`; the continuation supplies that state and an accepted `confirm: true` response. Approval is bound to the cluster, token, tool, class and canonical effective-argument SHA-256, expires within the call timeout and is single-use. Missing capability, changed arguments, expiry, replay, decline or unavailable confirmation **fails closed**, without executing the tool. Upstream elicitation is not exposed as an alternative confirmation path; upstream resources are not proxied.

Treat all returned content as **untrusted data, not instructions**. For upstreams without their own wrapping, the gateway adds a fresh per-call nonce boundary. [MongoDB](https://www.mongodb.com/)'s manifest declares its upstream untrusted-data wrapping. Structured/non-text results are normalized into text, with no parallel unwrapped structured result. Results are truncated with `[Result truncated]` to fit the byte budget; output is not a complete export mechanism.

Default gateway limits:

| Limit | Value |
| --- | --- |
| Tool-call rate per token | 60 calls/minute, token-bucket burst 10 |
| Concurrent calls per cluster | 4, shared across tokens |
| HTTP request body | 1 MiB |
| Serialized result budget | 256 KiB; usable text is smaller to reserve JSON escaping/envelope/boundary space |
| Tool call timeout | 60 seconds, including confirmation |

Rate/concurrency denial uses [MCP](https://modelcontextprotocol.io/) error **`-32029`**. Limits reduce exposure, not query cost: aggregations, scans, local list pagination and requeued-message reads can still load production.

## Managed identities and rotation

Data credentials are generated and sealed, used privately by the gateway/companion and **never revealed through the API**. Logical system identities are `karakoram-mcp-ro` and, only for cluster `readWrite`, `karakoram-mcp-rw`. Narrowing scope replaces managed grants; disabling removes identities once reconciliation completes. Rotate them using the engine's existing **rotate** operation, targeting these logical system-user names; do not create application users with these reserved names. Rotation updates companion configuration/fingerprints where applicable.

| Engine | Database enforcement |
| --- | --- |
| MongoDB/PSMDB | Users in `admin`; scoped `read`/`readWrite`, or `readAnyDatabase`/`readWriteAnyDatabase` when unscoped. No user/cluster administration roles. |
| PostgreSQL/Supabase | Actual LOGIN roles **`kk_mcp_ro` / `kk_mcp_rw`**, no superuser, createdb, createrole, replication or bypass-RLS; connection limit **10** each. `statement_timeout: 30s`, `idle_in_transaction_session_timeout: 60s`; reader additionally defaults `default_transaction_read_only: on`. Explicit grants are CONNECT, schema USAGE and table SELECT; writer adds INSERT/UPDATE/DELETE and scoped sequence USAGE/SELECT so serial/identity-default inserts work. Matching default table and sequence privileges cover application owner roles. API-driven role changes trigger an ACL refresh; objects and owners created directly in SQL are discovered by an additive refresh every ten minutes while readiness is observed. No ownership or DDL grants are added. Unscoped reader also receives `pg_read_all_data`. |
| Elasticsearch | Roles **`karakoram_mcp_ro` / `karakoram_mcp_rw`**: cluster `monitor`; scoped index `read`, `view_index_metadata`, `monitor`, plus `write` for RW. `allow_restricted_indices: false`; gateway separately excludes all hidden/system/dot indices. No index/user administration grants. |
| RabbitMQ | Both identities have **`monitoring`** tag. Per permitted vhost, Configure is `^$`, Read is `.*`; Write is `^$` for RO and `.*` for RW. The gateway, not the tag alone, prevents RO message consumption. |

**[PostgreSQL](https://www.postgresql.org/) PUBLIC caveat:** reconciliation does not revoke PUBLIC CONNECT/object grants, because that would change application privileges. Scope is not an isolation boundary for objects already accessible to PUBLIC. GUCs are role defaults, not a SQL sandbox or immutable resource quota. Review PUBLIC grants, callable functions and application RLS before enabling an agent. SQL that requires privileges not granted above will fail even after approval.

## Companion operations and status

[MongoDB](https://www.mongodb.com/), [PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) add one private stateless service named **`<cluster>mcp`**, separate from database membership. It runs the Karakoram agent on the provider runtime ([Ubuntu](https://ubuntu.com/) 24.04 on [Zerops](https://zerops.io/)) with pinned [native artifacts](native-artifacts.md#data-mcp-companion-artifacts): `mongodb-mcp-server` **3.1.0** or `pgedge-postgres-mcp` **1.1.0**. Development builds require **`KARAKORAM_ARTIFACT_MIRROR`** or a published **`KARAKORAM_ARTIFACT_RELEASE`** containing the exact locked MongoDB bundle because it has no upstream download URL. Admission fails if the artifact URL cannot be resolved. See [artifact resolution](native-artifacts.md#download-and-verification).

PostgreSQL and Supabase companions connect directly to the active database members on private port **5432**, bypassing [ProxySQL](https://proxysql.com/) and [Supavisor](https://github.com/supabase/supavisor) even when configured. Multi-host connections use `target_session_attrs=prefer-standby` for reads (falling back to the primary when no standby is available) and `target_session_attrs=read-write` for writes. This preserves [pgEdge](https://www.pgedge.com/)/[pgx](https://github.com/jackc/pgx) extended-protocol compatibility; enabling Data [MCP](https://modelcontextprotocol.io/) does not enable a proxy reader listener. TLS-enabled clusters retain `sslmode=verify-full` with the cluster CA. All three providers provide companion-to-member private networking: Zerops project DNS, [Dokploy](https://dokploy.com/) target-overlay service aliases, and [Nomad](https://developer.hashicorp.com/nomad) native service discovery mapped to routable allocation addresses. Operator-managed network policies must permit these private connections; no public database exposure is required.

Companion resources default to shared CPU and 0.25–0.5 GiB RAM. `spec.dataMcp.resources` overrides those defaults. In an override, omitted/zero resource bounds, an omitted CPU mode and an omitted `startCpu` are unmanaged: providers with partial-update semantics may retain their existing values. Removing an override field does not request a reset or block later configuration and credential updates. Removing the entire resources override reapplies the controller's explicit defaults while leaving fields outside those defaults unmanaged.

The companion supervises `mcp-ro` and, with cluster write opt-in, `mcp-rw`. Each upstream binds only to loopback and requires a process-local bearer. MongoDB receives connection/authentication through environment variables; pgEdge uses protected configuration and SHA-256 token files. Secrets are not command arguments. The authenticated agent routes `/v1/datamcp/ro` and `/v1/datamcp/rw` over private HTTPS port **9443**, using the existing dialer/relay; never publish that port. Credentials, desired spec and certificate changes advance config version and trigger reload. Disabling/deleting waits for ownership-checked companion cleanup, including volume-preserving cluster suspension.

On Nomad, an externally purged companion job does not block disabling Data MCP: reconciliation expires the missing job's rollout receipt, deletes the retained logical service and clears its sealed companion identity and configuration. Database identity removal still follows the normal disable reconciliation.

Inspect `status.dataMcp` independently of database member readiness:

| Field | Meaning |
| --- | --- |
| `phase` | Schema allows `Disabled`, `Provisioning`, `Ready`, `Degraded`; disabled clusters currently omit the entire status object |
| `kind` | `companion` or `native` |
| `upstream` | Manifest/provider name |
| `upstreamVersion` | Companion artifact version or reviewed native toolset manifest version |
| `access` | Provisioned **cluster-level** `read`/`readWrite`, not a particular token's effective permission |
| `message` | Diagnostic such as artifact or companion readiness unavailable |

Companion readiness checks applied config version and the configured upstream processes. Native status reports the selected provider; it is not proof that a particular database query succeeds. Installation/token/RBAC write gates may still hide writes when status says `readWrite`.

## Risks and operating practice

Stored text can contain prompt injection. Boundaries and confirmations help an agent recognize data but cannot prevent it from following malicious instructions. Use dedicated accounts/tokens, explicit scopes, read-only access by default and a client that actually shows meaningful confirmations. Never put production authority behind unattended blanket approvals.

Queries can consume CPU, memory, I/O and locks despite result bounds. Start with low-risk datasets, monitor database load and audit history, and review engine-specific privileges. Confirmations are not transactions or rollback. A transport/audit error after execution does not prove a mutation was undone; inspect state before repeating it.

Sessions, confirmation signing keys and consumed challenges, and rate/concurrency counters are local to each control-plane process. [MCP](https://modelcontextprotocol.io/) clients need instance affinity; restarting that process ends its sessions and pending confirmations.

Every `tools/call`, including denied, pending, declined, rate-limited and upstream-error calls, attempts an **`Audit`** event with resource **`cluster-data`**, reason `DataMcpToolCall`, cluster, actor, tool target, class, outcome, duration, confirmation state and **`argsHash`** (SHA-256 of canonical arguments; effective arguments after manifest defaults/clamps on admitted calls). `auditQueryText: true` additionally records `args`, which may contain sensitive query text or document values. Audit recording failure returns an error; it does not roll back a completed data change. Protect audit access and retention accordingly.
