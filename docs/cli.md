<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# karactl CLI

`karactl` manages Karakoram through its REST API, not directly through Zerops.

## Install

Follow [install karactl](install.md#install-karactl) for release downloads and checksum verification. Match the CLI release to your server; connecting an older binary does not add commands. From a compatible release checkout with Go 1.25+:

```sh
go build -o /tmp/karactl ./cmd/karactl
install -Dm755 /tmp/karactl "$HOME/.local/bin/karactl"
```

Put `$HOME/.local/bin` on `PATH`. `make build` alternatively produces `bin/karactl`.

## Login and contexts

```sh
karactl --server https://example.com --context production login --username admin
karactl --context production health
karactl --context production whoami
karactl config set-context production --namespace production
karactl --context production logout
```

Login prompts without echo on a terminal; otherwise it reads the first stdin line. Supply noninteractive passwords from a secret manager or protected file, never arguments/history. Complete any mandatory password change in the UI first. Login opens a temporary cookie session, creates an owner-scoped API token, saves the context as current, then closes the session. Re-login saves the replacement before best-effort revocation of the previous token; follow any revocation warning.

`config set-context NAME` saves explicitly supplied `--server`, `--token` and `--namespace` flags, preserving other saved values. It selects the context only if none is current. Replacing a token this way clears its saved token ID. There is no `contexts` command; use `--context NAME` to select one. Outside login, an explicitly selected missing context is rejected.

Contexts are stored at `~/.config/karakoram/config.yaml`, independent of XDG overrides, with file mode 0600 and newly created directory mode 0700. Treat this as a credential file.

| Selection | Precedence/behavior |
| --- | --- |
| Server/token | Explicit flag → `KARAKORAM_SERVER` / `KARAKORAM_TOKEN` → saved context |
| Context | `--context` → saved current context → `default` |
| Object namespace | `-n` → context namespace → `default` |
| Cross-cluster collections | Without explicit `-n`, include all authorized namespaces, even if the context has a namespace |

Server URLs accept the origin or `/api/v1` suffix. For automation inject `KARAKORAM_SERVER` and `KARAKORAM_TOKEN` securely. Tokens are Karakoram credentials, not Zerops integration tokens. Logout normally revokes the matching saved token before clearing credentials; `logout --local` leaves server authority active. With an overriding token, revoke other saved tokens explicitly by ID if necessary.

## Requests, output and waits

- `-o table` is the default; detail output uses sections and list output summarizes nested fields. `-o json` and `-o yaml` preserve complete structured responses.
- `-f FILE` accepts API-shaped YAML/JSON; `-f -` reads stdin. A supplied file defines the body instead of convenience body flags. Use protected files for secrets. Exceptions: `rbac policy set` takes raw CSV; engine-native role/key commands use their documented definitions.
- Cluster create files contain `{name, spec}`. `edit` replaces the complete `{spec}` and can open the configured editor without a file. `patch`, `scale` and `upgrade` files contain `{spec: {...}}` merge patches; arrays replace the previous array, not append to it.
- Complete request files reject explicitly supplied convenience body flags (usage error, exit 2), on every command accepting such a file: creation/source flags, scale resource/replica/topology/component flags, upgrade/FCV version, balancer settings and RBAC permission fields must go in the file rather than silently overriding or being ignored. Put initial cluster provenance in `spec.dataSource` (`cluster`, optional `namespace`, and exactly one of `backup` or `pitrTime`); put restore provenance in `source`/`sourceNamespace`. Defaults do not override file fields. Execution controls such as namespace/output selection, API query flags and cluster `--wait`/`--timeout` remain supported; `restores create --file FILE --into-new NAME` still clones using the source recorded in the file. Elasticsearch-native role/API-key definition files retain their documented merge/conflict behavior.
- Create convenience flags default to `psmdb`. An omitted/empty `--version` leaves version selection to the selected provider's catalog. For presets/components, inspect `providers get PROVIDER -o json`, customize a preset into a complete `{name, spec}` file, then use `clusters create -f FILE`. There is no `--preset` flag. See [MongoDB](mongodb/README.md), [RabbitMQ](rabbitmq/README.md) and [Elasticsearch creation](elasticsearch/README.md#create).
- PostgreSQL/Supabase creation and current artifact restrictions: [PostgreSQL](postgres/README.md), [Supabase](supabase/README.md). Use `full`, `diff` or `incr` explicitly for their backups; generic `logical` is not a PostgreSQL type.
- `clusters connection NAME` prints a password-free summary. `--reveal` explicitly retrieves sensitive engine-admin credentials and CA; do not use that identity for applications or log its output. `clusters ca NAME` exports only PEM to stdout, regardless of output format, but still requires `cluster-credentials:read`. Follow [MongoDB access](mongodb/access-and-tls.md), [RabbitMQ access](rabbitmq/access-and-tls.md) or [Elasticsearch access](elasticsearch/access-and-security.md).
- Cluster mutation `--wait` reports acceptance/operation ID on stderr, then phase changes and a fifteen-second heartbeat. The default timeout is **30m**. It waits for the operation and observed cluster generation; an **Error or Degraded cluster fails the wait**. `clusters wait NAME` observes the current desired generation or deletion/suspension without resubmitting.
- `operations wait ID` succeeds on **Succeeded**, even when the cluster is Degraded; **Failed/Cancelled** fail. Operation success is not a health guarantee. See [operations and status](clusters.md#operations-and-status).
- Observation retries cover transient connection/timeouts and HTTP **408/429/502/503/504**, with bounded backoff and `Retry-After` handling for 429/503. Invalid URLs, untrusted certificates and nonexistent DNS names fail immediately. Timeout/interruption never cancels server work. Keep the operation ID and use the printed non-mutating resume command.
- Backup/restore creation has no `--wait`: use the returned operation ID with `operations wait`, then inspect `backups get` / `restores get`. Namespace deletion has no durable Operation or `--wait`; poll `namespaces get` and lifecycle events. See [namespaces](namespaces.md).
- `operations cancel ID` follows the operation's persisted namespace, not the selected namespace. It needs cluster-update permission; acceptance can leave Running work in cooperative cleanup and never rolls back desired state. Unsafe/terminal states return 409. See [REST API](api.md).
- `clusters logs` returns a bounded process-log tail: `--lines` defaults to **100**, range **1–10000**; `--process` selects a provider-supported process or `agent`. **`--cursor` currently has no effect**; the server also ignores the API's `since` input and returns no next cursor.
- `events watch` streams until interrupted; see [pagination and SSE](api.md#pagination-and-events). `openapi` downloads raw server YAML without authentication. `completion` generates scripts without configuration/server access. `version` always reports client metadata; absent/unreachable server metadata does not itself fail the command, and lookup is limited to five seconds.

`karactl` has **no `--idempotency-key` flag and does not send `Idempotency-Key`**. For keyed mutation retries use HTTP or the Go client; see [safe retries](api.md#safe-retries-with-idempotency-key). Do not repeat a mutation merely because waiting or receiving its response failed.

## Command groups

This index follows `karactl --help`; key subcommands are illustrative, not an exhaustive flag reference. **All** means a shared interface subject to provider capabilities and RBAC.

| Group/command | Purpose | Key subcommands | Engine applicability |
| --- | --- | --- | --- |
| `accounts` | Local control-plane accounts | list, get, create, set, delete, reset-password, change-password | All |
| `auth` | Advanced cookie/OIDC flows | refresh, oidc-login, oidc-callback | All |
| `backends` | Registered infrastructure kinds and capabilities | list | All |
| `backups` | Backup inventory and creation/deletion | create, list, get, delete, pitr-window | All; pitr-window MongoDB/PostgreSQL/Supabase |
| `clusters` | Cluster lifecycle/observation | create, list, get, status, members, placement, connection, ca, logs, metrics, edit, patch, scale, upgrade, restart, retry-deployments, replace-member, resume, rotate-tls, delete, wait, balancer, raise-fcv | All; balancer/raise-fcv MongoDB only |
| `completion` | Generate shell scripts | bash, zsh, fish, powershell | Local |
| `config` | Save connection contexts | set-context | Local |
| `db-roles` | Custom database roles | list, get, create, set, delete | MongoDB only |
| `db-users` | Database users | list, get, create, set, delete | MongoDB only |
| `elasticsearch` | Native access | users, roles, api-keys | Elasticsearch only |
| `events` | Durable events/audit and SSE | list, watch | All |
| `health` | Control-plane health | — | All |
| `help` | Runtime help | any command path | Local |
| `login` | Password login and saved API token | — | All |
| `logout` | Revoke saved token/remove credentials | — | All |
| `metrics` | Prometheus exposition | — | All |
| `namespaces` | Project registration/token/relay health | list, get, create, update, validate, rotate-token, delete | All |
| `openapi` | Download server specification | — | All |
| `operations` | Observe/cancel asynchronous work | list, cluster, get, wait, cancel | All; cancellation capability varies |
| `orphaned-volumes` | Inspect pending platform cleanup | list | All |
| `permissions` | Current effective permissions | — | All |
| `postgres` | Native database access | roles list/get/create/update/patch/delete/rotate-password, databases list/get/create/delete, grants list/grant/revoke | PostgreSQL and Supabase |
| `providers` | Catalogs/defaults/limits | list, get | All |
| `rabbitmq` | Native access | vhosts, users, permissions, topic-permissions, policies, operator-policies | RabbitMQ only |
| `rbac` | Permission checks and policy | can, policy get/set | All |
| `restores` | Recovery/clone inventory | create, list, get | All |
| `settings` | Sign-in configuration | get, set-oidc | All |
| `storages` | S3 destination registration/probe | list, get, create, set, delete, verify | All |
| `system-users` | Provider-managed identities | list, rotate | Provider-supported identities |
| `supabase` | API credentials, SMTP and edge functions | credentials, smtp-password set, functions list/get/deploy/delete, rotate-jwt-secret | Supabase only |
| `tokens` | Control-plane API tokens | list, get, create, set, delete (alias revoke) | All |
| `version` | Client/server build metadata | — | All |
| `whoami` | Authenticated identity | — | All |

`karactl clusters retry-deployments NAME [--namespace NS] [-o json]` explicitly [re-arms exhausted platform deployments](clusters.md#retry-exhausted-platform-deployments) after fixing the underlying platform failure; it requires `clusters/update` and returns a conflict if no deployment is exhausted.

For engine-native payloads and permissions, use [RabbitMQ access](rabbitmq/access-and-tls.md), [Elasticsearch access](elasticsearch/access-and-security.md) and [MongoDB](mongodb/README.md). For orphan inventory and manual cleanup, use [orphaned platform volumes](operations.md#orphaned-platform-volumes).
PostgreSQL payloads/TLS and proxy compatibility are in [PostgreSQL access](postgres/access-and-tls.md); Supabase credentials, SMTP and function constraints are in [Supabase operations](supabase/operations.md).

## Global flags

| Flag | Use |
| --- | --- |
| `--context NAME` | Select saved connection |
| `--server URL` | Override environment/context server |
| `--token TOKEN` | Override API token; prefer environment injection to avoid history/process listings |
| `-n, --namespace NAME` | Explicit namespace filter/object scope |
| `-o, --output FORMAT` | table (default), json, yaml |
| `--cookie NAME=VALUE` | Repeatable advanced-session cookie, not saved |
| `--csrf-token TOKEN` | Advanced-session CSRF value, not saved |
| `--insecure-skip-tls-verify` | Disable certificate verification; unsafe |
| `-h, --help` | Command help |

## Important command-specific flags

Run `karactl <group> <command> --help` for every flag. Query flags reach the API only when explicitly supplied; help defaults do not imply resource updates.

| Command | Important flags |
| --- | --- |
| `clusters create [name]` | `-f`, `--provider` (psmdb), `--version`, `--backup-storage`, repeatable `--schedule 'NAME=CRON,type=TYPE,retain=COUNT[,storage=NAME]'`, `--source-namespace`, `--wait`, `--timeout` |
| `clusters scale NAME` | `-f`, `--replicas`, CPU/RAM/disk `--min-*`/`--max-*`, `--start-cpu`, `--cpu-mode`, `--wait`, `--timeout`; topology/component flags are provider-specific |
| `clusters patch NAME` | `-f`, `--wait`, `--timeout` |
| `clusters delete NAME` | `--keep-volumes`, `--keep-backups` (both false unless supplied), `--yes`, `--wait`, `--timeout`; see [delete or suspend](clusters.md#delete-or-suspend) |
| `backups create CLUSTER [NAME]` | `-f`, `--storage` (cluster default), `--type` (CLI default logical; choose the engine-supported type in [backup guidance](backups.md)) |
| `backups list CLUSTER` | `--limit`, `--cursor` |
| `restores create CLUSTER [NAME]` | `-f`, `--backup`, `--time` (RFC3339, where supported), `--source-cluster`, `--source-namespace`, `--into-new`, `--yes` |
| `namespaces create` | `--name`, `--project-id` (backend target ID), `--token-file`, `--description`, `--backend` (zerops), repeatable `--config key=value` (non-secret backend settings; Zerops accepts none, its API URL is operator-only); no `-f` |
| `storages create/set` | `-f`; `verify NAME` checks connectivity separately; list has `--limit`, `--cursor`; delete has `--yes` |
| `tokens create/set` | `-f`; list has `--limit`, `--cursor`; delete/revoke has `--yes` |
| `events list` | `--cluster`, `--type`, `--operation`, `--actor`, `--resource`, `--order asc\|desc`, `--limit`, `--cursor` |
| `events watch` | Same scope filters; `--cursor`, `--last-event-id`; no list limit/order |
| `operations list/cluster` | `--phase`, `--limit`, `--cursor`; list also `--cluster` |
| `operations wait/cancel ID` | wait: `--timeout` (30m); cancel: `--yes` |

### Supabase native access

`karactl supabase credentials CLUSTER` masks API keys, the dashboard password and JWT secret in every output format. Pass `--show-secrets` only when sensitive terminal or redirected output is protected. The endpoint requires the separate `cluster-credentials:read` grant and returns `Cache-Control: no-store`.

`karactl supabase smtp-password set CLUSTER` reads the password through a hidden terminal prompt or the first stdin line; passwords are never accepted as command arguments. For example, pipe a secret manager's output directly into this command.

Use `karactl supabase functions list CLUSTER`, `functions get CLUSTER NAME`, `functions deploy CLUSTER NAME DIR_OR_FILE`, and `functions delete CLUSTER NAME`. Deploy sends a map of relative file paths to source text; directories are recursive and require a nonempty `index.ts` entrypoint. A standalone source file is deployed as `index.ts`. Symlinks and special files are rejected, and source text is limited to 1 MiB per function. A cluster supports at most 100 functions. Names match `[a-z0-9][a-z0-9_-]{0,62}`; `main` is reserved for the runtime dispatcher, and parent traversal is forbidden. Sources are stored sealed but remain readable to callers with cluster-read permission, so do not embed application secrets in source files.

Function deletion and `karactl supabase rotate-jwt-secret CLUSTER` require confirmation (or `--yes`). Rotation queues a durable `rotate-jwt-secret` operation; track the returned ID with `karactl operations get` or `operations wait`. SMTP and function mutations require cluster-update permission; audit events contain identifiers, never passwords or source text.

### PostgreSQL native access

The `postgres` command group also works with Supabase clusters, which share the PostgreSQL core:

```sh
karactl postgres roles list CLUSTER
karactl postgres roles get CLUSTER ROLE
karactl postgres roles create CLUSTER -f role.json
karactl postgres roles update CLUSTER ROLE -f role.json
karactl postgres roles rotate-password CLUSTER ROLE --yes
karactl postgres databases create CLUSTER -f database.json
karactl postgres databases list CLUSTER
karactl postgres databases get CLUSTER DATABASE
karactl postgres grants list CLUSTER --database DATABASE
karactl postgres grants grant CLUSTER -f grant.json
karactl postgres grants revoke CLUSTER -f grant.json --yes
karactl postgres databases delete CLUSTER DATABASE --force --yes
karactl postgres roles delete CLUSTER ROLE --yes
```

A role document contains `name`, `login` (default true), `createdb`, `createrole`, `connectionLimit` (default -1), `memberOf`, optional RFC3339 `validUntil`, and optional `password`. Omitted passwords on login-role creation are generated and returned once as `generatedPassword`; rotation returns a new generated password. Responses are `no-store`; an identical request with the same idempotency key safely replays its sealed result. Never put passwords in command arguments or commit credential-bearing request documents. Privileged `superuser`, `replication`, and `bypassrls` flags, privileged memberships, and changes to existing privileged role passwords additionally require cluster-update permission. Engine system roles are hidden and immutable; Supabase service roles are protected too.

A database document contains `name`, `owner`, optional `encoding` (UTF8), `locale` (C.UTF-8), and `allowConnections` (true). Encoding and locale cannot be changed after creation. A grant document contains `role`, `database`, `privileges`, and optional `schema`: database privileges are CONNECT/CREATE/TEMP; schema privileges are USAGE/CREATE. Predefined memberships include pg_read_all_data, pg_write_all_data, pg_monitor, pg_read_all_stats, and pg_signal_backend.

Managed login passwords are sealed and synchronized to every ProxySQL member without restarting PostgreSQL. Roles created directly in SQL are not routable through ProxySQL. Use the proxy hostname, cluster CA, and `sslmode=verify-full` for TLS connections; public endpoints are advertised when enabled and configured by the infrastructure backend. Direct data-member endpoints remain private. Role/database deletion, grant revocation, and password rotation require confirmation (or `--yes`).

Access operations use authenticated internal POST actions on a freshly resolved primary. HA members also require the Patroni leader lock; no writable primary produces HTTP 503. Only a precomputed SCRAM-SHA-256 verifier crosses the role-action protocol, never the plaintext password in SQL. Membership replacement is transactional; schema grants connect to the requested database. `roles patch CLUSTER ROLE -f patch.json` changes only supplied fields; `roles update` replaces role attributes.

ProxySQL does not implement every PostgreSQL protocol feature. In particular, clients requesting different result formats per column must select a uniform format (for pgx, use simple protocol or request text results). Supabase's internal services bypass ProxySQL for features such as LISTEN/NOTIFY and logical replication.

## Safety and exit statuses

Deletion, in-place restores, member replacement, compatibility-version raises, credential/TLS/namespace-token rotation, account password resets and operation cancellation prompt **Type the name to confirm** when stdin is a terminal. Compound identities use the final argument; in-place restore asks for the target cluster. `--yes`/`-y` skips confirmation and is required when stdin carries a request body. Without a terminal, one line containing `y` or `yes` (case-insensitive) confirms; another nonempty answer cancels (1), and empty input/EOF is a usage error (2), without mutation.

Repeated cluster deletion with unchanged retention options returns the same in-flight Operation; changing either option supersedes its tracking record. `storages verify` fails even after HTTP success when `verified: false`. Errors include available field/reason/resource context on stderr.

| Exit | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Generic failure, failed/cancelled operation, declined confirmation, failed storage verification |
| 2 | Usage/argument/flag error or missing noninteractive confirmation |
| 3 | HTTP 404 |
| 4 | HTTP 409 |
| 5 | HTTP 401/403 |
| 6 | HTTP 400/415/422 validation/media error |
| 7 | Wait deadline or HTTP 408/504; accepted work may continue |
