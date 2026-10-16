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
- Create convenience flags default to `psmdb`. An omitted/empty `--version` leaves version selection to the selected provider's catalog. For presets/components, inspect `providers get PROVIDER -o json`, customize a preset into a complete `{name, spec}` file, then use `clusters create -f FILE`. There is no `--preset` flag. See [MongoDB](mongodb/README.md), [RabbitMQ](rabbitmq/README.md) and [Elasticsearch creation](elasticsearch/README.md#create).
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
| `backups` | Backup inventory and creation/deletion | create, list, get, delete, pitr-window | All; pitr-window MongoDB only |
| `clusters` | Cluster lifecycle/observation | create, list, get, status, members, placement, connection, ca, logs, metrics, edit, patch, scale, upgrade, restart, replace-member, resume, rotate-tls, delete, wait, balancer, raise-fcv | All; balancer/raise-fcv MongoDB only |
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
| `providers` | Catalogs/defaults/limits | list, get | All |
| `rabbitmq` | Native access | vhosts, users, permissions, topic-permissions, policies, operator-policies | RabbitMQ only |
| `rbac` | Permission checks and policy | can, policy get/set | All |
| `restores` | Recovery/clone inventory | create, list, get | All |
| `settings` | Sign-in configuration | get, set-oidc | All |
| `storages` | S3 destination registration/probe | list, get, create, set, delete, verify | All |
| `system-users` | Provider-managed identities | list, rotate | Provider-supported identities |
| `tokens` | Control-plane API tokens | list, get, create, set, delete (alias revoke) | All |
| `version` | Client/server build metadata | — | All |
| `whoami` | Authenticated identity | — | All |

For engine-native payloads and permissions, use [RabbitMQ access](rabbitmq/access-and-tls.md), [Elasticsearch access](elasticsearch/access-and-security.md) and [MongoDB](mongodb/README.md). For orphan inventory and manual cleanup, use [orphaned platform volumes](operations.md#orphaned-platform-volumes).

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
| `storages create/set` | `-f`; `verify NAME` checks connectivity separately; list has `--limit`, `--cursor`; delete has `--yes` |
| `tokens create/set` | `-f`; list has `--limit`, `--cursor`; delete/revoke has `--yes` |
| `events list` | `--cluster`, `--type`, `--operation`, `--actor`, `--resource`, `--order asc\|desc`, `--limit`, `--cursor` |
| `events watch` | Same scope filters; `--cursor`, `--last-event-id`; no list limit/order |
| `operations list/cluster` | `--phase`, `--limit`, `--cursor`; list also `--cluster` |
| `operations wait/cancel ID` | wait: `--timeout` (30m); cancel: `--yes` |

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
