<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# karactl CLI

`karactl` uses Karakoram's generated REST client; it does not call Zerops directly.

## Build and install

From a release checkout, build and install the CLI with Go 1.24 or newer:

```sh
go build -o /tmp/karactl ./cmd/karactl
install -Dm755 /tmp/karactl "$HOME/.local/bin/karactl"
```

Put `$HOME/.local/bin` on `PATH`, then run `karactl --help`. Alternatively, `make build` produces `bin/karactl`. The `rabbitmq` command group requires a binary built from a release containing RabbitMQ support; connecting an older binary to a newer server does not add commands. See the [RabbitMQ walkthrough](rabbitmq.md) for first-cluster, TLS and least-privilege examples.

## Login and contexts

```sh
karactl --server https://YOUR-SUBDOMAIN --context production login --username admin
karactl --context production health
karactl --context production whoami
karactl --context production logout
```

Login prompts without echo on a terminal, starts a temporary password session, creates an owner-scoped API token, saves it and closes the browser-style session. Finish a forced password change in the UI before CLI login. Login saves the selected context as current. There is no separate `contexts` command: `--context NAME` selects a saved connection, and login creates/updates it. An explicitly selected nonexistent context is rejected outside login.

With non-terminal stdin, login reads the password from the first line. For example, pipe a secret manager's output or redirect a protected file:

```sh
karactl --server https://YOUR-SUBDOMAIN --context production login --username admin < password.txt
```

Do not put passwords in arguments or shell history. Re-login saves the replacement token before revoking the previously saved context token, including when changing that context's server. Revocation is best effort: a warning means the new context is usable but the old token must be revoked separately.

Contexts live at `~/.config/karakoram/config.yaml` (independent of XDG overrides), in owner-only files (0600) and a newly created directory (0700). Each holds server, token, tokenID and an optional namespace. Protect it like a credential. Server/token precedence is **explicit flags > KARAKORAM_SERVER/KARAKORAM_TOKEN > selected saved context**. Namespace precedence for object commands is **`--namespace/-n` > context namespace > `default`**; collection commands without an explicit `-n` include all authorized namespaces. With no explicit context selection, the saved currentContext is used, otherwise `default`. Server URLs may use the origin or end in `/api/v1`; root health/metrics still use the installation base.

For automation inject `KARAKORAM_SERVER` and `KARAKORAM_TOKEN` through a secret manager. Avoid `--token` in process listings/history. Karakoram API tokens are not Zerops integration tokens. Logout normally revokes the matching saved CLI token before clearing local credentials; `logout --local` deliberately leaves server authority active. With an overriding token, do not assume logout revoked a different saved token—revoke its ID explicitly when needed.

## Requests, output and waits

- `-o table` is default; resource details use hierarchical sections rather than flattened nested cells, while lists stay compact and summarize nested fields with a hint to structured output. Numbers use decimal rather than scientific notation. Provider lists summarize engines, versions/defaults, topology types and preset names. Provider details show one row per topology with key defaults, separate limits and `from → to` upgrade paths; UI/component schemas and full resource defaults remain available with `-o json`/`-o yaml`. Status conditions appear as rows. Structured output preserves the full response unchanged.
- `karactl openapi > openapi.yaml` downloads the running server's authoritative OpenAPI document as raw YAML. This public endpoint does not require login.
- `-f FILE` accepts API-shaped YAML/JSON; `-f -` reads stdin. Use protected input for secret fields. When supplied, the file defines the request body instead of convenience body flags.
- Cluster **create** files contain `name` and `spec`; **edit** replaces the full `spec` and can open the configured editor when no file is supplied; **patch/scale/upgrade** files have `{spec: {...}}` merge-patch shape. Arrays such as schedules are replaced, not appended.
- Convenience create flags default to the `psmdb` provider and its topology/resource defaults. `--version` is optional: when omitted or empty, the CLI omits `spec.version` so the server applies the selected provider's catalog default. Supply `--version VERSION` to pin an exact version. For another provider, presets or optional components, run `karactl providers get <provider> -o json`, copy a preset's `spec` into a JSON/YAML body with `name` and `spec`, customize components, then submit with `clusters create -f FILE`. There is no `--preset` flag.
- `clusters create --backup-storage NAME` sets the default backup destination. A `--schedule` can instead supply its own `storage=NAME`; without `--backup-storage`, the CLI omits the optional default storage field, including for `--pitr` alone. Each schedule still needs either its own storage or the default, and the server validates PITR prerequisites.
- `rbac policy set -f FILE` is the exception: the file contains raw policy CSV.
- `clusters connection NAME` prints a password-free endpoint/TLS summary without requiring credentials-read permission. CA availability is not checked on this path; TLS-disabled clusters report that no CA is configured. Add `--reveal` to display sensitive engine credentials, copyable URIs and the CA as a multiline PEM block. Do not capture revealed output in logs.
- `karactl clusters ca NAME > ca.pem` exports only the PEM to stdout, regardless of output format. It uses the existing connection API and requires its separate `cluster-credentials:read` permission. Use `-n NAMESPACE` as with other cluster commands; an unavailable CA is an error.
- Cluster mutations with `--wait` immediately report acceptance and the cluster/generation or operation ID on stderr, then print phase/message changes and a progress heartbeat every fifteen seconds. Default timeout is thirty minutes; `--timeout` changes it. Observation failures from HTTP 408/429/502/503/504, connection errors and timeouts retry with bounded backoff until the deadline, with a short stderr note; the mutation is never resubmitted. Timeout or interruption does not cancel server work. An unfinished wait reports acceptance, the known operation ID and a non-mutating resume command, including when observation fails with a non-retryable error. `karactl clusters wait NAME --timeout 30m` observes the current desired generation (or deletion/suspension), and `karactl operations wait ID --timeout 30m` observes a persisted operation. Keep the same context/server and namespace; never repeat create merely to resume waiting. Suspension progress compares the observed generation with the operation's target generation, or the cluster's current desired generation if no target is returned.
- Observation retries are limited to timeouts, connection refusal/reset and truncated connections, plus the HTTP statuses above. Invalid server URLs, untrusted certificates and DNS names that do not exist fail immediately. HTTP 429/503 `Retry-After` delays are honored up to the remaining wait deadline. Once an operation succeeds, subsequent cluster-observation failures resume with `clusters wait`; a reported Error/Degraded cluster is a failure, not an unfinished operation wait.
- Backup/restore creation returns accepted records; inspect the corresponding Operation and record with `operations wait ID`, `backups get` or `restores get`. Those creation commands do not have an invented `--wait` flag.
- `events watch` streams SSE, supports replay/filter flags, and runs until interrupted. `operations wait` polls a persisted operation until its terminal phase.
- `karactl operations cancel ID` requests cancellation with cluster-update permission on the operation's recorded namespace/cluster. Pending work is cancelled immediately; Running work stops only at provider-supported safe checkpoints and may remain Running during cleanup. Terminal/unsafe states return 409. Cancellation does not undo accepted desired state or completed database work; inspect the returned operation and resume `operations wait ID`. See [API cancellation](api.md#cancel-an-operation).
  In a terminal, type the operation ID to confirm; `--yes` skips this prompt. Cancellation follows the persisted operation's namespace, not the current namespace selection.
- `events list --order asc|desc` selects oldest/newest-first ordering. The server's default applies when the flag is omitted.
- `completion bash|zsh|fish|powershell` generates a shell script without loading configuration or contacting a server.
- `tokens revoke ID` is an alias of `tokens delete ID`, with identical confirmation and exit behavior.
- Events and Operations show the initiating `actor` alongside `namespace` in table/JSON/YAML output, including cluster-scoped views and event watch. `events list --actor VALUE --resource VALUE` and `events watch --actor VALUE --resource VALUE` add exact, case-sensitive AND filters that compose with `--namespace/-n`; omit a flag to leave it unrestricted. Cluster records require `events:read` or `audit:read` on `<namespace>/<cluster>`, namespace-level audit requires `audit:read` on `<namespace>`, and global installation audit requires it on `*`. Use the full persisted account/token actor string and preserve all filters with `--cursor`.
- `--cookie`/`--csrf-token` and the `auth` commands support advanced cookie/OIDC API flows; normal automation should use bearer tokens. `auth oidc-login` reports a redirect, not an interactive browser/device login workflow.
- `--insecure-skip-tls-verify` is unsafe and unnecessary with a properly trusted HTTPS certificate.

Examples and safety prerequisites: [namespaces](namespaces.md), [replica sets](replica-sets.md), [sharding](sharding.md), [RabbitMQ](rabbitmq.md), [backup/restore](backups-and-restore.md), [scaling/upgrades](scaling-and-upgrades.md), [users/TLS](users-tls-access.md), [RBAC](rbac-and-auth.md), [API](api.md).

## Safety and exit statuses

Deleting clusters, backups, storages, API tokens, accounts, namespaces, database users or roles prompts **Type the name to confirm** when stdin is a terminal. The same safeguard applies to in-place restores, member replacement, compatibility-version raises, system-user/TLS/namespace-token rotation, account password resets and operation cancellation. For compound identities, type the final argument (the backup/member/user/role); in-place restore asks for the target cluster. `--yes` / `-y` skips confirmation without reading stdin. Without a terminal, the command reads one line: `y` or `yes` (case-insensitive) confirms; any other non-empty answer cancels with exit status 1 and no mutation submitted. Empty input or EOF without an answer exits with status 2 and asks for `--yes` for non-interactive use. Commands reading a payload from stdin (`-f -` or `--token-file -`) require `--yes` before reading any payload; they never use payload bytes as confirmation. Cloning with `restores create --into-new` does not replace an existing target and does not require destructive confirmation. Repeated deletion of a cluster already Deleting reports that deletion is already underway; it does not imply that cleanup has finished.

With unchanged `--keep-volumes` and `--keep-backups` options, a repeated cluster
delete returns the existing in-flight Operation; output and `--wait` use that
same operation ID. Changing either option supersedes the previous tracking
record with a new Operation.

Every failure exits non-zero, including a successful HTTP storage-verification response with `verified: false`. Output retains the verification message. Server errors print their human message plus available field, reason and resource details on stderr; command-line mistakes also print usage on stderr. Locally rejected flag values (including `--output` and `--shards` bounds, and `--shards`, `--config-servers` or `--routers` on `clusters create` without `--topology sharded`) and missing required convenience flags exit with status 2; request bodies rejected by the server exit with status 6.

| Exit status | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Generic failure, including failed operations, declined confirmation and failed storage verification |
| 2 | Usage error: unknown/missing subcommand, invalid arguments or flags, missing non-interactive confirmation |
| 3 | Resource not found (HTTP 404) |
| 4 | Resource conflict (HTTP 409) |
| 5 | Authentication or permission failure (HTTP 401/403) |
| 6 | Request validation/media error (HTTP 400/415/422) |
| 7 | Wait deadline exceeded (including persistent transient observation failures) or HTTP timeout (408/504); accepted work may continue |


## Orphaned platform volumes

List Local Storage volumes whose platform deletion was refused after their member runtimes were deleted:

```sh
karactl orphaned-volumes list
karactl orphaned-volumes list -o json
```

The default output is a table; `-o json` preserves the structured `{items: [...]}` response for automation. The inventory is read-only and includes only records whose original cluster you have permission to read; administrators see all records, even after cluster deletion. The control-plane janitor continues retrying deletion independently of this command. See [operations](../operations.md#orphaned-platform-volumes) for retry timing, events and manual Zerops cleanup.

## RabbitMQ access

The `rabbitmq` group administers RabbitMQ clusters through their Management API. It accepts the same `--namespace/-n` selection and `-o table|json|yaml` output options as other cluster commands. All requests require the corresponding `database-users` read/create/update/delete permission on the cluster; other providers return HTTP 409.

Each of `vhosts`, `users`, `permissions`, `topic-permissions`, `policies`, and `operator-policies` has `list`, `get`, `create`, `update`, and `delete` commands. Creation and update use API-shaped YAML/JSON with `-f FILE` or `-f -`. An update body must match the identities supplied in its arguments.

```sh
karactl rabbitmq vhosts list brokers -n production -o json
karactl rabbitmq vhosts get brokers /
karactl rabbitmq vhosts create brokers -f vhost.yaml
karactl rabbitmq users create brokers -f user.yaml -o json
karactl rabbitmq permissions update brokers / application -f permission.yaml
karactl rabbitmq policies get brokers / retention
karactl rabbitmq operator-policies delete brokers / safety --yes
```

Deletes require confirmation: interactively type the final argument (the vhost,
user, or policy name shown in the prompt), not the cluster name. Without a TTY,
pass `--yes` or pipe `y`/`yes`. An empty non-interactive input submits no request.

Pass vhost `/` literally in CLI arguments and UI fields. A raw HTTP API client must encode the name exactly once as `%2F` in a path segment, never `%252F`; generated clients accept the raw name. See the [API path example](rabbitmq.md#access-api-cli-and-ui).

A vhost body contains `name`, `tags`, optional `defaultQueueType` (`classic`, `quorum`, or `stream`), `description`, and `limits: {maxConnections: 100, maxQueues: 1000}`. A limit of `-1` means unlimited. A user body contains `name`, `tags`, and optionally `password` or `passwordHash` (mutually exclusive). Valid user tags are `management`, `policymaker`, `monitoring`, `administrator`, and `impersonator`; an empty tags array creates a non-management application user.

When a user creation omits its password, `generatedPassword` is returned **once**. Store it securely; subsequent reads never return passwords or password hashes. Updates that omit both password fields preserve the existing password. The protected identities `karakoram`, `admin`, `monitor` and `guest` cannot be created, modified or deleted through access commands, nor can their permission/topic-permission grants be changed there. Only `karakoram` and `admin` are rotatable system users: use `karactl -n NS system-users rotate brokers admin` (or `karakoram`). `guest` and `monitor` are reserved names that cannot be created or modified; they are not rotation targets.

The Access UI sends `Idempotency-Key` on create/update requests and retains it
when retrying an unchanged submission. Direct HTTP clients can also supply the header.
Matching retries replay the original successful response for 24 hours, including
the generated password; a changed body with the same key returns 409. This does
not make passwords recoverable through GET. See [safe API retries](api.md#safe-retries-with-idempotency-key).

Permission bodies contain `vhost`, `user`, and the `configure`, `write`, and `read` regular expressions. Topic permission bodies instead contain `exchange`, `write`, and `read`. Topic `get` and `delete` identify a `vhost`/`user` pair: **delete removes all topic permissions for that pair**, not only one exchange. Policy bodies contain `vhost`, `name`, `pattern`, `applyTo`, a nonnegative `priority`, and a JSON-object `definition`. Both regular and operator policy commands use this shape.

Patterns use RabbitMQ's PCRE syntax, including lookarounds and backreferences. Karakoram permits patterns of at most 4096 bytes and forwards them unchanged; RabbitMQ validates their syntax. Empty vhost/topic permission patterns deny access. Regular and operator policy patterns must remain nonempty. Invalid permission regexes and rejected policy-definition keys return field errors; correct the reported regex field or `definition` rather than retrying the unchanged payload.

User tags do not grant messaging permissions. See the [complete least-privilege vhost/user/grant payloads](rabbitmq.md#least-privilege-application-access), including the `amq.default` write permission required when publishing to the default exchange. Vhost audit events name the vhost under resource `rabbitmq-vhosts`; filter them with `karactl -n NS events list --cluster brokers --resource rabbitmq-vhosts`.

## Generated command tree

Use `karactl --help` and each subcommand's `--help` for the current command and flag reference. Make installs the CLI as `bin/karactl`. `[flags]` and inherited global flags are omitted from tree rows for readability.

Use `--namespace NAME` (`-n NAME`) to select a project namespace. Single-object, create and mutation commands resolve **flag > saved context namespace > `default`**. Unfiltered collections show all authorized namespaces and a `NAMESPACE` table column; an explicit `-n` filters them even when a context has a namespace saved. Set that optional context value with `karactl config set-context NAME --namespace PROJECT`. Namespace tokens are accepted only through `namespaces create/rotate-token --token-file FILE` (`-` reads stdin). Restore and clone commands accept `--source-namespace`; `restores create --into-new` creates its target in the namespace selected by `-n`.

```text
karactl
  accounts
    change-password
    create
    delete <account>
    get <account>
    list
    reset-password <account>
    set <account>
  auth
    oidc-callback
    oidc-login
    refresh
  backups
    create <cluster> [name]
    delete <cluster> <backup>
    get <cluster> <backup>
    list <cluster>
    pitr-window <cluster>
  clusters
    balancer
      get <cluster>
      set <cluster>
    ca <cluster>
    connection <cluster>
    create [name]
    delete <cluster>
    edit <cluster>
    get <cluster>
    list
    logs <cluster> <member>
    members <cluster> [member]
    metrics <cluster>
    patch <cluster>
    placement <cluster>
    raise-fcv <cluster>
    replace-member <cluster> <member>
    restart <cluster>
    resume <cluster>
    rotate-tls <cluster>
    scale <cluster>
    status <cluster>
    upgrade <cluster>
    wait <cluster>
  completion
    bash
    fish
    powershell
    zsh
  config
    set-context <name>
  db-roles
    create <cluster>
    delete <cluster> <database> <role>
    get <cluster> <database> <role>
    list <cluster>
    set <cluster> <database> <role>
  db-users
    create <cluster>
    delete <cluster> <database> <username>
    get <cluster> <database> <username>
    list <cluster>
    set <cluster> <database> <username>
  elasticsearch
    api-keys
      create <cluster>
      invalidate <cluster> <id>
      list <cluster>
    roles
      create <cluster> <name>
      delete <cluster> <name>
      get <cluster> <name>
      list <cluster>
      update <cluster> <name>
    users
      create <cluster> <name>
      delete <cluster> <name>
      get <cluster> <name>
      list <cluster>
      update <cluster> <name>
  events
    list
    watch
  health
  login
  logout
  metrics
  namespaces
    create
    delete <namespace>
    get <namespace>
    list
    rotate-token <namespace>
    update <namespace>
    validate <namespace>
  operations
    cluster <cluster>
    get <id>
    list
    wait <id>
  orphaned-volumes
    list
  permissions
  providers
    get <provider>
    list
  rbac
    can
    policy
      get
      set
  restores
    create <cluster> [name]
    get <cluster> <restore>
    list <cluster>
  settings
    get
    set-oidc
  storages
    create
    delete <storage>
    get <storage>
    list
    set <storage>
    verify <storage>
  system-users
    list <cluster>
    rotate <cluster> <user>
  tokens
    create
    delete <id>
    get <id>
    list
    revoke <id> (alias of delete)
    set <id>
  version
  whoami
  help [command]
```

## Global flags

```text
      --context string             Named connection context
      --cookie stringArray         Session/OIDC cookie NAME=VALUE (advanced auth flows; not saved)
      --csrf-token string          Session CSRF token (advanced auth flows; not saved)
  -h, --help                       help for karactl
      --insecure-skip-tls-verify   Disable server certificate verification (unsafe)
  -o, --output string              Output: table, json, yaml (default "table")
      --server string              Server URL (overrides KARAKORAM_SERVER and context)
      --token string               API token (prefer KARAKORAM_TOKEN to avoid shell history)
```

## Command-specific flags

Commands absent below have no additional flags except confirmation: destructive commands described above accept `-y, --yes`. Query flags only reach the API when explicitly supplied; help defaults do not necessarily change existing resources. For full request schemas see [OpenAPI](../../api/openapi.yaml).

### `karactl accounts change-password`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl accounts create`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl accounts list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl accounts reset-password`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl accounts set`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl auth oidc-callback`

```text
      --code string    API code parameter
      --state string   API state parameter
```

### `karactl auth oidc-login`

```text
      --return-to string   API returnTo parameter
```

### `karactl backups create`

```text
  -f, --file string      API request YAML/JSON file ('-' for stdin)
      --storage string   Backup storage (defaults to cluster configuration)
      --type string      logical or physical (default "logical")
```

### `karactl backups list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl clusters balancer set`

```text
      --enabled            Enable balancing (default true)
  -f, --file string        API request YAML/JSON file ('-' for stdin)
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters connection`

```text
      --reveal   Explicitly display database credentials
```

### `karactl clusters wait`

```text
      --timeout duration   Maximum wait duration (default 30m0s)
```


### `karactl clusters create`

```text
      --allow strings            Public firewall CIDR allowlist
      --backup-storage string    Default backup storage
      --component string         Scale resources of engine, configServer or router (default "engine")
      --config-servers int       Config-server replicas (default 3)
      --cpu-mode string          SHARED or DEDICATED (default "SHARED")
      --engine-config string     Provider configuration overrides file
      --engine-options string    Provider options as a JSON object or @file.json
  -f, --file string              API request YAML/JSON file ('-' for stdin)
      --from-backup string       Seed from a backup
      --from-pitr string         Seed from RFC3339 point in time
      --ip-type string           Public IP type: IPV6 or IPV4 (default "IPV6")
      --max-cpu float            CPU cores (default 2)
      --max-disk float           Resource size in GiB (default 10)
      --max-ram float            Resource size in GiB (default 2)
      --members int              Engine replicas per replica set (default 3)
      --min-cpu float            CPU cores (default 1)
      --min-disk float           Resource size in GiB (default 5)
      --min-ram float            Resource size in GiB (default 1)
      --monitoring               Enable monitoring (default true)
      --oplog-span-minutes int   PITR oplog slice interval (default 10)
      --pitr                     Enable continuous point-in-time recovery
      --provider string          Engine provider (default "psmdb")
      --public                   Expose public TCP endpoints
      --replicas int             Engine replicas (default 3)
      --routers int              Router replicas (default 2)
      --schedule stringArray     Schedule: NAME=CRON,type=TYPE,retain=COUNT[,storage=NAME]
      --shards int               Shard count (default 1)
      --source-cluster string    Source cluster for initial data
      --start-cpu float          CPU cores (default 1)
      --timeout duration         Maximum wait duration (default 30m0s)
      --tls-mode string          disabled, preferTLS or requireTLS (default "requireTLS")
      --topology string          Provider topology (replicaSet, sharded, cluster or dedicatedMasters) (default "replicaSet")
      --version string           Exact engine version (defaults to provider catalog default; see providers get <provider>)
      --wait                     Wait for operation completion and observed cluster generation
```

The default provider is explicitly `psmdb`. When `--provider` selects another
engine, create sends no implicit MongoDB topology, replica-count, or resource
defaults; the provider fills omitted settings. Use `--topology` and `--replicas`
to supply generic overrides, and `--engine-options '{"option":true}'` or
`--engine-options @options.json` for a provider-validated JSON object. Supported
backup types and operations are advertised by `karactl providers get PROVIDER`.

### `karactl clusters delete`

```text
      --keep-backups       API keepBackups parameter
      --keep-volumes       API keepVolumes parameter
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters edit`

```text
  -f, --file string        API request YAML/JSON file ('-' for stdin)
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl clusters logs`

```text
      --cursor string    API cursor parameter
      --lines int        API lines parameter (default 100)
      --process string   API process parameter
```

### `karactl clusters metrics`

```text
      --range string   API range parameter
      --step string    API step parameter
```

### `karactl clusters patch`

```text
  -f, --file string        API request YAML/JSON file ('-' for stdin)
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters raise-fcv`

```text
  -f, --file string        API request YAML/JSON file ('-' for stdin)
      --timeout duration   Maximum wait duration (default 30m0s)
      --version string     Target version
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters replace-member`

```text
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters restart`

```text
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters resume`

```text
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters rotate-tls`

```text
      --timeout duration   Maximum wait duration (default 30m0s)
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl clusters scale`

```text
      --component string     Scale resources of engine, configServer or router (default "engine")
      --config-servers int   Config-server replicas (default 3)
      --cpu-mode string      SHARED or DEDICATED (default "SHARED")
  -f, --file string          API request YAML/JSON file ('-' for stdin)
      --max-cpu float        CPU cores (default 2)
      --max-disk float       Resource size in GiB (default 10)
      --max-ram float        Resource size in GiB (default 2)
      --members int          Engine replicas per replica set (default 3)
      --min-cpu float        CPU cores (default 1)
      --min-disk float       Resource size in GiB (default 5)
      --min-ram float        Resource size in GiB (default 1)
      --replicas int         Engine replicas (default 3)
      --routers int          Router replicas (default 2)
      --shards int           Shard count (default 1)
      --start-cpu float      CPU cores (default 1)
      --timeout duration     Maximum wait duration (default 30m0s)
      --wait                 Wait for operation completion and observed cluster generation
```

### `karactl clusters upgrade`

```text
  -f, --file string        API request YAML/JSON file ('-' for stdin)
      --timeout duration   Maximum wait duration (default 30m0s)
      --version string     Target version
      --wait               Wait for operation completion and observed cluster generation
```

### `karactl completion bash`

```text
      --no-descriptions   disable completion descriptions
```

### `karactl completion fish`

```text
      --no-descriptions   disable completion descriptions
```

### `karactl completion powershell`

```text
      --no-descriptions   disable completion descriptions
```

### `karactl completion zsh`

```text
      --no-descriptions   disable completion descriptions
```

### `karactl db-roles create`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl db-roles list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl db-roles set`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl db-users create`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl db-users list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl db-users set`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl events list`

```text
      --actor string       API actor parameter
      --cluster string     API cluster parameter
      --cursor string      API cursor parameter
      --limit int          API limit parameter (default 100)
      --order string       Event order: asc or desc (server default when omitted)
      --operation string   API operationId parameter
      --resource string    API resource parameter
      --type string        API type parameter
```

### `karactl events watch`

```text
      --actor string           API actor parameter
      --cluster string         API cluster parameter
      --cursor string          API cursor parameter
      --last-event-id string   API Last-Event-ID parameter
      --operation string       API operationId parameter
      --resource string        API resource parameter
      --type string            API type parameter
```

### `karactl login`

```text
      --username string   Local account name (default "admin")
```

### `karactl logout`

```text
      --local   Only remove local credentials (server token remains active)
```

### `karactl operations cluster`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
      --phase string    API phase parameter
```

### `karactl operations list`

```text
      --cluster string   API cluster parameter
      --cursor string    API cursor parameter
      --limit int        API limit parameter (default 100)
      --phase string     API phase parameter
```

### `karactl operations wait`

```text
      --timeout duration   Maximum wait duration (default 30m0s)
```

### `karactl rbac can`

```text
      --action string     RBAC action (default "read")
  -f, --file string       API request YAML/JSON file ('-' for stdin)
      --object string     RBAC object (default "*")
      --resource string   RBAC resource
      --subject string    Optional subject
```

### `karactl rbac policy set`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl restores create`

```text
      --backup string           Backup name
  -f, --file string             API request YAML/JSON file ('-' for stdin)
      --into-new string         Clone source spec and restore into a new cluster
      --source-cluster string   Source cluster (defaults to target)
      --time string             RFC3339 restore time
```

### `karactl restores list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl settings set-oidc`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl storages create`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

New storage names use 2–63 lowercase letters, digits or hyphens, beginning with
a letter; a trailing hyphen is allowed. Existing stored names remain usable.

### `karactl storages list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl storages set`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

Connection or credential changes clear the stored verification result. Run
`karactl storages verify <storage>` after saving; description-only edits keep the
latest result. Storage reads include optional `verification` only after a check.

### `karactl tokens create`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

### `karactl tokens list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl tokens set`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

## Elasticsearch access

Elasticsearch native users, roles and API keys have a separate command group and
use the `database-users` RBAC resource with read/create/update/delete actions.
Every command accepts the inherited namespace and output flags.

```sh
karactl elasticsearch users list search -n production -o json
karactl elasticsearch users create search analyst --roles viewer --password-stdin < protected-password-file
karactl elasticsearch users update search analyst --full-name 'Search analyst' --disabled=false
karactl elasticsearch roles create search ingest -f role.json
karactl elasticsearch api-keys create search --name apm-agent --expiration 30d -f descriptors.json
karactl elasticsearch api-keys invalidate search KEY_ID
```

User creation requires `--password-stdin`; updates can retain the current password.
`--roles` replaces the role list, `--full-name` and `--email` set profile fields,
and `--disabled` disables an identity (`--disabled=false` re-enables it).
User updates retain fields whose flags are omitted. Reserved/built-in and
`karakoram*` users are hidden and cannot be mutated. Built-in roles are readable
but immutable; `karakoram*` roles are hidden and protected.

Role create/update requires `-f`: the file contains `cluster`, `indices` and
`applications` privilege arrays. The command's name is authoritative; a file name
must match it. Missing arrays become empty. Index entries use `names`,
`privileges`, and `allowRestrictedIndices`; application entries use `application`,
`privileges`, and `resources`. Updates replace all privileges.

API-key creation requires `--name`. `--expiration` accepts an Elasticsearch
duration; `-f` is an optional JSON map of role descriptors (roles without names),
not a complete create request. APM agent descriptors use application `apm`,
privileges `event:write` and `config_agent:read`, and resources `["*"]`.
Omitting descriptors inherits the creator's privileges, so supply least-privilege
descriptors for applications. Creation displays `apiKey` and `encoded` **once**;
protect this output and do not send it to logs. Listing never reveals secrets.
Only API keys marked as created through Karakoram are listed or invalidated.
All list commands accept `--limit` and `--cursor`.
