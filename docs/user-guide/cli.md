<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# karactl CLI

`karactl` uses Karakoram's generated REST client; it does not call Zerops directly. Build from the checkout root with `go build -o /tmp/karactl ./cmd/karactl`, or use `make build` for `bin/karactl`.

## Login and contexts

```sh
karactl --server https://YOUR-SUBDOMAIN --context production login --username admin
karactl --context production health
karactl --context production whoami
karactl --context production logout
```

Login prompts without echo on a terminal, starts a temporary password session, creates an owner-scoped API token, saves it and closes the browser-style session. Finish a forced password change in the UI before CLI login. Login saves the selected context as current. There is no separate `contexts` command: `--context NAME` selects a saved connection, and login creates/updates it. An explicitly selected nonexistent context is rejected outside login.

Contexts live at `~/.config/karakoram/config.yaml` (independent of XDG overrides), in owner-only files (0600) and a newly created directory (0700). Each holds server, token and tokenID. Protect it like a credential. Server/token precedence is **explicit flags > KARAKORAM_SERVER/KARAKORAM_TOKEN > selected saved context**. With no explicit selection, the saved currentContext is used, otherwise `default`. Server URLs may use the origin or end in `/api/v1`; root health/metrics still use the installation base.

For automation inject `KARAKORAM_SERVER` and `KARAKORAM_TOKEN` through a secret manager. Avoid `--token` in process listings/history. Karakoram API tokens are not Zerops integration tokens. Logout normally revokes the matching saved CLI token before clearing local credentials; `logout --local` deliberately leaves server authority active. With an overriding token, do not assume logout revoked a different saved token—revoke its ID explicitly when needed.

## Requests, output and waits

- `-o table` is default; `-o json`/`-o yaml` preserve structured responses for automation.
- `-f FILE` accepts API-shaped YAML/JSON; `-f -` reads stdin. Use protected input for secret fields. When supplied, the file defines the request body instead of convenience body flags.
- Cluster **create** files contain `name` and `spec`; **edit** replaces the full `spec` and can open the configured editor when no file is supplied; **patch/scale/upgrade** files have `{spec: {...}}` merge-patch shape. Arrays such as schedules are replaced, not appended.
- `rbac policy set -f FILE` is the exception: the file contains raw policy CSV.
- `clusters connection NAME --reveal` deliberately displays databaseAdmin credentials. Do not capture it in logs.
- Cluster mutations with `--wait` observe operation completion and cluster generation. Default timeout is thirty minutes; `--timeout` changes it. A wait timeout does not cancel server work. Inspect status before another submission.
- Backup/restore creation returns accepted records; inspect the corresponding Operation and record with `operations wait ID`, `backups get` or `restores get`. Those creation commands do not have an invented `--wait` flag.
- `events watch` streams SSE, supports replay/filter flags, and runs until interrupted. `operations wait` polls a persisted operation until its terminal phase.
- `--cookie`/`--csrf-token` and the `auth` commands support advanced cookie/OIDC API flows; normal automation should use bearer tokens. `auth oidc-login` reports a redirect, not an interactive browser/device login workflow.
- `--insecure-skip-tls-verify` is unsafe and unnecessary with a properly trusted HTTPS certificate.

Examples and safety prerequisites: [replica sets](replica-sets.md), [sharding](sharding.md), [backup/restore](backups-and-restore.md), [scaling/upgrades](scaling-and-upgrades.md), [users/TLS](users-tls-access.md), [RBAC](rbac-and-auth.md), [API](api.md).

## Orphaned platform volumes

List Local Storage volumes whose platform deletion was refused after their member runtimes were deleted:

```sh
karactl orphaned-volumes list
karactl orphaned-volumes list -o json
```

The default output is a table; `-o json` preserves the structured `{items: [...]}` response for automation. The inventory is read-only and includes only records whose original cluster you have permission to read; administrators see all records, even after cluster deletion. The control-plane janitor continues retrying deletion independently of this command. See [operations](../operations.md#orphaned-platform-volumes) for retry timing, events and manual Zerops cleanup.

## Generated command tree

Use `karactl --help` and each subcommand's `--help` for the current command and flag reference. Make installs the CLI as `bin/karactl`. `[flags]` and inherited global flags are omitted from tree rows for readability.

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
  completion
    bash
    fish
    powershell
    zsh
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
  events
    list
    watch
  health
  login
  logout
  metrics
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

Commands absent below have no additional non-help flags. Query flags only reach the API when explicitly supplied; help defaults do not necessarily change existing resources. For full request schemas see [OpenAPI](../../api/openapi.yaml).

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

### `karactl clusters create`

```text
      --allow strings            Public firewall CIDR allowlist
      --backup-storage string    Default backup storage
      --component string         Scale resources of engine, configServer or router (default "engine")
      --config-servers int       Config-server replicas (default 3)
      --cpu-mode string          SHARED or DEDICATED (default "SHARED")
      --engine-config string     YAML engine overrides file
  -f, --file string              API request YAML/JSON file ('-' for stdin)
      --from-backup string       Seed from a backup
      --from-pitr string         Seed from RFC3339 point in time
      --ip-type string           Public IP type: IPV6 or IPV4 (default "IPV6")
      --max-cpu float            Resource size (RAM/disk in GiB) (default 2)
      --max-disk float           Resource size (RAM/disk in GiB) (default 10)
      --max-ram float            Resource size (RAM/disk in GiB) (default 2)
      --members int              Engine replicas per replica set (default 3)
      --min-cpu float            Resource size (RAM/disk in GiB) (default 1)
      --min-disk float           Resource size (RAM/disk in GiB) (default 5)
      --min-ram float            Resource size (RAM/disk in GiB) (default 1)
      --monitoring               Enable monitoring (default true)
      --oplog-span-minutes int   PITR oplog slice interval (default 10)
      --pitr                     Enable continuous point-in-time recovery
      --provider string          Engine provider (default "psmdb")
      --public                   Expose public TCP endpoints
      --routers int              Router replicas (default 2)
      --schedule stringArray     Schedule: NAME=CRON,type=logical|physical,retain=COUNT[,storage=NAME]
      --shards int               Shard count (default 1)
      --source-cluster string    Source cluster for initial data
      --start-cpu float          Resource size (RAM/disk in GiB) (default 1)
      --timeout duration         Maximum wait duration (default 30m0s)
      --tls-mode string          disabled, preferTLS or requireTLS (default "requireTLS")
      --topology string          replicaSet or sharded (default "replicaSet")
      --version string           Exact engine version (see providers get psmdb)
      --wait                     Wait for operation completion and observed cluster generation
```

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
      --max-cpu float        Resource size (RAM/disk in GiB) (default 2)
      --max-disk float       Resource size (RAM/disk in GiB) (default 10)
      --max-ram float        Resource size (RAM/disk in GiB) (default 2)
      --members int          Engine replicas per replica set (default 3)
      --min-cpu float        Resource size (RAM/disk in GiB) (default 1)
      --min-disk float       Resource size (RAM/disk in GiB) (default 5)
      --min-ram float        Resource size (RAM/disk in GiB) (default 1)
      --routers int          Router replicas (default 2)
      --shards int           Shard count (default 1)
      --start-cpu float      Resource size (RAM/disk in GiB) (default 1)
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
      --cluster string     API cluster parameter
      --cursor string      API cursor parameter
      --limit int          API limit parameter (default 100)
      --operation string   API operationId parameter
      --type string        API type parameter
```

### `karactl events watch`

```text
      --cluster string         API cluster parameter
      --cursor string          API cursor parameter
      --last-event-id string   API Last-Event-ID parameter
      --operation string       API operationId parameter
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

### `karactl storages list`

```text
      --cursor string   API cursor parameter
      --limit int       API limit parameter (default 100)
```

### `karactl storages set`

```text
  -f, --file string   API request YAML/JSON file ('-' for stdin)
```

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
