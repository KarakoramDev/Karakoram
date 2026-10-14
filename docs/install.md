<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Install Karakoram on Zerops

Karakoram is self-hosted inside **your existing Zerops project**, its `default`
namespace. It operates Percona Server for MongoDB there using direct private
networking, and can manage additional existing projects through namespaces and
outbound relays. Zerops provides infrastructure; you operate the databases and
the Karakoram installation.

## Prerequisites

- An existing Zerops project, with permission to import services and deploy code.
  LIGHT is sufficient for a small installation; allow capacity for the database
  members you will create separately.
- A Zerops integration token with **Custom access per project → Full access** for
  this project only. Read-only access cannot provision services, update secrets,
  scale storage, or configure routing. Organization Owner/Admin access and project
  creation rights are not required. Create the token in **Settings → Access Tokens
  Management** and retain its one-time-displayed value in a password manager.
  See [Zerops token permissions](https://docs.zerops.io/features/rbac#integration-tokens).
- For a local checkout: [zcli](https://docs.zerops.io/references/cli), already logged
  in with deployment access, and a complete checkout including `ui/package-lock.json`.
  Go and Node are installed by the remote build; local compilation is not required.
- Free service names `karakoram` and `karakoramdata` in the target project.

The installer creates one Ubuntu 24.04 runtime (one container, 0.5–1 GB RAM,
shared CPU) and a 5 GB Local Storage volume. `zerops.yaml` builds the UI using
Node 22 and embeds it in a static Go binary. Zerops `go@1` currently starts with
Go 1.22; the module's `go 1.24.0` directive downloads the required toolchain
through Go's automatic toolchain selection. The build needs outbound access to
the Go toolchain/module registries and npm.

## Required secrets and generated settings

| Setting | Source and purpose |
| --- | --- |
| `KARAKORAM_MASTER_KEY` | Import generates 48 random characters. Encrypts stored credentials. Preserve it for the lifetime of the state database and back it up separately. |
| `KARAKORAM_ADMIN_PASSWORD` | Import generates 24 random characters. Used to create the first `admin` account only. |
| `KARAKORAM_ZEROPS_TOKEN` | Supply your project-scoped Zerops token. The templates deliberately contain an empty value. |
| `KARAKORAM_ZEROPS_PROJECT_ID` | Pipeline references Zerops's injected `projectId`; no manual project-ID secret is needed. |
| `KARAKORAM_DATA_DIR` | `/mnt/karakoramdata`; the persistent SQLite state is `karakoram.db`. |
| `KARAKORAM_PUBLIC_URL` | Pipeline derives the HTTPS origin from injected `zeropsSubdomain`. |
| `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` | Optional operator-controlled S3-verifier destination allowlist: comma-separated exact host or host:port entries for private endpoints/nonstandard ports. Normally unset for Zerops storage. |

**Zerops reserves the `ZEROPS_` prefix.** Importing or creating a user secret
named `ZEROPS_TOKEN` or `ZEROPS_PROJECT_ID` is rejected with
`userDataZeropsPrefixForbidden`. Karakoram therefore uses the
`KARAKORAM_ZEROPS_*` names above in all environments. The optional
`KARAKORAM_ZEROPS_API_URL` selects a non-default Zerops API endpoint.

Backup-storage verification requires HTTPS and permits public global-unicast S3
endpoints on port 443. The exact hostname `storage-prg1.zerops.io` is trusted by
default on that port, including its private resolution. For another private S3
endpoint or a nonstandard port, set `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` **on the
Karakoram service**, for example `backupstore:9000,storage.example.com`. Host-only
entries permit standard ports and host:port permits only that port, but neither
allows HTTP verification. Replace these examples with your actual trusted HTTPS
destinations and restart/redeploy after changing the variable. Do not broadly
allowlist project services.

Loopback, link-local/metadata-style, unspecified/non-global destinations and
100.64.0.0/10 remain blocked even for allowlisted hosts. The verifier rejects
redirects, disables proxy dialing and has a thirty-second deadline. Use the
real S3 origin with HTTPS and TLS verification enabled. This controls the
control-plane verification probe, not all PBM/engine egress; see
[security](security.md#s3-verifier-outbound-policy).

Never put the project token or master key into root `zerops.yaml`, source
control, deployment logs, or a shared project-level variable. Set secrets on
**the Karakoram service**, not on every member service. For a custom domain,
replace `run.envVariables.KARAKORAM_PUBLIC_URL` in `zerops.yaml` with its HTTPS
origin and redeploy. A secret with that same key does **not** override a
pipeline runtime variable.

## Installation sources

The public repository is available at
[github.com/KarakoramDev/Karakoram](https://github.com/KarakoramDev/Karakoram).
For a Git-based deployment, use
[`deploy/zerops/import.yaml`](../deploy/zerops/import.yaml) and follow
[the import procedure below](#import-yaml-installation-from-a-public-repository).
For deployment from your own checkout, clone the repository and use the local
procedure:

```sh
git clone https://github.com/KarakoramDev/Karakoram
cd Karakoram
```

## Local checkout installation

### Import, set the secret, then push

This sequence keeps credentials out of files and allows you to configure the
service before its first successful deployment:

```sh
# Run from the checkout root; substitute your real project ID.
PROJECT_ID='your-project-id'
zcli project service-import deploy/zerops/import-local.yaml -P "$PROJECT_ID"
```

In the Zerops dashboard open the newly created **karakoram → Environment
variables → Secret variables**, edit `KARAKORAM_ZEROPS_TOKEN`, and paste the
project token. Keep both generated Karakoram secrets unchanged. Then deploy:

```sh
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --no-git --disable-logs
```

After the deployment succeeds, activate its HTTP subdomain:

```sh
zcli service enable-subdomain karakoram -P "$PROJECT_ID"
```

The import requests subdomain access, but a code-free runtime has no HTTP port
yet. Explicit activation after the first deployment is required on this path.

The local template intentionally has neither `buildFromGit` nor an inline
pipeline. Zerops imports the runtime in `READY_TO_DEPLOY` state; the push uploads
this checkout and performs the build. This avoids the known failure caused by
importing an inline pipeline before its executable has arrived.

### One-command helper

If you prefer to supply the token **before import**, make a private, untracked
copy of `deploy/zerops/import-local.yaml`, replace its empty
`KARAKORAM_ZEROPS_TOKEN` value, and protect that copy from other users. Then run:

```sh
./deploy/zerops/install.sh "$PROJECT_ID" /private/path/import-local.yaml
```

The optional second argument selects your import file. Without it the helper
uses the checked-in token-free local template:

```sh
./deploy/zerops/install.sh "$PROJECT_ID"
```

In that case the initial server cannot become ready until you add the missing
token through the GUI and restart/redeploy. The helper invokes
`zcli project service-import` followed by `zcli service push --setup karakoram`
and `zcli service enable-subdomain`. It uses your existing zcli login and
environment and never loads or stores tokens.
It does not create projects, start a VPN, or overwrite an existing installation.
Delete the private import copy when no longer needed. Do not reimport the
installer to upgrade an existing installation.

## Import YAML installation from a public repository

1. Obtain [`deploy/zerops/import.yaml`](../deploy/zerops/import.yaml) from the
   public repository. Its `buildFromGit` already points to
   `https://github.com/KarakoramDev/Karakoram`; no fork is required. If deploying
   your own fork, replace it with your plain public GitHub/GitLab repository URL,
   without a trailing `.git`. Private repositories cannot be cloned through
   this import mechanism.
2. Replace the empty `KARAKORAM_ZEROPS_TOKEN` value in a private copy before import,
   or plan to add it in the GUI immediately afterward.
3. In the existing project's dashboard select **Import services** and paste the
   customized YAML. Alternatively:

   ```sh
   zcli project service-import /private/path/import.yaml -P "$PROJECT_ID"
   ```

4. Wait for the Git build and deploy processes, not just the import response.
   If the token was left empty, set it in the service GUI and restart/redeploy
   after the build. Secret changes do not change an already running process's
   environment.

Both templates use `#yamlPreprocessor=on` to generate the bootstrap password and
master key. If adding multi-argument preprocessor functions yourself, include a
space after each comma; Zerops's parser requires it.

## First sign-in and CLI connection

1. Wait until the `karakoram` service is `ACTIVE`. Open its HTTPS subdomain from
   **Public access**. Confirm readiness independently:

   ```sh
   curl --fail --show-error https://YOUR-SUBDOMAIN/healthz
   ```

2. Read `KARAKORAM_ADMIN_PASSWORD` from **karakoram → Environment variables → Secret
   variables** using an authorized Zerops account. Sign in as `admin`. Karakoram
   requires a password change before ordinary privileged API operations. Set a
   unique password and save it securely, then sign in again with the changed
   password before creating API tokens. The import password is a bootstrap
   value; changing the variable later does not reset an existing account.
   If you lose the changed local admin password, use [Zerops-native startup
   recovery](operations.md#recovering-local-admin-access): set the temporary
   `KARAKORAM_RECOVERY_PASSWORD` service secret, restart, sign in and change it,
   then delete the secret and restart. No container exec or fresh install is needed.
3. Build the CLI locally with Go 1.24 or newer:

   ```sh
   go build -o karactl ./cmd/karactl
   ./karactl --server https://YOUR-SUBDOMAIN --context production login --username admin
   ./karactl --context production health
   ```

   Login prompts without echoing the password, creates a Karakoram API token,
   and stores the selected connection context. Use your **changed** password.
   Protect the CLI configuration as a credential.
4. For automation, create a Karakoram API token for a suitably restricted account
   using the API or `karactl tokens create -f token.yaml` (`name` and optional
   `expiresAt`). Its `kk_` secret is returned once. Supply it from a secret manager:

   ```sh
   export KARAKORAM_SERVER='https://YOUR-SUBDOMAIN'
   # Set KARAKORAM_TOKEN securely in the calling environment, without logging it.
   ./karactl health
   ```

   Karakoram API tokens are **not Zerops integration tokens**. They inherit the
   owning account's Karakoram RBAC permissions and can be revoked independently.

## Adding a namespace

After installation, register an additional existing project through **Settings →
Namespaces → Add namespace → Validate and add**, or the CLI:

```sh
# Store the project token in a private, untracked file; '-' reads stdin.
karactl namespaces create --name dev --project-id YOUR_PROJECT_ID \
  --token-file namespace-token.txt
karactl namespaces get dev -o yaml
# Once the relay is connected, select the namespace for cluster commands.
karactl -n dev clusters list
```

The token needs **Custom access per project → Full access for that project**.
Read-only and organization-wide tokens are not the intended scope. The project
must differ from `default` and every registered project. Namespace names are
2–32 lowercase letters/digits/hyphens, start with a letter, end with a
letter/digit, and cannot be `default`.

The control-plane `KARAKORAM_PUBLIC_URL` must be an HTTPS origin reachable from
the additional project. Any extra proxy must forward WebSocket Upgrade at root
`/relay/v1/connect`; do not put browser login in front of that transport path.
The tunnel authenticates with inner mTLS, not browser cookies.

Registration synchronously validates token/project and the availability of the
relay hostname, returning **422** for invalid credentials/project scope,
duplicate project or an occupied hostname; duplicate namespace names return
**409**. Relay deployment is asynchronous after **201** acceptance.

Token validation checks project identity and service-list access, not every
mutation grant; a read-capable but insufficient token can fail later during
relay deployment. The requested Full access scope is still required.

Karakoram deploys one Ubuntu runtime with the same executable, started as
`./karakoram relay`. The hostname normally defaults to `<control-plane hostname>relay`
(usually `karakoramrelay`). If that exceeds 25 characters, it uses the first
16 hostname characters, four hexadecimal SHA-256 characters and `relay`.
`KARAKORAM_RELAY_HOSTNAME` overrides the derived name for new enrollments;
existing namespaces retain their recorded hostname. The service has
**private TCP 9443, no public access** and one
container. Allow project capacity for it; enrollment material is delivered as
service env secrets, not pipeline variables. Remote agents use
`https://<relay hostname>:9443` for end-to-end TLS configuration pulls.

Wait for `token.valid: true`, namespace `Ready`, relay `Ready` and
`connected: true`; inspect `lastError`, `lastSeenAt`, version and certificate
expiry in the UI detail or `namespaces get`. Platform relay readiness probes only
the local listener, not the tunnel. See [namespace lifecycle](user-guide/namespaces.md)
and [operations](operations.md#namespace-and-relay-operations).

## Network, security, and durability

- Public HTTPS terminates at Zerops's L7 balancer and forwards to HTTP port 8080.
  The installer enables the subdomain for UI/API and outbound relay connections.
  Use HTTPS; browser cookies, OIDC and remote namespaces depend on the correct
  public origin. For production,
  configure a custom domain and review access policies rather than relying on a
  shared development subdomain indefinitely.
- Proxies can buffer small SSE frames. Karakoram sends ~16-KiB random-base64
  initial comments and similarly padded fifteen-second heartbeats; small-event
  delivery can wait until a heartbeat. Budget ~16 KiB at startup and ~1.1 KiB/s
  per open stream before wire compression. Configure a nonbuffering
  custom-domain/proxy path where possible; a custom domain alone does not
  disable buffering.
  See [API transport guidance](user-guide/api.md#pagination-and-events).
- Port 9443 is the control plane's authenticated, private agent TLS endpoint.
  Declaring a TCP port does not create public TCP routing. Do not publish 9443.
  Member agent traffic uses pinned internal CAs and member-specific tokens.
- Cluster database ports are private by default. Public database access is a
  separate opt-in operation with an IP allowlist; an HTTP subdomain does not
  expose MongoDB TCP traffic. Public UI/CLI access needs no Zerops VPN.
- The project token can manage services and read service secrets in this project.
  Treat Karakoram and project full-access users as trusted infrastructure
  administrators. Retain Zerops's default service environment isolation.
- SQLite lives on Local Storage, not the disposable runtime filesystem. Keep
  `maxContainers: 1` and `deploy.temporaryShutdown: true`; never run two control
  planes against this volume. Upgrades briefly interrupt the control plane.
- The control plane and Local Storage are not highly available. Zerops does not
  guarantee database-member anti-affinity across physical hosts. Replica count
  alone is not a host-failure-tolerance guarantee.
- Back up the control-plane state and master key, and configure PBM backups for
  database data separately. See [Upgrading and recovering Karakoram](upgrade.md).

## Troubleshooting

- `READY_TO_DEPLOY`: infrastructure exists but no executable has been pushed.
- `userDataZeropsPrefixForbidden`: rename user secrets to the documented
  `KARAKORAM_ZEROPS_*` keys.
- Failed readiness/startup: inspect service deployment and application logs;
  check the project token, project access, master-key length (at least 32
  characters), public URL, and mounted volume. Do not paste secrets into reports.
- A newly edited secret is not visible: restart the service or deploy again.
- Subdomain missing after an earlier failed deploy: after a successful HTTP-port
  deployment, enable **Subdomain access** in the service's Public access page.
- Token unauthorized: ensure it is scoped to this project with Full access;
  replacing the Karakoram API token does not fix the Zerops integration token.
