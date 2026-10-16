<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Install Karakoram on Zerops

Karakoram is self-hosted inside **your existing Zerops project**, its `default`
namespace. It operates Percona Server for MongoDB, RabbitMQ and Elasticsearch there
using direct private networking, and can manage additional existing projects
through namespaces and outbound relays. Zerops provides infrastructure; you
operate the databases, brokers and the Karakoram installation. Installation is
shared across providers; choose an engine when creating a cluster. See the
[RabbitMQ guide](user-guide/rabbitmq.md) for broker creation, TLS, access and
definitions-only backups, and read its [limitations](limitations.md#rabbitmq)
before production use. Follow the [Elasticsearch and Elastic Stack guide](user-guide/elasticsearch.md)
for its runtime sizing, licensing, project-network connections and native snapshots;
MongoDB/PBM procedures do not apply to either engine.

Just want it running? The [quick start](quickstart.md) covers the dashboard-only
install in five steps; this guide is the complete reference.

## Prerequisites

- An existing Zerops project, with permission to import services and deploy code.
  LIGHT is sufficient for a small installation; allow capacity for the database
  members you will create separately.
- A Zerops integration token with **Custom access per project → Full access** for
  this project only. Read-only access cannot provision services, update secrets,
  scale storage, or configure routing. Organization Owner/Admin access and project
  creation rights are not required. Create the token in **Settings → Access Tokens
  Management** and retain its one-time-displayed value in a password manager.
  **Sudo mode is not required; create the token without sudo mode** for least
  privilege. A sudo-mode token can read every sensitive variable in the project,
  including database and Karakoram secrets; Karakoram never needs to read them.
  See [Zerops token permissions](https://docs.zerops.io/features/rbac#integration-tokens).
- For a local checkout: [zcli](https://docs.zerops.io/references/cli), already logged
  in with deployment access. The default prebuilt setup needs no Go or Node
  toolchain, locally or on Zerops.
- Free service names `karakoram` and `karakoramdata` in the target project, or
  [choose different names](#use-different-service-names) before importing.

At startup, the control plane logs whether its Zerops token is in sudo mode.
This is informational, not a requirement for operation.

The installer creates one Ubuntu 24.04 runtime (one container, 0.5–1 GB RAM,
shared CPU) and a 5 GB Local Storage volume.

## Setups: prebuilt release or source build

The root `zerops.yaml` offers two setups with the same runtime and state volume:

- **`karakoram` (default):** downloads the pinned release binary, verifies the
  SSH signature on its checksum manifest and the archive's SHA-256 checksum,
  then checks the binary's version. The build takes seconds and needs no Go or
  Node toolchain. A download or verification failure fails the build.
- **`karakoram-source`:** builds the exact deployed commit, including the UI,
  using Node 22 and Go. Allow a few minutes. Use it for forks, development or
  unreleased fixes; changing source alone does not change the prebuilt binary.
  Zerops `go@1` starts with Go 1.22; the module's `go 1.25.0` directive downloads
  the required toolchain through Go's automatic toolchain selection. Only this
  setup needs outbound access to the Go toolchain/module registries and npm.

[`deploy/zerops/RELEASE`](../deploy/zerops/) pins the release tag used by the
prebuilt setup. A published release tag pins and deploys itself; `main` between
releases pins the latest published release, not its newer source commits.
To build source instead, set `zeropsSetup: karakoram-source` in the import YAML,
pass `--source` to `install.sh`, or use `--setup karakoram-source` with
`zcli service push`. A fork deployed with `buildFromGit` must do this as well:
the prebuilt setup always downloads the upstream KarakoramDev release, never the
fork's own source.

### Build provenance for local pushes

This section applies only to **`karakoram-source`**. Prebuilt binaries already
carry their release version, tagged commit and reproducible build date.
`zcli service push --no-git` uploads source without `.git`. Before a source push,
obtain the release tag/source label and source revision from the checkout:

```sh
git describe --tags --always
git rev-parse HEAD
```

Add those **literal values** under `build.envVariables` in the `zerops.yaml`
that you pass to `--zerops-yaml-path`, preserving its existing `base`,
`buildCommands`, `deployFiles`, `deploy` and `run` settings. For example, the
relevant part of that file becomes:

```yaml
zerops:
  - setup: karakoram-source
    build:
      envVariables:
        KARAKORAM_VERSION: v0.x.y
        KARAKORAM_COMMIT: <git rev-parse HEAD>
```

Replace `v0.x.y` with the chosen release tag/source label and
`<git rev-parse HEAD>` with the command's actual output; neither is an instruction
for the remote build to run Git. This is an excerpt to merge into the existing
file, not a replacement pipeline.

The existing Go `buildCommands` reads `KARAKORAM_VERSION` and `KARAKORAM_COMMIT`
from the **build environment** and links them into the binary's version/commit
fields. The explicit commit wins over `git rev-parse --short HEAD` discovery.
Without these inputs, a no-Git archive falls back to `dev`/`unknown`;
`run.envVariables`, shell variables set only on your workstation, or runtime
variables changed after deployment do not supply these remote build inputs or
relink the executable.

Local builds accept the same names:

```sh
KARAKORAM_VERSION="$(git describe --tags --always)" \
KARAKORAM_COMMIT="$(git rev-parse HEAD)" make build
```

Explicit Make `VERSION`/`COMMIT` overrides still work. Check public
`GET /api/v1/version` after deployment: `version`, `commit`, `buildDate` and
`apiVersion` identify the executable. If metadata is absent, the API and UI say
**`unknown (built without git metadata)`** instead of implying an identifiable
release. The UTC build timestamp is still recorded by the Zerops pipeline.

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

Backup-storage verification permits public global-unicast S3 endpoints on ports
80/443. The exact hostname `storage-prg1.zerops.io` is trusted by default on
those ports, including its private resolution. For another private S3 endpoint
or a nonstandard port, set `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` **on the Karakoram
service**, for example `backupstore:9000,storage.example.com`; host-only entries
allow 80/443, while host:port allows only the specified port. Replace these
examples with your actual trusted destinations and restart/redeploy after
changing the variable. Do not broadly allowlist project services.

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
project token. Keep both generated Karakoram secrets unchanged, then deploy the
pinned prebuilt release from the checkout root:

```sh
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Git mode with `--workspace-state clean` uploads only the committed `HEAD` tree.
zcli's default workspace state (`all`) also uploads untracked, non-ignored files,
and `--no-git` uploads the whole directory, including `node_modules` and private
import copies; use neither for this push.
The [helper below](#one-command-helper) safely uses `--no-git` by staging only
the prebuilt setup's required files. For source builds, choose
`--setup karakoram-source`; if using `--no-git`, first supply the
[source build provenance](#build-provenance-for-local-pushes).

After the deployment succeeds, activate its HTTP subdomain:

```sh
zcli service enable-subdomain karakoram -P "$PROJECT_ID"
```

The import requests subdomain access, but a code-free runtime has no HTTP port
yet. Explicit activation after the first deployment is required on this path.

The local template intentionally has neither `buildFromGit` nor an inline
pipeline. Zerops imports the runtime in `READY_TO_DEPLOY` state; the push uploads
the deployment files and downloads the pinned binary during the build. This
avoids importing an inline pipeline before its executable has arrived.

### Use different service names

If either default name is already taken, use the manual local-checkout procedure
above with private copies of `deploy/zerops/import-local.yaml` and `zerops.yaml`.
For example, choose runtime `dbcontrol` and Local Storage `dbcontroldata`:

1. In the import copy, change both service `hostname:` values from `karakoram`
   and `karakoramdata` to the chosen runtime and storage names. Import this copy
   instead of the checked-in template, then set the runtime's
   `KARAKORAM_ZEROPS_TOKEN` in the dashboard as above.
2. In the pipeline copy, rename setup `karakoram` to `dbcontrol`. Rename
   `karakoram-source` to `dbcontrol-source` and change its `extends:` to
   `dbcontrol`. In the base setup, change `run.volume.hostname` to
   `dbcontroldata` and `run.envVariables.KARAKORAM_DATA_DIR` to
   `/mnt/dbcontroldata`; the hostname and mount path must agree.
   Source builds using `--no-git` also need the
   [build provenance values](#build-provenance-for-local-pushes) under the
   renamed source setup's `build.envVariables`.
3. Update any explicit hostname references in your customized configuration,
   including `KARAKORAM_ADVERTISE_URL` if set, to the new runtime hostname.
   By default the internal URL uses the injected runtime hostname and
   `KARAKORAM_PUBLIC_URL` uses its injected `zeropsSubdomain`; neither needs
   a hard-coded replacement. Keep the executable/start command `./karakoram server`.
4. For prebuilt deployment, stage the private pipeline copy together with all
   files it reads, push to the new runtime, then enable its subdomain:

   ```sh
   UPLOAD_DIR="$(mktemp -d)"
   mkdir -p "$UPLOAD_DIR/deploy/zerops"
   cp /private/path/zerops.yaml "$UPLOAD_DIR/zerops.yaml"
   cp deploy/allowed_signers "$UPLOAD_DIR/deploy/"
   cp deploy/zerops/RELEASE deploy/zerops/fetch-release.sh "$UPLOAD_DIR/deploy/zerops/"
   zcli service push dbcontrol -P "$PROJECT_ID" --setup dbcontrol \
     --working-dir "$UPLOAD_DIR" --zerops-yaml-path zerops.yaml --no-git --disable-logs
   rm -r "$UPLOAD_DIR"
   zcli service enable-subdomain dbcontrol -P "$PROJECT_ID"
   ```

   For a source build, push from the checkout root with `--setup dbcontrol-source`
   and `--zerops-yaml-path /private/path/zerops.yaml` instead of the staged upload.
   Use Git mode with `--workspace-state clean`, or carefully exclude private
   files before a `--no-git` upload of the whole checkout.

Use the resulting runtime origin for browser/CLI access and any configured
external URL references. Do not use the one-command helper for renamed services:
it uses the default runtime and setup names.

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
and `zcli service enable-subdomain`. The default stages only `zerops.yaml`,
`deploy/allowed_signers`, `deploy/zerops/RELEASE` and
`deploy/zerops/fetch-release.sh` in a temporary upload directory; no local build
or full-checkout upload is needed. It uses your existing zcli login and
environment and never loads or stores tokens.
It does not create projects, start a VPN, or overwrite an existing installation.
Delete the private import copy when no longer needed. Do not reimport the
installer to upgrade an existing installation.

To build the exact source checkout instead of the pinned release, use:

```sh
./deploy/zerops/install.sh --source "$PROJECT_ID" /private/path/import-local.yaml
```

`--source` selects `karakoram-source` and uploads the whole checkout with
`--no-git`. Keep private import files outside the checkout, exclude other
sensitive files from the upload, and configure
[source build provenance](#build-provenance-for-local-pushes) first.

## Import YAML installation from a public repository

1. Obtain [`deploy/zerops/import.yaml`](../deploy/zerops/import.yaml) from the
   public repository. Its `buildFromGit` already points to
   `https://github.com/KarakoramDev/Karakoram`; no fork is required. If deploying
   your own fork, replace it with your plain public GitHub/GitLab repository URL,
   without a trailing `.git`, and set `zeropsSetup: karakoram-source`: the
   default setup deploys the upstream release binary, not the fork's source.
   Private repositories cannot be cloned through this import mechanism.
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

## Install karactl

`karactl` binaries for all three engines are attached to
[GitHub releases](https://github.com/KarakoramDev/Karakoram/releases) starting
with **v0.1.0**. Assets are available for Linux and macOS (`linux`/`darwin`,
`amd64`/`arm64`) and Windows (`windows-amd64.exe`).
For example, download and verify the Linux amd64 binary before installing:

```sh
RELEASE=v0.1.2
ASSET=karactl-linux-amd64
BASE="https://github.com/KarakoramDev/Karakoram/releases/download/$RELEASE"
curl --fail --location --proto '=https' --proto-redir '=https' \
  --remote-name "$BASE/$ASSET" \
  --remote-name "$BASE/checksums.txt" \
  --remote-name "$BASE/checksums.txt.sig" &&
curl --fail --location --proto '=https' --proto-redir '=https' --output allowed_signers \
  "https://raw.githubusercontent.com/KarakoramDev/Karakoram/$RELEASE/deploy/allowed_signers" &&
ssh-keygen -Y verify -f allowed_signers -I release@karakoram.dev \
  -n karakoram-release -s checksums.txt.sig < checksums.txt &&
sha256sum --ignore-missing -c checksums.txt &&
  install -Dm755 "$ASSET" "$HOME/.local/bin/karactl"
```

Signed checksum manifests are available starting with **v0.1.2**. Verify the
signing key against a trusted repository checkout or independently trusted
fingerprint; downloading a key beside an artifact alone does not establish
its authenticity. Run signature and checksum verification in the download
directory and proceed only after the selected asset reports `OK`.
On macOS use `shasum -a 256 --check --ignore-missing checksums.txt` for the
checksum step; on Windows use OpenSSH signature verification and a SHA-256
verifier against the same manifest.
Put `$HOME/.local/bin` on `PATH` for the POSIX example. Building from source
with Go 1.25 or newer remains an option:

```sh
go build -o /tmp/karactl ./cmd/karactl
install -Dm755 /tmp/karactl "$HOME/.local/bin/karactl"
```

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
3. [Install the CLI](#install-karactl), then connect:

   ```sh
   karactl --server https://YOUR-SUBDOMAIN --context production login --username admin
   karactl --context production health
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
   karactl health
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
Sudo mode is not required; create it without sudo mode to avoid granting access
to read every sensitive variable in the project. Karakoram never needs to read
those values. Read-only and organization-wide tokens are not the intended scope.
The project must differ from `default` and every registered project. Namespace
names are 2–32 lowercase letters/digits/hyphens, start with a letter, end with a
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
- Engine endpoints are private by default. MongoDB/AMQP public TCP access is a
  separate opt-in operation with an IP allowlist; an HTTP subdomain does not
  expose those protocols. RabbitMQ management and Kibana/APM public HTTPS are
  separate engine-specific options. Public Karakoram UI/CLI access needs no Zerops VPN.
- A Full access project token can manage services; sudo mode additionally lets it
  read every sensitive variable in the project. Leave sudo mode disabled.
  Treat Karakoram and project full-access users as trusted infrastructure
  administrators. Retain Zerops's default service environment isolation.
- SQLite lives on Local Storage, not the disposable runtime filesystem. Keep
  `maxContainers: 1` and `deploy.temporaryShutdown: true`; never run two control
  planes against this volume. Upgrades briefly interrupt the control plane.
- The control plane and Local Storage are not highly available. Zerops does not
  guarantee database-member anti-affinity across physical hosts. Replica count
  alone is not a host-failure-tolerance guarantee.
- Back up the control-plane state and master key separately from engine data.
  Use PBM for MongoDB, native snapshots for Elasticsearch and definitions backups
  for RabbitMQ (which do not contain messages). See
  [Upgrading and recovering Karakoram](upgrade.md) and each engine guide.

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
- `service <cluster>m0 has no verified cluster ownership`: upgrade to v0.1.1 and
  follow [ownership recovery](operations.md#unverified-cluster-ownership) for a
  cluster stuck during initial provisioning.
- `zerops HTTP 400 (processNotFound)` repeating every minute: upgrade to v0.1.1;
  see [expired platform processes](operations.md#repeated-processnotfound-errors).
