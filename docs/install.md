<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Install Karakoram

Choose an infrastructure backend before installing. Karakoram is multi-platform; backend capabilities and operator responsibilities differ. See [Backends](backends.md) and [Pricing and cost estimates](pricing.md).

| Backend | Installation path |
| --- | --- |
| Zerops — reference backend | [Zerops backend guide](zerops.md); follow this page or the dashboard-only [Quick start](quickstart.md) to install. |
| Dokploy | [Install the control plane on Dokploy](dokploy.md#install-the-control-plane-on-dokploy). |
| Nomad | [Install the control plane on Nomad](nomad.md#install-the-control-plane-on-nomad). |

The deployment steps below are for the **Zerops backend**, in an existing Zerops project. CLI installation is shared across backends.

## Prerequisites

- Permission to import services and deploy code in an existing project. Organization Owner/Admin access and project-creation rights are not required.
- **Control-plane capacity:** one Ubuntu 24.04 container, shared CPU, 0.5–1 GiB RAM, 1–2 GiB runtime disk and a separate 5 GiB Local Storage volume. A LIGHT project can host this footprint; cluster services require additional capacity. Review engine minimums ([MongoDB](mongodb/README.md), [RabbitMQ](rabbitmq/README.md), [Elasticsearch](elasticsearch/README.md)) and the wizard's aggregate footprint before creating clusters.
- A Zerops token: **Settings → Access Tokens Management → Custom access per project → Full access**, for this project only, **sudo mode off**. Read-only access cannot provision, update secrets, grow storage or configure routing. Save the one-time value in a password manager; see [Security](security.md) and [Zerops permissions](https://docs.zerops.io/features/rbac#integration-tokens).
- Free service names `karakoram` and `karakoramdata`, or customize them below.
- For local deployment: authenticated [zcli](https://docs.zerops.io/references/cli) with deployment access. The default prebuilt setup needs no Go or Node toolchain locally or on Zerops.

## Setups: prebuilt release or source build

Both setups in root `zerops.yaml` use the same runtime and state volume:

| Setup | Build and use |
| --- | --- |
| `karakoram` (default) | Downloads the upstream pinned binary; verifies the checksum manifest's SSH signature, archive SHA-256 and binary version. Seconds; no Go/Node. Download or verification failure fails the build. |
| `karakoram-source` | Builds the exact deployed commit and UI with Node 22 and Go; allow minutes. For forks, development and unreleased fixes. Requires the complete checkout, including `ui/package-lock.json`, and outbound Go toolchain/module registry and npm access. Zerops `go@1` starts with Go 1.22; `go 1.25.0` selects the required toolchain automatically. |

[`deploy/zerops/RELEASE`](../deploy/zerops/RELEASE) contains the pinned SemVer tag. The v0.2.0 release checkout pins `v0.2.0`; a development checkout can still pin the previous published release. **Never edit it by hand:** only the [release tool](../CONTRIBUTING.md#releasing) updates it. A published tag pins itself; `main` between releases pins the latest published release, not newer source. Prebuilt binaries already embed release version, tagged commit and reproducible build date.

For source builds select `zeropsSetup: karakoram-source` in imports, `--source` with the helper, or `--setup karakoram-source` with zcli. Forks must select source: the default always downloads KarakoramDev's release, never the fork's source.

## Required secrets and generated settings

| Setting | Source and purpose |
| --- | --- |
| `KARAKORAM_MASTER_KEY` | Import generates 48 random characters; encrypts stored credentials. Preserve for the state database's lifetime; back up separately. |
| `KARAKORAM_ADMIN_PASSWORD` | Import generates 24 random characters; creates the first `admin` only, not a password-reset mechanism. |
| `KARAKORAM_ZEROPS_TOKEN` | Supply the project token; templates leave it empty. |
| `KARAKORAM_ZEROPS_PROJECT_ID` | Pipeline uses injected `projectId`; no manual secret needed. |
| `KARAKORAM_DATA_DIR` | `/mnt/karakoramdata`; contains persistent `karakoram.db` state. |
| `KARAKORAM_PUBLIC_URL` | Pipeline derives HTTPS origin from injected `zeropsSubdomain`. |
| `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` | Optional trusted private/nonstandard-port S3-verifier destinations; see [Backups and storage](backups.md). Normally unset for Zerops storage. |
| `KARAKORAM_ZEROPS_API_URL` | Optional non-default Zerops API endpoint. |

Zerops reserves `ZEROPS_`: user secrets named `ZEROPS_TOKEN` or `ZEROPS_PROJECT_ID` fail with `userDataZeropsPrefixForbidden`. Use `KARAKORAM_ZEROPS_*`.

**Warning:** Put secrets only on the Karakoram service, not in root `zerops.yaml`, source control, deployment logs or shared project variables. Preserve default service environment isolation. Set an explicit bootstrap password through service secrets on custom deployment paths: if absent, the server generates and logs an initial admin password once.

For a custom domain, replace `run.envVariables.KARAKORAM_PUBLIC_URL` in the pipeline with its HTTPS origin and redeploy. A same-named secret does not override a pipeline runtime variable. Secret changes require restart/redeployment to reach running processes.

## Installation sources

Use [`deploy/zerops/import.yaml`](../deploy/zerops/import.yaml) for public-repository imports, or [`import-local.yaml`](../deploy/zerops/import-local.yaml) for local deployment:

```sh
git clone https://github.com/KarakoramDev/Karakoram
cd Karakoram
```

**Warning:** These are new-install templates. Never re-import over an existing installation; use [Upgrade](upgrade.md).

## Local checkout installation

Import from the checkout root:

```sh
PROJECT_ID='<project-id>'
zcli project service-import deploy/zerops/import-local.yaml -P "$PROJECT_ID"
```

Open **karakoram → Environment variables → Secret variables**, set `KARAKORAM_ZEROPS_TOKEN`, and preserve the generated master key and bootstrap password. Deploy the pinned prebuilt release:

```sh
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
zcli service enable-subdomain karakoram -P "$PROJECT_ID"
```

`--workspace-state clean` uploads only committed `HEAD`; zcli's default (`all`) also uploads untracked, non-ignored files. Do not add `--no-git`: it uploads the whole checkout, including private imports and `node_modules`. The [helper](#one-command-helper) instead stages only four required files. Commit source/pipeline changes before a clean push.

The local template has neither `buildFromGit` nor an inline pipeline: it creates a `READY_TO_DEPLOY` runtime without code. The push uploads deployment files and downloads the release. Enable the subdomain after deployment: the initial runtime had no HTTP port.

### Build provenance for local pushes

Only `karakoram-source` needs these inputs. `--no-git` source uploads lack Git metadata; obtain:

```sh
git describe --tags --always
git rev-parse HEAD
```

Merge their **literal output** into the existing `zerops.yaml`, preserving all other pipeline settings:

```yaml
zerops:
  - setup: karakoram-source
    build:
      envVariables:
        KARAKORAM_VERSION: <release-tag-or-source-label>
        KARAKORAM_COMMIT: <commit-id>
```

Refresh both values for each push. These are remote **build** inputs; workstation exports, `run.envVariables` and post-deploy runtime edits cannot relink the executable. The explicit commit overrides Git discovery; the pipeline records a UTC build date. Without provenance the API/UI show `unknown (built without git metadata)`.

Local builds accept the same inputs (or explicit Make `VERSION`/`COMMIT` overrides):

```sh
KARAKORAM_VERSION="$(git describe --tags --always)" \
KARAKORAM_COMMIT="$(git rev-parse HEAD)" make build
```

Check public `GET /api/v1/version` for `version`, `commit`, `buildDate` and `apiVersion`.

### Use different service names

Use private copies of the local import and pipeline, not the helper. For runtime `dbcontrol` and volume `dbcontroldata`:

1. Change both import `hostname` values and import that copy; set the token on the renamed runtime.
2. Rename setup `karakoram` to `dbcontrol`, `karakoram-source` to `dbcontrol-source`, and its `extends` to `dbcontrol`. In the base setup, change `run.volume.hostname` to `dbcontroldata` and `run.envVariables.KARAKORAM_DATA_DIR` to `/mnt/dbcontroldata`. Mount hostname/path must agree. No-Git source builds need provenance under the renamed source setup.
3. Update explicit hostname references, including `KARAKORAM_ADVERTISE_URL` if configured. Default internal/public URLs use injected runtime hostname/subdomain; no hard-coded replacement is needed. Keep `./karakoram server` unchanged.
4. Stage the private prebuilt pipeline and every file it reads; `--no-git` is safe here because this is not the checkout:

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

For source, push from the checkout with `--setup dbcontrol-source`, `--zerops-yaml-path /private/path/zerops.yaml` and `--workspace-state clean`; a whole-checkout `--no-git` upload requires excluding private files first.

Use the resulting origin for browser, CLI and external URL references.

### One-command helper

To supply the token before import, create a private untracked copy of `import-local.yaml`, fill its token and restrict access:

```sh
./deploy/zerops/install.sh "$PROJECT_ID" /private/path/import-local.yaml
```

Without that optional argument, the helper uses the token-free template:

```sh
./deploy/zerops/install.sh "$PROJECT_ID"
```

Then add the token in the GUI and restart/redeploy for readiness. Using existing zcli authentication, the helper imports, pushes setup `karakoram`, and enables the subdomain. It stages only `zerops.yaml`, `deploy/allowed_signers`, `deploy/zerops/RELEASE` and `deploy/zerops/fetch-release.sh` in a temporary upload directory. It never loads/stores tokens, creates projects, starts a VPN, renames services or overwrites installations. Delete private import copies afterward.

To build the exact checkout:

```sh
./deploy/zerops/install.sh --source "$PROJECT_ID" /private/path/import-local.yaml
```

`--source` selects `karakoram-source` and uploads the whole checkout with `--no-git`. Keep private imports outside it, exclude sensitive files and configure [provenance](#build-provenance-for-local-pushes) first.

## Import YAML installation from a public repository

1. Obtain [`deploy/zerops/import.yaml`](../deploy/zerops/import.yaml). Its `buildFromGit` points to `https://github.com/KarakoramDev/Karakoram`; no fork required. Forks need a plain **public** GitHub/GitLab URL without trailing `.git` and `zeropsSetup: karakoram-source`; private repositories are unsupported.
2. Set the empty token in a private copy, or add it in the GUI immediately after import.
3. Choose **Import services** in the existing project's dashboard, or run:

   ```sh
   PROJECT_ID='<project-id>'
   zcli project service-import /private/path/import.yaml -P "$PROJECT_ID"
   ```

4. Wait for build and deployment, not just import acceptance. If the token was empty, set it and restart/redeploy after the build.

Both templates require `#yamlPreprocessor=on` to generate secrets. Multi-argument preprocessor functions require a space after each comma. `buildFromGit` is a one-time source import, not an automatically configured webhook integration.

## Install karactl

Match the CLI release to the **deployed control-plane version**, not the latest release independently. Read `https://example.com/api/v1/version`; with an existing CLI, `karactl --server https://example.com version` shows client/server versions. If the server reports unknown provenance, identify its deployed release/revision before choosing a binary.

[Release assets](https://github.com/KarakoramDev/Karakoram/releases) cover Linux/macOS (`linux`/`darwin`, `amd64`/`arm64`) and Windows (`windows-amd64.exe`) from v0.1.0; signed manifests start at **v0.1.2**. Linux amd64:

```sh
RELEASE=v0.2.0
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

Authenticate the signing key against a trusted checkout or independently trusted fingerprint: downloading it beside the binary does not establish authenticity. Verify in the download directory; proceed only after signature success and the selected asset reports `OK`. On macOS use `shasum -a 256 --check --ignore-missing checksums.txt` for checksums; Windows needs OpenSSH signature verification and SHA-256 verification. Add `$HOME/.local/bin` to `PATH`.

Alternatively build the matching checkout with Go 1.25+:

```sh
go build -o /tmp/karactl ./cmd/karactl
install -Dm755 /tmp/karactl "$HOME/.local/bin/karactl"
```

## First sign-in and CLI connection

1. Wait for `karakoram` to become `ACTIVE`, open its **Public access** HTTPS subdomain, and check:

   ```sh
   curl --fail --show-error https://example.com/healthz
   ```

2. Read the service's `KARAKORAM_ADMIN_PASSWORD`; sign in as `admin`, choose a unique password of at least 12 characters, and sign in again before creating API tokens. Changing the bootstrap variable does not reset existing accounts; see [admin recovery](operations.md#recovering-local-admin-access).
3. Connect using the changed password:

   ```sh
   karactl --server https://example.com --context production login --username admin
   karactl --context production health
   karactl --context production version
   ```

   Login prompts without echo, creates an API token and stores the context. Protect CLI configuration as a credential.
4. For automation, create a token for a restricted account through the API or `karactl tokens create -f token.yaml` (`name`, optional `expiresAt`). Save its one-time `kk_` secret in a secret manager:

   ```sh
   export KARAKORAM_SERVER='https://example.com'
   # Supply KARAKORAM_TOKEN securely without logging it.
   karactl health
   ```

Karakoram API tokens are not Zerops tokens; they inherit their owner's current [Karakoram permissions](access-control.md#api-tokens-and-browser-sessions).

Add other projects through [namespace registration and rotation](namespaces.md#add-validate-and-rotate).

## Deployment boundaries

- Use HTTPS for the public UI/API; review exposure and origin requirements in [Security](security.md). **Do not publish port 9443**, the private authenticated agent endpoint.
- Retain one control-plane container and `deploy.temporaryShutdown: true`; review [availability and placement limits](limitations.md).
- Configure proxies for [events and pagination](api.md#pagination-and-events); a custom domain alone does not disable buffering.
- Back up [control-plane state and master key](operations.md#control-plane-state-backup-and-recovery) separately from [engine backups](backups.md).

For startup, deployment or credential failures, see [Operations and troubleshooting](operations.md).
