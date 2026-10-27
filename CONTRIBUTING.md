<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Contributing to Karakoram

Contribute to Apache-2.0 orchestration for [MongoDB](https://www.mongodb.com) ([Percona Server](https://www.percona.com/mongodb/software/percona-server-for-mongodb)/[PBM](https://docs.percona.com/percona-backup-mongodb/)), [RabbitMQ](https://www.rabbitmq.com) and [Elasticsearch](https://www.elastic.co/elasticsearch) on [Zerops](https://zerops.io).

Read [architecture](docs/architecture.md), [ADRs](docs/adr/README.md), [security](docs/security.md) and [limitations](docs/limitations.md). No [Kubernetes](https://kubernetes.io) dependencies or implied vendor/[OpenEverest](https://openeverest.io) affiliation or endorsement.

## Conduct and contribution process

Follow the [Code of Conduct](CODE_OF_CONDUCT.md): be respectful, critique ideas rather than people, keep discussion constructive and respect reviewers' time. Harassment and discriminatory behavior are not accepted. Report conduct concerns to conduct@karakoram.dev; maintainers may remove content or block participants.

1. Search existing issues. For changes beyond a small fix, open an issue to agree on the problem/approach; architectural changes need an ADR update.
2. Create a topic branch and keep each pull request to one logical change.
3. Use clear commit messages: summary, blank line, then what changed and why.
4. Update affected code, callers, tests and documentation together; remove obsolete paths and run the applicable [checks](#checks).
5. Sign off every commit. Maintainers review pull requests and decide acceptance.

Every commit requires a `Signed-off-by: Your Name <you@example.com>` line under the [Developer Certificate of Origin 1.1](https://developercertificate.org/). `git commit -s` adds it. This certifies your right to contribute under Apache-2.0; use your real name, not anonymous/bot sign-offs.

## AI/LLM-assisted contributions

AI assistance is allowed, but a human must submit, understand and take responsibility for every line, including correctness, security and licensing. Automated/bot-driven pull requests and unreviewed generated output are not accepted. Large/complex reconciliation, data-safety, backup/restore and security logic must be authored and fully understood by the contributor.

Disclose meaningful AI use in the pull request: tool and purpose. Check generated material for incompatible licenses, secrets and private data. Private AI self-review is allowed; do not post AI review bots into human review threads. Maintainers may request clarification or decline contributions. This policy is adapted from [Cloud Hypervisor's guidelines](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/CONTRIBUTING.md#aillm-assistance--generated-code).

Coding agents must also read [AGENTS.md](AGENTS.md) for repository layout, commands and invariants.

## Development setup

Requirements: **[Go](https://go.dev) 1.26+**, [Make](https://cgit.git.savannah.gnu.org/cgit/make.git/), **[Node](https://nodejs.org) 22.6+** and [npm](https://docs.npmjs.com/). The UI test runner uses Node's built-in [TypeScript](https://www.typescriptlang.org) support. Agent integration tests require [Docker](https://www.docker.com) and [Linux](https://kernel.org) host networking; unit tests need no Zerops account.

```sh
make ui
make build
make test
```

Binaries are `bin/karakoram` and `bin/karactl`. `make build-all` builds UI assets first. Plain Go builds without existing UI output do not embed the SPA. Build metadata uses Make's `VERSION`, `COMMIT` and `BUILD_DATE` variables.

`make build` is for development. Publish artifacts with `make release`, not by copying `bin/`; see [Releasing](#releasing).

For frontend development, run `npm run dev` in `ui/`: [Vite](https://vite.dev) listens on 5173 and proxies API requests to localhost:8080. `KARAKORAM_API_URL` selects another backend; see [ui/README.md](ui/README.md). Local server configuration is defined in [internal/server/config.go](internal/server/config.go): use a persistent data directory, master key, public origin and real backend settings. Do not commit populated environment files or substitute fake production credentials.

## Releasing

Maintainers release from `main` for major changes, security fixes or accumulated fixes—not every commit. Pushes do not automatically create tags/releases.

Use `vMAJOR.MINOR.PATCH`: before 1.0, MINOR for features/breaking changes and PATCH for fixes/security; from 1.0, MAJOR for breaking changes, MINOR for compatible features, PATCH for fixes. Document breaking changes/upgrade steps. Prereleases such as `-rc.1` are allowed; deploy them only deliberately.

Default Zerops deploy/run configuration must remain compatible with the binary pinned in `deploy/zerops/RELEASE`, even on newer `main`. Ship binary-dependent configuration with its release or use `karakoram-source`. **Never edit `RELEASE` by hand:** the release tool writes its single SemVer tag.

### Prerequisites and release notes

- Development tools above, [Bash](https://www.gnu.org/software/bash/), [Git](https://git-scm.com), [GNU tar](https://cgit.git.savannah.gnu.org/cgit/tar.git/)/[coreutils](https://www.gnu.org/software/coreutils/) (`date`, `sha256sum`), [gzip](https://cgit.git.savannah.gnu.org/cgit/gzip.git/), [OpenSSH](https://www.openssh.com) `ssh-keygen` supporting `-Y sign`/`-Y verify`.
- Publication: `gh` authenticated with public-repository release rights; Git push rights for `origin` tags and `main`.
- Signing key: `~/.config/karakoram/release-signing-ed25519`, overridden by `KARAKORAM_RELEASE_SIGNING_KEY`. Authorize its public key in [`deploy/allowed_signers`](deploy/allowed_signers) for `release@karakoram.dev`; never commit the private key.
- Write `NOTES` outside the checkout (or commit before preparation): changes, security fixes, upgrade steps, state-schema/rollback compatibility. Notes are not generated. Review changes and run applicable [checks](#checks); release targets do not replace them.

### Prepare, build, and publish

```sh
make release-prepare VERSION=v0.1.2
make release-build VERSION=v0.1.2
make release VERSION=v0.1.2 NOTES=../release-notes-v0.1.2.txt
```

| Target | Requirements and result |
| --- | --- |
| `release-prepare` | Clean tree, `main` equal to `origin/main`, tag absent locally/remotely, version newer than pin. Detaches HEAD, writes pin, creates signed-off `Release v0.1.2` commit and annotated tag. Local `main` unchanged; no push. Review commit/tag. |
| `release-build` | Tag must exist and pin itself; signing key required. Dry run exports tag with `git archive`, builds UI once, server for Linux amd64/arm64 and CLI for Linux/macOS amd64/arm64 + Windows amd64. Writes server archives (binary/LICENSE/NOTICE), raw CLI binaries, `checksums.txt` and SSH signature to `dist/v0.1.2/`; verifies signature/all checksums. No publication/push. |
| `release` | Clean tree, HEAD at tag, `origin/main` ancestor; conflicting remote tag rejected. Rebuilds, pushes tag, creates draft from `NOTES`, downloads every asset, compares bytes and verifies signature/checksums. Then publishes (latest, or prerelease for suffixed tags) and fast-forwards `origin/main`, never pinning unpublished assets. |

Rerun the same publication command after failure: unfinished drafts are recreated; published releases are re-verified before fast-forwarding `main`. Afterwards:

```sh
git checkout main && git merge --ff-only origin/main
```

Reproducibility: `CGO_ENABLED=0`, `-trimpath`, `-buildvcs=false`, full tagged commit SHA and its UTC timestamp. Archive paths/order/owner/group/timestamps are normalized; gzip omits timestamps. Identical checksums require the **exact same Go toolchain**, tagged source and locked npm dependencies, not merely a compatible Go version. Inspect with `go version -m <binary>`.

### Signing-key rotation and verification

Commit the new public key to `deploy/allowed_signers` before preparing/signing with it. Retain old keys for old releases; use OpenSSH `valid-after`/`valid-before` for validity windows. Distribute trust-file updates through a trusted channel.

Verify a complete local release:

```sh
RELEASE=v0.1.2
(cd "dist/$RELEASE" &&
  ssh-keygen -Y verify -f ../../deploy/allowed_signers \
    -I release@karakoram.dev -n karakoram-release \
    -s checksums.txt.sig < checksums.txt &&
  sha256sum --strict -c checksums.txt)
```

For downloads, [authenticate the signing key, verify the manifest signature, then the selected binary/archive checksum](docs/install.md#install-karactl) before use.

## Code and change boundaries

| Area | Responsibility |
| --- | --- |
| `internal/server`, `internal/api` | Runtime wiring, strict HTTP contract, handlers and authorization |
| `internal/domain`, `internal/store` | Models, [SQLite](https://sqlite.org) migrations, durable intent/checkpoints/events/secrets |
| `internal/reconciler` | Serialized lifecycle/operations, observation and cleanup |
| `internal/engine` | In-process provider registry and reconciliation/data-plane interfaces |
| `internal/engine/psmdb`, `internal/engine/psmdb/pbm` | MongoDB topology/access/TLS and PBM backup/PITR/restore |
| `internal/engine/rabbitmq` | Broker lifecycle, Management API access and definitions recovery |
| `internal/engine/elastic` | Elasticsearch/Stack lifecycle, native security and snapshot recovery |
| `internal/infra/zerops`, `internal/namespaces`, `internal/relay` | Zerops wire formats, project lifecycle and relay transport |
| `internal/agent`, `internal/agentapi` | Engine process supervision/rendering and private protocol |
| `internal/auth`, `internal/rbac` | Identity, sessions/tokens and policy |
| `internal/scheduler`, `internal/events`, `internal/monitoring` | Schedules/retention, durable activity/SSE and metrics |
| `internal/cli`, `ui/`, `internal/ui` | CLI, typed [React](https://react.dev) application and embedded assets |

- Keep generic durability in reconciler/store; engine mechanisms stay within their provider; Zerops wire formats stay in the backend adapter.
- Provider metadata owns versions, topology constraints and UI defaults. Do not create a second CLI/UI catalog. Providers are compiled in, not dynamically installable plugins.
- Perform at most one disruptive action per reconciliation step. Persist intent/checkpoints and remote identifiers before progressing, then observe completion. SQLite and remote services are not one transaction.
- Preserve agent config/control/receipt compatibility through rolling deployments. Maintenance/process holds survive restarts and pulls; ambiguous initiation is not permission to repeat destructive work.
- Ordinary status/log/event reads must remain credential-free. Preserve separate `cluster-credentials:read`, per-object filtering and deny-overrides policies.

## Public API changes and regeneration

1. Edit [api/openapi.yaml](api/openapi.yaml): schemas, `x-karakoram-rbac`, write-only secrets and errors.
2. Regenerate Go server/client/RBAC and TypeScript artifacts; do not hand-edit generated files.
3. Migrate handlers, CLI, UI and tests together. When CLI commands change, update the command-group table in [docs/cli.md](docs/cli.md) from recursive `karactl --help`.
4. Run the contract drift check with UI dependencies installed.

From the repository root:

```sh
make generate
(cd ui && npm ci && npm run gen:api)
make openapi-check
```

Go generation uses pinned [oapi-codegen](https://github.com/oapi-codegen/oapi-codegen) 2.4.1 for `internal/api/gen.go` and `pkg/client/client.gen.go`, plus the RBAC generator for `internal/api/rbacmap.gen.go`. TypeScript generation writes `ui/src/api/schema.d.ts` using lockfile-pinned [openapi-typescript](https://openapi-ts.dev). Keep handwritten public client helpers separate from generated files and independent of `internal/`; external consumers import `pkg/client`. See [Clients/SDK](docs/api.md#clientssdk).

`make openapi-check` rejects YAML aliases and regenerates/compares Go contract/client/RBAC and TypeScript files; it is a drift check, not live behavior verification. If editing introduces YAML anchors/aliases, normalize without changing meaning:

```sh
go run ./internal/tools/specnoalias
go run ./internal/tools/specnoalias -check
go generate ./api/...
(cd ui && npm run gen:api)
```

Normalization preserves existing component-response references; `-check` fails if anchors/aliases remain.

## Checks

```sh
make test
make test-integration
make lint
make openapi-check
```

Integration tests use real database containers; read their prerequisites. Skipped tests are not evidence of passing behavior. `make lint` runs Go vet, [staticcheck](https://staticcheck.dev) 2025.1.1 and UI typecheck/[ESLint](https://eslint.org); install UI dependencies first.

Live tests are opt-in and destructive to their resources:

```sh
# Securely load KARAKORAM_E2E_URL and KARAKORAM_E2E_TOKEN first.
make e2e
```

Follow [test/e2e/README.md](test/e2e/README.md) for [S3](https://aws.amazon.com/s3/), IPv6 allowlist, CLI and [OIDC](https://openid.net/developers/how-connect-works/) settings. Use an isolated installation in a dedicated project: the suite mutates global policy/settings, so never run against production or concurrently with another live mutation suite. Do not modify the platform's `core` service. Report scenarios exercised and results in the pull request.

## Security and licensing

Never commit credentials, master keys, passwords, private PEMs, signed upload/log URLs or populated secret fixtures. Protect CLI configuration/state archives as credentials; use synthetic fixtures, redacted reports and no shell tracing around secrets.

New source files need `SPDX-License-Identifier: Apache-2.0` and `Copyright 2026 The Karakoram Authors` comments. Verify incorporated code's license compatibility and retain required headers/NOTICE attribution. Apache-2.0 grants no third-party trademark rights.

Report vulnerabilities privately through [security reporting](docs/security.md#reporting-vulnerabilities), security@karakoram.dev: include impact, revision, prerequisites and a minimal redacted reproduction, not public exploit details or secrets.
