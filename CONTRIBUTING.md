<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Contributing to Karakoram

Thank you for helping. Karakoram is an Apache-2.0 orchestrator for Percona Server for MongoDB and Percona Backup for MongoDB on the [Zerops](https://zerops.io) platform. It is a community project: it is not affiliated with or endorsed by Zerops, Percona, MongoDB or the OpenEverest project.

Start with the [architecture](docs/architecture.md), [ADRs](docs/adr/README.md), [security model](docs/security.md) and [limitations](docs/limitations.md). Do not introduce Kubernetes dependencies or imply third-party endorsement.

## Code of conduct

Everyone taking part in this project — in issues, pull requests, reviews, discussions and chat — is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md). In short:

- Be respectful and assume good intent. Critique code and ideas, never people.
- No harassment, personal attacks, discriminatory language or unwelcome attention, in public or in private.
- Keep discussions on topic and constructive; explain the reasoning behind a request or a rejection.
- Respect maintainers' and reviewers' time: search existing issues first, keep pull requests focused, and answer review questions.

Report unacceptable behaviour to conduct@karakoram.dev. Maintainers may remove comments, close threads or block participants who do not follow these rules.

## How to contribute

1. For anything beyond a small fix, open an issue first to agree on the problem and the approach. Architectural changes need an ADR update.
2. Fork the repository, create a topic branch and keep each pull request to one logical change.
3. Write clear commit messages: a short summary line, a blank line, then what changed and why.
4. Add or update tests and documentation with the change, and make sure the [checks](#checks) pass locally.
5. Every commit must carry a `Signed-off-by` line (see below). A maintainer reviews every pull request; acceptance is at the maintainers' discretion.

### Developer Certificate of Origin

To keep a clear chain of provenance, every commit must be signed off under the [Developer Certificate of Origin 1.1](https://developercertificate.org/). By adding the line

```
Signed-off-by: Your Name <you@example.com>
```

(`git commit -s` does it for you) you certify that you wrote the change or otherwise have the right to submit it under the project's Apache-2.0 license. Use your real name; anonymous or bot sign-offs are not accepted.

## AI/LLM-assisted contributions

AI coding assistants and LLMs are allowed. We ask for a careful, conservative approach guided by engineering judgment — in other words: use common sense and do not blindly accept generated suggestions.

- **A human submits every contribution.** Automated or bot-driven pull requests are not accepted.
- **You are fully responsible for what you submit, including everything an AI produced for you.** You must understand both the design and the implementation, be able to explain and defend every line in review, and own its correctness, security and licensing exactly as if you had typed it yourself. "The AI wrote it" is never an answer to a review question.
- LLMs are fine for prototyping, boilerplate, tests scaffolding and early self-review. Large or complex logic — especially reconciliation, data-safety, backup/restore and security code — must be authored and fully understood by you; do not submit unreviewed generated output.
- Make sure generated code does not reproduce third-party code under an incompatible license, and that it contains no secrets or private data from your environment.
- **Disclose AI use** in the pull request description when it meaningfully contributed to the change (which tool, and for what).
- Before asking a human for review, you are welcome to run an LLM review of your own patch to catch typos and rough edges. Do not post AI review bots into pull request threads; keep discussions between people.

Maintainers may ask for clarification or decline contributions where AI use raises concerns. This policy will be revisited as tools mature; it is adapted from the [Cloud Hypervisor contribution guidelines](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/CONTRIBUTING.md#aillm-assistance--generated-code).

## Development setup

Requirements: Go 1.25+, Make, Node 22.6+ and npm for the UI (the UI test runner uses Node's built-in TypeScript support). Docker and a Linux host are needed for the current agent integration suite's host networking. A Zerops account is not needed for unit tests. Install UI dependencies and build:

```sh
make ui
make build
make test
```

The binaries are `bin/karakoram` and `bin/karactl`. `make build-all` builds the UI first. A plain Go build without existing UI output intentionally does not embed the SPA. Build metadata comes from Make's `VERSION`, `COMMIT` and `BUILD_DATE` variables.

`make build` is the development build. Published release artifacts come from
`make release`, not from copying `bin/`; see [Releasing](#releasing).

For frontend work, run `npm run dev` in `ui/`; Vite serves port 5173 and proxies API requests to localhost:8080. `KARAKORAM_API_URL` selects a different development backend. See [ui/README.md](ui/README.md). A local server still needs the actual settings in [internal/server/config.go](internal/server/config.go), including a persistent data directory, master key, public origin and Zerops backend configuration; do not substitute fake production credentials or commit a populated environment file.

## Releasing

A maintainer cuts a release from `main` after a major change, a security fix,
or enough accumulated fixes to warrant an upgrade. Releases are not made for
every commit, and pushing commits does not automatically create tags or releases.

Use SemVer tags `vMAJOR.MINOR.PATCH`. Before 1.0, increment MINOR for new
features or breaking changes (document breaking changes and upgrade steps),
and PATCH for fixes and security updates. From 1.0 onward, breaking changes
increment MAJOR, backward-compatible features MINOR, and fixes PATCH.
A prerelease suffix such as `-rc.1` is allowed; pin one for deployment only
when deliberately releasing it.

The default Zerops setup deploys the release pinned in `deploy/zerops/RELEASE`,
even when `main` contains newer source. **Keep its deploy/run sections compatible
with the pinned binary.** A runtime configuration change requiring a new binary
must land with the release that ships it, or use `karakoram-source` until then.

### Prerequisites and release notes

Use Go 1.25+, Node 22.6+ with npm, Make, Bash, Git, GNU tar, GNU coreutils
(including `date` and `sha256sum`), gzip, and OpenSSH `ssh-keygen` with
`-Y sign`/`-Y verify` support. For publication, install `gh`, authenticate it
with release rights for the public repository, and have Git push access to
`origin` for tags and `main`.

The release signing private key defaults to
`~/.config/karakoram/release-signing-ed25519`; override it with
`KARAKORAM_RELEASE_SIGNING_KEY`. Its public key must be authorized in
[`deploy/allowed_signers`](deploy/allowed_signers) for principal
`release@karakoram.dev`. Keep the private key out of the repository.

Write a release-notes file describing changes, security fixes, upgrade steps,
and state-schema/rollback compatibility. Keep it outside the checkout (or commit
it before preparation) so the release tool's clean-tree checks succeed.
The `NOTES` argument names that file; the tool does not generate release notes.
Run the applicable [checks](#checks) and review the changes before preparation;
the release targets do not replace the test and lint suites.

### Prepare, build, and publish

For example, to release `v0.1.2`:

```sh
make release-prepare VERSION=v0.1.2
make release-build VERSION=v0.1.2
make release VERSION=v0.1.2 NOTES=../release-notes-v0.1.2.txt
```

- **`release-prepare`** requires a clean tree on `main` equal to `origin/main`,
  a tag absent both locally and on `origin`, and a version newer than the current
  pin. It detaches HEAD (local `main` stays unchanged), writes
  `deploy/zerops/RELEASE`, creates a signed-off `Release v0.1.2` commit and an
  annotated tag. It does not push; review this commit and tag before publishing.
- **`release-build`** is a local dry run: it checks that the tag exists and pins
  itself, exports that tag with `git archive`, builds the UI once and cross-builds
  the server for Linux amd64/arm64 and `karactl` for Linux/macOS amd64/arm64 and
  Windows amd64. It writes reproducible server archives (binary, LICENSE,
  NOTICE), raw CLI binaries, `checksums.txt` and its SSH signature to
  `dist/v0.1.2/`, then verifies the signature and all checksums. It requires the
  signing key but does not publish or push.
- **`release`** requires a clean tree with HEAD at the tagged commit and
  `origin/main` an ancestor of it; a conflicting remote tag is rejected. It
  rebuilds, pushes the tag, creates a draft GitHub release using `NOTES`,
  downloads all draft assets, compares them byte-for-byte with the local files
  and verifies their signature/checksums. Only then does it publish the release
  (as latest, or as a prerelease for `-rc.N` versions) and fast-forward
  `origin/main` to the release commit. This ordering keeps `main` from pinning an
  unpublished release. Rerun the same command after a failure: an unfinished
  draft is deleted and recreated, and an already published release is only
  re-verified before `main` is fast-forwarded. Afterwards update the local
  `main` with `git checkout main && git merge --ff-only origin/main`.

Release builds use `CGO_ENABLED=0`, `-trimpath`, `-buildvcs=false`, the full tagged
commit SHA, and the tag commit's UTC timestamp as the build date. Archive paths,
ordering, owner/group and timestamps are normalized, and gzip omits timestamps.
To reproduce binary checksums, use the **same exact Go toolchain version** as
the original build, the tagged source and its locked npm dependencies; inspect
an artifact with `go version -m <binary>` to identify its Go version and build
settings. A merely compatible newer Go version need not produce identical bytes.

### Signing-key rotation and verification

Before rotating keys, add the new public key to `deploy/allowed_signers` and
commit that change, then prepare and sign the next release with the new private
key. Retain old authorized keys for old-release verification; OpenSSH
`allowed_signers` supports `valid-after`/`valid-before` options when setting
key-validity windows. Distribute the updated trust file through a trusted channel.

To verify the complete locally built release:

```sh
RELEASE=v0.1.2
(cd "dist/$RELEASE" &&
  ssh-keygen -Y verify -f ../../deploy/allowed_signers \
    -I release@karakoram.dev -n karakoram-release \
    -s checksums.txt.sig < checksums.txt &&
  sha256sum --strict -c checksums.txt)
```

For downloaded release assets, follow the
[download and signature-verification procedure](docs/install.md#install-karactl):
authenticate the signing key, verify the signature over `checksums.txt`, then
verify the selected binary or server archive against that manifest before use.

## Change boundaries

- Generic desired-state durability belongs in `internal/reconciler`/`internal/store`; MongoDB mechanisms belong in `internal/engine/psmdb`; Zerops wire formats belong in `internal/infra/zerops`.
- Provider metadata drives versions, topology constraints and UI defaults. Do not hand-maintain a second catalog in the CLI/UI.
- Perform at most one disruptive reconciliation action per step. Persist intent/checkpoints and externally returned identifiers before progressing; observe actual completion. SQLite and remote services do not form one atomic transaction.
- Preserve agent config/controls/receipt compatibility across rolling deployments. Maintenance/process holds must survive restart and periodic pulls. An ambiguous PBM initiation is not authorization to issue another restore.
- Ordinary resource/status/log/event responses must not reveal credentials. Preserve separate `cluster-credentials:read` enforcement, per-object list/stream filtering and deny-overrides policies.
- Make focused changes, add regression coverage for the actual behavior, update affected guides and callers, and remove obsolete paths. For a changed architectural decision, update the numbered ADR with its consequences and evidence.

## Public API changes

1. Edit [api/openapi.yaml](api/openapi.yaml), including `x-karakoram-rbac`, request/response schemas, write-only secret fields and errors.
2. Run `make generate` to regenerate the Go server (`internal/api/gen.go`), public client (`pkg/client/client.gen.go`, package `client`) and RBAC operation map. Do not edit generated files directly. Keep hand-written client helpers separate and independent of `internal/` packages; `pkg/client` is imported by external applications as well as the CLI. See the [consumer SDK guide](docs/user-guide/api.md#clientssdk).
3. In `ui/`, run `npm run gen:api` to regenerate TypeScript types; this is separate from Go generation.
4. Migrate server handlers, CLI commands, UI consumers and tests together. Regenerate the CLI guide from recursive `karactl --help` when its surface changes.
5. Run `make openapi-check`; it rejects YAML aliases and regenerates/compares both Go contract/client/RBAC files and TypeScript types. Install the UI dependencies before running it.

## Checks

```sh
make test
make test-integration
make lint
make openapi-check
```

Integration tests use real database containers. Read the test's prerequisites before running them; skipped integration tests are not evidence of passing database behavior. `make lint` runs Go vet, staticcheck 2025.1.1 and UI typecheck/ESLint, so install UI dependencies first.

Live tests are opt-in and destructive to their own resources:

```sh
# Load credentials using a secret manager, without shell tracing.
# KARAKORAM_E2E_URL and KARAKORAM_E2E_TOKEN must already be set.
make e2e
```

Read [test/e2e/README.md](test/e2e/README.md) for S3, IPv6 allowlist, CLI and OIDC settings. Use an isolated test installation in a dedicated Zerops project. The suite changes global policy/settings and restores them; do not run against production or concurrently with another live mutation suite. Do not modify the platform's `core` service. In pull requests, state which live scenarios you ran and their results.

## Security and licensing

Never include Zerops integration tokens, API tokens, master keys, passwords, PEM private keys, signed upload/log URLs or populated secret fixtures in commits, screenshots or issue reports. Treat CLI configuration and state archives as credentials. Use synthetic test secrets, redact reports, and avoid enabling shell trace around credential-bearing commands.

New source files should carry `SPDX-License-Identifier: Apache-2.0` and `Copyright 2026 The Karakoram Authors`, using the language's comment syntax. If third-party implementation is deliberately incorporated, verify license compatibility and retain required original file headers and NOTICE attribution. Apache-2.0 does not grant third-party trademark rights.

For suspected vulnerabilities, follow the private-reporting guidance in [security.md](docs/security.md#reporting-vulnerabilities) (security@karakoram.dev), rather than posting exploit details or secrets publicly. Describe impact, affected revision, prerequisites and a minimal redacted reproduction.
