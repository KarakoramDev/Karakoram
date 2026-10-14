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

Requirements: Go 1.24+, Make, Node 22.6+ and npm for the UI (the UI test runner uses Node's built-in TypeScript support). Docker and a Linux host are needed for the current agent integration suite's host networking. A Zerops account is not needed for unit tests. Install UI dependencies and build:

```sh
make ui
make build
make test
```

The binaries are `bin/karakoram` and `bin/karactl`. `make build-all` builds the UI first. A plain Go build without existing UI output intentionally does not embed the SPA. Build metadata comes from Make's `VERSION`, `COMMIT` and `BUILD_DATE` variables.

For frontend work, run `npm run dev` in `ui/`; Vite serves port 5173 and proxies API requests to localhost:8080. `KARAKORAM_API_URL` selects a different development backend. See [ui/README.md](ui/README.md). A local server still needs the actual settings in [internal/server/config.go](internal/server/config.go), including a persistent data directory, master key, public origin and Zerops backend configuration; do not substitute fake production credentials or commit a populated environment file.

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
5. Run `make openapi-check`; it compares Go contract/client generation against the worktree. Also review/check the generated TypeScript diff—Make's OpenAPI target does not cover that artifact.

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
