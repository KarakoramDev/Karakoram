# 0002 — Go, React/TypeScript and a shared server/agent binary

Status: Accepted

## Context

The control plane needs long-running reconciliation, a MongoDB driver, a small supervised node process and a scriptable CLI. Installation must be self-contained and avoid a public source repository dependency for every database member. The UI needs a typed contract shared with the API.

## Decision

Use Go for the control plane, node agent and CLI, initially with the repository's Go 1.24 directive; change the toolchain only on demonstrated dependency/build requirements. Use React 18, TypeScript and Vite for the SPA. Embed production UI assets in the Go server with `go:embed`.

Ship one `karakoram` executable with `server`, `agent`, `version` and administrative subcommands. The control plane uploads its own executable as the member artifact; members run `karakoram agent` (ADR 0007). Ship `karactl` as a separate Go/Cobra executable using the generated REST client. The browser and CLI call the same public API; neither talks directly to Zerops or receives its token. Generate API types rather than hand-maintaining parallel models (ADR 0015).

## Consequences

One server/agent release artifact reduces deployment drift and eliminates a member-side compiler or Node.js runtime. Members carry unused embedded UI/server code, accepting a larger binary for simpler distribution. Build-time UI tooling remains necessary. Server/agent protocol compatibility must be maintained during rolling deployment; upgrading the control plane does not authorize an engine upgrade. Go package boundaries remain distinct even though the executable is shared.

## Alternatives considered

- **Separate server and agent executables:** smaller member artifacts, but more release/distribution compatibility combinations.
- **TypeScript control plane and agent:** one language with the UI, but adds node runtime distribution and weaker reuse of Go database/operational libraries.
- **Server-rendered UI only:** reduces frontend tooling but does not fit the required interactive operational dashboard as well.
