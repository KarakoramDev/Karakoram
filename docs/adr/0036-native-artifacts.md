# 0036 — Embedded native artifact lock and release-bound publication

Status: Accepted

## Context

PostgreSQL's client proxy and modular Supabase need native executables that are not all available through the selected distribution repositories. Runtime downloads must identify exact bytes, source and licensing rather than trust a floating release. Source-built applications also need a publication path independent of control-plane authentication and the infrastructure backend.

## Decision

Embed `internal/engine/nativeartifacts/lock.json` in the Karakoram binary. Each available artifact records `name`, `version`, `file`, `sha256`, `size`, `upstreamURL`, `license`, immutable `source.repo`/`source.commit`, and `build`. Source bundles may include `runtimeDeps`; the PostgreSQL 17 extension bundle additionally declares only qualified `extensions` and `extensionFlags` for preload requirements and unavailable features. The lock is the availability authority. Unavailable artifacts have no placeholder entry, invented digest or fallback package.

Resolve the URL in the control plane, in this order:

1. `KARAKORAM_ARTIFACT_MIRROR` plus `/<file>` when configured, including official artifacts;
2. the locked official `upstreamURL`;
3. `https://github.com/KarakoramDev/Karakoram/releases/download/<running-version>/<file>` for a versioned release and a Karakoram-built artifact.

A development build without a mirror cannot resolve a Karakoram-built entry and returns an admission error naming the artifact and environment variable. A mirror changes transport location, not version, checksum or allowed capabilities. Serve exact locked bytes and use HTTPS in production. A mirror cannot make an absent lock entry available.

Preparation uses the existing executable/runtime deployment model. It downloads and checks SHA-256 before installation, resolves Debian dependencies with apt, and preserves application archives under `/opt/karakoram/<name>-<version>` with runtime ownership. Raw executables receive executable permissions. The matching Percona 17 extension archive instead extracts filesystem-root paths as root without recursively changing destination ownership. Package autostart prevention remains the engine's responsibility. Lock size is also checked during build/publication; member preparation's integrity check is SHA-256.

Native tooling in `scripts/native-artifacts/` uses disposable Ubuntu 24.04 Incus builder and clean-runner system containers, not application images or image extraction. Recipes pin application source commits, toolchain versions and archive hashes. Node bundles retain their pinned Node runtime; BEAM bundles retain ERTS/NIFs; extensions use Percona headers. Declared compatibility patches and their checksums accompany the affected source-built bundles. Normalized archive timestamps/order improve deterministic packaging without claiming byte-reproducible compiler, npm or Next.js output.

Keep outputs and provenance under ignored `dist/native-artifacts/`. An unfinished candidate remains outside the lock. Only successful native runtime qualification permits a source-built lock update; extension capabilities require successful native creation for each advertised extension. Qualification includes documented runtime packages and appropriate application/database checks, not merely source compilation. See the [tooling guide](../native-artifacts.md) for commands, prerequisites and build resource safeguards.

`make native-artifacts` drives the tooling. `scripts/release.sh build` stages Karakoram-built files using the **tagged** lock, checks their size and SHA-256, and fails when required files are absent or differ. It copies them into `dist/<version>/`, where release `checksums.txt` covers them alongside server/CLI artifacts and uses the existing release signer. The uploader publishes locked files and verifies anonymous downloads against their digests; it does not publish unqualified candidates. Official artifacts can remain upstream-hosted unless an operator mirror is selected.

Provenance distinguishes official binaries from Karakoram source builds. Record the actual full source commit, toolchains, build recipe, runtime packages, licenses and declared patches. Upstream signed metadata and byte integrity are separate claims: a matching SHA-256 alone is not a maintainer signature. Studio's full selected Supabase source commit is not described as a byte-identical rebuild of a separately published image abbreviation. Pin updates require evaluating dependency advisories, not silently rewriting upstream lockfiles.

## Consequences

Engine creation is bounded by the release's real artifact inventory. A usable source-built component in a development deployment requires both a lock entry and a configured mirror. Release preparation must produce all locked Karakoram-built bytes before staging. Mirrors can support private/offline supply chains without changing the engine specification, but operators must preserve the locked files and their provenance. The current artifact baseline is Linux amd64/Ubuntu 24.04; this is not cross-architecture availability.

## Alternatives considered

- **Control-plane download endpoint:** preparation cannot rely on authenticated control-plane artifact access on every backend; it would also couple provisioning bandwidth/availability to the coordinator.
- **Floating upstream latest URLs:** lose immutable version identity and cannot establish reproducible deployment inputs.
- **Extract application images:** adds an application-container supply chain and does not prove native runtime dependencies on Ubuntu 24.04.
- **Compile applications on each database/service member:** expands privileged prepare work, toolchain/resource requirements and deployment variance.
- **Advertise all recipes as available:** confuses intended support with produced, qualified native bytes. Reject unavailable components/extensions explicitly instead.

## References

[Artifact operations and provenance](../native-artifacts.md), [PostgreSQL decision](0034-postgres-engine.md), [Supabase decision](0035-supabase-engine.md), [backend-neutral deployment contract](0030-backend-neutral-infrastructure-contract.md).
