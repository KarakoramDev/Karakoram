# 0036 — Embedded native artifact lock and release-bound publication

Status: Accepted

## Context

[PostgreSQL](https://www.postgresql.org)'s client proxy and modular [Supabase](https://supabase.com) need native executables that are not all available through the selected distribution repositories. Runtime downloads must identify exact bytes, source and licensing rather than trust a floating release. Source-built applications also need a publication path independent of control-plane authentication and the infrastructure provider.

## Decision

Embed `internal/engine/nativeartifacts/lock.json` in the Karakoram binary. Each available artifact records `name`, `version`, `file`, `sha256`, `size`, `upstreamURL`, `license`, immutable `source.repo`/`source.commit`, and `build`. Source bundles may include `runtimeDeps`; PostgreSQL extension bundles additionally declare `kind: "pg-extension"`, `pgMajor` (16, 17, or 18), and only qualified `extensions` and `extensionFlags` for preload requirements and unavailable features. The original `supabase-pg17-extensions` entry retains its absent kind/major fields and a narrow PostgreSQL 17 compatibility mapping with a frozen validator to preserve its rendered preparation command byte-for-byte. New bundles require explicit metadata. The lock is the availability authority. Unavailable artifacts have no placeholder entry, invented digest or fallback package.

Resolve the URL in the control plane, in this order:

1. `KARAKORAM_ARTIFACT_MIRROR` plus `/<file>` when configured, including official artifacts;
2. the locked official `upstreamURL`;
3. `https://github.com/KarakoramDev/Karakoram/releases/download/<release>/<file>` for a Karakoram-built artifact, using explicit `KARAKORAM_ARTIFACT_RELEASE` when configured, otherwise the running release version.

A development build without a mirror or explicit artifact release cannot resolve a Karakoram-built entry and returns an admission error naming the artifact and configuration choices. A mirror or artifact release changes transport location, not version, checksum or allowed capabilities. Serve exact locked bytes and use HTTPS in production. Neither setting can make an absent lock entry available. An artifact release override leaves official upstream URLs intact; the global mirror still overrides those URLs too.

Git-describe distance/hash versions (for example `v1.2.3-4-gabcdef0`, optionally
ending in `-dirty`) are development builds for resolution, not release tags.
Reject their implicit release fallback with the same missing-mirror error before
member deployment. An operator may select a published artifact release only when
all required locked bytes are present and match. The override does not probe
remote existence or change the running executable used for member deployment.

Preparation uses the existing executable/runtime deployment model. It downloads and checks SHA-256 before installation, resolves Debian dependencies with apt, and preserves application archives under `/opt/karakoram/<name>-<version>` with runtime ownership. Raw executables receive executable permissions. PostgreSQL extension archives instead validate the declared major, fail closed on missing/invalid majors, and accept only regular files/directories within that major's `usr/lib/postgresql/<pgMajor>` and `usr/share/postgresql/<pgMajor>` trees and their directory ancestors. Root extracts into private staging and copies only those two trees without changing destination ownership; archives never extract directly into the system root. Package autostart prevention remains the engine's responsibility. Lock size is also checked during build/publication; member preparation's integrity check is SHA-256.

Native tooling in `scripts/native-artifacts/` uses disposable [Ubuntu](https://ubuntu.com) 24.04 [Incus](https://linuxcontainers.org/incus/) builder and clean-runner system containers, not application images or image extraction. Recipes pin application source commits, toolchain versions and archive hashes. [Node](https://nodejs.org) bundles retain their pinned Node runtime; BEAM bundles retain ERTS/NIFs; extensions use [Percona](https://www.percona.com) headers. Declared compatibility patches and their checksums accompany the affected source-built bundles. Normalized archive timestamps/order improve deterministic packaging without claiming byte-reproducible compiler, [npm](https://docs.npmjs.com/) or [Next.js](https://nextjs.org) output.

Keep outputs and provenance under ignored `dist/native-artifacts/`. An unfinished candidate remains outside the lock. Only successful native runtime qualification permits a source-built lock update; extension capabilities require successful native creation for each advertised extension. Qualification includes documented runtime packages and appropriate application/database checks, not merely source compilation. See the [tooling guide](../native-artifacts.md) for commands, prerequisites and build resource safeguards.

`make native-artifacts` drives the tooling. `scripts/release.sh build` stages Karakoram-built files using the **tagged** lock, checks their size and SHA-256, and fails when required files are absent or differ. It copies them into `dist/<version>/`, where release `checksums.txt` covers them alongside server/CLI artifacts and uses the existing release signer. The uploader publishes locked files and verifies anonymous downloads against their digests; it does not publish unqualified candidates. Official artifacts can remain upstream-hosted unless an operator mirror is selected.

Provenance distinguishes official binaries from Karakoram source builds. Record the actual full source commit, toolchains, build recipe, runtime packages, licenses and declared patches. Upstream signed metadata and byte integrity are separate claims: a matching SHA-256 alone is not a maintainer signature. Studio's full selected Supabase source commit is not described as a byte-identical rebuild of a separately published image abbreviation. Pin updates require evaluating dependency advisories, not silently rewriting upstream lockfiles.

## Consequences

Engine creation is bounded by the release's real artifact inventory. A usable source-built component in a development deployment requires both a lock entry and a configured mirror or explicit artifact release containing the locked bytes. Release preparation must produce all locked Karakoram-built bytes before staging. Mirrors can support private/offline supply chains without changing the engine specification, but operators must preserve the locked files and their provenance. The current artifact baseline is [Linux](https://kernel.org) amd64/Ubuntu 24.04; this is not cross-architecture availability.

## Alternatives considered

- **Control-plane download endpoint:** preparation cannot rely on authenticated control-plane artifact access on every provider; it would also couple provisioning bandwidth/availability to the coordinator.
- **Floating upstream latest URLs:** lose immutable version identity and cannot establish reproducible deployment inputs.
- **Extract application images:** adds an application-container supply chain and does not prove native runtime dependencies on Ubuntu 24.04.
- **Compile applications on each database/service member:** expands privileged prepare work, toolchain/resource requirements and deployment variance.
- **Advertise all recipes as available:** confuses intended support with produced, qualified native bytes. Reject unavailable components/extensions explicitly instead.

## References

[Artifact operations and provenance](../native-artifacts.md), [PostgreSQL decision](0034-postgres-engine.md), [Supabase decision](0035-supabase-engine.md), [provider-neutral deployment contract](0030-backend-neutral-infrastructure-contract.md).
