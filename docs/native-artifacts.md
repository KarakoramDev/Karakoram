# Native engine artifacts

`internal/engine/nativeartifacts/lock.json` is embedded in the control plane. Each entry names exact [Linux](https://www.kernel.org/) amd64 bytes, their SHA-256 and size, upstream URL (empty for Karakoram-built files), SPDX license, immutable upstream repository/commit, and build recipe. Source-built entries also record the clean-runner `runtimeDeps`. Extension bundles expose an `extensions` list containing only names that passed native creation smoke; engine driver catalog/admission must use it rather than advertise failed builds. An unavailable artifact has no lock entry: enabling a component must fail admission rather than download a floating release or install a substitute.

## Artifact inventory

| Artifact | Version | Source | License | Installation |
| --- | --- | --- | --- | --- |
| `openbao` | 2.7.1 | [Official release](https://github.com/openbao/openbao/releases/tag/v2.7.1) | MPL-2.0 | `/opt/karakoram/openbao-2.7.1/bao`; archive executable mode is preserved. |
| `openbao-plugin-database-mongodb` | 0.0.1 | [Official release](https://github.com/openbao/openbao-plugins/releases/tag/database-mongodb-v0.0.1) | MPL-2.0 | Versioned plugin directory; lock `executableMembers` marks `openbao-plugin-database-mongodb_linux_amd64_v1` executable as the runtime owner. |
| `openbao-plugin-kms-aws` | 0.1.0 | [Official release](https://github.com/openbao/openbao-plugins/releases/tag/kms-aws-v0.1.0) | MPL-2.0 | Versioned plugin directory; lock `executableMembers` marks `openbao-plugin-kms-aws_linux_amd64_v1` executable as the runtime owner. |

[OpenBao](https://openbao.org/) installs the [MongoDB](https://www.mongodb.com/) database plugin on every member so the first connection does not need a plugin-install redeployment; the [AWS KMS](https://aws.amazon.com/kms/) plugin is installed only for `seal.type: awskms`, an immutable creation choice. Plugin registration uses the locked executable/version, not a runtime OCI download. Artifact availability does not establish provider lifecycle or database-connection acceptance; see [OpenBao verification](verification.md#openbao-coverage).
[Temporal](https://temporal.io/) uses official locked `temporal-server-1.31` (1.31.4), `temporal-server-1.32` (1.32.1), and `temporal-ui-server` (2.56.0) artifacts. Server archives contain `temporal-server`, `temporal-sql-tool`, `tdbg` and configuration; the UI archive supplies `ui-server`. The UI version follows the release lock, not `spec.version`. See [Temporal prerequisites](temporal/README.md).

## Download and verification

Resolution order is:

1. `KARAKORAM_ARTIFACT_MIRROR`, with `/<file>` appended;
2. the artifact's official `upstreamURL`;
3. for a versioned release, `https://github.com/KarakoramDev/Karakoram/releases/download/<running-version>/<file>`.

A development build needs a mirror for Karakoram-built files. The typed `MissingMirrorError` names the artifact and required environment variable. Configure the variable on the control plane; resolved URLs are rendered into member preparation commands. Mirrors must serve the exact locked bytes. Production mirrors should use HTTPS.

[Neo4j](https://neo4j.com/) Community uses the official `neo4j-community-2026.09.0-unix.tar.gz` or `neo4j-community-2026.10.0-unix.tar.gz`, plus the official [Eclipse Temurin](https://adoptium.net/) 21 JRE `21.0.12.1+1` archive. All three have locked SHA-256, size and upstream provenance; mirrors must preserve those bytes. They are upstream releases, not Karakoram source rebuilds. See the [Neo4j guide](neo4j/README.md).

Preparation uses one privileged shell and a fresh root-owned mode-0700 staging directory per artifact. Downloads refuse existing files and symlinks, SHA-256 is checked inside that directory, and every install reads the same protected file; an exit trap removes staging on success or failure. [Python](https://www.python.org/) 3's standard-library `tarfile` validates actual archive headers before extraction, independently of GNU tar's locale-dependent listing/quoting. Unicode names and internal relative symlinks are supported; absolute/traversal member paths, control characters, escaping or cyclic links, members beneath symlink ancestors, and nonregular hardlink targets are rejected. Application archives preserve their layout under the caller's immutable `/opt/karakoram/<name>-<version>` directory. Root opens the verified artifact as standard input, then drops to the provider's configured runtime user before extracting or installing into application paths; existing runtime-owned destination symlinks therefore cannot cause privileged writes. GNU tar uses `--no-same-owner --no-same-permissions --delay-directory-restore`. [PostgreSQL](https://www.postgresql.org/) extension archives declare `kind: "pg-extension"` and `pgMajor: 16`, `17`, or `18` in the lock. Preparation passes that major to the validator and fails closed on absent/unsupported majors or non-tar extension artifacts. Only regular files/directories under `usr/lib/postgresql/<pgMajor>` and `usr/share/postgresql/<pgMajor>` (plus their ancestor directories) are accepted; root extracts into private staging with the additional `--keep-old-files` option, then copies only those two trees to the system-owned destination, without preserving archive ownership. It never extracts into `/` or changes destination ownership with chown. Raw binaries are installed as `<destDir>/<artifact.name>`. [Debian](https://www.debian.org/) packages are installed with `apt-get install -y` from protected staging so dependency resolution remains with apt. The engine must suppress package-service autostart when required by its process supervision model.

On a fresh host, preparation creates missing application parent directories with mode 0755 and creates the installation leaf before dropping privileges. Only a newly created leaf receives a nonrecursive, no-follow ownership handoff to the configured runtime user; pre-existing directories and symlinks are never chowned. Application content publication remains unprivileged even when the parent hierarchy was initially root-owned.

### Preparation memory and small allocations

Native `.deb` installation sets `DPKG_DEB_THREADS_MAX=1` **inside the privileged
installer shell**, so apt and its dpkg-deb children inherit it even when sudo
clears the caller's environment. Dependencies, maintainer scripts and package
registration still run through apt; installation is not replaced by copying
files out of a package. Failures retain apt's original status and diagnostics
and add the artifact name and a reminder to inspect allocation memory limits.

Size preparation as well as the running service. In particular, the pinned
[ProxySQL](https://proxysql.com/) 3.0.11 package expands approximately 36.2 MiB into 151.3 MiB of tar
content. Its XZ stream has seven blocks with 8 MiB dictionaries and 24 MiB
expanded full blocks. Automatic dpkg-deb parallel decoding can buffer several
blocks based on host memory/CPU rather than the task's available cgroup budget:
it has caused an OOM during preparation of a 256 MiB [Nomad](https://developer.hashicorp.com/nomad) proxy.
Single-threaded dpkg-deb avoids that host-sized parallelism without increasing
the configured service memory or requiring Nomad memory oversubscription.
Allow additional memory for apt/dpkg, package scripts, and reclaimable filesystem
cache; archive size or dictionary size alone is not a whole-preparation budget.

This policy applies to every catalog `.deb` on all providers, including PostgreSQL
and [Supabase](https://supabase.com/) proxies. Nomad runs preparation within the task's configured memory
limit before the agent starts. [Dokploy](https://dokploy.com/) prepares the image during its build, not
inside the eventual service task; its builder needs its own capacity. [Zerops](https://zerops.io/)
receives the same preparation commands and controls their execution resources.
The policy does not change runtime sizing defaults/minima or make arbitrarily
small positive RAM values safe. Non-catalog repository packages and tarballs
retain their own compression requirements: gzip uses a 32 KiB history window;
the pinned Supabase Auth XZ archive requires a 64 MiB dictionary (REST: 8 MiB),
in addition to installer and application overhead. Increase an undersized
explicit configuration rather than relying on retries to get lucky.
Consumers select new extension bundles through `nativeartifacts.ExtensionArtifact(product, major)`, which resolves `<product>-pg<major>` only when the locked `kind` and `pgMajor` match. A recipe alone does not establish availability. The existing `supabase-pg17-extensions` entry intentionally retains its original name and absent kind/major fields: a narrow compatibility mapping installs it as PostgreSQL 17 with its frozen validator and original invocation. The validator source is embedded in rendered commands, so even adding a major argument or changing its source would change existing deployment command bytes. A golden test pins that complete command; all new extension bundles use the generalized validator and explicit metadata.

Application tarballs also retain the frozen validator source and original `application` invocation so their existing deployment commands remain byte-identical. Only explicitly typed PostgreSQL extension artifacts select the generalized validator.
### Percona MySQL

[MySQL](https://www.oracle.com/mysql/) catalog releases pair a versioned `percona-server-<version>` entry with the same-major `percona-xtrabackup-<version>` entry. These are official GPL-2.0-only [Linux](https://www.kernel.org/) amd64 glibc2.35 **minimal** tarballs, with immutable upstream tag commits and verified download hashes and sizes. The 8.4 line uses [XtraBackup](https://www.percona.com/mysql/software/percona-xtrabackup) 8.4.0-7; the 8.0 line uses 8.0.35-36. The lock also pins [Percona](https://www.percona.com/) XtraDB Cluster 8.4.11-11.1 and 8.0.46-38.1 as official GPL-2.0-only Linux amd64 glibc2.35 minimal tarballs. The Server and XtraBackup entries use `libaio1t64`, `libnuma1`, and `libev4t64` (the latter is needed by `xbcloud`); [PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster) additionally uses `socat` and `openssl` for its SST scripts.

Percona Server entries also require Ubuntu 24.04's `libtirpc3t64`, which supplies
`libtirpc.so.3` for Group Replication. PXC's pinned tarballs instead bundle their
own `libtirpc.so`; its Galera topology does not load Group Replication.
The X plugin is disabled by the managed server configuration.
Checking only `mysqld --version` does not exercise dynamic plugin loading.
The shared MySQL/PXC preparation explicitly installs this package on every
backend: inside Nomad's exec rootfs before agent startup, in Dokploy's generated
Ubuntu 24.04 image build, and through Zerops `prepareCommands` on `ubuntu@24.04`.
Do not rely on a backend base image or unrelated package to supply it implicitly.
These dependencies are APT packages, not additional native mirror objects; the
locked Percona tarball bytes and checksums are unchanged.

Archive extraction retains the upstream top-level directory. Install into `/opt/karakoram/percona-server-<version>`, `/opt/karakoram/percona-xtrabackup-<version>`, or `/opt/karakoram/percona-xtradb-cluster-<version>` and set the member's `serverDir` / `xtraBackupDir` to the contained upstream directory, not the outer installation root. No package service is installed. Member payloads contain sealed-store credentials resolved for authenticated delivery and must be cached with owner-only permissions; never log them. Only allowlisted my.cnf parameters may cross the member boundary.

## Official Weaviate artifacts

The lock includes these upstream [Linux](https://www.kernel.org/) amd64 release archives; they are official downloads, not Karakoram source rebuilds:

| Lock entry | Version | Archive |
| --- | --- | --- |
| `weaviate-1.40` | `1.40.1` | `weaviate-v1.40.1-linux-amd64.tar.gz` |
| `weaviate-1.39` | `1.39.12` | `weaviate-v1.39.12-linux-amd64.tar.gz` |

Upstream assets are published at `https://github.com/weaviate/weaviate/releases/download/v<VERSION>/<ARCHIVE>`. The lock records exact byte size, SHA-256 cross-checked against upstream release checksum sidecars, and the dereferenced source tag commit. Member preparation downloads from upstream or an exact-byte operator mirror, verifies the locked digest and installs the root-layout archive into the versioned [Weaviate](https://weaviate.io/) directory. Checksum agreement establishes byte integrity, not an independent maintainer signature.

The executable contains BSD-3-Clause community code and runtime-gated enterprise code: its lock license is `BSD-3-Clause AND LicenseRef-Weaviate-Enterprise`, **not BSD-only**. Karakoram enables only community features, forces namespaces/self-recovery/backup-dedupe off, never supplies a license key, and does **not redistribute Weaviate binaries**. Do not publish these archives as Karakoram release payloads. Review [Weaviate licensing and feature limits](weaviate/README.md#licensing) and the [upstream license map](https://github.com/weaviate/weaviate/blob/v1.40.1/LICENSE).

## Native rebuild

Prerequisites on the build host: [Python](https://www.python.org/) 3.11+, sufficient disk space, and either [Incus](https://linuxcontainers.org/incus/) with [Ubuntu](https://ubuntu.com/) 24.04 images and an egress-capable network or the [rootless Ubuntu transport](#rootless-ubuntu-transport) for standalone [PostgreSQL](https://www.postgresql.org/) extensions. No application-container runtime is used.

```sh
export NATIVE_ARTIFACT_NETWORK=incusbr0
make native-artifacts                         # source builds + verified official downloads
make native-artifacts ARTIFACT=supabase-postgres-meta
scripts/native-artifacts/build.sh supabase-realtime supabase-studio  # ordered subset
```

`NATIVE_ARTIFACT_BUILDER` and `NATIVE_ARTIFACT_RUNNER` override the disposable instance names. The builder is limited to 6 GiB/8 vCPU, [Node](https://nodejs.org/)'s heap to 4 GiB, and BEAM to two schedulers plus one dirty CPU/IO scheduler. A clean 2 GiB runner starts only after the builder has stopped. Before builder startup and each heavy compilation step, `free -m` must report at least 14 GiB available host memory; otherwise the build waits. At least 120 GiB free disk is required, and configured running-instance memory for the shared name prefix must stay within 28 GiB. Set `NATIVE_ARTIFACT_MEMORY_PREFIX` when sharing a separately named lab. Do not run independent builds concurrently.

`recipes.json` pins source commits and toolchain versions; `extensions.json` pins each PostgreSQL extension. `toolchains.json` pins exact toolchain URLs, SHA-256 values, sizes, and the official checksum provenance. These pins were independently checked against downloaded bytes, including older releases' official SHA-256 checksum assets. Rebuilds use these checked-in pins, not mutable release API metadata. Source checkouts are detached and checked against the pinned commit. Toolchain download caches, build logs, manifests (including toolchain transport hashes), smoke logs, and `build-report.json` are retained under the gitignored `dist/native-artifacts/` directory. Ubuntu runtime apt dependencies are recorded per recipe and in the packaged manifest.

Reusable compiled OTP installations require `/opt/otp-<version>/.karakoram-build-inputs`, written only after configure, compilation, installation and the executable check succeed. Before considering reuse, the builder verifies the pinned source archive again and compares the receipt against its source SHA-256, the builder script hash/configure arguments, fixed compilation environment, machine architecture and sorted installed-package versions (including the system compiler and development libraries). Missing or different receipts discard both the source/build tree and installation before extracting and compiling the verified archive; an existing `bin/erl` alone never authorizes reuse. Package or build-script changes conservatively invalidate the cache, even at the same OTP version. Other precompiled toolchains are installed afresh into cleared directories; extension builds also clear compiled cargo-pgrx CLI and PGRX state rather than allowing Cargo's same-version installation shortcut.

The local OTP cache-integrity tests run the real verification/build shell with `TMPDIR` set to their disposable fixture directory, so shell-created receipts use the same filesystem as the toy source and installation rather than a separately quota-limited host `/tmp`. Shell failures retain their exit status and include captured stdout/stderr in the traceback.

Build bootstraps are inside the same integrity boundary: `toolchains.json` pins [npm](https://docs.npmjs.com/) 11.12.1, [pnpm](https://pnpm.io/) 11.13.1, the amd64 [Turbo](https://turbo.build/) 2.9.14 binary, [Hex](https://hex.pm/) 2.5.3 and [Rebar3](https://rebar3.org/) 3.24.0 (the OTP-27 build, compatible with both pinned OTP 27 and 28). Their exact bytes are SHA-256-checked on download and again inside the builder, and included in manifest toolchain transports. npm/pnpm include their bundled dependencies and are unpacked directly; Turbo uses its verified platform binary rather than `pnpm dlx` or an installer that resolves optional dependencies. npm registry SHA-512 metadata and Hex's published SHA-512 CSVs were checked when selecting these pins. These byte pins, not mutable metadata fetched during a build, anchor rebuild integrity. This does not authenticate unpinned Ubuntu build packages or claim byte-reproducible compiler output.

Mix bootstrap authentication differs across the pinned [Elixir](https://elixir-lang.org/) versions: [1.18.2 verifies Hex's signed CSV using its embedded public key](https://github.com/elixir-lang/elixir/blob/v1.18.2/lib/mix/lib/mix/local.ex), whereas [1.19.5 uses HTTPS metadata without that signature check](https://github.com/elixir-lang/elixir/blob/v1.19.5/lib/mix/lib/mix/local.ex). Rebar's default installer also selects a compatible version rather than accepting an explicit version argument. Consequently recipes install the verified local Hex archive and Rebar executable; they do not rely on a uniform Mix signature guarantee or reuse an arbitrary pre-existing bootstrap.

The all-artifact order is Storage, postgres-meta, Realtime, Studio, the [Supabase](https://supabase.com/) PostgreSQL 17 extension bundle, [TimescaleDB](https://www.timescale.com/) PG16/17/18, [Citus](https://www.citusdata.com/) PG16/17/18, [imgproxy](https://imgproxy.net/), [Supavisor](https://github.com/supabase/supavisor), [Logflare](https://logflare.app/), Edge Runtime. Every fresh candidate manifest records `buildInputFingerprint`, a deterministic SHA-256 over the selected `recipes.json` entry and the checked-in build scripts/helpers, toolchain/bootstrap byte pins, auxiliary-asset/extension pins, and native patches. The support-tree hash deliberately includes inputs for other recipes too, preferring conservative invalidation to missed dependencies. A retained `.tar.gz.candidate` is re-smoked without recompiling only when its packaged recipe and build-input fingerprint exactly match; old manifests without the fingerprint and changed inputs are rejected. Candidates remain excluded from the lock until qualification and provenance validation succeed. Remove an obsolete candidate explicitly before requesting a rebuild. Fresh builds refresh recipe/helper scripts and patches even when reusing a running builder.

Node applications include their own pinned Node runtime. Elixir artifacts retain their complete mix release, ERTS and NIFs. Imgproxy targets Ubuntu's libvips baseline, not an upstream custom codec library image. Edge Runtime includes the main worker from the pinned Supabase source selection, with its two function-directory paths adapted to the required `FUNCTIONS_ROOT` runtime environment setting; original source and both hashes are included. The PostgreSQL extension tarball uses filesystem-root paths under `usr/lib/postgresql/17` and `usr/share/postgresql/17`; provenance and per-extension results live under `usr/share/postgresql/17/karakoram-native-artifacts/`. Install it only into the matching [Percona](https://www.percona.com/) PostgreSQL 17 environment.

`FUNCTIONS_ROOT` must point outside `/tmp` (for example `/var/lib/karakoram/functions`) and contain `<function-name>/index.ts` plus a root `deno.jsonc` import map. Edge Runtime mounts a separate private `/tmp` filesystem in each worker: host-created `/tmp/functions` is not visible to the main worker's directory check and returns `NOT_FOUND`, even when the files exist in the host namespace. The clean smoke invokes both a plain hello function and a function resolving a pinned npm import through the packaged main worker.

The lock's per-extension `extensionFlags` map defines the mode actually qualified. Only entries with `preload: true` may be added to `shared_preload_libraries`; this is `pg_net` for the source-built bundle. `pgsodium` is intentionally not preloaded and supplies crypto functions with caller-supplied keys only. Server root-key management, key-id operations and transparent column encryption (TCE) are unavailable and listed in `unavailableFeatures`. Do not install an ephemeral root key or enable those features implicitly. Vault 0.3.x does not require pgsodium.

Declared native compatibility patches in `scripts/native-artifacts/patches/` are applied to the exact pinned sources and included in affected bundles with `SHA256SUMS`. Supavisor and Logflare accept `KARAKORAM_DB_CA_FILE` to verify the upstream database CA through a TLS-transparent loopback forwarder, covering metadata, cluster discovery and analytics provider connections. Hostname verification is intentionally not used for that loopback endpoint; the CA chain is still verified. Supavisor tenant configuration must also select upstream TLS, peer verification and the upstream CA. Realtime's compiled pgdelta helper preserves its parser WASM files at the fixed `/opt/karakoram/<name>-<version>/app/pg-toolbelt` layout; the smoke exercises SQL parsing, not only BEAM startup.

Studio's native patch selects two build workers, webpack memory optimizations and Next's webpack build worker; its recipe uses `next build --webpack` with two Rayon threads. The webpack callback disables compiler caching, limits module parallelism to one, preserves upstream raw-text handling for Markdown/Deno declarations and explicitly aliases the two declaration imports to the pinned public assets (the TS path resolver otherwise fails those imports). This replaces the unbounded Turbopack attempt that exhausted the original builder reservation; it does not replace Studio's standalone Next application or change the pinned application source selection.

`studio-assets.json` pins the Deno v1.45.0 and Supabase functions-js 2.4.4 editor declarations selected by the upstream `build:deno-types` script, together with their MIT licenses. These generated public files must be present before compiling Studio. Their downloaded hashes are recorded in the artifact manifest; the functions-js declaration also matches JSR's versioned package checksum manifest. Versioned download pins provide transport integrity, not an independent maintainer signature.

The Studio recipe explicitly preserves the frozen dependency tree's `libpg-query` WASM at its original pnpm path and carries its license. Next's standalone tracer omits that dynamically opened file; HTTP health alone can pass while SQL parsing raises an asynchronous `ENOENT`. The clean smoke therefore runs a real `SELECT 1` parse using the bundled Node runtime before starting Studio, in addition to the required page/profile and live-meta HTTP probes.

Supavisor uses the pinned OTP 27.3.4.6 patch release with Elixir 1.18.2. The earlier OTP 27.2.1 attempt could not securely bootstrap Hex (`unsupported_certificate`, `key_usage_mismatch`); updating within OTP 27 avoids disabling certificate verification. Hex dependency fetches use Ubuntu's `/etc/ssl/certs/ca-certificates.crt` trust bundle via `HEX_CACERTS_PATH`. Bootstrap installs use the verified local pins described above, replacing pre-existing tools with the selected versions.

Only a successful native HTTP health smoke (or `CREATE EXTENSION` for each packaged extension) permits updating the lock. The clean runner installs the documented runtime packages and Python/[curl](https://curl.se/) smoke tooling. DB-dependent fixtures additionally install `gnupg lsb-release` for the verified repository bootstrap, the pinned Percona 17 server, and contrib. Storage qualification requires `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, and `NATIVE_ARTIFACT_SMOKE_PREFIX` (optional `S3_REGION`) in the build host environment: credentials are transported through a temporary mode-0600 file, never command arguments or packaged manifests. The Storage smoke configures that [S3](https://aws.amazon.com/s3/) bucket and tenant prefix and performs upload/download after `/status`. Meta additionally queries `/tables`; imgproxy performs a PNG transform. Supavisor/Logflare fixtures generate a temporary trusted database certificate for `database.internal`, reject plaintext TCP database connections, and require a live TLS database session after HTTP health. Supavisor also exercises a tenant query over verified upstream TLS, and Logflare probes its analytics provider repository. Failed recipes remain absent from the lock and cause a nonzero build exit. Both disposable instances are deleted after the run. Build success is not a compatibility claim for codecs, external integrations or features not exercised by the smoke.

Studio qualification also launches the already locked postgres-meta archive against the pinned Percona fixture, requires `/` and `/api/platform/profile` HTTP200 from the packaged standalone Next server, and checks meta `/health` and `/tables`. This is native server/rendering and reachable database metadata evidence, not browser-level administrative workflow acceptance. Supavisor's pinned `/api/health` contract is HTTP204 (not200); qualification requires that exact response plus a transaction-mode query through port6543, checks that its upstream PostgreSQL provider actually uses TLS, and performs a BEAM TLS handshake with the patched repository's peer-verification options and an unrelated CA to require an `unknown_ca` rejection.

## Standalone per-major PostgreSQL extensions

Six independent recipes build `timescaledb-pg16`, `timescaledb-pg17`, `timescaledb-pg18`, `citus-pg16`, `citus-pg17`, and `citus-pg18`. These are not additions to the [Supabase](https://supabase.com/)-only PG17 bundle. A qualified lock entry declares `kind: "pg-extension"` and integer `pgMajor`, as well as its verified `extensions` and `extensionFlags`; an unbuilt recipe alone does not make a profile available.

Each recipe installs both `percona-postgresql-<major>` and `percona-postgresql-server-dev-<major>` at the same exact Noble version: PG16 `2:16.15-1.noble`, PG17 `2:17.11-1.noble`, PG18 `3:18.6-1.noble`. Builds explicitly select `/usr/lib/postgresql/<major>/bin/pg_config`. The archive contains only that major's `usr/lib/postgresql/<major>` and `usr/share/postgresql/<major>` trees. Product-specific manifests, upstream license/notice files, commands, installed package versions and extension build reports live in `usr/share/postgresql/<major>/karakoram-native-artifacts/<artifact-name>/`, avoiding collisions between products and the existing Supabase bundle. Install only into the matching major.

[TimescaleDB 2.31.0](https://github.com/timescale/timescaledb/tree/4fa2a7c6b579171a1a8966fd87a0d4189ebab43b) is built in Apache-2.0-only mode, not the full Community/TSL distribution. Its bootstrap uses `-DAPACHE_ONLY=1 -DREGRESS_CHECKS=OFF -DSEND_TELEMETRY_DEFAULT=OFF -DWARNINGS_AS_ERRORS=OFF` and a Release CMake build. Packaging and smoke reject TSL shared libraries; the clean runner preloads TimescaleDB, creates the extension, requires `timescaledb.license` to report `apache`, and inserts/queries a hypertable. TSL functionality is explicitly unavailable in the lock flags.

[Citus 14.2.0](https://github.com/citusdata/citus/tree/58702e722c6efbae45e3cade885b476e4fddc7e1) is built from unmodified upstream source with explicit `PG_CONFIG`, `--without-libcurl` on PG16/17 and `--with-libcurl` on PG18. [LZ4](https://lz4.org/) and [Zstandard](https://facebook.github.io/zstd/) remain enabled; anonymous reporting defaults off. Runtime dependencies include Noble's `libpq5`, `libssl3t64`, `liblz4-1` and `libzstd1`. [Percona](https://www.percona.com/) PG18's server headers independently define `HAVE_LIBCURL`, so unmodified Citus compiles its [curl](https://curl.se/) helpers even if configure disables curl. PG18 must therefore configure and link curl explicitly, additionally installing `libcurl4-openssl-dev` at build time and declaring `libcurl4t64` at runtime. No upstream header or source patch removes that inherited capability. Its clean runner creates a coordinator and two local worker instances, preloads/creates Citus on all three, registers the workers, distributes and queries a table, starts/waits for rebalance, checks placement on both workers, and creates a distributed restore point. It shuts down all three instances after the probe; the disposable runner is deleted even on failure.

### Citus distribution and source

Citus's upstream [LICENSE](https://github.com/citusdata/citus/blob/58702e722c6efbae45e3cade885b476e4fddc7e1/LICENSE) supplies GNU AGPL version 3, without an additional “or later” grant; the artifact records `AGPL-3.0-only`. Citus is a separately downloaded, unmodified database extension, not code linked into the Apache-2.0 Karakoram control plane. This does not change Karakoram's own license or waive obligations for Citus distributors/operators.

Every Citus archive includes the full upstream AGPL text and a `SOURCE-OFFER.txt` identifying the immutable upstream commit, corresponding-source download and packaged build configuration. Distributors must provide corresponding source access in an AGPL-compliant manner alongside binary distribution; the upstream link is provenance, not a substitute for maintaining required source availability. If Citus is ever patched, publish the complete corresponding modified source and build scripts, retain notices, and satisfy the AGPL's applicable distribution and remote-network-interaction obligations before shipping. Do not describe a patched artifact as unmodified.

Build and qualification are one sequential command (there is no bypass that publishes unqualified candidates):

```sh
scripts/native-artifacts/build.sh timescaledb-pg16 timescaledb-pg17 timescaledb-pg18
scripts/native-artifacts/build.sh citus-pg16 citus-pg17 citus-pg18
```

The resource guards above apply: one 6 GiB/8-vCPU builder, then one 2 GiB/2-vCPU clean runner, with compilation limited to four jobs. Citus's three [PostgreSQL](https://www.postgresql.org/) processes share that runner. Plan at least 120 GiB host free disk and 14 GiB available host memory. Actual build duration and archive sizes are recorded only by a real native qualification; recipe/unit-test coverage is not a claim that release bytes have been built.

### Rootless Ubuntu transport

For these six standalone extension recipes, `NATIVE_ARTIFACT_TRANSPORT=rootless`
selects a local userspace builder instead of [Incus](https://linuxcontainers.org/incus/):

```sh
NATIVE_ARTIFACT_TRANSPORT=rootless scripts/native-artifacts/build.sh \
  timescaledb-pg16 timescaledb-pg17 timescaledb-pg18 \
  citus-pg16 citus-pg17 citus-pg18
```

This requires [Linux](https://www.kernel.org/) user/mount namespaces, `unshare`, `bwrap`, `newuidmap`,
`newgidmap`, and configured subordinate UID **and** GID ranges. A single-ID
root mapping cannot install the real PostgreSQL ownership model. The transport
verifies the pinned official [Ubuntu](https://ubuntu.com/) 24.04 minimal root archive, extracts one
writable builder under `dist/native-artifacts/.rootless/`, and runs stages
sequentially. Every smoke runner is freshly extracted from that pristine archive,
not copied from the builder, and deleted after qualification, including failures.
No host installation, daemon, host home mount, or application-container runtime
is needed. The builder directory and verified archive remain available for reuse.
The leading-dot directory keeps [Go](https://go.dev/) package traversal out of the subordinate-ID
guest filesystem, whose package-cache permissions are guest-owned.

The guest has private mount, PID, IPC and UTS namespaces, synthetic `/dev` and
`/run`, a cleared environment, and only the host resolver file mounted read-only.
Package/source downloads use host networking; the smoke switches to its own
loopback-only network namespace after installing packages and before starting
PostgreSQL. Its coordinator/worker ports therefore do not occupy host ports.
Root capabilities are confined to the subordinate-ID user namespace. The existing
120 GiB disk and 14 GiB available-memory guards and four-job compile bound still
apply; unlike Incus, this transport does not impose cgroup memory/CPU limits.
Do not launch independent builders concurrently. Only successful unchanged
native smoke permits archive promotion and lock updates, exactly as for Incus.


## Publication and provenance

Load [S3](https://aws.amazon.com/s3/) credentials securely into `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY`, then run:

```sh
python3 scripts/native-artifacts/upload.py
```

The default bucket/prefix is `kk-artifacts/native`; override with `NATIVE_ARTIFACT_BUCKET` and `NATIVE_ARTIFACT_PREFIX`. Upload uses SigV4 and verifies anonymous GET bytes against every lock digest, including `SHA256SUMS`. Do not put credentials into the repository, artifact manifest or command arguments.

`release.sh build` uses the **tagged** lock, validates all Karakoram-built files in `dist/native-artifacts/` before copying them to `dist/<version>/`, and fails on missing files, size or SHA-256 differences. Release `checksums.txt` covers those files together with the server/CLI and is signed by the existing release signer.

Official artifacts retain upstream provenance rather than being described as source rebuilds. [Envoy](https://www.envoyproxy.io/) 1.39.1's clear-signed checksums were verified with the [official maintainers key](https://raw.githubusercontent.com/envoyproxy/envoy/main/envoy-maintainers-public.key), fingerprint `0AFCE836BA4D1D35763C8523D8CDC3750181F31F`. SHA-256 transport integrity alone is not a maintainer signature. Studio's native source selection uses the full [Supabase](https://supabase.com/) commit in the recipe; it is not represented as a byte-identical rebuild of an independently published image tag. Pin updates require evaluating upstream dependency advisories rather than silently rewriting application lockfiles.

## Data MCP companion artifacts

| Artifact | Version | Origin | License | Runtime |
| --- | --- | --- | --- | --- |
| `pgedge-postgres-mcp` | 1.1.0 | Official pgEdge Linux x86_64 server release | PostgreSQL | Native executable at archive root |
| `mongodb-mcp-server` | 3.1.0 | Karakoram assembly of the published npm package and frozen production dependency tree | Apache-2.0 (dependencies retain their own licenses) | Bundled Node 22.23.3, npm 10.9.9; `ca-certificates`, `libstdc++6` |

The [pgEdge](https://www.pgedge.com/) archive is [the upstream server tarball](https://github.com/pgEdge/pgedge-postgres-mcp/releases/download/v1.1.0/pgedge-postgres-mcp-server_1.1.0_linux_x86_64.tar.gz), not the similarly named CLI release. Its SHA-256 is `ca79e3054455b5505d3ac4ed4bc5d4cd074a57f108b8fe9ddcd3e24d16c5452a` (8,483,086 bytes). [GitHub](https://github.com/)'s tag API resolves annotated tag `v1.1.0` through tag object `336174b178e7ddc65a581b2fa5f2e895789f2885` to full source commit `25c2aaed03398584ceea0dd71f7f67631c3dff4e`. The downloaded archive matches the published release digest; this is byte-integrity evidence, not a verified maintainer signature.

[MongoDB](https://www.mongodb.com/)'s [v3.1.0 tag](https://github.com/mongodb-js/mongodb-mcp-server/tree/f369482783bdef116c35d352fd9d92f6c984242f) resolves to `f369482783bdef116c35d352fd9d92f6c984242f`. The recipe assembles the published [npm](https://docs.npmjs.com/) distribution; it does not claim a source rebuild of its JavaScript. `scripts/native-artifacts/mongodb-mcp/package-lock.json` freezes the complete resolved tree with registry URLs and npm integrity values. Installation uses `npm ci --omit=dev --ignore-scripts`: no package lifecycle hooks or native addon builds run. The [Node](https://nodejs.org/) runtime comes from the official [Linux](https://www.kernel.org/) x64 tarball pinned in `toolchains.json`, SHA-256 `df450af89261115ef9f9e3830c3eeb2cc9213b63c720b1af623cb5dcbe2e02de`. The bundle retains `node/`, `app/`, and executable `bin/start`; the launcher changes to `app/` and execs `node/bin/node dist/esm/index.js`. `LICENSES/` collects distributed license/notice/copyright files (including Node's consolidated notices); `THIRD_PARTY.json` records every locked package's version, license, URL and integrity, and `provenance/` preserves the exact npm inputs.

The normal [Incus](https://linuxcontainers.org/incus/) recipe uses the same staging helper. Where Incus is unavailable, this equivalent unprivileged host command downloads and verifies Node, assembles the same layout, runs native qualification, packages normalized tar metadata, and updates only the qualified MongoDB lock entry:

```sh
# Requires Python 3.11+, tar, xz, and a native Linux x64 mongod.
# First download the official fixture and verify its published checksum.
mkdir -p .tmp-int
curl -fL https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-8.0.16.tgz \
  -o .tmp-int/mongodb-linux-x86_64-ubuntu2404-8.0.16.tgz
printf '%s\n' '7d67c164da52d31ec144778b40ec9ab6bdaa0c250be020b44dacdcf91a7de3c0  .tmp-int/mongodb-linux-x86_64-ubuntu2404-8.0.16.tgz' | sha256sum -c -
tar -xf .tmp-int/mongodb-linux-x86_64-ubuntu2404-8.0.16.tgz -C .tmp-int
python3 scripts/native-artifacts/mongodb_mcp.py \
  --mongod .tmp-int/mongodb-linux-x86_64-ubuntu2404-8.0.16/bin/mongod
```

Use a fresh output stage (the helper refuses to replace an existing stage). Qualification binds mongod and the actual bundle launcher to dynamically selected `127.0.0.1` ports, limits [WiredTiger](https://source.wiredtiger.com/) cache to 256 MiB, sets `MDB_MCP_TRANSPORT=http`, then requires successful [MCP](https://modelcontextprotocol.io/) `initialize` and nonempty `tools/list`. Both child processes are terminated on success or failure. Logs, the complete handshake/tool response, packaged manifest and bundle remain under ignored `dist/native-artifacts/`. A failed qualification never publishes archive bytes or updates the lock. Host qualification is not a clean [Ubuntu](https://ubuntu.com/)-runner claim; the Incus path repeats the protocol smoke in a clean Ubuntu 24.04 runner with the documented runtime packages.

The MongoDB bundle has no upstream binary URL: a Karakoram release **must publish the exact locked tarball**. Development deployments require `KARAKORAM_ARTIFACT_MIRROR`; neither npm nor a floating upstream archive is a runtime fallback. Rebuilding may produce different compressed bytes: publish the qualified locked file, or deliberately requalify and update its digest and size.

The qualified MongoDB 3.1.0 tarball is 239,960,287 bytes with SHA-256 `a5c6f723b4c4eaf87ecbc3da336a5c1ec784e47a087e351e89aff65e70018242`. The direct-host smoke returned server version `3.1.0`, negotiated MCP `2025-03-26`, and listed 20 tools in read-only mode against the official MongoDB 8.0.16 Ubuntu 24.04 tarball. These are launcher/protocol qualification results, not evidence that every database tool or optional native dependency has been exercised.
