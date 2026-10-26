# Native engine artifacts

`internal/engine/nativeartifacts/lock.json` is embedded in the control plane. Each entry names exact Linux amd64 bytes, their SHA-256 and size, upstream URL (empty for Karakoram-built files), SPDX license, immutable upstream repository/commit, and build recipe. Source-built entries also record the clean-runner `runtimeDeps`. Extension bundles expose an `extensions` list containing only names that passed native creation smoke; provider catalog/admission must use it rather than advertise failed builds. An unavailable artifact has no lock entry: enabling a component must fail admission rather than download a floating release or install a substitute.

## Download and verification

Resolution order is:

1. `KARAKORAM_ARTIFACT_MIRROR`, with `/<file>` appended;
2. the artifact's official `upstreamURL`;
3. for a versioned release, `https://github.com/KarakoramDev/Karakoram/releases/download/<running-version>/<file>`.

A development build needs a mirror for Karakoram-built files. The typed `MissingMirrorError` names the artifact and required environment variable. Configure the variable on the control plane; resolved URLs are rendered into member preparation commands. Mirrors must serve the exact locked bytes. Production mirrors should use HTTPS.

Preparation uses one privileged shell and a fresh root-owned mode-0700 staging directory per artifact. Downloads refuse existing files and symlinks, SHA-256 is checked inside that directory, and every install reads the same protected file; an exit trap removes staging on success or failure. Python 3's standard-library `tarfile` validates actual archive headers before extraction, independently of GNU tar's locale-dependent listing/quoting. Unicode names and internal relative symlinks are supported; absolute/traversal member paths, control characters, escaping or cyclic links, members beneath symlink ancestors, and nonregular hardlink targets are rejected. Application archives preserve their layout under the caller's immutable `/opt/karakoram/<name>-<version>` directory. Root opens the verified artifact as standard input, then drops to the backend's configured runtime user before extracting or installing into application paths; existing runtime-owned destination symlinks therefore cannot cause privileged writes. GNU tar uses `--no-same-owner --no-same-permissions --delay-directory-restore`. The PostgreSQL extension bundle instead accepts only regular files/directories under `usr/lib/postgresql/17` and `usr/share/postgresql/17` (plus their ancestor directories); root extracts into private staging with the additional `--keep-old-files` option, then copies only those two trees to the system-owned destination, retaining root ownership. It never extracts into `/` or recursively changes destination ownership. Raw binaries are installed as `<destDir>/<artifact.name>`. Debian packages are installed with `apt-get install -y` from protected staging so dependency resolution remains with apt. The engine must suppress package-service autostart when required by its process supervision model.

On a fresh host, preparation creates missing application parent directories with mode 0755 and creates the installation leaf before dropping privileges. Only a newly created leaf receives a nonrecursive, no-follow ownership handoff to the configured runtime user; pre-existing directories and symlinks are never chowned. Application content publication remains unprivileged even when the parent hierarchy was initially root-owned.

### Preparation memory and small allocations

Native `.deb` installation sets `DPKG_DEB_THREADS_MAX=1` **inside the privileged
installer shell**, so apt and its dpkg-deb children inherit it even when sudo
clears the caller's environment. Dependencies, maintainer scripts and package
registration still run through apt; installation is not replaced by copying
files out of a package. Failures retain apt's original status and diagnostics
and add the artifact name and a reminder to inspect allocation memory limits.

Size preparation as well as the running service. In particular, the pinned
ProxySQL 3.0.11 package expands approximately 36.2 MiB into 151.3 MiB of tar
content. Its XZ stream has seven blocks with 8 MiB dictionaries and 24 MiB
expanded full blocks. Automatic dpkg-deb parallel decoding can buffer several
blocks based on host memory/CPU rather than the task's available cgroup budget:
it has caused an OOM during preparation of a 256 MiB Nomad proxy.
Single-threaded dpkg-deb avoids that host-sized parallelism without increasing
the configured service memory or requiring Nomad memory oversubscription.
Allow additional memory for apt/dpkg, package scripts, and reclaimable filesystem
cache; archive size or dictionary size alone is not a whole-preparation budget.

This policy applies to every catalog `.deb` on all backends, including PostgreSQL
and Supabase proxies. Nomad runs preparation within the task's configured memory
limit before the agent starts. Dokploy prepares the image during its build, not
inside the eventual service task; its builder needs its own capacity. Zerops
receives the same preparation commands and controls their execution resources.
The policy does not change runtime sizing defaults/minima or make arbitrarily
small positive RAM values safe. Non-catalog repository packages and tarballs
retain their own compression requirements: gzip uses a 32 KiB history window;
the pinned Supabase Auth XZ archive requires a 64 MiB dictionary (REST: 8 MiB),
in addition to installer and application overhead. Increase an undersized
explicit configuration rather than relying on retries to get lucky.

## Native rebuild

Prerequisites on the build host: Python 3.11+, Incus with Ubuntu 24.04 images and an egress-capable network, and sufficient disk space. No application-container runtime is used.

```sh
export NATIVE_ARTIFACT_NETWORK=incusbr0
make native-artifacts                         # source builds + verified official downloads
make native-artifacts ARTIFACT=supabase-postgres-meta
scripts/native-artifacts/build.sh supabase-realtime supabase-studio  # ordered subset
```

`NATIVE_ARTIFACT_BUILDER` and `NATIVE_ARTIFACT_RUNNER` override the disposable instance names. The builder is limited to 6 GiB/8 vCPU, Node's heap to 4 GiB, and BEAM to two schedulers plus one dirty CPU/IO scheduler. A clean 2 GiB runner starts only after the builder has stopped. Before builder startup and each heavy compilation step, `free -m` must report at least 14 GiB available host memory; otherwise the build waits. At least 120 GiB free disk is required, and configured running-instance memory for the shared name prefix must stay within 28 GiB. Set `NATIVE_ARTIFACT_MEMORY_PREFIX` when sharing a separately named lab. Do not run independent builds concurrently.

`recipes.json` pins source commits and toolchain versions; `extensions.json` pins each PostgreSQL extension. `toolchains.json` pins exact toolchain URLs, SHA-256 values, sizes, and the official checksum provenance. These pins were independently checked against downloaded bytes, including older releases' official SHA-256 checksum assets. Rebuilds use these checked-in pins, not mutable release API metadata. Source checkouts are detached and checked against the pinned commit. Toolchain download caches, build logs, manifests (including toolchain transport hashes), smoke logs, and `build-report.json` are retained under the gitignored `dist/native-artifacts/` directory. Ubuntu runtime apt dependencies are recorded per recipe and in the packaged manifest.

Reusable compiled OTP installations require `/opt/otp-<version>/.karakoram-build-inputs`, written only after configure, compilation, installation and the executable check succeed. Before considering reuse, the builder verifies the pinned source archive again and compares the receipt against its source SHA-256, the builder script hash/configure arguments, fixed compilation environment, machine architecture and sorted installed-package versions (including the system compiler and development libraries). Missing or different receipts discard both the source/build tree and installation before extracting and compiling the verified archive; an existing `bin/erl` alone never authorizes reuse. Package or build-script changes conservatively invalidate the cache, even at the same OTP version. Other precompiled toolchains are installed afresh into cleared directories; extension builds also clear compiled cargo-pgrx CLI and PGRX state rather than allowing Cargo's same-version installation shortcut.

Build bootstraps are inside the same integrity boundary: `toolchains.json` pins npm 11.12.1, pnpm 11.13.1, the amd64 Turbo 2.9.14 binary, Hex 2.5.3 and Rebar3 3.24.0 (the OTP-27 build, compatible with both pinned OTP 27 and 28). Their exact bytes are SHA-256-checked on download and again inside the builder, and included in manifest toolchain transports. npm/pnpm include their bundled dependencies and are unpacked directly; Turbo uses its verified platform binary rather than `pnpm dlx` or an installer that resolves optional dependencies. npm registry SHA-512 metadata and Hex's published SHA-512 CSVs were checked when selecting these pins. These byte pins, not mutable metadata fetched during a build, anchor rebuild integrity. This does not authenticate unpinned Ubuntu build packages or claim byte-reproducible compiler output.

Mix bootstrap authentication differs across the pinned Elixir versions: [1.18.2 verifies Hex's signed CSV using its embedded public key](https://github.com/elixir-lang/elixir/blob/v1.18.2/lib/mix/lib/mix/local.ex), whereas [1.19.5 uses HTTPS metadata without that signature check](https://github.com/elixir-lang/elixir/blob/v1.19.5/lib/mix/lib/mix/local.ex). Rebar's default installer also selects a compatible version rather than accepting an explicit version argument. Consequently recipes install the verified local Hex archive and Rebar executable; they do not rely on a uniform Mix signature guarantee or reuse an arbitrary pre-existing bootstrap.

The all-artifact order is Storage, postgres-meta, Realtime, Studio, PostgreSQL extensions, imgproxy, Supavisor, Logflare, Edge Runtime. Every fresh candidate manifest records `buildInputFingerprint`, a deterministic SHA-256 over the selected `recipes.json` entry and the checked-in build scripts/helpers, toolchain/bootstrap byte pins, auxiliary-asset/extension pins, and native patches. The support-tree hash deliberately includes inputs for other recipes too, preferring conservative invalidation to missed dependencies. A retained `.tar.gz.candidate` is re-smoked without recompiling only when its packaged recipe and build-input fingerprint exactly match; old manifests without the fingerprint and changed inputs are rejected. Candidates remain excluded from the lock until the clean smoke passes. Remove an obsolete candidate explicitly before requesting a rebuild. Fresh builds refresh recipe/helper scripts and patches even when reusing a running builder.

Node applications include their own pinned Node runtime. Elixir artifacts retain their complete mix release, ERTS and NIFs. Imgproxy targets Ubuntu's libvips baseline, not an upstream custom codec library image. Edge Runtime includes the main worker from the pinned Supabase source selection, with its two function-directory paths adapted to the required `FUNCTIONS_ROOT` runtime environment setting; original source and both hashes are included. The PostgreSQL extension tarball uses filesystem-root paths under `usr/lib/postgresql/17` and `usr/share/postgresql/17`; provenance and per-extension results live under `usr/share/postgresql/17/karakoram-native-artifacts/`. Install it only into the matching Percona PostgreSQL 17 environment.

`FUNCTIONS_ROOT` must point outside `/tmp` (for example `/var/lib/karakoram/functions`) and contain `<function-name>/index.ts` plus a root `deno.jsonc` import map. Edge Runtime mounts a separate private `/tmp` filesystem in each worker: host-created `/tmp/functions` is not visible to the main worker's directory check and returns `NOT_FOUND`, even when the files exist in the host namespace. The clean smoke invokes both a plain hello function and a function resolving a pinned npm import through the packaged main worker.

The lock's per-extension `extensionFlags` map defines the mode actually qualified. Only entries with `preload: true` may be added to `shared_preload_libraries`; this is `pg_net` for the source-built bundle. `pgsodium` is intentionally not preloaded and supplies crypto functions with caller-supplied keys only. Server root-key management, key-id operations and transparent column encryption (TCE) are unavailable and listed in `unavailableFeatures`. Do not install an ephemeral root key or enable those features implicitly. Vault 0.3.x does not require pgsodium.

Declared native compatibility patches in `scripts/native-artifacts/patches/` are applied to the exact pinned sources and included in affected bundles with `SHA256SUMS`. Supavisor and Logflare accept `KARAKORAM_DB_CA_FILE` to verify the upstream database CA through a TLS-transparent loopback forwarder, covering metadata, cluster discovery and analytics backend connections. Hostname verification is intentionally not used for that loopback endpoint; the CA chain is still verified. Supavisor tenant configuration must also select upstream TLS, peer verification and the upstream CA. Realtime's compiled pgdelta helper preserves its parser WASM files at the fixed `/opt/karakoram/<name>-<version>/app/pg-toolbelt` layout; the smoke exercises SQL parsing, not only BEAM startup.

Studio's native patch selects two build workers, webpack memory optimizations and Next's webpack build worker; its recipe uses `next build --webpack` with two Rayon threads. The webpack callback disables compiler caching, limits module parallelism to one, preserves upstream raw-text handling for Markdown/Deno declarations and explicitly aliases the two declaration imports to the pinned public assets (the TS path resolver otherwise fails those imports). This replaces the unbounded Turbopack attempt that exhausted the original builder reservation; it does not replace Studio's standalone Next application or change the pinned application source selection.

`studio-assets.json` pins the Deno v1.45.0 and Supabase functions-js 2.4.4 editor declarations selected by the upstream `build:deno-types` script, together with their MIT licenses. These generated public files must be present before compiling Studio. Their downloaded hashes are recorded in the artifact manifest; the functions-js declaration also matches JSR's versioned package checksum manifest. Versioned download pins provide transport integrity, not an independent maintainer signature.

The Studio recipe explicitly preserves the frozen dependency tree's `libpg-query` WASM at its original pnpm path and carries its license. Next's standalone tracer omits that dynamically opened file; HTTP health alone can pass while SQL parsing raises an asynchronous `ENOENT`. The clean smoke therefore runs a real `SELECT 1` parse using the bundled Node runtime before starting Studio, in addition to the required page/profile and live-meta HTTP probes.

Supavisor uses the pinned OTP 27.3.4.6 patch release with Elixir 1.18.2. The earlier OTP 27.2.1 attempt could not securely bootstrap Hex (`unsupported_certificate`, `key_usage_mismatch`); updating within OTP 27 avoids disabling certificate verification. Hex dependency fetches use Ubuntu's `/etc/ssl/certs/ca-certificates.crt` trust bundle via `HEX_CACERTS_PATH`. Bootstrap installs use the verified local pins described above, replacing pre-existing tools with the selected versions.

Only a successful native HTTP health smoke (or `CREATE EXTENSION` for each packaged extension) permits updating the lock. The clean runner installs the documented runtime packages and Python/curl smoke tooling. DB-dependent fixtures additionally install `gnupg lsb-release` for the verified repository bootstrap, the pinned Percona 17 server, and contrib. Storage qualification requires `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, and `NATIVE_ARTIFACT_SMOKE_PREFIX` (optional `S3_REGION`) in the build host environment: credentials are transported through a temporary mode-0600 file, never command arguments or packaged manifests. The Storage smoke configures that S3 bucket and tenant prefix and performs upload/download after `/status`. Meta additionally queries `/tables`; imgproxy performs a PNG transform. Supavisor/Logflare fixtures generate a temporary trusted database certificate for `database.internal`, reject plaintext TCP database connections, and require a live TLS database session after HTTP health. Supavisor also exercises a tenant query over verified upstream TLS, and Logflare probes its analytics backend repository. Failed recipes remain absent from the lock and cause a nonzero build exit. Both disposable instances are deleted after the run. Build success is not a compatibility claim for codecs, external integrations or features not exercised by the smoke.

Studio qualification also launches the already locked postgres-meta archive against the pinned Percona fixture, requires `/` and `/api/platform/profile` HTTP200 from the packaged standalone Next server, and checks meta `/health` and `/tables`. This is native server/rendering and reachable database metadata evidence, not browser-level administrative workflow acceptance. Supavisor's pinned `/api/health` contract is HTTP204 (not200); qualification requires that exact response plus a transaction-mode query through port6543, checks that its upstream PostgreSQL backend actually uses TLS, and performs a BEAM TLS handshake with the patched repository's peer-verification options and an unrelated CA to require an `unknown_ca` rejection.

## Publication and provenance

Load S3 credentials securely into `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY`, then run:

```sh
python3 scripts/native-artifacts/upload.py
```

The default bucket/prefix is `kk-artifacts/native`; override with `NATIVE_ARTIFACT_BUCKET` and `NATIVE_ARTIFACT_PREFIX`. Upload uses SigV4 and verifies anonymous GET bytes against every lock digest, including `SHA256SUMS`. Do not put credentials into the repository, artifact manifest or command arguments.

`release.sh build` uses the **tagged** lock, validates all Karakoram-built files in `dist/native-artifacts/` before copying them to `dist/<version>/`, and fails on missing files, size or SHA-256 differences. Release `checksums.txt` covers those files together with the server/CLI and is signed by the existing release signer.

Official artifacts retain upstream provenance rather than being described as source rebuilds. Envoy 1.39.1's clear-signed checksums were verified with the [official maintainers key](https://raw.githubusercontent.com/envoyproxy/envoy/main/envoy-maintainers-public.key), fingerprint `0AFCE836BA4D1D35763C8523D8CDC3750181F31F`. SHA-256 transport integrity alone is not a maintainer signature. Studio's native source selection uses the full Supabase commit in the recipe; it is not represented as a byte-identical rebuild of an independently published image tag. Pin updates require evaluating upstream dependency advisories rather than silently rewriting application lockfiles.
