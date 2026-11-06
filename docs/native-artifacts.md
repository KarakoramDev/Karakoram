# Native engine artifacts

`internal/engine/nativeartifacts/lock.json` is embedded in the control plane. Each entry names exact [Linux](https://www.kernel.org/) amd64 bytes, their SHA-256 and size, upstream URL (empty for Karakoram-built files), SPDX license, immutable upstream repository/commit, and build recipe. Source-built entries also record the clean-runner `runtimeDeps`. Extension bundles expose an `extensions` list containing only names that passed native creation smoke; engine driver catalog/admission must use it rather than advertise failed builds. An unavailable artifact has no lock entry: enabling a component must fail admission rather than download a floating release or install a substitute.

## Artifact inventory

| Artifact | Version | Source | License | Installation |
| --- | --- | --- | --- | --- |
| `openbao` / `openbao-2.7.1` | 2.7.2 / 2.7.1 (deprecated) | [Official release](https://github.com/openbao/openbao/releases/tag/v2.7.2) | MPL-2.0 | `/opt/karakoram/openbao-<version>/bao`; old archive retained for existing runtimes and upgrades. |
| `mongodb-mongosh-2.14.1` | 2.14.1 | [Official standalone archive](https://downloads.mongodb.com/compass/mongosh-2.14.1-linux-x64.tgz) | Apache-2.0 | `/opt/karakoram/mongosh-2.14.1`; a local dpkg diversion links `/usr/bin/mongosh` to the archive while retaining Percona's server dependency. PSMDB 7.0/8.0 only. |
| `openbao-plugin-database-mongodb` / `openbao-plugin-database-mongodb-0.0.1` | 0.0.2 / 0.0.1 (retained) | [Official release](https://github.com/openbao/openbao-plugins/releases/tag/database-mongodb-v0.0.2) | MPL-2.0 | Both real binaries remain installed and declaratively registered; 0.0.2 is published as a distinctly named executable directly inside `plugin_directory`, not a subdirectory symlink. |
| `openbao-plugin-kms-aws` | 0.1.1 | [Official release](https://github.com/openbao/openbao-plugins/releases/tag/kms-aws-v0.1.1) | MPL-2.0 | Lock `executableMembers` marks `openbao-plugin-kms-aws_linux_amd64_v1` executable as the runtime owner. |

[OpenBao](https://openbao.org/) installs the [MongoDB](https://www.mongodb.com/) database plugin on every member so the first connection does not need a plugin-install redeployment; the [AWS KMS](https://aws.amazon.com/kms/) plugin is installed only for `seal.type: awskms`, an immutable creation choice. Plugin registration uses the locked executable/version, not a runtime OCI download. Artifact availability does not establish provider lifecycle or database-connection acceptance; see [OpenBao verification](verification.md#openbao-coverage).
[Temporal](https://temporal.io/) uses official locked `temporal-server-1.31.5` (1.31.5), `temporal-server-1.32.2` (1.32.2), and `temporal-ui-server` (2.58.0) artifacts. The older `temporal-server-1.31` (1.31.4) and `temporal-server-1.32` (1.32.1) locks retain their original bytes and installation identities for existing clusters. Server archives contain `temporal-server`, `temporal-sql-tool`, `tdbg` and configuration; SQL schemas are embedded in the SQL tool. The UI archive supplies `ui-server`; its version follows the release lock, not `spec.version`. The Linux amd64 files were verified against the SHA-256 release-asset digests from the official [1.31.5](https://github.com/temporalio/temporal/releases/tag/v1.31.5), [1.32.2](https://github.com/temporalio/temporal/releases/tag/v1.32.2) and [UI 2.58.0](https://github.com/temporalio/ui-server/releases/tag/v2.58.0) releases; lock entries record exact size, URL and resolved tag commit. PostgreSQL schema source trees are unchanged within both patch pairs (persistence 1.19, visibility 1.14). See [Temporal prerequisites](temporal/README.md) and [upgrade compatibility notes](temporal/operations.md#v047-compatibility-notes).

PSMDB server/router packages use the official Noble APT indexes ([7.0](https://repo.percona.com/psmdb-70/apt/dists/noble/main/binary-amd64/Packages), [8.0](https://repo.percona.com/psmdb-80/apt/dists/noble/main/binary-amd64/Packages)), not native mirror objects. v0.4.7 adds [7.0.45-24](https://docs.percona.com/percona-server-for-mongodb/7.0/release_notes/7.0.45-24.html) and default [8.0.34-15](https://docs.percona.com/percona-server-for-mongodb/8.0/release_notes/8.0.34-15.html); both require the independently versioned `percona-server-mongodb-tools=100.19.1-2.noble`. Historical catalog patches retain their exact matching tools packages. PBM remains `2.16.0-1.noble`.

PSMDB 7.0/8.0 install the locked official [`mongosh-2.14.1-linux-x64.tgz`](https://downloads.mongodb.com/compass/mongosh-2.14.1-linux-x64.tgz) (94,615,134 bytes; SHA-256 `8d24940ebe3675959b518fd41ea16b4e2182d6a75c09777db09cfa64e18834a1`, verified against the [official GitHub release-asset digest](https://github.com/mongodb-js/mongosh/releases/expanded_assets/v2.14.1); the downloads-host `.sha256` sidecar returned HTTP 403). [Source tag v2.14.1](https://github.com/mongodb-js/mongosh/tree/456a58b13c1d9dd83bd4e0985c10237ba83c18e1) resolves to `456a58b13c1d9dd83bd4e0985c10237ba83c18e1`. Normal native-artifact resolution honors `KARAKORAM_ARTIFACT_MIRROR` before the official URL; mirror-only installations must add this exact filename and byte sequence. Preparation verifies SHA-256 and archive safety before extraction into `/opt/karakoram/mongosh-2.14.1`. It keeps `percona-mongodb-mongosh` installed because the server depends on that concrete package; an idempotent local dpkg diversion sends package updates to `/usr/bin/mongosh.percona` while `/usr/bin/mongosh` links to the standalone 2.14.1 executable. Percona's 2.13.0 still lacks the [2.14 security fix](https://www.mongodb.com/docs/mongodb-shell/changelog/) for CVE-2026-115930 in bundled Node TLS roots. Deprecated 6.0 retains 2.8.3: no claim of modern-shell compatibility or security remediation is made for that line.

### PostgreSQL backup package provenance

PostgreSQL's `percona-pgbackrest` is installed by exact APT pin, not the native
archive lock. On 2026-11-05 the official Noble amd64 indexes for
[PG16](https://repo.percona.com/ppg-16/apt/dists/noble/main/binary-amd64/Packages),
[PG17](https://repo.percona.com/ppg-17/apt/dists/noble/main/binary-amd64/Packages) and
[PG18](https://repo.percona.com/ppg-18/apt/dists/noble/main/binary-amd64/Packages)
all offer `1:2.59.1-1.noble` as their newest pgBackRest package, not 2.59.3.
Upstream [2.59.3](https://pgbackrest.org/release.html#2.59.3) is the latest stable
2.59 patch; 2.60.0 is a separate line with restore/expiration checksum behavior
changes. The requested patch is blocked on vendor packaging or a separately
qualified source-build supply chain; no unavailable APT version is substituted.
See the [encryption remediation](postgres/backups.md#encryption-rng-security-notice).

### PostgreSQL etcd package provenance

The same Percona Noble PG16/17/18 indexes currently provide `etcd-server` and
`etcd-client` at `1:3.5.33-1.noble`, not the requested upstream
[3.5.35](https://github.com/etcd-io/etcd/blob/main/CHANGELOG/CHANGELOG-3.5.md).
Both pins remain aligned and unchanged. A locked official etcd-io release archive
would be an alternative supply chain, not a drop-in APT revision: install paths,
Patroni/DCS process supervision and snapshot/client compatibility need separate
qualification. See [quorum-preserving preparation rollout](postgres/README.md#dependency-package-rollouts).

### PostgreSQL vector source provenance

`pgvector-pg16`, `pgvector-pg17` and `pgvector-pg18` supply upstream 0.8.7 as
independent locked `pg-extension` artifacts, also used by Supabase PG17.
The [official release tag](https://api.github.com/repos/pgvector/pgvector/git/ref/tags/v0.8.7)
resolves to commit `f37c13f68b57d2c3472b2214fbcff699d6d34876`.
The 0.8.7 [security patch](https://github.com/pgvector/pgvector/issues/1036)
is absent from Percona's Noble indexes. Preparation preserves installed package
identities and dependents, diverts their files to `.distrib` without renaming
live libraries, and atomically publishes the verified bundle. New members omit
the old APT package. Database extension objects are retained.
Each archive includes the upstream PostgreSQL license, `SOURCE.txt`, exact build
manifest/environment, portable PGXS commands (`OPTFLAGS=` rather than
`-march=native`) and a capability report. See [SQL and index checks](postgres/profiles.md#pgvector-security-maintenance).

### PostgreSQL set_user package provenance

The Supabase PG17 APT dependency `percona-pgaudit17-set-user` remains pinned to
`1:4.2.0-6.noble`, the newest version in the PG17 Noble index on 2026-11-05.
Upstream [4.3.0](https://github.com/pgaudit/set_user/releases/tag/REL4_3_0) is a
security-fix minor release, but no corresponding Percona package is available.
Options are vendor packaging or a separately qualified PG17 source bundle built
from the release commit `45fc481de6874c2afc5199d7efbd2e32c66d702e`; the latter
must replace the conflicting APT files, preserve extension SQL upgrade scripts
and prove grant/allowlist/reset-token behavior. Neither option is represented as
an already shipped artifact. See the [compatibility review](postgres/profiles.md#set_user-security-maintenance).

### Elastic stack archive provenance

The aligned Elastic stack offers **9.5.6** (default), **9.4.9** and **8.19.24**, retaining older catalog patches as upgrade sources. Elasticsearch, Kibana, Logstash and APM Server use official `https://artifacts.elastic.co/downloads/<product>/<product>-<version>-linux-x86_64.tar.gz` archives rather than native-lock entries. All twelve new archive URLs and matching `.sha512` sidecars were available on 2026-11-05; member preparation verifies the vendor SHA-512 before extraction. See the [9.5.6 release announcement](https://www.elastic.co/blog/elastic-stack-9-5-6-released) and [security announcements](https://discuss.elastic.co/c/announcements/security-announcements/31). New patch entries do not change existing members until their cluster version is upgraded; cross-major 8.19→9.x upgrades retain the fresh-backup requirement.

### RabbitMQ package provenance

RabbitMQ **4.3.7** uses the locked [official Debian release asset](https://github.com/rabbitmq/rabbitmq-server/releases/download/v4.3.7/rabbitmq-server_4.3.7-1_all.deb), 23,918,436 bytes, SHA-256 `d9d9ec96d9eb2ba1a9d45de5d383cf7632af5e176e49edc93660820838862b64`, source commit `cd87622aee0d4fd8b119d713f2e03420b0642a89`. Its adjacent `.asc` signature was verified with RabbitMQ release key `0A9AF2115F4687BD29803A206B73A36E6026DFCA`. Both configured [deb1](https://deb1.rabbitmq.com/rabbitmq-erlang/ubuntu/noble/dists/noble/main/binary-amd64/Packages.gz) and [deb2](https://deb2.rabbitmq.com/rabbitmq-erlang/ubuntu/noble/dists/noble/main/binary-amd64/Packages.gz) Noble indexes published all fourteen required Erlang packages at **`1:27.3.4.19-1`** on 2026-11-05. Their `rabbitmq-server` indexes did not offer `4.3.7-1`, so the broker is installed through verified private staging and local apt installation (including `DPKG_DEB_THREADS_MAX=1`), while Erlang stays pinned through apt. Existing 4.3.5/4.3.6 preparations retain their original broker apt pins.

### OpenBao and plugin archive provenance

OpenBao 2.7.2 and AWS KMS plugin 0.1.1 use official release bytes checked against GitHub release-asset SHA-256 digests and sizes. The dereferenced source tags are `9e0a0d3c3504a0a00b45e61355afac5e35c78379` (server) and `229bc6b32bc58a1fe971a50edbfef534ad59efe8` (KMS); exact URLs, sizes and digests are in the lock. `_v1` in the plugin archive/executable name denotes the plugin protocol, not the release version.

MongoDB plugin 0.0.2 uses source commit `229bc6b32bc58a1fe971a50edbfef534ad59efe8`; its downloaded SHA-256 and size were checked against the official GitHub release-asset digest. The 0.0.1 archive remains separately locked with its original bytes and command for persisted connection pins. Both upstream archives share a protocol-based filename; separate installation directories prevent accidental overwrites. Removal of 0.0.1 is deferred beyond v0.4.7 and requires an operator-mount migration; see [OpenBao plugin upgrades](openbao/README.md#mongodb-plugin-upgrades).

For an operator mirror, publish the unchanged 0.0.2 archive bytes as `openbao-plugin-database-mongodb_0.0.2_linux_amd64_v1.tar.gz` (the lock's `file`) and retain `openbao-plugin-database-mongodb_linux_amd64_v1.tar.gz` for 0.0.1. The versioned local archive name prevents the flat mirror namespace from conflating two different releases with the same upstream filename; the official upstream URL is unchanged.

## Download and verification

Resolution order is:

1. `KARAKORAM_ARTIFACT_MIRROR`, with `/<file>` appended;
2. the artifact's official `upstreamURL`;
3. `https://github.com/KarakoramDev/Karakoram/releases/download/<release>/<file>`, using `KARAKORAM_ARTIFACT_RELEASE` when set, otherwise the running version if it is a release tag.

A development build needs a mirror or an explicit published artifact release for Karakoram-built files. The typed `MissingMirrorError` names the artifact and configuration choices. Configure these variables on the control plane; resolved URLs are rendered into member preparation commands. Both choices must serve the exact locked bytes. Production mirrors should use HTTPS.

`git describe` versions such as `v1.2.3-4-gabcdef0` (including a trailing
`-dirty`) identify development builds, not published native-asset releases.
They require an explicit artifact source even though their suffix resembles a SemVer prerelease.
Actual release tags such as `v1.2.3` and `v1.2.3-rc.1` retain the release
fallback; operators must publish their assets using the release tooling.
Pushing a source commit does not publish assets under its describe version.

For a source build with native bytes unchanged from a published release, select
that release **only after comparing required files and SHA-256 with the embedded
lock**:

```yaml
# Control-plane runtime environment, not build-only or member environment.
KARAKORAM_ARTIFACT_RELEASE: v1.2.3
```

This selects the GitHub release directory for Karakoram-built artifacts only;
official artifacts retain their locked upstream URLs. Invalid tags and
git-describe development versions are rejected before deployment. This does not
probe remote asset existence: the operator must select a published release that
actually contains the required locked bytes.

`KARAKORAM_ARTIFACT_MIRROR` still takes precedence for **all** locked artifacts,
including official upstream files; it must contain every needed artifact, with
no missing-object fallback. Do not use a normal release directory as a global
mirror for a mixed inventory: releases stage Karakoram-built files, not every
official download. Keep the control-plane build version/commit truthful; do not
disguise a source build as an old release. These settings affect native dependency
downloads only. Member deployment still packages the running control-plane
executable (`karakoram agent`), not a server binary downloaded from that release.

[Neo4j](https://neo4j.com/) Community uses the official `neo4j-community-2026.09.0-unix.tar.gz`, `neo4j-community-2026.10.0-unix.tar.gz` or `neo4j-community-2026.10.1-unix.tar.gz`, plus the official [Eclipse Temurin](https://adoptium.net/) 21 **JRE** `21.0.13+9` archive (Linux x64 HotSpot, not JDK). All four have locked SHA-256, size and upstream provenance; mirrors must preserve those bytes. They are upstream releases, not Karakoram source rebuilds. Neo4j 2026.10.1 is the latest stable in the [official release archive](https://neo4j.com/release-notes/database/); its [official SHA-256 sidecar](https://dist.neo4j.org/neo4j-community-2026.10.1-unix.tar.gz.sha256) verifies the 246054933-byte tarball. Temurin's [Adoptium JRE API metadata](https://api.adoptium.net/v3/assets/latest/21/hotspot?architecture=x64&image_type=jre&os=linux&vendor=eclipse) and [release checksum](https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jre_x64_linux_hotspot_21.0.13_9.tar.gz.sha256.txt) verify the 52181297-byte JRE. Annotated source tags `2026.10.1` and `jdk-21.0.13+9_adopt` dereference to the commits recorded in the lock. The shared JRE applies to every offered Neo4j version; existing members switch only after successful runtime preparation during rollout. See the [Neo4j guide](neo4j/README.md).

The separately locked `temurin-jre-21-legacy` archive retains the previous official JRE `21.0.12.1+1` solely for transitional preparation. Replacements of members still using that Java root extract both JREs into `/opt/karakoram/temurin-jre-21`, so cached configuration can launch before the deployment receipt is acknowledged—even if that receipt is lost. Once the member records `21.0.13+9`, subsequent preparation omits the legacy artifact. This is not a user-selectable runtime or a security-fixed JRE. The optional `TestLegacyJREReplacementMissingReceiptRealPreparation` regression uses `KARAKORAM_NEO4J_JRE_ARTIFACT_DIR` containing both locked archives to execute checksum-verified preparation and both Java executables on a fresh replacement filesystem.

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

Percona Server/PXC/XtraBackup 8.0 reached [community EOL](https://www.percona.com/release-lifecycle-overview/)
on 2026-04-01. The existing Server 8.0.46-37 / 8.0.45-36, PXC 8.0.46-38.1 and
XtraBackup 8.0.35-36 locks are retained unchanged for existing clusters, recovery
and migration to 8.4. New empty 8.0 clusters are rejected; validated same-major
`dataSource` recovery remains possible. No post-EOL 8.0.46-39 artifact is implied
or added, and retained locks do not imply continuing community security support.

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
| `weaviate-1.40` | `1.40.3` | `weaviate-v1.40.3-linux-amd64.tar.gz` |
| `weaviate-1.39` | `1.39.15` | `weaviate-v1.39.15-linux-amd64.tar.gz` |
| `weaviate-1.40.1` (deprecated) | `1.40.1` | `weaviate-v1.40.1-linux-amd64.tar.gz` |
| `weaviate-1.39.12` (deprecated) | `1.39.12` | `weaviate-v1.39.12-linux-amd64.tar.gz` |

Upstream assets are published at `https://github.com/weaviate/weaviate/releases/download/v<VERSION>/<ARCHIVE>`. The lock records exact byte size, SHA-256 cross-checked against upstream release checksum sidecars, and the dereferenced source tag commit. Member preparation downloads from upstream or an exact-byte operator mirror, verifies the locked digest and installs the root-layout archive into the versioned [Weaviate](https://weaviate.io/) directory. Checksum agreement establishes byte integrity, not an independent maintainer signature.

The current patches are [1.39.15](https://github.com/weaviate/weaviate/releases/tag/v1.39.15) and [1.40.3](https://github.com/weaviate/weaviate/releases/tag/v1.40.3), the latest stable releases in their respective lines at qualification. SHA-256 was checked against each archive's upstream `.sha256` sidecar and release-asset digest. Annotated tags were dereferenced to commits `4f2afa66fadda236d28e34ab6a845a650c698ea3` and `075fe4065cb24e5668733661b551b5b6710f98b9`, respectively. Historical version-named entries preserve exact-byte installation for existing deprecated clusters; no backup version metadata is rewritten. See [patch maintenance considerations](weaviate/operations.md#patch-maintenance-considerations).

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

`NATIVE_ARTIFACT_STORAGE` optionally names the Incus storage pool for both instances (passed as `incus init -s`); otherwise the default profile's pool is used.

Incus lifecycle, file-transfer and metadata clients have a 600-second timeout so an unresponsive client cannot indefinitely retain a serialized build window. Guest compilation and smoke execution are not given that control-operation deadline. Every `incus init` carries a random per-call `user.karakoram.creation` marker. After a timed-out `init`, `start`, `stop` or `delete`, the builder waits for the daemon's operations on that instance to finish and checks the result: a completed creation is adopted only when the instance carries that call's marker and requested limits (any other same-name instance fails the build), and a state change that already took effect is not repeated. Otherwise, and for metadata or file-transfer calls, the call is retried once. A second timeout fails the build; a timeout never promotes or qualifies an artifact.

`recipes.json` pins source commits and toolchain versions; `extensions.json` pins each PostgreSQL extension. `toolchains.json` pins exact toolchain URLs, SHA-256 values, sizes, and the official checksum provenance. These pins were independently checked against downloaded bytes, including older releases' official SHA-256 checksum assets. Rebuilds use these checked-in pins, not mutable release API metadata. Source checkouts are detached and checked against the pinned commit. Toolchain download caches, build logs, manifests (including toolchain transport hashes), smoke logs, and `build-report.json` are retained under the gitignored `dist/native-artifacts/` directory. Ubuntu runtime apt dependencies are recorded per recipe and in the packaged manifest.

Public GitHub submodules that upstream declares with `git@github.com:` URLs
are fetched over HTTPS using a command-scoped Git URL rewrite. This avoids
requiring SSH credentials or host-key exceptions in the clean builder while
retaining each submodule commit pinned by the parent repository.

Reusable compiled OTP installations require `/opt/otp-<version>/.karakoram-build-inputs`, written only after configure, compilation, installation and the executable check succeed. Before considering reuse, the builder verifies the pinned source archive again and compares the receipt against its source SHA-256, the builder script hash/configure arguments, fixed compilation environment, machine architecture and sorted installed-package versions (including the system compiler and development libraries). Missing or different receipts discard both the source/build tree and installation before extracting and compiling the verified archive; an existing `bin/erl` alone never authorizes reuse. Package or build-script changes conservatively invalidate the cache, even at the same OTP version. Other precompiled toolchains are installed afresh into cleared directories; extension builds also clear compiled cargo-pgrx CLI and PGRX state rather than allowing Cargo's same-version installation shortcut.

The local OTP cache-integrity tests run the real verification/build shell with `TMPDIR` set to their disposable fixture directory, so shell-created receipts use the same filesystem as the toy source and installation rather than a separately quota-limited host `/tmp`. Shell failures retain their exit status and include captured stdout/stderr in the traceback.

Build bootstraps are inside the same integrity boundary: `toolchains.json` pins [npm](https://docs.npmjs.com/) 11.12.1, [pnpm](https://pnpm.io/) 11.13.1, the amd64 [Turbo](https://turbo.build/) 2.9.14 binary, [Hex](https://hex.pm/) 2.5.3 and [Rebar3](https://rebar3.org/) 3.24.0 (the OTP-27 build, compatible with both pinned OTP 27 and 28). Their exact bytes are SHA-256-checked on download and again inside the builder, and included in manifest toolchain transports. npm/pnpm include their bundled dependencies and are unpacked directly; Turbo uses its verified platform binary rather than `pnpm dlx` or an installer that resolves optional dependencies. npm registry SHA-512 metadata and Hex's published SHA-512 CSVs were checked when selecting these pins. These byte pins, not mutable metadata fetched during a build, anchor rebuild integrity. This does not authenticate unpinned Ubuntu build packages or claim byte-reproducible compiler output.

Mix bootstrap authentication differs across the pinned [Elixir](https://elixir-lang.org/) versions: [1.18.6 verifies Hex's signed CSV using its embedded public key](https://github.com/elixir-lang/elixir/blob/v1.18.6/lib/mix/lib/mix/local.ex), whereas [1.19.7 uses HTTPS metadata without that signature check](https://github.com/elixir-lang/elixir/blob/v1.19.7/lib/mix/lib/mix/local.ex). Rebar's default installer also selects a compatible version rather than accepting an explicit version argument. Consequently recipes install the verified local Hex archive and Rebar executable; they do not rely on a uniform Mix signature guarantee or reuse an arbitrary pre-existing bootstrap.

The all-artifact order is [imgproxy](https://imgproxy.net/), Storage, postgres-meta, Realtime, Studio, the [Supabase](https://supabase.com/) PostgreSQL 17 extension bundle, [TimescaleDB](https://www.timescale.com/) PG16/17/18, [Citus](https://www.citusdata.com/) PG16/17/18, pgvector PG16/17/18, [Supavisor](https://github.com/supabase/supavisor), [Logflare](https://logflare.app/), Edge Runtime, MongoDB MCP. Imgproxy precedes Storage because its qualified archive is required by Storage's transformation smoke; postgres-meta similarly precedes Studio. Every fresh candidate manifest records `buildInputFingerprint`, a deterministic SHA-256 over the selected `recipes.json` entry and the checked-in build scripts/helpers, toolchain/bootstrap byte pins, auxiliary-asset/extension pins, and native patches. The support-tree hash deliberately includes inputs for other recipes too, preferring conservative invalidation to missed dependencies. A retained `.tar.gz.candidate` is re-smoked without recompiling only when its packaged recipe and build-input fingerprint exactly match; old manifests without the fingerprint and changed inputs are rejected. Candidates remain excluded from the lock until qualification and provenance validation succeed. Remove an obsolete candidate explicitly before requesting a rebuild. Fresh builds refresh recipe/helper scripts and patches even when reusing a running builder.

Node applications include their own pinned Node runtime. Elixir artifacts retain their complete mix release, ERTS and NIFs. Imgproxy targets Ubuntu's libvips baseline, not an upstream custom codec library image. Edge Runtime includes the main worker from the pinned Supabase source selection, with its two function-directory paths adapted to the required `FUNCTIONS_ROOT` runtime environment setting; original source and both hashes are included. The PostgreSQL extension tarball uses filesystem-root paths under `usr/lib/postgresql/17` and `usr/share/postgresql/17`; provenance and per-extension results live under `usr/share/postgresql/17/karakoram-native-artifacts/`. Install it only into the matching [Percona](https://www.percona.com/) PostgreSQL 17 environment.

The imgproxy recipe selects [4.0.21](https://github.com/imgproxy/imgproxy/releases/tag/v4.0.21)
at source commit `302105ccd183d0038ef71aa11ab4ff3de5f0c989`, built from `./cli`
with Go 1.27.3 (upstream `go.mod` requires Go 1.27). The Go archive's SHA-256
and size match the [official Go download metadata](https://go.dev/dl/?mode=json).
The bundle carries the upstream Apache-2.0 LICENSE and dependency NOTICE.
It continues to depend on Noble's `libvips42t64` rather than vendoring codecs;
the [Noble source publication](https://api.launchpad.net/1.0/ubuntu/+archive/primary/+sourcepub/15914439)
is libvips 8.15.1. The imgproxy source-installation guide does not declare a
new numeric v4 libvips minimum; compatibility must be established by the native
build and clean-runner transformation smoke, not by the container base version.
4.0.21 includes the fixes for
[CVE-2026-115582](https://github.com/imgproxy/imgproxy/security/advisories/GHSA-8w7m-q6hg-mfqm),
[CVE-2026-115583](https://github.com/imgproxy/imgproxy/security/advisories/GHSA-96f3-j94p-x9jv),
[ICO allocation](https://github.com/imgproxy/imgproxy/security/advisories/GHSA-mmq5-xm89-9pfg)
and [ICO memory access](https://github.com/imgproxy/imgproxy/security/advisories/GHSA-494m-4556-wf5p)
plus later BMP/ICO and SVG fixes. Native smoke uses the rendered production
environment and Storage's percent-encoded HTTP-origin URL format, checks PNG
dimensions and WebP negotiation, verifies SVG sanitization, and rejects a
loopback source without relaxing source-address guards. The final release
bundle must be rebuilt twice from the integrated build-input tree and have
identical archive hashes; a slice build is not release reproducibility evidence.

Realtime [2.153.3](https://github.com/supabase/realtime/releases/tag/v2.153.3) is built with OTP 28.5.0.8, Elixir 1.19.7 and asset-build-only Node 24.22.2. This includes the tenant-ID advisory fix introduced in 2.146.0; 2.143.4 alone does not fix [GHSA-2rcv-vpjw-5g6c](https://github.com/supabase/realtime/security/advisories/GHSA-2rcv-vpjw-5g6c). The upstream Dockerfile still selects Bun 1.3.14 and pg-toolbelt commit `107ac3df4b889c527215d1f6a37df64b33154c16`; the native helper retains those pins. Qualification requires a healthy GET tenant payload (HTTP 200 or HEAD alone is insufficient), completed tenant migrations, rejection of unsafe tenant identifiers, and rejection of tenant credentials by the private administration API using an independent signing secret.

Storage [1.81.5](https://github.com/supabase/storage/releases/tag/v1.81.5) is built from `03a68fa7d4871ac4c87b46d003863f0bee2b5337` with bundled [Node 24.22.2](https://nodejs.org/en/blog/release/v24.22.2). Updating the host's Node package does not update this copied runtime. The 1.81.1–1.81.5 patch range adds no database migrations and retains the S3 environment contract; its new TUS request-body idle timeout defaults to 60 seconds. See [Storage compatibility](supabase/components.md#configuration). Native qualification uses a disposable, separately credentialed S3 fixture and verifies object upload/download, not just HTTP health.

Storage qualification checks REST upload/download, S3 SigV4 PUT/GET, TUS creation/HEAD/PATCH/download, and authenticated image transformation through a real imgproxy process. `storage-env.json` supplies the non-secret environment contract checked against the production renderer. Place the locked imgproxy archive in the output directory, or select a separately qualified fixture with `NATIVE_ARTIFACT_IMGPROXY` and its required `NATIVE_ARTIFACT_IMGPROXY_SHA256`. The runner verifies its digest before use and records it in the build log.

postgres-meta 0.100.0 retains source commit `9a2effd147f2f028fa36553ec2c7eeec80d7f8fc`, with its bundled runtime updated to [Node 22.23.4](https://nodejs.org/en/blog/release/v22.23.4). Studio retains source commit `bc27d2661edd09a5d0b8d76ce33da6d4071a756e`, its native compatibility patch, editor declarations and parser WASM, with the same Node 22.23.4 update. Application dependency locks are not regenerated for this runtime maintenance.

MongoDB MCP retains version 3.1.0 and its frozen security dependency tree (body-parser 2.4.0, @hono/node-server 2.1.6 and basic-ftp 6.2.6), with bundled Node updated to 22.23.4. Its clean-runner qualification starts a native MongoDB fixture, initializes the actual HTTP MCP launcher, and requires nonempty `tools/list`; the responses are retained in `mongodb-mcp-server.qualification.json`.

Edge Runtime [1.77.9](https://github.com/supabase/edge-runtime/releases/tag/v1.77.9) uses source commit `0dd15ef2281b60ea03413ef5d2a60ebb25b0e21d` and upstream's unchanged Rust 1.98.0 toolchain. The `--locked` dependency build retains rusty_v8 130.0.7 and its existing verified static archive/binding bytes. The patched main worker and launch arguments remain unchanged; native smoke invokes both the plain hello function and npm-importing function through the dispatcher with JWT verification disabled, then enabled with missing-bearer rejection and valid signed requests.

Realtime qualification uses the real Karakoram renderer and curated bootstrap
SQL rather than a separately maintained environment. Export its test-only
fixture before rebuilding:

```sh
KARAKORAM_REALTIME_SMOKE_FIXTURE=/tmp/realtime-smoke.json \
  go test ./internal/agent/supabase -run '^TestRealtimeNativeFixture$' -count=1
KARAKORAM_REALTIME_SMOKE_FIXTURE=/tmp/realtime-smoke.json \
  scripts/native-artifacts/build.sh supabase-realtime
```

The runner applies that bootstrap on PostgreSQL 17 with the catalog's pinned
wal2json package, runs both metadata and tenant migrations, and requires a real
websocket join, broadcast echo and `postgres_changes` INSERT. The complete
[configuration parity table](supabase/components.md#realtime-21433--21533-configuration-parity)
covers each emitted setting and the intermediate upstream minor releases.

`FUNCTIONS_ROOT` must point outside `/tmp` (for example `/var/lib/karakoram/functions`) and contain `<function-name>/index.ts` plus a root `deno.jsonc` import map. Edge Runtime mounts a separate private `/tmp` filesystem in each worker: host-created `/tmp/functions` is not visible to the main worker's directory check and returns `NOT_FOUND`, even when the files exist in the host namespace. The clean smoke invokes both a plain hello function and a function resolving a pinned npm import through the packaged main worker.

The lock's per-extension `extensionFlags` map defines the mode actually qualified. Only entries with `preload: true` may be added to `shared_preload_libraries`; this is `pg_net` for the source-built bundle. `pgsodium` is intentionally not preloaded and supplies crypto functions with caller-supplied keys only. Server root-key management, key-id operations and transparent column encryption (TCE) are unavailable and listed in `unavailableFeatures`. Do not install an ephemeral root key or enable those features implicitly. Vault 0.3.x does not require pgsodium.

The Supabase PG17 bundle revision `17.11.1-2` refreshes
[pg_net 0.20.5](https://github.com/supabase/pg_net/releases/tag/v0.20.5),
[Vault 0.3.2](https://github.com/supabase/vault/releases/tag/v0.3.2),
[pgsodium 3.1.11](https://github.com/michelp/pgsodium/releases/tag/v3.1.11),
[pg_graphql 1.6.3](https://github.com/supabase/pg_graphql/releases/tag/v1.6.3),
[pg_jsonschema 0.3.4](https://github.com/supabase/pg_jsonschema/releases/tag/v0.3.4),
[HypoPG 1.4.5](https://github.com/HypoPG/hypopg/releases/tag/1.4.5), and
[index_advisor 0.2.1](https://github.com/supabase/index_advisor/releases/tag/v0.2.1).
GraphQL's newer minor is required for
[GHSA-3xwc-rjxj-m99h](https://github.com/supabase/pg_graphql/security/advisories/GHSA-3xwc-rjxj-m99h);
1.5.12 does not contain that fix. GraphQL uses cargo-pgrx 0.19.2;
JSON Schema uses 0.16.1; both compile with pinned Rust 1.98.0 and explicit PG17
features. Native PGXS extensions retain upstream SQL upgrade chains. The pgrx
extensions carry the previous qualified bundle's install SQL plus generated
function-replacement upgrade SQL; GraphQL event triggers, function identities,
grants and dependent user objects are preserved rather than dropped/recreated.
The clean runner creates every extension at its current default and separately
creates the previous installed version and runs `ALTER EXTENSION UPDATE`.

The pg_net v0.20.5 source tag still declares SQL/default version `0.20.4` in its
upstream Makefile; the capability report records source release `0.20.5`
separately from `sqlVersion: "0.20.4"`. Qualification expects that upstream SQL
default, not an invented 0.20.5 control file or empty upgrade.
The unchanged plpgsql_check 2.8.11 and pg_hashids 1.3.0 source releases likewise
retain upstream SQL defaults `2.8` and `1.3`, respectively.

pgsodium's new IP encryption API requires libsodium 1.0.22, not Noble's 1.0.18.
The builder compiles the [official 1.0.22 tarball](https://download.libsodium.org/libsodium/releases/libsodium-1.0.22.tar.gz)
as static PIC, hiding archive symbols with `--exclude-libs,ALL` when linking
`pgsodium.so`. No process-wide library replacement or sodium symbol export is
permitted; `nm -D` enforces the latter. The tarball is 2,008,529 bytes,
SHA-256 `adbdd8f16149e81ac6078a03aca6fc03b592b89ef7b5ed83841c086191be3349`.
Its [Minisign sidecar](https://download.libsodium.org/libsodium/releases/libsodium-1.0.22.tar.gz.minisig)
and trusted comment were verified with the [published libsodium key](https://doc.libsodium.org/installation#integrity-checking)
`RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3`.
The ISC license ships under `LICENSES/libsodium/LICENSE`. Vault still links
Noble's libsodium23, so that runtime dependency remains. Qualification runs
libsodium's own `make check`, an IP encryption/decryption round trip, and the
existing SHA-256 SQL API with pgsodium absent from preload libraries.
Caller-supplied keys only remains the supported mode.

Declared native compatibility patches in `scripts/native-artifacts/patches/` are applied to the exact pinned sources and included in affected bundles with `SHA256SUMS`. Supavisor and Logflare accept `KARAKORAM_DB_CA_FILE` to verify the upstream database CA through a TLS-transparent loopback forwarder, covering metadata, cluster discovery and analytics provider connections. Hostname verification is intentionally not used for that loopback endpoint; the CA chain is still verified. Supavisor tenant configuration must also select upstream TLS, peer verification and the upstream CA. Realtime's compiled pgdelta helper preserves its parser WASM files at the fixed `/opt/karakoram/<name>-<version>/app/pg-toolbelt` layout; the smoke exercises SQL parsing, not only BEAM startup.

Studio's native patch selects two build workers, webpack memory optimizations and Next's webpack build worker; its recipe uses `next build --webpack` with two Rayon threads. The webpack callback disables compiler caching, limits module parallelism to one, preserves upstream raw-text handling for Markdown/Deno declarations and explicitly aliases the two declaration imports to the pinned public assets (the TS path resolver otherwise fails those imports). This replaces the unbounded Turbopack attempt that exhausted the original builder reservation; it does not replace Studio's standalone Next application or change the pinned application source selection.

`studio-assets.json` pins the Deno v1.45.0 and Supabase functions-js 2.4.4 editor declarations selected by the upstream `build:deno-types` script, together with their MIT licenses. These generated public files must be present before compiling Studio. Their downloaded hashes are recorded in the artifact manifest; the functions-js declaration also matches JSR's versioned package checksum manifest. Versioned download pins provide transport integrity, not an independent maintainer signature.

The Studio recipe explicitly preserves the frozen dependency tree's `libpg-query` WASM at its original pnpm path and carries its license. Next's standalone tracer omits that dynamically opened file; HTTP health alone can pass while SQL parsing raises an asynchronous `ENOENT`. The clean smoke therefore runs a real `SELECT 1` parse using the bundled Node runtime before starting Studio, in addition to the required page/profile and live-meta HTTP probes.

Supavisor 2.9.13 uses the compatible pinned OTP 27.3.4.19 patch release with Elixir 1.18.6: its locked Artificery dependency requires `IO.ANSI.Sequence`, removed in Elixir 1.19, so the OTP 28.5.0.8 / Elixir 1.19.7 build cannot compile unchanged dependencies. Logflare 1.50.15 targets OTP 28.5.0.8 with Elixir 1.19.7 and asset-build-only Node 22.23.4. Both retain their native database TLS patches; Logflare additionally retains the Cainophile temporary-slot keepalive acknowledgment patch. Hex dependency fetches use Ubuntu's `/etc/ssl/certs/ca-certificates.crt` trust bundle via `HEX_CACERTS_PATH`. Bootstrap installs use the verified local pins described above, replacing pre-existing tools with the selected versions.

Only a successful native HTTP health smoke (or `CREATE EXTENSION` for each packaged extension) permits updating the lock. The clean runner installs the documented runtime packages and Python/[curl](https://curl.se/) smoke tooling. DB-dependent fixtures additionally install `gnupg lsb-release` for the verified repository bootstrap, the pinned Percona 17 server, and contrib. Storage qualification requires `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, and `NATIVE_ARTIFACT_SMOKE_PREFIX` (optional `S3_REGION`) in the build host environment: credentials are transported through a temporary mode-0600 file, never command arguments or packaged manifests. The Storage smoke configures that [S3](https://aws.amazon.com/s3/) bucket and tenant prefix and performs upload/download after `/status`. Meta additionally queries `/tables`; imgproxy performs a PNG transform. Supavisor/Logflare fixtures generate a temporary trusted database certificate for `database.internal`, reject plaintext TCP database connections, and require a live TLS database session after HTTP health. Supavisor also exercises a tenant query over verified upstream TLS, and Logflare probes its analytics provider repository. Failed recipes remain absent from the lock and cause a nonzero build exit. Both disposable instances are deleted after the run. Build success is not a compatibility claim for codecs, external integrations or features not exercised by the smoke.

Studio qualification also launches the already locked postgres-meta archive against the pinned Percona fixture, requires `/` and `/api/platform/profile` HTTP200 from the packaged standalone Next server, and checks meta `/health` and `/tables`. This is native server/rendering and reachable database metadata evidence, not browser-level administrative workflow acceptance. Supavisor's pinned `/api/health` contract is HTTP204 (not200); qualification requires that exact response plus a transaction-mode query through port6543, checks that its upstream PostgreSQL provider actually uses TLS, and performs a BEAM TLS handshake with the patched repository's peer-verification options and an unrelated CA to require an `unknown_ca` rejection.

## Standalone per-major PostgreSQL extensions

Nine independent recipes build `timescaledb-pg16/17/18`, `citus-pg16/17/18` and `pgvector-pg16/17/18`. These are not additions to the Supabase-only PG17 bundle; Supabase installs `pgvector-pg17` alongside its existing bundle. A qualified lock entry declares `kind: "pg-extension"` and integer `pgMajor`, as well as its verified `extensions` and `extensionFlags`; an unbuilt recipe alone does not make a profile available.

Each recipe installs both `percona-postgresql-<major>` and `percona-postgresql-server-dev-<major>` at the same exact Noble version: PG16 `2:16.15-1.noble`, PG17 `2:17.11-1.noble`, PG18 `3:18.6-1.noble`. Builds explicitly select `/usr/lib/postgresql/<major>/bin/pg_config`. The archive contains only that major's `usr/lib/postgresql/<major>` and `usr/share/postgresql/<major>` trees. Product-specific manifests, upstream license/notice files, commands, installed package versions and extension build reports live in `usr/share/postgresql/<major>/karakoram-native-artifacts/<artifact-name>/`, avoiding collisions between products and the existing Supabase bundle. Install only into the matching major.

[TimescaleDB 2.31.0](https://github.com/timescale/timescaledb/tree/4fa2a7c6b579171a1a8966fd87a0d4189ebab43b) is built in Apache-2.0-only mode, not the full Community/TSL distribution. Its bootstrap uses `-DAPACHE_ONLY=1 -DREGRESS_CHECKS=OFF -DSEND_TELEMETRY_DEFAULT=OFF -DWARNINGS_AS_ERRORS=OFF` and a Release CMake build. Packaging and smoke reject TSL shared libraries; the clean runner preloads TimescaleDB, creates the extension, requires `timescaledb.license` to report `apache`, and inserts/queries a hypertable. TSL functionality is explicitly unavailable in the lock flags.

[pgvector 0.8.7](https://github.com/pgvector/pgvector/tree/f37c13f68b57d2c3472b2214fbcff699d6d34876)
uses unmodified upstream PGXS with an explicit per-major `PG_CONFIG` and empty
`OPTFLAGS` for portable amd64 binaries. The clean runner first installs Percona's
0.8.6 package and creates tables with IVFFlat and HNSW indexes using that SQL.
It runs the same diversion/atomic publisher as member preparation twice,
restarts PostgreSQL, and requires `ALTER EXTENSION vector UPDATE` to reach 0.8.7.
Existing indexes must remain valid and indexed kNN results must match exact
search on a deterministic sample, with explicit index plans. IVFFlat REINDEX
and fresh default-version creation must also succeed.
`vector` needs no preload. No automatic user-index rebuild is performed.

[Citus 14.2.0](https://github.com/citusdata/citus/tree/58702e722c6efbae45e3cade885b476e4fddc7e1) is built from unmodified upstream source with explicit `PG_CONFIG`, `--without-libcurl` on PG16/17 and `--with-libcurl` on PG18. [LZ4](https://lz4.org/) and [Zstandard](https://facebook.github.io/zstd/) remain enabled; anonymous reporting defaults off. Runtime dependencies include Noble's `libpq5`, `libssl3t64`, `liblz4-1` and `libzstd1`. [Percona](https://www.percona.com/) PG18's server headers independently define `HAVE_LIBCURL`, so unmodified Citus compiles its [curl](https://curl.se/) helpers even if configure disables curl. PG18 must therefore configure and link curl explicitly, additionally installing `libcurl4-openssl-dev` at build time and declaring `libcurl4t64` at runtime. No upstream header or source patch removes that inherited capability. Its clean runner creates a coordinator and two local worker instances, preloads/creates Citus on all three, registers the workers, distributes and queries a table, starts/waits for rebalance, checks placement on both workers, and creates a distributed restore point. It shuts down all three instances after the probe; the disposable runner is deleted even on failure.

### Citus distribution and source

Citus's upstream [LICENSE](https://github.com/citusdata/citus/blob/58702e722c6efbae45e3cade885b476e4fddc7e1/LICENSE) supplies GNU AGPL version 3, without an additional “or later” grant; the artifact records `AGPL-3.0-only`. Citus is a separately downloaded, unmodified database extension, not code linked into the Apache-2.0 Karakoram control plane. This does not change Karakoram's own license or waive obligations for Citus distributors/operators.

Every Citus archive includes the full upstream AGPL text and a `SOURCE-OFFER.txt` identifying the immutable upstream commit, corresponding-source download and packaged build configuration. Distributors must provide corresponding source access in an AGPL-compliant manner alongside binary distribution; the upstream link is provenance, not a substitute for maintaining required source availability. If Citus is ever patched, publish the complete corresponding modified source and build scripts, retain notices, and satisfy the AGPL's applicable distribution and remote-network-interaction obligations before shipping. Do not describe a patched artifact as unmodified.

Build and qualification are one sequential command (there is no bypass that publishes unqualified candidates):

```sh
scripts/native-artifacts/build.sh timescaledb-pg16 timescaledb-pg17 timescaledb-pg18
scripts/native-artifacts/build.sh citus-pg16 citus-pg17 citus-pg18
scripts/native-artifacts/build.sh pgvector-pg16 pgvector-pg17 pgvector-pg18
```

The resource guards above apply: one 6 GiB/8-vCPU builder, then one 2 GiB/2-vCPU clean runner, with compilation limited to four jobs. Citus's three [PostgreSQL](https://www.postgresql.org/) processes share that runner. Plan at least 120 GiB host free disk and 14 GiB available host memory. Actual build duration and archive sizes are recorded only by a real native qualification; recipe/unit-test coverage is not a claim that release bytes have been built.

### Rootless Ubuntu transport

For these nine standalone extension recipes, `NATIVE_ARTIFACT_TRANSPORT=rootless`
selects a local userspace builder instead of [Incus](https://linuxcontainers.org/incus/):

```sh
NATIVE_ARTIFACT_TRANSPORT=rootless scripts/native-artifacts/build.sh \
  timescaledb-pg16 timescaledb-pg17 timescaledb-pg18 \
  citus-pg16 citus-pg17 citus-pg18 \
  pgvector-pg16 pgvector-pg17 pgvector-pg18
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

Official artifacts retain upstream provenance rather than being described as source rebuilds. [Envoy](https://www.envoyproxy.io/) [1.39.6's clear-signed checksums](https://github.com/envoyproxy/envoy/releases/download/v1.39.6/checksums.txt.asc) were verified with the [official maintainers key](https://raw.githubusercontent.com/envoyproxy/envoy/main/envoy-maintainers-public.key), fingerprint `0AFCE836BA4D1D35763C8523D8CDC3750181F31F`. The official standalone amd64 executable is 105,105,368 bytes, SHA-256 `116ef1d0d4e0685bbb0749134aa84ebd6d56a37c568ce6418f1242138d9d10cf`, from dereferenced tag commit `aacd0340210b536e99ec44ebe59ab9d16b259552`. SHA-256 transport integrity alone is not a maintainer signature. Studio's native source selection uses the full [Supabase](https://supabase.com/) commit in the recipe; it is not represented as a byte-identical rebuild of an independently published image tag. Pin updates require evaluating upstream dependency advisories rather than silently rewriting application lockfiles.

Vector uses the official [0.61.0 Linux amd64 GNU archive](https://github.com/vectordotdev/vector/releases/tag/v0.61.0), 58,860,480 bytes, SHA-256 `84a27c3ad6b772fdb05bb76ea01a31498bfc8f5e71f32cc78a0d96841f1aa827`, checked against upstream [`vector-0.61.0-SHA256SUMS`](https://github.com/vectordotdev/vector/releases/download/v0.61.0/vector-0.61.0-SHA256SUMS) and the GitHub asset digest. The dereferenced tag is source commit `17e017e3c07bca3f2048f7cc4ec618a23d9289c5`. This is an official download, not a Karakoram rebuild; the checksum sidecar is not an independently verified maintainer signature. See the [Supabase configuration qualification](supabase/components.md#vector-upgrade-qualification).

## Data MCP companion artifacts

| Artifact | Version | Origin | License | Runtime |
| --- | --- | --- | --- | --- |
| `pgedge-postgres-mcp` | 1.1.0 | Official pgEdge Linux x86_64 server release | PostgreSQL | Native executable at archive root |
| `mongodb-mcp-server` | 3.1.0 | Karakoram assembly of the published npm package and frozen production dependency tree | Apache-2.0 (dependencies retain their own licenses) | Bundled Node 22.23.4, npm 10.9.9; `ca-certificates`, `libstdc++6` |

The [pgEdge](https://www.pgedge.com/) archive is [the upstream server tarball](https://github.com/pgEdge/pgedge-postgres-mcp/releases/download/v1.1.0/pgedge-postgres-mcp-server_1.1.0_linux_x86_64.tar.gz), not the similarly named CLI release. Its SHA-256 is `ca79e3054455b5505d3ac4ed4bc5d4cd074a57f108b8fe9ddcd3e24d16c5452a` (8,483,086 bytes). [GitHub](https://github.com/)'s tag API resolves annotated tag `v1.1.0` through tag object `336174b178e7ddc65a581b2fa5f2e895789f2885` to full source commit `25c2aaed03398584ceea0dd71f7f67631c3dff4e`. The downloaded archive matches the published release digest; this is byte-integrity evidence, not a verified maintainer signature.

[MongoDB](https://www.mongodb.com/)'s [v3.1.0 tag](https://github.com/mongodb-js/mongodb-mcp-server/tree/f369482783bdef116c35d352fd9d92f6c984242f) resolves to `f369482783bdef116c35d352fd9d92f6c984242f`. The recipe assembles the published [npm](https://docs.npmjs.com/) distribution; it does not claim a source rebuild of its JavaScript. `scripts/native-artifacts/mongodb-mcp/package-lock.json` freezes the complete resolved tree with registry URLs and npm integrity values. Installation uses `npm ci --omit=dev --ignore-scripts`: no package lifecycle hooks or native addon builds run. The [Node](https://nodejs.org/) runtime comes from the official [Linux](https://www.kernel.org/) x64 tarball pinned in `toolchains.json`, SHA-256 `1805bcd35e6815559bd2eb97db438d3fdb0eb3a5e6a977b7c1badca76337e9ce`. The bundle retains `node/`, `app/`, and executable `bin/start`; the launcher changes to `app/` and execs `node/bin/node dist/esm/index.js`. `LICENSES/` collects distributed license/notice/copyright files (including Node's consolidated notices); `THIRD_PARTY.json` records every locked package's version, license, URL and integrity, and `provenance/` preserves the exact npm inputs.

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

The MongoDB bundle has no upstream binary URL: a Karakoram release **must publish the exact locked tarball**. Development deployments require `KARAKORAM_ARTIFACT_MIRROR`, or `KARAKORAM_ARTIFACT_RELEASE` naming a published release whose assets contain the identical locked tarball; neither npm nor a floating upstream archive is a runtime fallback. Rebuilding may produce different compressed bytes: publish the qualified locked file, or deliberately requalify and update its digest and size.

The locked MongoDB 3.1.0 tarball's size and SHA-256 are recorded in `internal/engine/nativeartifacts/lock.json`. Its frozen dependency tree includes body-parser 2.4.0, fixing [CVE-2026-111181 / GHSA-27r3-jxmh-p828](https://github.com/expressjs/body-parser/security/advisories/GHSA-27r3-jxmh-p828): trailing bytes after a compressed HTTP request body could terminate the server. MongoDB's HTTP runner installs Express JSON parsing before its header checks, so updating the packaged dependency is required, not just its npm lockfile. Exact npm overrides also select `@hono/node-server` 2.1.6 for [GHSA-rmxm-3fg6-px4f](https://github.com/advisories/GHSA-rmxm-3fg6-px4f) and basic-ftp 6.2.6 for [GHSA-c475-qrg2-pj4r](https://github.com/advisories/GHSA-c475-qrg2-pj4r); the pinned parent packages still permit affected releases. The Hono static-serving path is not used by the bundled transports. The FTP parser is conditionally reachable when an FTP-hosted proxy auto-configuration source is configured; basic-ftp 6 also rejects separate transfer hosts by default. When this bundle was first qualified (Node 22.23.3, before the v0.4.7 runtime update above), two fresh assemblies produced identical tarball bytes, and the direct-host smoke returned server version `3.1.0`, negotiated MCP `2025-03-26`, and listed 20 tools in read-only mode against the verified official MongoDB 8.0.16 Ubuntu 24.04 tarball. Patched Unix FTP listing/download and the actual get-uri FTP download path were exercised against a local fixture. These are launcher/protocol qualification results, not evidence that every database tool or optional native dependency has been exercised.
