<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL profiles

Profiles add a curated extension workload to the existing [PostgreSQL](https://www.postgresql.org/) lifecycle.
They do not create a second database engine or change HA, backup or access authority.
Select any compatible combination on plain `postgres`:

```yaml
spec:
  provider: postgres
  version: 17.11.1
  engineOptions:
    profiles: [postgis, pgvector]
```

An absent `profiles` field means no profiles. Existing plain PostgreSQL specifications
and member configurations retain their previous representation. Selection is a set:
reordering or repeating names does not install or bootstrap a profile twice. The
registry's declaration order determines installation, preload and SQL ordering.

## Available workloads

| Profile | PostgreSQL majors | Extensions created in `postgres` | Source |
| --- | --- | --- | --- |
| `postgis` | 16, 17, 18 | `postgis`, `postgis_topology`, `postgis_raster` | Exact-pinned Percona PostGIS library and scripts packages |
| `pgvector` | 16, 17, 18 | `vector` (0.8.7) | Matching smoke-qualified `pgvector-pg<major>` locked native artifact |
| `timescaledb` | 16, 17, 18 | `timescaledb` (Apache-only 2.31.0) | Matching smoke-qualified `timescaledb-pg<major>` locked native artifact |
| `supabase` | 17 | Existing Supabase curated plan | Implied exclusively by the `supabase` engine driver; not selectable on plain PostgreSQL |

The running engine driver description is the discovery authority:

```sh
karactl providers get postgres -o json
```

`engineOptions.profiles.items` reports each profile's `majors`, available extension
metadata and availability/rejection reason per major. The creation wizard uses this
metadata and disables unavailable choices. Admission rejects unknown profiles,
unsupported majors, missing locked artifacts and artifacts without a resolvable URL.
A source recipe or an artifact mirror cannot make an absent lock entry available.
The lock includes qualified TimescaleDB and pgvector artifacts for majors 16–18. Development
deployments still require a mirror serving those exact archived bytes; release
deployments require the matching release assets. Missing locks or unresolved URLs
are rejected rather than provisioning a server that cannot load the extension.

### Package provenance

The package names and pins are recorded in `internal/engine/postgres/pgprofile`.
[PostGIS](https://postgis.net/) uses `percona-postgresql-<major>-postgis-3` and matching
`percona-postgresql-<major>-postgis-3-scripts`: `2:3.5.7-2.noble` on 16/17,
`2:3.5.7-3.noble` on 18. The PostGIS pins were resolved from the
[Percona](https://www.percona.com/) Noble amd64 indexes:
[16](https://repo.percona.com/ppg-16/apt/dists/noble/main/binary-amd64/Packages.gz),
[17](https://repo.percona.com/ppg-17/apt/dists/noble/main/binary-amd64/Packages.gz),
[18](https://repo.percona.com/ppg-18/apt/dists/noble/main/binary-amd64/Packages.gz).
pgvector 0.8.7 is built from the pinned upstream release as separate PG16/17/18
bundles because Percona's indexes still offer only 0.8.6. Supabase PG17 uses the
same `pgvector-pg17` artifact; its remaining APT pins are retained.
Do not substitute PGDG packages for Percona packages.

## Additive lifecycle

Profiles can be **added, not removed**. Removal could strand extension objects or
preload-dependent databases; disabling a switch is not an extension-uninstall API.
Adding a profile reprepares data runtimes with the pinned packages/artifacts before
publishing the profile-authorized member configuration. Existing shared-memory
ordering, pending-restart handling and primary switchover rules remain in force.
Profile parameters are engine-owned; users cannot override them through arbitrary
GUCs or inject a `shared_preload_libraries` value.

After PostgreSQL is ready, the control plane selects the writable primary and asks
the member agent to apply the registry's curated SQL. Requests carry profile IDs,
never caller SQL. The agent checks those IDs against its authorized member
configuration and supported major, checks the writable-primary state, serializes
execution with an advisory lock and records each migration's SQL checksum in
`_karakoram.profile_migrations`. Each migration and its receipt commit atomically;
matching receipts skip, differing checksums fail, and interrupted transactions retry.
Restore generations invalidate the control-plane bootstrap digest without rewriting
migration receipts. The installed-extension updater runs before publishing the new
bootstrap digest, so restoring older extension SQL on current binaries updates
each installed database even without a PostgreSQL server-version change. Failed
SQL keeps the digest unapplied and retries without redeploying members.
Physical backups snapshot the normalized selected profile set in
`postgres.profiles`, including coordinated backups and scheduled recovery points.
Restore and clone targets must select a superset before provisioning or destructive
recovery work: a backup containing PostGIS/vector objects also needs their runtime
libraries. Requirements come from the backup, not the source's current selection,
and survive source deletion. Legacy backups without this metadata require no
profiles; existing Supabase engine driver compatibility remains unchanged.

Profiles make their extension set available to [native database creation and bindings](access-and-tls.md).
A profile creates its extensions in `postgres`, not in `template1`; other databases
request their own extensions explicitly. After minor and major upgrades, selected
profile extensions are updated with `ALTER EXTENSION ... UPDATE` in every
connectable database where installed. Major-upgrade admission requires the selected
profiles to be available for the target major, and preparation includes both ABIs.
Readiness remains false while post-upgrade extension updates are pending; the
`ExtensionsUpdated=False` condition reports `ExtensionUpdatePending` until a retry
completes successfully.

TimescaleDB updates use a dedicated fresh backend for each database: the first SQL
statement is `ALTER EXTENSION "timescaledb" UPDATE`. Even a catalog existence query
can load the old version through TimescaleDB's preload hook, so discovery and other
extension updates must not run on that backend first. Installed-extension discovery
still skips absent extensions; PostGIS, vector and other extensions keep their
normal update path. Unsupported-operation errors are permanent request rejections,
not temporary-unavailability responses. This agent-side behavior is shared across
backends and the PostgreSQL core used by Supabase; existing installations need an
agent rollout, not only a control-plane upgrade.

APT-only member preparation also needs this SQL update: after `upgrade-agents`
has acknowledged every member, it invokes the same installed-extension updater,
including `vector` and all installed managed Supabase extensions such as `set_user`.
A failed SQL update keeps extension completion pending and is retried; replacing
binaries alone does not prove that each database's extension catalog is current.
Supabase updates also cover formerly selected extensions that remain installed.

Non-Supabase profile bootstrap and per-database extension creation retain the
management connection's pinned `search_path = pg_catalog`. Extensions without a
control-file schema are explicitly installed in `public`; fixed-schema extensions
use their declared namespace (for example, `postgis_topology` uses `topology`).
Per-database creation reads the default version's schema from PostgreSQL's extension
catalogue, so it also honors fixed schemas for extensions outside the curated profiles.

### pgvector security maintenance

The [0.8.7 release](https://github.com/pgvector/pgvector/blob/v0.8.7/CHANGELOG.md)
fixes [CVE-2026-103484](https://github.com/pgvector/pgvector/issues/1036), an IVFFlat
index-build buffer overflow affecting 0.8.6 and earlier. v0.4.7 supplies the fixed
binaries as locked source bundles on PostgreSQL 16/17/18 and Supabase 17.
Member preparation retains any installed `percona-postgresql-<major>-pgvector`
package and its dependents. Local no-rename diversions preserve old files in
`.distrib`; verified bundle files are published with same-directory atomic
renames, so a running member never loses `vector.so`. Subsequent APT operations
write the diverted paths, not the active bundle. New members omit the old APT
package but establish the same diversions before publication.
If preparation stops before diversion registration, a retry accepts an existing
backup only when its SHA-256 matches the still-active file. Backups are published
only after a complete temporary copy; partial or unrelated `.distrib` files are
rejected without modifying the active library.

Same-version restores, including the Supabase flavour, update installed extension
SQL before acknowledging their restored bootstrap generation. SQL failure leaves
bootstrap incomplete; retrying does not require another member deployment.

Use the [rolling preparation procedure](README.md#dependency-package-rollouts);
restarting native processes alone does not install the bundle. Roll out binaries
to every member before `ALTER EXTENSION vector UPDATE` in each database where installed. Check
`SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'vector'` in
every such database. Follow upstream's [monitoring guidance](https://github.com/pgvector/pgvector/tree/v0.8.7#monitoring):
compare representative approximate-index query results with exact search in a
transaction using `SET LOCAL enable_indexscan = off`, and inspect query plans
and index validity after rollout. Approximate recall is not expected to be 100%.
The 0.8.7 advisory does not prescribe a blanket index rebuild; investigate failed
or suspect builds and apply upstream's index-specific repair guidance rather
than assuming a binary replacement repairs existing indexes.

## Supabase compatibility

The `supabase` engine driver composes the same PostgreSQL core with the registry's
Supabase profile and its application components. It remains engine driver `supabase`,
wire flavour `supabase`, PostgreSQL major 17, with the same packages, runtime
preparation commands, selected-preload order, curated SQL bytes, advisory lock and
`_karakoram.supabase_migrations` ledger. Existing specs gain no explicit profile field.
Golden fixtures protect data/proxy engine bytes, prepare slices, bootstrap plans and
digests, application restart tokens and the core spec digest.

### set_user security maintenance

Supabase PG17 currently installs `percona-pgaudit17-set-user=1:4.2.0-6.noble`.
The [4.3.0 release](https://github.com/pgaudit/set_user/releases/tag/REL4_3_0)
fixes reset-token bypass/backend crashes, group allowlist matching and
case-sensitive role matching, and compares reset tokens in constant time.
As of 2026-11-05, 4.3.0 is not packaged in the Percona PG17 Noble index, so the
pin remains 4.2.0 and these fixes are **not** claimed as delivered.

The managed profile renders no `set_user.*` GUCs, uses lower-case managed role
names, and does not call `set_user`, `set_user_u`, `reset_user` or
`set_session_auth` in its bootstrap. It does not automatically preload `set_user`.
There is therefore no rendered group allowlist or reset-token call to migrate.
This does not establish that an application's role grants, GUC settings or
extension calls are unaffected. Do not enable a new preload or loosen function
grants as part of the package update.

Before qualifying 4.3.0, audit application function grants and configured
`set_user.superuser_allowlist` / `set_user.nosuperuser_target_allowlist` against
the [upstream rules](https://github.com/pgaudit/set_user/blob/REL4_3_0/README.md):
unquoted entries fold to lower case, quoted names match exactly, and quotes
around a mixed-case group must include the `+` (for example, `"+Admin"`).
Test positive and denied group transitions, mixed-case roles and correct,
missing and wrong reset tokens. A reset token is a best-effort safeguard, **not
a security boundary** for untrusted code or pooled sessions.
After every member has the fixed binaries, update `set_user` in each database
where installed and check `pg_catalog.pg_extension.extversion`; preserve function
privileges and application/session behavior. Supabase bootstrap and the shared
post-rollout installed-extension updater cover installed managed extensions,
including ones removed from the current selection.

## TimescaleDB edition

The descriptor requires a major-specific Apache-only TimescaleDB artifact, preloads
`timescaledb` and forces `timescaledb.telemetry_level=off`. No Timescale Community/TSL
modules are supplied by this profile. Compression policies, continuous-aggregate
refresh policies and other TSL features are unavailable. The
[native artifact lock](../native-artifacts.md) records real per-major archives
qualified by extension creation, Apache license mode and a hypertable insert/query.
Availability still requires a resolvable mirror or release URL for those bytes.

See [ADR 0044](../adr/0044-postgres-profiles.md) for the design contract.
