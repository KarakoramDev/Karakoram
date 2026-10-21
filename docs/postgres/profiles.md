<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL profiles

Profiles add a curated extension workload to the existing PostgreSQL lifecycle.
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
| `pgvector` | 16, 17, 18 | `vector` | Exact-pinned Percona pgvector package |
| `timescaledb` | 16, 17, 18 | `timescaledb` (Apache-only 2.31.0) | Matching smoke-qualified `timescaledb-pg<major>` locked native artifact |
| `supabase` | 17 | Existing Supabase curated plan | Implied exclusively by the `supabase` provider; not selectable on plain PostgreSQL |

The running provider description is the discovery authority:

```sh
karactl providers get postgres -o json
```

`engineOptions.profiles.items` reports each profile's `majors`, available extension
metadata and availability/rejection reason per major. The creation wizard uses this
metadata and disables unavailable choices. Admission rejects unknown profiles,
unsupported majors, missing locked artifacts and artifacts without a resolvable URL.
A source recipe or an artifact mirror cannot make an absent lock entry available.
The lock includes qualified TimescaleDB artifacts for majors 16–18. Development
deployments still require a mirror serving those exact archived bytes; release
deployments require the matching release assets. Missing locks or unresolved URLs
are rejected rather than provisioning a server that cannot load the extension.

### Package provenance

The package names and pins are recorded in `internal/engine/postgres/pgprofile`.
PostGIS uses `percona-postgresql-<major>-postgis-3` and matching
`percona-postgresql-<major>-postgis-3-scripts`: `2:3.5.7-2.noble` on 16/17,
`2:3.5.7-3.noble` on 18. pgvector uses
`percona-postgresql-<major>-pgvector='1:0.8.6-1.noble'` on all three majors.
These were resolved from the Percona Noble amd64 indexes:
[16](https://repo.percona.com/ppg-16/apt/dists/noble/main/binary-amd64/Packages.gz),
[17](https://repo.percona.com/ppg-17/apt/dists/noble/main/binary-amd64/Packages.gz),
[18](https://repo.percona.com/ppg-18/apt/dists/noble/main/binary-amd64/Packages.gz).
Supabase keeps its historical PG17 package pins and command ordering unchanged.
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
migration receipts. Physical backups snapshot the normalized selected profile set in
`postgres.profiles`, including coordinated backups and scheduled recovery points.
Restore and clone targets must select a superset before provisioning or destructive
recovery work: a backup containing PostGIS/vector objects also needs their runtime
libraries. Requirements come from the backup, not the source's current selection,
and survive source deletion. Legacy backups without this metadata require no
profiles; existing Supabase provider compatibility remains unchanged.

Profiles make their extension set available to [native database creation and bindings](access-and-tls.md).
A profile creates its extensions in `postgres`, not in `template1`; other databases
request their own extensions explicitly. After minor and major upgrades, selected
profile extensions are updated with `ALTER EXTENSION ... UPDATE` in every
connectable database where installed. Major-upgrade admission requires the selected
profiles to be available for the target major, and preparation includes both ABIs.
Readiness remains false while post-upgrade extension updates are pending; the
`ExtensionsUpdated=False` condition reports `ExtensionUpdatePending` until a retry
completes successfully.

Non-Supabase profile bootstrap and per-database extension creation retain the
management connection's pinned `search_path = pg_catalog`. Extensions without a
control-file schema are explicitly installed in `public`; fixed-schema extensions
use their declared namespace (for example, `postgis_topology` uses `topology`).
Per-database creation reads the default version's schema from PostgreSQL's extension
catalogue, so it also honors fixed schemas for extensions outside the curated profiles.

## Supabase compatibility

The `supabase` provider composes the same PostgreSQL core with the registry's
Supabase profile and its application components. It remains provider `supabase`,
wire flavour `supabase`, PostgreSQL major 17, with the same packages, runtime
preparation commands, selected-preload order, curated SQL bytes, advisory lock and
`_karakoram.supabase_migrations` ledger. Existing specs gain no explicit profile field.
Golden fixtures protect data/proxy engine bytes, prepare slices, bootstrap plans and
digests, application restart tokens and the core spec digest.

## TimescaleDB edition

The descriptor requires a major-specific Apache-only TimescaleDB artifact, preloads
`timescaledb` and forces `timescaledb.telemetry_level=off`. No Timescale Community/TSL
modules are supplied by this profile. Compression policies, continuous-aggregate
refresh policies and other TSL features are unavailable. The
[native artifact lock](../native-artifacts.md) records real per-major archives
qualified by extension creation, Apache license mode and a hypertable insert/query.
Availability still requires a resolvable mirror or release URL for those bytes.

See [ADR 0044](../adr/0044-postgres-profiles.md) for the design contract.
