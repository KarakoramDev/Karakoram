# 0044 — PostgreSQL profiles

Status: Accepted

## Context

The [PostgreSQL core](0034-postgres-engine.md) installs one fixed [Percona](https://www.percona.com) package set. [Supabase](https://supabase.com) ([ADR 0035](0035-supabase-engine.md)) extends it through a flavour hook that hard-codes extra packages, a native extension bundle, forced settings, preload rules and curated SQL. Common database workloads — geospatial, vector search and time series — need the same kind of extension bundle on plain PostgreSQL, and a second hard-coded flavour per workload would duplicate availability, preload and upgrade rules.

## Decision

### Profile model

A **profile** is a declarative, immutable descriptor in one registry (`internal/engine/postgres/pgprofile`) consumed by the PostgreSQL core, the Supabase provider and the member agent. A descriptor declares:

- identity and the PostgreSQL majors it supports;
- per-major Percona apt packages with exact version pins, and per-major [native artifacts](0036-native-artifacts.md) from the embedded lock;
- the extensions it makes available, their source (package or locked artifact), required preload and dependency order;
- forced configuration parameters and base `shared_preload_libraries`;
- a curated, immutable SQL plan applied on the writable primary under an advisory lock with a migration ledger;
- reserved roles and schemas protected from the native access API;
- upgrade rules: majors that require the profile's packages or artifacts on both sides of `pg_upgrade`, and `ALTER EXTENSION … UPDATE` after minor and major upgrades.

Admission is the availability authority: a profile is selectable only for a major whose packages are pinned and whose artifacts are present in the lock and resolvable. A recipe or source pin alone never makes a profile available.

### Profiles

| Profile | Extensions | Source | Preload | Notes |
|---|---|---|---|---|
| `supabase` | Supabase baseline (ADR 0035) | Percona packages + `supabase-pg17-extensions` | `pg_stat_statements`, qualified selections | Implied by the `supabase` provider; not selectable on `postgres` |
| `postgis` | `postgis`, `postgis_topology`, `postgis_raster` | Percona `percona-postgis` packages | none | majors 16–18 |
| `pgvector` | `vector` | Percona `percona-pgvector` packages | none | majors 16–18 |
| `timescaledb` | `timescaledb` | Karakoram-built `timescaledb-pg<major>` artifact, Apache-2 edition | `timescaledb` | telemetry off; TSL features absent |

The `supabase` provider becomes PostgreSQL core + the `supabase` profile + its application components. The cutover is byte-identical: persisted specs, the bootstrap digest and its plan, member engine specs, runtime preparation commands and native-artifact sets do not change, so existing clusters neither redeploy nor re-run migrations. The provider identity, wire flavour `supabase`, `_karakoram.supabase_migrations` ledger and advisory lock remain.

Plain PostgreSQL selects profiles with `engineOptions.profiles` (a set; absent means none, preserving existing specs). Profiles compose when their preloads and parameters do not conflict; preload order is the registry's declaration order. Profiles are additive: adding one installs packages/artifacts, applies parameters through the existing reload or rolling-restart rules and runs its SQL plan; removing one is rejected because extension objects may already exist in user databases.

### Extension creation

A profile creates its extensions in the `postgres` database. Databases created through the native access API or a [binding](0045-postgres-bindings.md) may request extensions from the cluster's available set; the agent creates them as superuser on the primary in that database. Extensions outside the available set are rejected at admission, not deferred to SQL errors.

### TimescaleDB edition

Karakoram builds [TimescaleDB](https://www.timescale.com) from unmodified upstream source with the Apache-only build option against the matching Percona server headers, one artifact per major. Only Apache-2.0 functionality ships; compression policies, continuous-aggregate refresh policies and other TSL features are unavailable and documented as such. Telemetry defaults to off.

## Consequences

Profiles reuse one HA, backup and upgrade implementation. Adding a workload becomes a registry entry plus pinned packages or a qualified artifact. A profile that needs a Karakoram-built artifact is unavailable until that artifact is qualified and locked; admission says so. Major upgrades require the profile's bits for both majors. Restored clusters keep profile extensions because they are part of the physical backup; the selected profile set must match the backup's.

## Alternatives considered

- **One flavour per workload:** duplicates availability, preload, SQL and upgrade rules.
- **Free-form `shared_preload_libraries` and package lists:** unqualified combinations, no availability authority, unbounded preparation.
- **PGDG extension packages:** collide with Percona packaging (observed in ADR 0035).
- **TimescaleDB Community/TSL build:** licensed under the Timescale License, not Apache-2.0.
- **Create profile extensions in `template1`:** silently changes every future database and complicates background workers; explicit per-database selection is clearer.

## References

[Profiles guide](../postgres/profiles.md), [PostgreSQL decision](0034-postgres-engine.md), [Supabase decision](0035-supabase-engine.md), [native artifacts](0036-native-artifacts.md), [TimescaleDB editions](https://docs.timescale.com/about/latest/timescaledb-editions/).
