# 0007 — One runtime and Local Storage volume per mongod

Status: Accepted

## Context

Runtime disks are wiped on deployment. MongoDB requires a persistent filesystem with locking/mmap semantics and only one writer per dbpath. Zerops Local Storage is suitable but single-host; SeaweedFS shared storage is unsafe for databases. Horizontal containers behind one service do not create replica-set membership.

## Decision

Create each data-bearing mongod, including config-server members, as its own `ubuntu@24.04` runtime service with `maxContainers: 1` and its own `local-storage:single@1` service mounted through `run.volume`. Each member gets a stable identity (ADR 0009). A sharded topology uses distinct shard/config-server members and one stateless mongos service that may have multiple containers and no data volume.

Import runtime/volume services without inline pipeline YAML. Create an app version, upload a self-contained tar.gz artifact containing the running control plane's executable (`os.Executable()`), and deploy it with the complete generated pipeline and matching runtime setup. Start `./karakoram agent`; do not require `buildFromGit`, a public source checkout, or a member-side Go build.

Install exact PSMDB/PBM packages through `run.prepareCommands`. Embed selected version values in the command text: Zerops caches the prepared runtime by that text, so changing environment variables alone does not refresh packages. Set `deploy.temporaryShutdown: true`; old mongod receives SIGTERM and exits before a replacement opens the same dbpath. Readiness durations are strings and retry periods are at least 10 seconds; engine readiness still requires MongoDB observation.

## Consequences

Every member can be independently resized, upgraded or replaced without sharing writable data. Service/volume count and deployment cost grow with topology size. Clean shutdown and preserved volumes are mandatory; a rolling deploy is ordered by the provider, not the platform. Distinct volumes/services do not guarantee host separation (ADR 0016). Runtime deletion and volume/backup retention remain separate decisions.

## Alternatives considered

- **Multiple mongod containers on one mounted volume:** risks concurrent writers and supplies no independent member identity.
- **SeaweedFS dbpaths:** inappropriate database filesystem semantics.
- **Public Git builds per member:** requires external source availability and build infrastructure instead of distributing the already-built artifact.
