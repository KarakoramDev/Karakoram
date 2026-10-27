# Control-plane backup and rebuild

Control-plane backups preserve [SQLite](https://sqlite.org/) coordinator state, not database-engine data. Continue using [database backups](backups.md) for application data. The control plane remains a single-replica service.

This Ops feature is **verified offline** (tests without a live provider). A successful state snapshot does not establish engine-data recovery or a provider rebuild recovery-time guarantee.

## Policy and runs

An administrator with `settings:read` can inspect policy and runs; changing policy and starting a run require `settings:update`.

```sh
karactl control-plane backup-policy set --enabled --storage production/state-archive \
  --schedule '0 3 * * *' --retain-count 14
karactl control-plane backup-policy get -o yaml
karactl control-plane backups run -o json
karactl control-plane backups list --limit 100 -o yaml
```

`--storage` accepts `[namespace/]name`; without a namespace it uses the selected namespace, or `default`. The storage must already exist and be [S3](https://aws.amazon.com/s3/)-compatible; verification is recommended but not required. A policy-referenced storage cannot be deleted. `set` replaces policy, rather than patching it; defaults are disabled, `0 3 * * *`, and 14 retained successful backups. Supply all desired fields, or use an API-shaped `-f` file. Retention is 1–1000.

Schedules use five-field UTC cron with at most one catch-up. Manual runs use the same controller, storage policy and retention as scheduled runs. Only one backup executes at a time. Records progress through Pending, Running, Succeeded or Failed. Inspect the completed record: acceptance of `run` is not evidence of a usable backup. Failures emit `ControlPlaneBackupFailed` warning events.

A run interrupted while Running is marked Failed after control-plane restart, without automatic retry. Review the record and start a new manual run when needed.

## Object layout and custody

Objects are stored under:

```text
<prefix>/karakoram-control-plane/<installationId>/<UTC timestamp>-<id>.db.gz
<prefix>/karakoram-control-plane/<installationId>/<UTC timestamp>-<id>.json
```

The timestamp is `YYYYMMDDTHHMMSSZ`. The JSON manifest contains `format: karakoram-state/v1`, `installationId`, `createdAt`, `karakoramVersion`, `commit`, `schemaVersion` (last migration filename), `keyFingerprint`, `sha256` and `sizeBytes`. SHA-256 and size cover the compressed archive. Retention removes archive/manifest pairs only for successful runs belonging to this installation, oldest first beyond the configured count.

A snapshot includes desired state, inventory, operation journals, accounts, policies, installation identity, sealed secrets and PKI. Secrets and private keys remain sealed using the original master key. **The master key is not in the backup.** Preserve `KARAKORAM_MASTER_KEY` independently, securely, and test recovery. The fingerprint detects a wrong key; it is not a substitute for a strong master key or trusted backup custody. Hashes detect corruption, not malicious replacement of both archive and manifest.

SQLite `VACUUM INTO` snapshots every table, including `postgres_bindings` and its source-incarnation/dependency metadata, not a hand-maintained table allowlist. Desired options, internal lifecycle journals and sealed cluster secrets for all registered engines are included, including [Keycloak](https://www.keycloak.org/), [OpenBao](https://openbao.org/), [Temporal](https://temporal.io/), [MySQL](https://www.oracle.com/mysql/), [PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster), [Neo4j](https://neo4j.com/) and [Weaviate](https://weaviate.io/). These records do not include those engines' external data or volumes.

The snapshot includes API-token hashes and immutable restriction metadata,
[OAuth](https://oauth.net/2/) grant/token hashes, and sealed Data [MCP](https://modelcontextprotocol.io/) companion state/configuration.
Raw API-token bearer secrets are not stored; Data MCP credentials stay in their
existing sealed cluster-secret records. Snapshots are complete installation
recovery artifacts, not restricted-token exports: protect backup storage and
local files as installation-wide sensitive material. Restoring an older snapshot
also restores its token revocation/expiry state; review and revoke stale tokens
before reopening access. Export and restore are local administrator commands,
not remotely exposed MCP tools; offline replacement requires explicit `--replace`.

The backup does **not** include engine volumes/data, binaries, deployment definitions, environment-supplied provider tokens, endpoint/public/advertise configuration or the master key. Independently preserve required environment and provider configuration. A sealed namespace credential stored in SQLite is restored, but that does not replace environment-supplied provider credentials.

## Local export and offline restore

Use the same data-directory and master-key environment as the server. Do not put keys in shell arguments or logs.

```sh
karakoram admin backup-state --out /secure/state.db.gz
# Produces /secure/state.db.gz and /secure/state.db.gz.json, both mode 0600.
```

Local export works while the server runs: it authenticates the supplied master key against persisted encrypted state before snapshotting or publishing a fingerprint, then uses a transactionally consistent WAL-safe SQLite snapshot and checks integrity. A fresh database with no encrypted state can be exported with its configured key. Preserve both files together.

Stop the control-plane service and disable automatic restarts before restoring. Set `KARAKORAM_DATA_DIR` to the destination and supply the original `KARAKORAM_MASTER_KEY` through your secret manager. Restore before first normal startup:

```sh
karakoram admin restore-state --file /secure/state.db.gz
```

For S3, use the archive object key, not the manifest key:

```sh
# Inject KARAKORAM_RESTORE_S3_ACCESS_KEY_ID and
# KARAKORAM_RESTORE_S3_SECRET_ACCESS_KEY securely into the environment.
karakoram admin restore-state --s3-endpoint https://s3.example.com \
  --bucket state-backups --key prefix/karakoram-control-plane/INSTALLATION/TIMESTAMP-ID.db.gz \
  --region us-east-1 --path-style
```

Only those two restore-specific environment variables supply S3 credentials. `--region` and `--path-style` are optional. Restore fetches the sibling `.json` object (replacing `.db.gz`), using the same guarded endpoint policy as backup storage: no redirects/proxy bypass, TLS verification, forbidden metadata/loopback destinations, and `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` for permitted private endpoints. Do not weaken endpoint safety to recover an untrusted archive.

Restore verifies format, compressed size/hash, gzip, SQLite integrity, schema compatibility and matching key fingerprint before publishing. A mismatch reports both fingerprints and requires the original master key. A binary cannot restore a snapshot from a newer schema; use a compatible or newer release.

A running/open database is refused. An existing database is refused unless `--replace` is given. With replacement, SQLite first checkpoints the old database into a standalone main file and removes its WAL/SHM sidecars; failure to do so refuses replacement. A hard link preserves the old database as `karakoram.db.replaced-YYYYMMDDTHHMMSSZ`, and that backup directory entry is fsynced before publication. The authoritative `karakoram.db` name is never removed: a private, validated and fsynced temporary file atomically replaces it, followed by directory fsync. Interruption before publication leaves the old authoritative database, not an absent path that could initialize empty state. Keep replacement files until recovery is verified. Local archive and manifest are individually atomically published; if export is interrupted, do not use a mismatched pair.

## Provider rebuild runbooks

For every provider: isolate the old coordinator, retain engine services/volumes, redeploy only the control plane, restore state **before its first start**, then start exactly one coordinator with the same master key. Avoid having old and restored coordinators reconcile simultaneously. Preserve the original endpoint/advertise configuration and connectivity to members, object storage and provider APIs. Review readiness, events, operations and credentials after startup before enabling routine changes.

### Zerops

1. Stop or isolate the old control-plane service; do not delete database member services or their storage.
2. Redeploy the control-plane service with a persistent data mount and a compatible binary. Keep normal startup disabled while preparing recovery.
3. Restore the original master key, [Zerops](https://zerops.io/) provider token/project configuration and public/advertised endpoints from independent secret/configuration custody. These environment settings are not recovered by restoring SQLite.
4. Run offline restore against the mounted data directory, then enable normal startup. Check namespace connectivity and existing member trust before accepting mutations.

### Nomad

1. Stop the old control-plane allocation/job without purging member jobs or CSI volumes.
2. Redeploy the control-plane job with one allocation, the intended persistent state volume and compatible binary; use a one-shot recovery task or an allocation shell before enabling the server task.
3. Reinstall the same master key and the independently retained [Nomad](https://developer.hashicorp.com/nomad) endpoint, namespace/region, ACL token, TLS and advertised-address configuration; see [Nomad](nomad.md).
4. Restore on the mounted volume, remove the recovery override and start one server allocation. Verify provider access and member connectivity before resuming automated work.

### Dokploy

1. Stop the old control-plane application/container and prevent automatic restart; retain database applications/services and volumes.
2. Redeploy only the control-plane application with its persistent data volume and compatible binary, temporarily overriding normal server startup for recovery.
3. Reapply the original master key and independently saved [Dokploy](https://dokploy.com/) API token, endpoint and deployment/public-address configuration; see [Dokploy](dokploy.md).
4. Run offline restore on the mounted state directory, restore the normal command, then start one replica. Review application ownership and member reachability before permitting changes.

## Stale snapshots

Restoring old state does not discover resources created after the snapshot: those resources become orphans and need operator inventory/reconciliation. Do not blindly recreate or delete them. In-flight operations can resume from their journals or fail because infrastructure changed since the snapshot. Review pending/running/failed work, provider resources and events before retrying mutations. Restoring state does not roll engine data back to snapshot time; perform engine-specific data recovery separately when required.
