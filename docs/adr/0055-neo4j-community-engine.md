# 0055 — Neo4j Community singleton with offline dumps

Status: Accepted

## Context

Neo4j adds a graph database with the Cypher query language. Neo4j Community Edition is GPLv3 and runs as one JVM over a local store. Clustering, online backup, role-based access control and the Prometheus metrics endpoint are Enterprise-only features under a commercial licence. The only Community recovery tool is `neo4j-admin database dump`/`load`, which requires the database to be stopped. A Community engine is therefore useful only if its availability and recovery limits are explicit and the runtime still meets the project's self-hosting, TLS and recovery standards.

## Decision

### Scope and runtime

The `neo4j` provider offers Neo4j Community only, topology `single`, exactly one engine replica. Requests for more replicas, other topologies or components are rejected. Enterprise features, including bring-your-own-licence Enterprise, are out of scope.

Members install the official `neo4j-community-<version>-unix.tar.gz` and a pinned Eclipse Temurin 21 JRE through the native artifact lock (ADR 0036), each into its own immutable directory under `/opt/karakoram`. Offered versions are 2026.09.0 and 2026.10.0 (default). Neo4j 2025.01 and newer require Java 21.

The agent supervises the server JVM directly. It derives the exact server command from `bin/neo4j console --dry-run` for the rendered configuration and caches it per version, memory and settings; the launcher's bootloader JVM (about 190 MB resident) is not kept running. Data, transaction logs, configuration, certificates, logs and run files live below the member volume's data directory. A graceful stop uses SIGTERM; checkpointing can take tens of seconds.

### Memory

The provider derives JVM heap and page cache from the engine component's minimum RAM: heap 40 % and page cache 25 % of the minimum, rounded down to 64 MiB, with heap between 256 MiB and 31 GiB (compressed object pointers) and page cache at least 64 MiB. The remainder serves Lucene, Netty, native JVM memory, the operating system and the agent. `engineOptions.heapMB` and `engineOptions.pageCacheMB` override the defaults when their sum stays within 80 % of the minimum RAM. The minimum is 1 GiB; 2 GiB is the default. Changes take effect through a restart, which is a full outage.

### Transport security and hardening

Bolt (7687) and HTTPS (7473) always use TLS from the cluster CA; Bolt requires TLS and plain HTTP is disabled. The leaf certificate covers the member host names, localhost, loopback and public endpoints. TLS rotation reissues the leaf and restarts the server. Public exposure publishes Bolt only; the HTTPS Query API and browser stay on the private network.

The managed configuration disables usage reporting, fleet discovery broadcasts and Fleet Manager, X-Forwarded header processing and file URL `LOAD CSV`, and enables strict configuration validation. User settings are limited to an allowlist and cannot override managed keys.

The supported settings are `db.transaction.timeout`, `db.transaction.concurrent.maximum`, `db.lock.acquisition.timeout`, `db.logs.query.enabled`, `db.logs.query.threshold`, `db.query.default_language` and `dbms.cypher.planner`. Values are single-line strings. Other keys are rejected, including managed paths, connectors, TLS, authentication and JVM settings. Generic `engineConfig` text is not accepted.

### Users

The managed administrator `neo4j` receives its password through the loopback-only credential recovery start described under restore, before the server first listens on the network; `neo4j-admin dbms set-initial-password` is not used because it takes the password as a command-line argument. The password is sealed in the secret store and returned only through the credential reveal API. Rotation is an operation: the agent changes the password idempotently (accepting a password that already authenticates), then the control plane commits it. A lost or drifted administrator password is repaired the same loopback-only way.

The repair entry point is rotation of the `neo4j` system user. Only explicit rejection of both the current and staged next password authorizes the offline recovery job; transport and other server failures do not. The control plane polls an operation-derived receipt, commits the staged password only after recovery succeeds, and records that the repair caused a database outage.

A native API manages application users (list, create, change password, delete). Community has no role-based access control, so every user is an administrator with full read, write and administrative rights; the API and documentation say so. Passwords are returned once on creation and never stored.

### Backups, restore and clone

The only backup type is `dump`. A backup is offline: the agent places a durable stop hold, stops the server, dumps `system` (users) and `neo4j` with `neo4j-admin database dump --to-stdout`, streams each dump to S3 with a multipart upload while hashing it, writes a manifest last, starts the server and releases the hold. The database is unavailable for the whole backup. A backup prefix without a manifest is incomplete and never restorable. The agent runs the job in the background under a receipt keyed by the operation; the control plane polls the same receipt. An interrupted job is reported failed after the server is started again; it is never resumed blindly.

Scheduled backups are rejected unless `engineOptions.acceptBackupDowntime` is `true`, because the scheduler has no maintenance-window concept and every scheduled run is an outage.

Restore and clone load a manifest's dumps offline. The agent verifies size and SHA-256 while fetching, loads into a staging data directory, and swaps it in only after every load succeeded. Because the `system` dump carries the source's users and passwords, the agent then starts Neo4j once with authentication disabled and every connector bound to `127.0.0.1` on separate recovery ports, sets the target's managed administrator password, stops and starts normally. Source application users are preserved. A backup from a newer Neo4j version than the target is rejected; older dumps are upgraded on start.

The control plane's durable restore-maintenance marker excludes ordinary convergence only; it does not disable the member's desired Neo4j process or set member maintenance. The agent job's durable stop hold alone owns the offline stop/start window, so configuration pulls and member recovery cannot prevent the job from restarting Neo4j before the control plane verifies readiness.

The member stores owner-only, fsync-and-rename job receipts below `karakoram/neo4j/receipts`. Receipts contain a request digest, never object-store credentials or administrator passwords. Reusing a receipt with another request is rejected; polling does not restart work. Restore journals both directory renames and retains the previous store until credential recovery and server launch succeed. An interrupted uncommitted swap rolls back before the server is released; a committed swap retains the replacement. Interrupted operations fail explicitly and require a new operation identifier.

A failed stop-hold release remains a running, recovery-pending receipt rather than a terminal failure. The member retries release with bounded backoff before publishing the job outcome; configuration reconciliation and agent restart retry any remaining pending release before accepting another offline job. If terminal receipt publication fails after its rename, reconciliation retries the parent-directory sync before clearing the matching admission latch.

### Upgrades

A version change is a spec update. It requires a fresh successful backup, installs the new artifacts, then restarts the server on the new version, which upgrades the store and system database in place. Downgrades are rejected.

### Monitoring

Community has no metrics endpoint. The agent reports availability, heap usage from the JVM memory bean (`dbms.queryJmx`), configured heap and page cache, node and relationship counts, active transactions and the on-disk store size, alongside the generic process-tree CPU and memory and data-volume metrics.

## Consequences

- Every backup, restart, TLS rotation, memory change and upgrade is a full outage; there is no Community high availability.
- Dumps are full copies; there is no point-in-time recovery and no incremental backup.
- A dump-time lock failure (another process holding the store) fails the backup instead of copying a live store.
- The same-name `neo4j` database is the only user database; `CREATE DATABASE` is Enterprise-only.
- Application users are all administrators; network allowlists and credential hygiene are the only access boundaries.

## Alternatives considered

- **Neo4j Enterprise or clustering:** commercial licence; current Enterprise source is not published.
- **Online file copy or filesystem snapshot:** unsupported by Neo4j for a running store and not portable across backends.
- **Restoring only the `neo4j` database and recreating users:** the control plane does not store application passwords, so users would be lost.
- **Running the `neo4j console` launcher:** keeps an extra JVM resident for the life of the server.
- **Control-plane transfer of dump bytes:** would route full database copies through the control plane; the member streams directly to object storage instead.
