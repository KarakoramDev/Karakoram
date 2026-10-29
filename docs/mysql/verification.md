<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# MySQL and PXC verification

[Overview](README.md) · [Operations](operations.md) · [General verification](../verification.md)

These are procedures, not a claim that a checkout or deployment passed. Missing prerequisites and skipped tests mean **unverified**, not successful. Use disposable data and storage, no production credentials, and record redacted terminal results and recovered contents.

The ordinary engine package test
`TestBackupOperationAdmitsAgentParsedCheckpoints` passes on-disk native full and
incremental checkpoint fixtures through the production agent metadata parser,
then through control-plane backup dispatch, polling and successful inventory
admission. It requires no native binaries. Native agent-only recovery cases below
exercise real subprocesses and storage, but do not themselves prove control-plane
receipt admission; retain both layers of coverage.

## Native prerequisites

Use checksum-verified native [Percona](https://www.percona.com/) artifacts compatible with the machine and all required runtime libraries. No [Docker](https://www.docker.com/), privileged package installation or global service changes are needed for these cases. Extract prerequisites into locations selected by your environment; do not assume a checkout-relative cache or a particular workstation filesystem.

| Variable | Input |
| --- | --- |
| `KARAKORAM_MYSQL_BIN_DIR` | Absolute `bin` directory of the selected Percona Server artifact; select PXC's directory for Galera cases |
| `KARAKORAM_MYSQL_XTRABACKUP_DIR` | Absolute root of the compatible XtraBackup artifact for physical backup cases |
| `KARAKORAM_PROXYSQL_BIN` | Absolute native ProxySQL executable for routing/failover cases |
| `KARAKORAM_MYSQL_MINIO_BIN` | Absolute native MinIO executable; required by the physical backup/PITR integration case |
| `KARAKORAM_MYSQL_TEST_TMPDIR` | Optional short, writable disposable scratch root; defaults to the system temporary directory |
| `KARAKORAM_MYSQL_LD_LIBRARY_PATH` | Optional extracted runtime-library search path |

Use a short scratch root because Unix socket names have length limits. [PXC](https://docs.percona.com/percona-xtradb-cluster/) SST additionally needs its bundled compatible [XtraBackup](https://docs.percona.com/percona-xtrabackup/) tools and native `socat`, `openssl` and shell utilities. Keep upstream SST scripts intact. Tests allocate ephemeral loopback listeners; do not configure a shared fixed database port or run against an existing database. Serialize resource-intensive suites. Follow the checkout's Go toolchain selection (via mise where configured) and the environment's resource-limiting wrapper if required.

Native subprocesses also retain the caller's `TMPDIR`; the runner checks the
effective server `tmpdir`. Select an owned scratch directory when shared
temporary storage is unsuitable; the short socket scratch root above can be
chosen independently.

Require environment inputs explicitly in the shell rather than silently selecting local binaries:

```sh
: "${KARAKORAM_MYSQL_BIN_DIR:?Select the native server bin directory}"
: "${KARAKORAM_PROXYSQL_BIN:?Select the native ProxySQL executable}"
# Set a short disposable root only if the system temporary directory is unsuitable.
# KARAKORAM_MYSQL_TEST_TMPDIR is otherwise optional.

go test -tags integration -count=1 -v -timeout 20m ./internal/agent/mysql \
  -run '^(TestNativeMySQLAgent|TestNativeMySQLAccess|TestNativeGroupRunner|TestNativeMySQLGroupFailover)$'
```

The raw group proof waits through native shutdown/transport transients by replaying only the same durable clone intent. Every retry requires a matching request and native baseline already on disk; it cannot issue a new CLONE after a lost reply. The injected restart interruption must still be observed explicitly, and production readiness/process-exit fences remain enforced.

With [Percona Server](https://docs.percona.com/percona-server/) and compatible XtraBackup selected, physical recovery additionally requires:

```sh
: "${KARAKORAM_MYSQL_XTRABACKUP_DIR:?Select compatible native XtraBackup}"
: "${KARAKORAM_MYSQL_MINIO_BIN:?Select a native MinIO executable}"
go test -tags integration -count=1 -v -timeout 45m ./internal/agent/mysql \
  -run '^TestNative(MySQLBackupRestorePITR|MySQLFreshFullBaselinePITR|GroupFreshFullBaselinePITR)$'
```

The physical recovery fixture obtains the source's `physicalIdentity` from the
status API and requires the full-backup receipt to retain it. Incrementals send
that receipt's identity together with its `toLSN`; a missing or changed source
incarnation is rejected rather than treated as a usable parent. The PXC full
backup/restore fixture checks the same status-to-receipt identity binding.
Backup HTTP failures expose only allowlisted, credential-free reason codes;
the native harness records the response reason before reporting a failure.
The fresh-full cases retain both a predecessor source's archives and the new
source's pre-full logs. Singleton and real three-member Group Replication
sources then restore the latest verified archive endpoint, an exact GTID
boundary and a same-second time boundary into independent targets. They require
the later rows and exact GTIDs without replaying retained pre-baseline schema
or data transactions. The original full/incremental, SHA256 and recovered-account
proof remains a separate case.

For [Galera](https://galeracluster.com/), export the real control-plane member configurations into a private
directory before running the separate native runner. The bridge invokes
production `Core.Sync`, sealed PKI and `MemberConfig`; it simulates infrastructure
and transport, not the security policy. It writes initial, formed, traffic-rotated
and 3→5 membership checkpoints with their actual credentials, so protect and
remove this directory after acceptance.

Run the following procedure separately for catalog versions `8.4.11-11.1` and
`8.0.46-38.1`, selecting that version's checksum-verified PXC artifact and bundled
compatible XtraBackup. Use a separate export directory for each version:

```sh
: "${KARAKORAM_PXC_VERSION:?Select the PXC catalog version}"
: "${KK_PXC_NATIVE_CONFIG_DIR:?Select a private disposable export directory}"
: "${KARAKORAM_MYSQL_BIN_DIR:?Select that PXC artifact's bin directory}"
: "${KARAKORAM_PROXYSQL_BIN:?Select the native ProxySQL executable}"
: "${KARAKORAM_MYSQL_XTRABACKUP_DIR:?Select compatible bundled XtraBackup}"
: "${KARAKORAM_MYSQL_MINIO_BIN:?Select a native MinIO executable}"
go test -count=1 ./internal/engine/mysql \
  -run '^TestPXCExportNativeMemberConfigs$'
go test -tags integration -count=1 -v -timeout 40m ./internal/agent/mysql \
  -run '^TestNativeGaleraRunner$'
```

The native bridge retains the exported member-specific SQL/agent leaves and
dedicated shared traffic trust. It remaps only native addresses, ports and paths;
recovery history and positions come from actual runtime actions/observations.
Missing exports skip the opt-in runner and do not constitute acceptance.

The Galera case covers live configuration reapplication, explicit and automatic
crash restarts, encrypted SST/IST, single-writer routing, quorum recovery, and a
production backup/restore into new cluster history followed by SST of both peers.
It records effective crash-durability/binlog settings and separately exercises
a gracefully stopped missing-write IST rejoin and two SIGKILL/missing-write IST
rejoins. After each rejoin, re-formation, restore and membership addition, the
same quorum GTID comparator used by production continuity admission checks the
executed histories. Any detected upstream GTID re-identification is reported
explicitly, not hidden by matching rows or a later SST; durable-latch and fresh
full-baseline transitions are covered by the control-plane regressions.
Destructive hostile-traffic cases run last, against the two members added by
3→5 scaling: an untrusted certificate is presented to each joiner's IST listener
during a legitimate IST. The runner requires the remote certificate alert,
unchanged quorum rows/GTIDs, and recovery of the attacked member to five Synced
members with equal rows, payload hashes and GTIDs: once after an operator stop
and start, once by the supervisor alone. Each attempt logs whether the receiver
aborted and whether upstream reset its state; only attempts with an observed
abort count as interrupted-transfer recovery evidence.
Unused [PostgreSQL](https://www.postgresql.org/) admin and frontend listeners are confined to Unix sockets in
the owner-only proxy directory (the pinned build rejects empty listeners). Native
tests inspect runtime variables and sockets, so no default PostgreSQL TCP port
is silently occupied by this [MySQL](https://www.mysql.com/)-only proxy.

Cleanup waits for owned runtime shutdown before removing disposable data and fails closed when termination cannot be proven. Contributor-level platform semantics are described in [native subprocess lifecycle](../verification.md#native-mysql-subprocess-lifecycle).

PXC encryption acceptance must use the control-plane-generated certificate shape: a dedicated traffic CA with a shared Galera/SST identity, separate SQL/agent trust and distinct per-member SQL leaves. A fixture that shares one SQL/server certificate across all members does not establish this contract. Exercise encrypted SST, subsequent IST and ordinary SQL access; wrong-CA Galera/IST and SST attempts, including attempts using SQL/agent leaves, must be rejected. SQL access must independently reject untrusted certificates. Confirm explicit channel TLS remains enabled with the automatic convenience flag off.

Verify traffic rotation replaces both the dedicated CA and shared leaf/key under full-stop/recovery, and that reformed members reject the old traffic credential. Check this outage separately from ordinary membership addition, which must not rotate traffic identity or incur an implicit TLS outage. Confirm SQL FILE import/export is disabled while native XtraBackup backup/recovery remains usable. These are required acceptance assertions, not a report that they passed.

Inspect the selected tests' assertions: access, physical backup/PITR and Galera runner are different cases. A standalone server or streamed object round-trip does not prove the complete API lifecycle, every PXC upgrade path or deployment-level recovery. Keep evidence for both server majors and each engine driver separately; do not transfer MySQL results to PXC just because they share access code.

## Deployment and browser acceptance

1. Discover both engine driver cards. Create MySQL single/group and PXC cluster using catalog versions. PXC must reject 1/2/4 members, absent proxies, Group Replication controls and engine driver conversion. Group must reject loss of its required proxy.
2. Inspect desktop and mobile wizard/review/detail layouts with administrator and restricted identities. Refresh nested MySQL access, backups, restores and upgrades routes. Unauthorized actions must remain inaccessible; readers must not gain mutation controls.
3. Create a scoped database/user/grant, connect using CA-verified TLS through the application endpoint, and verify denied plaintext/unauthorized access. Rotate with a retained password, confirm both during transition, discard old and confirm rejection. Check externally allowed and denied clients separately when exposing the endpoint.
4. Confirm expected replication eligibility and one routed writer. Write identifiable transactions, lose the writer, reconnect and check their outcomes without blind retry. Lose quorum and confirm minority traffic rejection/fencing; do not advertise a failover-time guarantee from one sample.
5. For PXC, confirm encrypted SST and subsequent IST when cache permits, compatible schema enforcement and flow-control visibility. For full-outage recovery, capture every recovered same-history position; deliberately missing or conflicting evidence must block bootstrap. Never perform destructive partition recovery without independently fencing missing members.
6. Verify [S3](https://aws.amazon.com/s3/) full/incremental provenance, chain-aware retention and checksum-verified archive continuity. Restore an exact same-second transaction boundary and show the target transaction present and later transaction absent. Introduce an archive gap in disposable data and confirm that the PITR endpoint does not jump past it.
7. Restore in place and to a new same-engine driver/major cluster. Confirm source isolation, new server UUIDs, preserved GTIDs and new Group Replication/Galera history. Confirm recovered rows, account/grant behavior and proxy authentication, including snapshot/rotation caveats. Cross-engine driver and cross-major restores must be rejected.
8. Exercise only offered upgrade paths, backup freshness gates, 3↔5 scaling, replacement, rolling settings and suspend/resume. Observe convergence and application contents after every operation. Inspect and remove disposable resources after capturing private evidence.

