# 0046 — Citus distributed PostgreSQL topology

Status: Accepted

## Context

Some workloads outgrow one [PostgreSQL](https://www.postgresql.org) writer: multi-tenant SaaS, very large tables, real-time analytics. [Citus](https://www.citusdata.com) distributes tables across PostgreSQL nodes while keeping the PostgreSQL protocol and extension ecosystem, and [Patroni](https://patroni.readthedocs.io) 3+ manages Citus formations natively. Neither [Percona](https://www.percona.com) nor the PostgreSQL community repositories for [Ubuntu](https://ubuntu.com) 24.04 ship Citus for [Percona Server for PostgreSQL](https://docs.percona.com/postgresql/), and the [existing HA design](0034-postgres-engine.md) assumes one writer, one Patroni scope with one leader key, one [etcd](https://etcd.io) ensemble per member set and one [pgBackRest](https://pgbackrest.org) stanza.

## Decision

### Topology

The `postgres` provider gains topology `distributed`: one **coordinator group** (Citus group 0) and 1–8 **worker groups** (groups 1…N), each a Patroni HA set. `spec.replicas` sets the members per group, uniformly `1` (no HA, development) or `3` (HA). `engineOptions.citus` holds `workerGroups` (default 2) and the distributed `database` (default `app`). Members are named `<cluster>p<index>` in the coordinator group and `<cluster>w<group>p<index>` in worker groups; indexes stay monotonic per group. Distributed clusters are created as such; conversion from `single`/`ha` is not offered.

Every data member installs the `citus-pg<major>` [native artifact](0036-native-artifacts.md), built by Karakoram from **unmodified** upstream source against the matching Percona server headers. Admission requires it in the lock. The topology forces `shared_preload_libraries` to start with `citus` and `wal_level=logical` (online shard moves use logical replication). Supported majors are the intersection of the Percona catalog and the selected Citus release (Citus 14.2: PostgreSQL 16, 17 and 18). A native proof built unmodified Citus 14.2.0 against Percona Server for PostgreSQL 17.11.1 headers, loaded it alongside `pg_tde` (Citus must be listed first) and exercised distribution, rebalancing, restore points and Patroni-managed worker failover.

### Coordination

All groups share one Patroni scope `<cluster>-<uid>` under namespace `/karakoram/` with Patroni's `citus: {group, database}` setting, so keys become `/karakoram/<scope>/<group>/…`. The **formation DCS** is the etcd ensemble co-located on the coordinator members (1 or 3 voters); worker members run no etcd and use it as clients over TLS with client certificates. Losing coordinator etcd quorum makes every group fail closed, which matches the fact that the coordinator is the formation's only client entry point. Superuser, replication and REST credentials and the cluster PKI are formation-wide, as Patroni's Citus integration requires.

Worker quorum probes divide their remaining deadline among the coordinator endpoints. A blackholed first endpoint therefore cannot consume the entire two-second status probe budget and hide healthy later voters.

Patroni owns the Citus metadata: it creates the database and extension, registers the coordinator and each worker group's primary in `pg_dist_node`, and updates them on failover. Karakoram observes the formation (one leader per group, `pg_dist_node` matching observed leaders) and reports drift, but never writes `pg_dist_node` while Patroni manages it, except for explicit worker-group drain and removal.

Replication, failover, replacement, rolling configuration and minor upgrades reuse the existing HA algorithms through a per-group view: leader selection, switchover, learner/voter handling and retirement are evaluated within one group, and formation-wide maintenance orders worker groups before the coordinator. DCS scope resets delete only a fenced group's subtree; a whole-formation reset requires every group fenced.

### Client access

The [ProxySQL](https://proxysql.com) `proxy` component publishes only coordinator members; its native read-only monitoring routes writes to the coordinator primary. Worker members are reachable only on the private network. Applications distribute tables with Citus SQL (`create_distributed_table`, `create_reference_table`). Native access API actions run on the coordinator in the distributed database and are propagated by Citus to workers; roles are created with Citus' DDL propagation enabled.

### Scaling and rebalancing

Increasing `workerGroups` provisions new groups; Patroni registers them, and they hold no shards until a rebalance. **Rebalance** is an imperative operation (`POST /clusters/{cluster}/actions/rebalance`, optional strategy): it starts `citus_rebalance_start()` on the coordinator primary, records the native job ID in its checkpoint before polling `citus_rebalance_status()`, and on retry inspects the recorded job rather than starting another. Cancellation calls `citus_rebalance_stop()`. Decreasing `workerGroups` removes the highest groups only after `citus_drain_node` has moved their shards away and `citus_remove_node` succeeded; then their runtimes and volumes retire.

The per-group count `components.engine.replicas` is chosen at creation and cannot change for an existing distributed cluster. Admission rejects both 1→3 and 3→1 with an invalid-request error because group-local replica-count reconciliation is not implemented. This restriction does not prevent scaling `engineOptions.citus.workerGroups`. The real PostgreSQL provider description advertises the `rebalance` operation; API admission still restricts it to distributed PostgreSQL clusters.

### Backup and recovery

Each group has its own pgBackRest stanza and repository path `<prefix>/postgres/<namespace>/<cluster>-<uid>/g<group>`, written from that group's primary. A **coordinated backup** freezes formation membership, takes the requested backup type in every group, then — strictly after all base backups — calls `citus_create_restore_point('<name>')` on the coordinator (which blocks distributed writes briefly and writes the named point on every node), forces a WAL switch on each group primary, waits until every group's archive covers its restore-point location and only then records one backup with a per-group manifest (labels, system IDs, timelines, restore-point name and LSNs).

Retiring a worker group advances its persisted repository incarnation. A recreated
group writes to `g<group>-i<incarnation>` rather than reusing the retired system
identifier's stanza; incarnation zero retains the original `g<group>` path.
Retained manifests keep their original repository paths. Scheduled restore
points require bases matching the current group set and repository incarnations.
If a coordinated backup becomes invalid after promotion, recovery observes its
outstanding native receipts without starting new backups, records terminal
failure, and releases the formation fence only once those receipts are terminal.

Archive coverage resolves the requested LSN to its exact current-timeline WAL segment. A local PostgreSQL `.done` receipt proves archival immediately; if WAL recycling removed that receipt, the agent uses synchronous pgBackRest `archive-get` against the configured group repository into a private temporary destination, with a ten-second timeout capped by the request deadline. A missing segment remains pending; repository failures fail closed. The probe disables asynchronous spool handling and command logging, never supplies credentials in command arguments, and removes the temporary fetch on completion. Timestamps and later segments are not evidence for this barrier.

**PITR** for distributed clusters means recovery to a coordinated restore point. When PITR is enabled, the control plane also creates named restore points on a schedule (default every 15 minutes) and lists each one whose location is archived in every group. Arbitrary timestamps are not offered because independent group timelines do not establish a cluster-consistent state between restore points.

The public backup inventory exposes the coordinated target as
`postgres.restorePoint` and its replication-group manifests as `postgres.groups`.
Restore requests select it with `source.restorePoint`; selecting the aggregate
backup by `source.backup` also restores to that backup's coordinated named point.
Each group carries its own repository, pgBackRest label, system identifier and
archive-fence LSN. Citus returns the coordinator's exact marker location; sampled
worker positions are conservative **post-marker upper bounds** used only to
prove archive coverage, never substituted for the coordinator's position.
Scheduled quarter-hour targets reuse already-completed group bases and become
successful inventory entries only after every group's archive barrier passes.

In-place restore fences the whole formation and holds the proxy closed, restores each group with `--type=name --target=<point>` from that group's backup set, rebuilds replicas per group, verifies `pg_dist_node` and releases client access only after every group is ready. Clones must have the same number of worker groups as the backup. Major upgrades of distributed clusters are not offered in this version: Citus requires coordinated `citus_prepare_pg_upgrade`/`citus_finish_pg_upgrade` around per-node `pg_upgrade`; minor upgrades roll per group.

### License

Citus is licensed under the AGPL-3.0. Karakoram's Apache-2.0 code does not link it: the artifact is downloaded separately onto data members, built from an unmodified upstream tag whose repository and commit are recorded in the lock and packaged license files. If Karakoram ever patches Citus, it publishes the patched source with the release. Operators offering a modified Citus over a network carry the AGPL obligations themselves.

## Consequences

Distributed clusters reuse Patroni, pgBackRest, ProxySQL and the PKI. A formation needs `(N+1) × replicas` data members and coordinator voters are a shared dependency. Distribution choices (distribution columns, reference tables, co-location) remain the application's. Cluster-consistent recovery is only as fine-grained as the restore-point schedule. Until the `citus-pg<major>` artifact is qualified and locked, the topology is rejected at admission.

## Alternatives considered

- **Independent HA clusters as groups:** Patroni's Citus integration needs one scope, one DCS tree and shared credentials; separate clusters break discovery and failover registration.
- **etcd on every member:** turns `(N+1)×3` members into voters; quorum grows with workers.
- **Karakoram-managed `pg_dist_node`:** competes with Patroni during failover.
- **Arbitrary-timestamp distributed PITR:** not consistent across groups without coordinated markers.
- **Restore point before base backups:** a backup cannot recover backwards to an earlier marker.
- **[YugabyteDB](https://www.yugabyte.com):** dropped from the roadmap in favour of Citus on real PostgreSQL.
- **Community Citus packages:** built against community PostgreSQL, not Percona's server.

## References

[Distributed guide](../postgres/distributed.md), [Patroni Citus support](https://patroni.readthedocs.io/en/latest/citus.html), [Citus UDF reference](https://docs.citusdata.com/en/stable/develop/api_udf.html), [pgBackRest restore](https://pgbackrest.org/command.html#command-restore), [profiles](0044-postgres-profiles.md), [native artifacts](0036-native-artifacts.md).
