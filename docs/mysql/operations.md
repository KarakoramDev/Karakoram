<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Operations and recovery

[Overview](README.md) · [Configuration](configuration.md) · [Backups](backups.md) · [Verification](verification.md)

## Observe before mutating

Use Overview, Members, Monitoring, Events and Operations together. Acceptance is not convergence: compare desired/observed generation, terminal operation state, member versions, replication eligibility and a real application query. A reachable listener alone is not a healthy writer. While suspended, observations are historical and endpoints are unavailable.

```sh
karactl -n NS clusters status CLUSTER -o json
karactl -n NS clusters members CLUSTER -o json
karactl -n NS operations cluster CLUSTER -o json
karactl -n NS events list --cluster CLUSTER -o json
```

For [MySQL](https://www.mysql.com/) groups, require `ONLINE` membership, one primary, read-only secondaries and the expected member count. The default `BEFORE_ON_PRIMARY_FAILOVER` policy waits for the new primary's backlog before it serves traffic. For [PXC](https://docs.percona.com/percona-xtradb-cluster/), require `Primary`, `Synced` and `wsrep_ready=ON`; `Primary` is [Galera](https://galeracluster.com/) component membership, **not** a promise that every node is the application writer. [ProxySQL](https://proxysql.com/) selects one routed writer. Donor/joining states and SST can temporarily reduce eligibility.

Before Group Replication bootstrap, a SQL-ready server may expose an `OFFLINE`
performance-schema row with an empty member ID. Absence of a row matching that
server's UUID is reported as `OFFLINE` with no member role, not as an unreachable
native status endpoint. This is a nonmember observation, not quorum readiness:
the control plane still requires its durable bootstrap epoch and observed
`ONLINE` primary before admitting the writer. SQL query failures remain errors.
Single-server and PXC readiness use their own existing native checks.

During initial provisioning, members not yet imported into the provider are
reported unavailable without a placement. This includes ProxySQL while it waits
for the first eligible writer. Such members are not a failed infrastructure
lookup; the cluster stays non-Ready until their runtime and native health are
observed. This applies to both MySQL and PXC.

Before a runtime is imported and deployed, observation does not dial its agent
or query Galera: its planned DNS name may not exist yet. PXC keeps these members
in the quorum comparison as unavailable, without advancing the GTID safe
watermark. Galera status probes use the bounded native observation timeout
(currently four seconds), not the entire reconciliation deadline.
PITR/archive status selects only joined data members, so enabling binary-log
archiving during creation does not resolve planned names either.
Initial imports still advance through durable per-member plan, submit and
completion checkpoints over successive reconciliations. A control-plane upgrade
can resume an otherwise healthy create stalled at these checkpoints; do not edit
internal state to bypass observation.

PXC deployment initially starts only the agent. All data runtimes must deploy
before the control plane publishes the seed's bootstrap epoch; only that seed
may start mysqld until it forms a Primary component. Remaining members then start
as SST/IST joiners. A runtime deployment is not permission to launch an
uninitialized seed as an ordinary joiner.

Galera's local bind address is distinct from its advertised member names.
For agents advertising `capability=mysql-galera-listen-host` on their
authenticated configuration pull, the control plane supplies a numeric IPv4
wildcard on IPv4-only Nomad/Dokploy layouts and an IPv6 wildcard
(`[::]:4567`) on Zerops's IPv6-capable layout. The capability applies even on the
first pull and only to that request. Older agents receive the unchanged legacy
schema without `listenHost`, avoiding their strict decoder's unknown-field
rejection. A fixed agent talking to an older control plane can still receive a
fieldless legacy configuration; its DNS-host fallback is IPv4-only, so full
Zerops IPv6 bind support requires both the fixed control plane and fixed agent.
The pinned Galera provider resolves a hostname listener but subsequently parses
that original host as a numeric outgoing source bind, so using a member DNS name
as `gmcast.listen_addr` can abort connection establishment with `Invalid argument`.
Peer discovery, advertised node/IST/SST identities and encrypted traffic remain
unchanged. The IPv6 wildcard's IPv4 acceptance follows the host kernel's
dual-stack policy; it is not a claim of an IPv4 listener on an IPv6-only host.

An older create may already have started the seed without bootstrap permission
and left uninitialized/foreign-looking data. A control-plane upgrade alone does
not authorize erasing or reinitializing that data. Preserve the logs, volume
contents and identity receipts and follow explicit recovery; disposable empty
test clusters can instead be fully deleted and freshly created after operator
authorization. Never relax the identity guard to make such a create pass.

Proxy provisioning waits for a native-online writer to complete its data-member
join checkpoint, so its first configuration contains an eligible backend.
A pending proxy deployment does not block that initial data join, including
when reconciliation resumes a proxy deployment accepted by an earlier version.

Monitor connections versus limits, running threads, query/network rates, replication queues/lag, archive lag and proxy provider state. No metric value or local failover experiment establishes a production recovery-time or zero-loss guarantee. Check placements independently: three members co-located in one failure domain do not tolerate losing that domain.

### PXC GTID history safety

`GTIDHistorySafe=True` means a freshly observed healthy, same-Galera-history
majority agrees on historical SID components, differs only by nested
current-state SID prefixes, and has no active continuity latch. Normal
active-write lag is accepted; equal counts, SID spelling and Galera sequence
numbers alone do not establish GTID equivalence. Restored historical namespaces
are legitimate when shared by the quorum; an unshared extra namespace is not
treated as ordinary replication lag.

`GTIDHistorySafe=False` / `GTIDHistoryDiscontinuityLatched` records a durable divergence
boundary, the detection time and affected members. The condition appears in the
cluster API and the overview's conditions table. A later restart, equal GTID
sets, SST or forced re-formation does not silently erase that observation.
`Unknown` / `QuorumComparisonUnavailable` means a safe majority comparison
cannot currently be established.
The safe watermark advances only when every configured member is successfully
observed eligible in the same component. An available majority with an unseen
member does not certify that unseen member's intervening history.

The guard does **not** stop database service or prohibit full backups. It blocks
operations that would claim GTID continuity across the unsafe interval:
incremental backup chains and PITR/binlog replay spanning it. An independently
valid backup and archived target before the recorded safe boundary remain
restorable. Do not equate the detection timestamp with the first unsafe
transaction: an IST can replay an older transaction under a different GTID.

Preserve logs, executed GTID sets and application evidence. Identify the trusted
quorum and use managed member replacement to provision fresh data from it;
verify both application contents and GTID history. Take a **new full backup**
after detection to establish a fresh PITR baseline. That explicit, successful
full backup is the operator acknowledgement: no flag, restart or status edit
clears the latch. Successful completion records the new baseline and clears the
active continuity latch; old chains retain their unsafe boundary. A failed,
cancelled or already-running old full backup cannot clear it. A new conflict
creates a new latch.

Both transitions are audited as cluster events: `GTIDHistoryDiverged`
(Warning; boundary, detection time and affected members) when the latch is
recorded, and `GTIDHistoryBaselineEstablished` (linked to the backup operation)
when a fresh full clears it. The upstream cause is tracked as
[PXC-4493](https://perconadev.atlassian.net/browse/PXC-4493).

Do not reset, normalize or inject GTIDs, force bootstrap as a GTID repair, or
attach an asynchronous replica across the affected history merely because rows
or later sets match. An inverse SID alone is not an error: compare the complete
history with quorum. See [backup continuity boundaries](backups.md#pxc-gtid-continuity).

## Restart, configuration and scale

- Dynamic allowlisted parameters can apply live. Startup-only parameters and resource changes use managed rolling maintenance. Keep configuration in the specification, not `SET PERSIST`.
- MySQL `single` restarts cause downtime. Groups maintain secondaries first, then move the primary and finish its maintenance. Require a majority throughout; applications reconnect after writer movement.
- PXC maintenance preserves a Primary component, waits for healthy Synced members and avoids intentionally removing a majority. Rejoining members use IST when available, SST otherwise. Do not continue a rolling change while state transfer or quorum health is unresolved. A member whose previous launch never reached Synced (an interrupted IST/SST) waits 130 seconds before relaunching so the previous donor's retries expire; see [hostile cluster traffic](access-and-tls.md#hostile-cluster-traffic). A member evicted by Galera for an inconsistency is not rejoined automatically; replace it.
- MySQL allows `single`→`group`, adopting the existing data as the seed; `group`→`single` is rejected. Groups scale 3↔5. PXC remains `cluster`, 3↔5 only. Cross-engine driver conversion is unsupported.
- Replacement introduces a new member identity before retiring the old one. Use the managed replace action; do not copy data identity or a stale bootstrap flag into another member. Disk shrink is not supported.
- Suspension with retained volumes preserves the dataset and identities. Resume after all members stop uses the full-outage recovery rules below; it is not an unconditional restart into a writable cluster.

PXC cluster-traffic certificate rotation is an **offline exception** to normal rolling maintenance: all data members stop, replace the dedicated traffic CA and shared traffic leaf/key, and recover using the full-outage position rules below. Reformed members reject credentials signed by the old traffic root. Plan database downtime and verify recovery readiness first. SQL and agent trust remain separate from the traffic CA. Fence a compromised member's secret/network access before distributing replacements; shared traffic trust has no independent per-member revocation. Ordinary membership changes do not rotate the traffic identity or require this TLS outage.

Retry a wait/observation, not an already accepted mutation. Inspect the operation ID and conditions after interruption. A cancelled or failed operation does not necessarily undo data or desired-state changes.

Group clone intents are bound to the configured donor, version and recovery receipt. A lost CLONE reply is an unknown outcome, not permission to resend the destructive statement or interrupt an active copy. Before restarting after a lost reply, the agent observes a newer matching native operation that is completed or has finished file synchronization and is ready for restart. If that observation is unavailable, the intent remains pending rather than forcing a restart. Recipient restart uses a fresh administration pool, and receipt finalization still requires matching native completion. Interrupted replay can resume that operation's native restart phase; a stale completion, active copy, failed clone or uncorrelated legacy intent cannot satisfy the pending receipt. Existing members still require explicit recovery-clone authorization.

Pending-receipt replay treats a failed SQL probe as unavailable evidence, not proof that the recipient process exited. Recovery may start an unreachable recipient only when its supervisor has confirmed process exit; otherwise the operation remains pending without stopping a potentially active clone.

## Upgrade

The **Upgrades** tab selects the current engine driver's offered paths; MySQL and PXC have separate version/artifact catalogs. Minor upgrades use the managed rolling order. The supported 8.0→8.4 major path requires a recent successful **current-version** backup validated by the provider. Test that backup and review schema/application compatibility before starting. An arbitrary old successful backup shown in the UI is not proof of eligibility.

Group Replication upgrades secondaries before moving the primary. A newer member can remain read-only during mixed-version membership; do not force lower-version joins or use cross-major clone as an upgrade technique. PXC uses its own Galera-compatible path and waits for eligible members before advancing. Follow per-member versions and the operation, not only the desired version.

Downgrades and engine driver conversion are rejected. Rollback requires a tested compatible pre-upgrade backup and independent recovery plan, not starting old binaries against an upgraded dataset. Keep backup objects and sealed account provenance until the upgraded workload is accepted.

## Writer loss and quorum loss

A MySQL group elects a new primary natively while a majority remains; a PXC Primary component keeps eligible members and ProxySQL moves the routed writer. Established majority failover does not require the control plane to elect a writer. This does not guarantee uninterrupted client sessions or identical behavior for every network partition.

MySQL minority writes can block, then fail after the configured unreachable-majority timeout and read-only exit. PXC non-Primary members reject reads and writes (typically error 1047) and must leave routing. Never interpret a timeout as proof that a write did not commit. Use unique application request identities and inspect outcomes before non-idempotent retries.

Do **not** force `pc.bootstrap=YES` merely to clear a health condition. It can create a second Primary component during a partition. Minority recovery requires explicit operator authority and evidence that the selected survivor has the highest committed position in the same Galera history. The supported action refuses an existing Primary component, fences the exact missing owned members and waits for durable fencing completion before authorizing one survivor. Missing or conflicting evidence blocks recovery; there is no data-loss bypass.

### Explicit PXC minority recovery

The PXC-only **Recovery** tab requires cluster update permission and an advertised `recover-quorum` operation. In Degraded/Error state, open **Recover quorum**, read the danger warning and type `all-absent-members-fenced` before submitting. The confirmation authorizes fencing; it does not replace the provider's own fencing or position checks.

The corresponding REST action is `POST /api/v1/clusters/{cluster}/mysql/recover-quorum` with the exact body:

```json
{"confirmation":"all-absent-members-fenced"}
```

The CLI command is `karactl -n NS mysql recover-quorum CLUSTER -f recovery.json` with that body in `recovery.json`; follow the CLI's explicit destructive-action confirmation. The response is an accepted Operation, not proof of recovered quorum. Follow its ID in Operations and verify Primary/Synced/readiness, the expected member count and application contents. A transport timeout does not cancel the operation. Do not submit or manually bootstrap another member while recovery is pending.

Full-outage recovery below compares stopped members' recovered positions and is distinct from this live-minority action.


## Full-outage recovery

All members must be stopped/fenced before choosing a seed. Recovery fails closed if evidence is missing or incompatible; do not choose the first reachable member.

| MySQL Group Replication | PXC / Galera |
| --- | --- |
| Observe every member's offline executed GTID set and data identity | Recover every stopped member's Galera state UUID and sequence number using redo recovery |
| Select a member whose GTID set is a superset of every other member | Require one common state UUID, then select the highest recovered sequence number |
| Incomparable sets or missing observations block re-formation | Missing observations, conflicting UUIDs or unusable positions block bootstrap |
| Bootstrap only that seed, then join the others normally | Mark only the selected seed safe, bootstrap with its recovered position, then rejoin the others |

After a crash, `grastate.dat` often contains sequence `-1`; `safe_to_bootstrap` alone cannot rank data freshness. Graceful suspension uses the same evidence-based resume policy rather than assuming shutdown order. An ordinary PXC restart also recovers its local position before rejoining; it must not turn stale bootstrap intent into a new independent cluster. Bootstrap authorization is bound to a formation epoch, preventing an old desired configuration from replaying bootstrap.

PXC state transfer uses upstream SST behavior rather than custom keep-list overrides. Managed data-identity tracking permits UUID regeneration only for an explicitly authorized state transfer and records the accepted identity after Synced state. Re-formation/restoration may explicitly reset a stopped joiner's own dataset for SST; an arbitrary identity mismatch is not permission to erase or initialize it. This is distinct from independent [backup restore](backups.md#offline-restore-and-clone), which creates new replication history.

If recovery is blocked, preserve all remaining data and position evidence, isolate application traffic and resolve the missing observations or recover from a verified backup. Never reset GTIDs, invent a Galera position or edit bootstrap flags across multiple members to force a green status.

### Group Replication bootstrap epoch consumed

`GroupReformationBlocked=True` with reason `BootstrapEpochConsumed` means the
selected seed durably claimed its bootstrap epoch but is not observed as an
`ONLINE` primary. The claim is recorded **before** enabling bootstrap or sending
`START GROUP_REPLICATION`. It is an at-most-once safety receipt, not a successful
join receipt. Even a rejected enable statement can therefore consume an epoch.

An HTTP 503 from `/v1/mysql/bootstrap` does not prove that Group Replication never
started. The same response covers native configuration/permission errors, a lost
START reply, failure to clear the bootstrap flag, and failure while observing or
recording an ONLINE join. An OFFLINE snapshot or zero currently ONLINE members
does not establish that no group previously formed. Do not retry bootstrap on a
different member, decrement the identity epoch, edit managed flags, or use the
PXC-only `recover-quorum` action for this condition.

Corrected data-member agents return a bounded, credential-redacted native error
on bootstrap and start-group failures. Inspect the `ReconcileError` event for the
MySQL error number/message and, when available, recent relevant entries from the
managed `mysql-error.log`. The log excerpt is explicitly historical and may
predate the request; it is not proof that a particular statement executed.
Unavailable logs do not suppress the primary error. The safety condition remains
`BootstrapEpochConsumed`; it does not authorize another attempt. Existing agents
need replacement through supported lifecycle to gain these diagnostics; a
control-plane-only update cannot add them retroactively.

Preserve native error logs, effective GR/listener/TLS variables, member identities
and executed GTIDs before recovery. For a previously formed group, fence/stop all
members and use the full-outage evidence requirements above; automatic all-offline
recovery requires the recorded formation to have completed. Retained-volume
suspend/resume also collects every member's stopped, initialized identity and
offline GTIDs before selecting a seed. Neither is an unconditional epoch reset.
Missing observations require resolving the cause or recovering a verified backup.

For a **disposable, never-formed create with no application data**, preserve the
diagnostics, correct the native startup cause, then use owner-authorized normal
delete and fresh create with corrected control-plane/member artifacts. This is
the supported clean recovery, not in-place retry of a consumed epoch. Mere
deletion/re-creation with unchanged artifacts/configuration is not a native fix.
If the create may contain valuable data, retain its volumes and seek
evidence-based recovery instead; there is no public Group Replication
`reset-bootstrap-epoch` action. Updating only the control plane cannot undo an
agent's durable claim or change its native renderer. MySQL does not support
`upgrade-agents`.
