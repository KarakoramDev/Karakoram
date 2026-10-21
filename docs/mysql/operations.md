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

For MySQL groups, require `ONLINE` membership, one primary, read-only secondaries and the expected member count. The default `BEFORE_ON_PRIMARY_FAILOVER` policy waits for the new primary's backlog before it serves traffic. For PXC, require `Primary`, `Synced` and `wsrep_ready=ON`; `Primary` is Galera component membership, **not** a promise that every node is the application writer. ProxySQL selects one routed writer. Donor/joining states and SST can temporarily reduce eligibility.

Monitor connections versus limits, running threads, query/network rates, replication queues/lag, archive lag and proxy backend state. No metric value or local failover experiment establishes a production recovery-time or zero-loss guarantee. Check placements independently: three members co-located in one failure domain do not tolerate losing that domain.

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
- MySQL allows `single`→`group`, adopting the existing data as the seed; `group`→`single` is rejected. Groups scale 3↔5. PXC remains `cluster`, 3↔5 only. Cross-provider conversion is unsupported.
- Replacement introduces a new member identity before retiring the old one. Use the managed replace action; do not copy data identity or a stale bootstrap flag into another member. Disk shrink is not supported.
- Suspension with retained volumes preserves the dataset and identities. Resume after all members stop uses the full-outage recovery rules below; it is not an unconditional restart into a writable cluster.

PXC cluster-traffic certificate rotation is an **offline exception** to normal rolling maintenance: all data members stop, replace the dedicated traffic CA and shared traffic leaf/key, and recover using the full-outage position rules below. Reformed members reject credentials signed by the old traffic root. Plan database downtime and verify recovery readiness first. SQL and agent trust remain separate from the traffic CA. Fence a compromised member's secret/network access before distributing replacements; shared traffic trust has no independent per-member revocation. Ordinary membership changes do not rotate the traffic identity or require this TLS outage.

Retry a wait/observation, not an already accepted mutation. Inspect the operation ID and conditions after interruption. A cancelled or failed operation does not necessarily undo data or desired-state changes.

Group clone intents are bound to the configured donor, version and recovery receipt. A lost CLONE reply is an unknown outcome, not permission to resend the destructive statement or interrupt an active copy. Before restarting after a lost reply, the agent observes a newer matching native operation that is completed or has finished file synchronization and is ready for restart. If that observation is unavailable, the intent remains pending rather than forcing a restart. Recipient restart uses a fresh administration pool, and receipt finalization still requires matching native completion. Interrupted replay can resume that operation's native restart phase; a stale completion, active copy, failed clone or uncorrelated legacy intent cannot satisfy the pending receipt. Existing members still require explicit recovery-clone authorization.

Pending-receipt replay treats a failed SQL probe as unavailable evidence, not proof that the recipient process exited. Recovery may start an unreachable recipient only when its supervisor has confirmed process exit; otherwise the operation remains pending without stopping a potentially active clone.

## Upgrade

The **Upgrades** tab selects the current provider's offered paths; MySQL and PXC have separate version/artifact catalogs. Minor upgrades use the managed rolling order. The supported 8.0→8.4 major path requires a recent successful **current-version** backup validated by the backend. Test that backup and review schema/application compatibility before starting. An arbitrary old successful backup shown in the UI is not proof of eligibility.

Group Replication upgrades secondaries before moving the primary. A newer member can remain read-only during mixed-version membership; do not force lower-version joins or use cross-major clone as an upgrade technique. PXC uses its own Galera-compatible path and waits for eligible members before advancing. Follow per-member versions and the operation, not only the desired version.

Downgrades and provider conversion are rejected. Rollback requires a tested compatible pre-upgrade backup and independent recovery plan, not starting old binaries against an upgraded dataset. Keep backup objects and sealed account provenance until the upgraded workload is accepted.

## Writer loss and quorum loss

A MySQL group elects a new primary natively while a majority remains; a PXC Primary component keeps eligible members and ProxySQL moves the routed writer. Established majority failover does not require the control plane to elect a writer. This does not guarantee uninterrupted client sessions or identical behavior for every network partition.

MySQL minority writes can block, then fail after the configured unreachable-majority timeout and read-only exit. PXC non-Primary members reject reads and writes (typically error 1047) and must leave routing. Never interpret a timeout as proof that a write did not commit. Use unique application request identities and inspect outcomes before non-idempotent retries.

Do **not** force `pc.bootstrap=YES` merely to clear a health condition. It can create a second Primary component during a partition. Minority recovery requires explicit operator authority and evidence that the selected survivor has the highest committed position in the same Galera history. The supported action refuses an existing Primary component, fences the exact missing owned members and waits for durable fencing completion before authorizing one survivor. Missing or conflicting evidence blocks recovery; there is no data-loss bypass.

### Explicit PXC minority recovery

The PXC-only **Recovery** tab requires cluster update permission and an advertised `recover-quorum` operation. In Degraded/Error state, open **Recover quorum**, read the danger warning and type `all-absent-members-fenced` before submitting. The confirmation authorizes fencing; it does not replace the backend's own fencing or position checks.

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
