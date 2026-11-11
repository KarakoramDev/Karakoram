<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Upgrade and recover Karakoram

Upgrade the control plane and recover its state; engine-version changes are separate operations documented under each engine.

## Upgrading to v0.4.8

### Neo4j minimum memory

New Neo4j Community clusters and resource changes now require **at least 2 GiB
minimum engine RAM** on Zerops, Dokploy and Nomad. The default remains 1 CPU /
2 GiB; the CPU floor remains one whole CPU. The former 1 GiB admission floor
could exhaust native-memory headroom during credential recovery and fail creation.
This is a control-plane admission correction; no agent rollout is required.

Existing smaller clusters are not automatically resized or rejected:
reconciliation, agent upgrades and unrelated spec updates retain admission when
engine resources are unchanged, including full replacements that preserve those
values. Any engine resource edit must raise minimum RAM to at least 2 GiB.
This includes reverting an unapplied disk-size increase: the comparison uses
persisted requested resources, not corrected observations of the actual volume.
Plan a singleton outage, preserve a backup, and resize through the normal API:

```sh
printf '%s\n' '{"spec":{"components":{"engine":{"resources":{"minRam":2,"maxRam":2}}}}}' | karactl -n NS clusters patch CLUSTER -f - --wait
```

If the current maximum exceeds 2 GiB, preserve that higher maximum in the patch.
Verify the update operation succeeds and the cluster and member return Ready
before resuming application traffic. A larger maximum alone does not supply
guaranteed headroom: heap and page cache derive from minimum RAM.

### Check database placement after v0.4.7 replacements

This control-plane correction makes MySQL GR/PXC and PostgreSQL HA/Supabase
replacement imports preserve the predecessor identity and exclude nodes occupied
by surviving data members on Dokploy and Nomad. An already completed v0.4.7
replacement may have left voting members co-located even when an unused eligible
node existed. Upgrading does **not** move those members automatically.

After the control-plane upgrade, run:

```sh
karactl -n NS clusters status CLUSTER -o json
karactl -n NS clusters members CLUSTER
```

Compare `PLACEMENTGROUP` values (JSON: `placementGroup`) for voting data members.
`FailureDomainsSeparated=False`, reason `SharedNode`, plus its warning event
identify repairable shared placement and name a repair command. Take a fresh full
backup and verify quorum, replication and extra-member capacity. For healthy GR
replacement, first complete the surviving data-agent rollout described under
[GR capability and legacy recovery](#gr-capability-and-legacy-recovery), then use
`karactl -n NS clusters replace-member CLUSTER COLOCATED_MEMBER --wait`.
After completion require distinct placements and `FailureDomainsSeparated=True`.
`Unknown/InsufficientEligibleNodes` calls for more eligible nodes rather than
blind repeat replacement; missing placement is `Unknown/PlacementUnknown`.
Running voters on nodes no longer eligible for new allocations still count as
occupied domains; their presence does not hide a repair onto an eligible free
node. Nomad keeps its public name/ID placement labels while normalizing aliases
internally to physical node IDs; unresolved aliases report placement unknown.
See [the complete recovery procedure](operations.md#repairing-shared-database-failure-domains).

A replacement allocated by the old control plane but **not yet frozen** recovers
its hint from the durable predecessor volume record before import planning;
missing predecessor identity blocks import rather than guessing from a name.
An import **already frozen before upgrade** keeps its exact original request:
do not edit its internal state to change placement. Let native safety gates
settle it, then inspect and repair the surviving placement as above.
Insufficient eligible nodes explicitly refuse new replacement admission.
Nomad also refuses insufficient eligible CPU/memory capacity; Dokploy can leave
the new task pending on its pinned unoccupied node until reservations fit,
without retiring the old member or falling back to an occupied node.
Do not drain running database nodes to force placement, and do not repeatedly
submit replacement after a timeout: observe the existing operation.
Zerops remains best-effort and has no authoritative eligible-node inventory.
Detection and placement planning are control-plane-only corrections. Healthy GR
replacement retirement additionally requires full-view-capable survivor agents;
PXC and PostgreSQL HA do not acquire a GR capability requirement.

### Agent heartbeats and the platform read budget

Upgrading the control plane immediately enables shared platform snapshots and
steady-cluster scheduling. Healthy, converged clusters reconcile every 30
minutes while availability is evaluated every five seconds. Non-steady clusters
and lifecycle safety decisions retain fresh reads. Existing v0.4.7 agents keep
serving live pulls; pinned proxy connections through a relay still authorize
each dial against fresh task inventory. The control-plane upgrade itself changes
no member runtime specification or deployed hash and redeploys no member.

Run `karactl -n NS clusters upgrade-agents NAME --wait --timeout 30m`, or use the
opt-in automatic agent rollout, to install heartbeat-capable agents. Native
process restart alone does not replace the agent binary. Heartbeats use the
existing authenticated agent-to-control-plane HTTPS channel, including its
relay stream where applicable: every three seconds agents send generic status
and subscribed read-only native observations. Pushed samples are usable for
seven seconds; the 15-second health lease remains anchored to the oldest health
evidence, not the time it was republished. Normal config pulls remain every
60 seconds; updated agents also act on coalesced config-invalidating heartbeat
notices within **≤7 s** of heartbeat scheduling delay (3 s wait plus up to 4 s
bounded sampling; transport/server time and failed-heartbeat retries excluded), then perform
the fresh render/apply work. See the provider guide for the listing-only corner
and `karactl clusters reconcile` for an immediate admitted fresh pass.
Agents must be upgraded for heartbeat-based health renewal; old agents continue
using live pulls rather than acquiring this capability from a control-plane restart.

Dokploy proxies need proxy serving admission, which supplies
`KARAKORAM_TASK_ID`, for task-bound heartbeat samples. Pinned reads of older,
unmarked proxies continue to use live pulls.

- Idle clusters now reconcile every **30 minutes**, rather than about every
  **five seconds**. Platform drift outside the shared listing is repaired at
  the next deep reconcile; `karactl clusters reconcile CLUSTER` forces a fresh
  pass now. See [drift bounds and forced reconciliation](operations.md#out-of-band-platform-changes-and-forcing-a-reconcile).
  Settled namespace relays check their platform service every **60 seconds**;
  unsettled relays retain the **15-second** namespace tick cadence.

### Upgrade procedure

1. Take the [consistent pre-upgrade state backup](#consistent-pre-upgrade-state-backup)
   and preserve the master key, installation settings and previous executable.
2. Upgrade the control plane using the procedure for your provider. Check
   control-plane health, cluster availability and observed generations.
3. Explicitly install member executables with
   `karactl -n NS clusters upgrade-agents CLUSTER --wait`, respecting native safety
   gates and maintenance windows. Use `--include-current` for explicit same-build
   recovery. A control-plane upgrade does not replace member agents. Settle the
   legacy recovery cases below before rollout if structural work or confirmed
   loss already exists; upgrade healthy GR survivors before new replacement,
   scale-in or placement repair. Upgrade old marked proxies before count/resource
   changes. The [heartbeat and platform-read redesign](#agent-heartbeats-and-the-platform-read-budget)
   requires upgraded agents for heartbeat-based renewal.
4. Use the [monitoring metrics](monitoring.md) to check heartbeat ingestion,
   accepted steady observations versus fresh fallbacks, snapshot hits and platform
   reads by caller. Compare idle traffic after convergence, not during an
   operation or while old agents still require pinned relay dials. Listing checks
   remain shared per target; periodic deep reconciles still generate fresh reads.

### GR capability and legacy recovery

New healthy GR replacement and scale-in require all surviving agents to expose
full `GroupView` capability before allocation or native leave. Refusal is:

```text
GR structural work refused: members MEMBERS lack full GroupView capability; run karactl -n NS clusters upgrade-agents CLUSTER --wait before retrying
```

Unknown or unreachable capability is not evidence of legacy capability:

```text
GR structural work refused: full GroupView capability is unknown or unreachable for members MEMBERS; restore agent reachability, then run karactl -n NS clusters upgrade-agents CLUSTER --wait before retrying
```

If an unapplied scale-in count blocks executable rollout, restore that unapplied
desired count, upgrade agents, then reapply scale-in. Full-capability work uses
the exact survivor UUID-set barrier, with no count-based or timeout bypass.
Unknown persisted UUIDs require recovery, not state editing.

There are two explicit weaker legacy exceptions:

- Structural work already in flight before the control-plane upgrade can finish
  using the released pre-upgrade ONLINE/count proof, **not** full UUID identity
  convergence. It emits a Warning event, `LegacyGRRetirement`:
  “Completing legacy in-flight GR structural work using the pre-upgrade ONLINE/count proof, not the full UUID identity barrier; upgrade surviving agents before starting new structural work”.
  Observe the existing operation through native cleanup, then roll agents before
  starting healthy structural work; do not submit another replacement over it.
- Explicit or automatic **confirmed-loss** replacement is admitted when reachable
  survivors are positively known to lack full capability, using the released
  ONLINE/count proof after the existing majority, same-group, GTID and donor
  checks. Admission emits Warning `LegacyGRRetirement`:
  “Admitting confirmed-loss GR replacement using the released ONLINE/count proof because surviving agents lack full GroupView capability; upgrade agents after recovery with karactl -n NS clusters upgrade-agents CLUSTER --wait”.
  Complete recovery first, then upgrade agents. Unknown/unreachable capability
  still refuses admission; already strict work never falls back to legacy proof.

Cancelling confirmed-loss GR replacement after the joiner has joined completes
forward rather than attempting to restore the lost predecessor. Before join,
cancel rolls back the new allocation, retaining the lost member and its loss
fence for a fresh `replace-member` after cleanup. Existing rollback owners resume
under these rules. Cancellation acknowledgement or a Cancelled queue result is
**not** proof that native recovery has finished: observe membership, cleanup,
cluster health and operation ownership before retrying or upgrading agents.
See [GR retirement and recovery](mysql/operations.md#gr-retirement-and-legacy-recovery).
Automatic confirmed-loss replacement remains experimental and off by default;
the discovery-withdrawal boundary is unchanged.

### ProxySQL admission, startup and recovery

The stronger Dokploy serving path requires both the fixed control plane and
fixed MySQL/PXC or PostgreSQL/Supabase proxy executables. Upgrade old marked
proxies before count/resource changes: capability preflight refuses those
changes before submission. Legacy/unmarked first upgrades retain the disruptive
path; cold deployments do not wait for control-plane admission. Old deployment
receipts retain legacy observation and do not acquire the new guarantees.

Exact-task/member startup with an unapplied or unready configuration stays
pending without admission; Ready agents reporting a stale revision remain
rejected. Fixed MySQL proxy agents retry only local ProxySQL admin connection
refusal, not arbitrary startup errors.

#### Ordinary proxy update recovery from v0.4.7

An ordinary Dokploy ProxySQL count/resource update can already be paused with
healthy incumbents while its v0.4.7 public operation remains Running/Degraded
until the generic retry budget expires. Upgrade the control plane to v0.4.8:
the fixed observer re-inspects retained old-format receipts and fails an
authoritatively paused/rolled-back attempt promptly, without replaying it.
Inspect the Swarm service, incumbent and replacement health, configuration pull,
ONLINE writer readiness and spare capacity; fix the underlying cause first.

Then explicitly PATCH `spec.components.extra.proxy.replicas` and/or
`spec.components.extra.proxy.resources` to a **different**, valid target that
fits the available capacity, including a known-good previous specification.
For example, if the failed target requested three replicas and two fit, save:

```json
{"spec":{"components":{"extra":{"proxy":{"replicas":2}}}}}
```

as `proxy-recovery.json`, then run:

```sh
karactl -n NS clusters patch CLUSTER -f proxy-recovery.json --wait
```

For a resource correction, the same file/command can instead contain, for example:

```json
{"spec":{"components":{"extra":{"proxy":{"resources":{"minRam":1,"maxRam":1}}}}}}
```

RAM values are GiB. Use bounds proven suitable for your workload and provider,
not these example values blindly; if they equal the failed target, choose a
genuinely different corrected count/resource target. Both fields may be patched
together. MySQL/PXC repair the resource receipt before a separately gated count
change; configuration acknowledgement, maintenance and serving admission still
apply. Do not manually increment generations or edit operation checkpoints.

If you already patched the cluster to a different target on v0.4.7 and that
patch was accepted but never rolled out, no further patch is needed: after the
control-plane upgrade, Karakoram rolls the proxies forward to the stored target
through the same gated start-first replacement. Wait for the cluster to become
Ready (`karactl -n NS clusters get CLUSTER`).

An identical apply or `karactl -n NS clusters reconcile CLUSTER` only observes:
neither re-arms the failed receipt nor authorizes another rollout. Pending,
stalled, expired or transport-ambiguous attempts must be inspected and settled,
not replayed. A CLI wait failure does not cancel accepted work: inspect the
returned operation ID and use `operations wait ID` rather than resubmitting.
See [Dokploy count/resource recovery](dokploy.md#proxysql-count-and-resource-updates)
for retained receipt and conservative lower-ForceUpdate rollback boundaries.

#### Agent-only proxy rollout recovery

A terminal paused/rollback **agent-upgrade** receipt fails on its first
authoritative observation; expired/stalled receipts or transport ambiguity are
not proof of failure. Inspect tasks, configuration pulls, writer readiness and
surge capacity, resolve the cause, then explicitly rerun:

```sh
karactl -n NS clusters upgrade-agents CLUSTER --include-current --wait
```

This agent-only action is not a universal substitute for correcting an ordinary
count/resource target while a MySQL/PXC rolling checkpoint owns recovery.

PostgreSQL/Supabase cancellation now shares MySQL/PXC's request-bound lost-reply
settlement: it observes accepted serving work without another upload. Terminal
failure can require manual recovery. A cancellation request does not establish
that the original rollout never committed. Do not manually resume failed Swarm
updates or restart Docker daemons/Swarm agents during marked rollouts; executor
re-entry remains a residual risk. See [Dokploy proxy updates](dokploy.md#proxysql-count-and-resource-updates).

### Companion refresh and Dokploy observation

A changed deployed agent archive refreshes enabled MongoDB/PostgreSQL/Supabase
Data MCP companions once, even if upstream URLs are unchanged. Maintenance
windows gate the rollout and companion access can be interrupted. Ordinary
same-build control-plane restarts do not refresh them. This is not a data-member
upgrade; see [Data MCP](data-mcp.md) and [maintenance windows](operations.md#maintenance-windows).

Control-plane-only parallel read-only observations and pass-local snapshots
reduce Dokploy request amplification; per-dial authorization remains fresh.
Timers and freshness leases are not extended by these optimizations; the
[heartbeat scheduling changes](#agent-heartbeats-and-the-platform-read-budget)
are described separately above. Slow/unreachable reads still fail closed.
Karakoram does not eliminate Dokploy API-key accounting writes or promise a
disk-I/O SLA. See the [platform read budget](providers.md#platform-read-budget)
and [Dokploy accounting caveats](dokploy.md#dokploy-api-key-accounting-load);
the observation allowance is not an aggregate read-plus-mutation limit.

Platform request metrics now attribute internal member configuration pulls to
`caller="agent-config"`, monitoring to `monitoring`, and startup/background work
to `controller` on Dokploy, Zerops and Nomad. Existing `availability`, `reconcile`,
`namespace` and `listing` labels are unchanged. Updated agents send their applied
configuration revision on minute pulls. A matching payload certificate and
current steady baseline return unchanged without per-member reads. If the
target proof has expired, the pull shares a listing refresh instead of rendering
every peer; successful full responses already carry their fresh payload token.
Changed or uncertain baselines retain fresh peer validation. **Upgrade member
agents** to enable this optimization:
older agents continue receiving complete configurations, including periodic
PostgreSQL proxy peer checks. See [metric labels](monitoring.md).
Updated agents also acknowledge certificate-bound unchanged replies and recheck
heartbeat target stamps before reloading. Pure force notices with unchanged
tracked content wait for a settled baseline; changed content still notifies
immediately. Two consecutive notified unchanged replies for one target limit
further notices for that target to 60 s and produce one warning. A different
nonempty target bypasses the bound, and ordinary successful full pulls reset
it. PostgreSQL proxy notices ignore unrelated target mutation epochs while
certificates retain that evidence; own generation/content and listing drift
still notify. Working heartbeats provide prompt listing-driven refresh;
failing v0.4.8 heartbeats can require listing plus the next minute pull
(approximately 125 s before rendering). v0.4.7 agents keep unconditional
60 s full pulls. See [provider bounds and residual cost](providers.md#platform-read-budget).
The optional content stamp travels in an HTTP header so older strict heartbeat
decoders remain compatible.
`karakoram_agent_config_requests_total{result="full|unchanged|error"}` exposes
config HTTP traffic separately from provider GETs.


### Compatibility and rollback

A v0.4.8 agent talking to an older control plane receives HTTP 404 for the
heartbeat endpoint, waits ten minutes before retrying and continues serving
ordinary pulls. This heartbeat compatibility alone is **not a downgrade guarantee**.
Heartbeat samples, subscriptions and platform snapshots are process-local.

There is no SQLite schema migration in this patch, but additive JSON decoding
does not make new protocol/state intent safely reversible: older typed writers
can discard unknown fields. In particular, Swarm templates already opted into
control-plane serving admission keep requiring that protocol for new tasks
after a downgrade; an older control plane cannot admit them. Existing admitted
containers continuing to serve does not prove replacement tasks will work.
Do not downgrade a control plane managing opted-in templates or active new-protocol
work. Retain or restore the fixed control plane and settle active work for
recovery; do not hand-edit metadata or remove admission variables as a workaround.
The general [rollback procedure](#rollback) does not override these restrictions.

## Upgrading to v0.4.8

v0.4.8 is a hotfix of v0.4.7 with one change: the Zerops provider classifies
Zerops API errors by their error code instead of their HTTP status. It does
not include the features planned for the next feature release, v0.4.9.

### Fixed in v0.4.8

**Support for the new Zerops API error codes:** Zerops is changing how its API
reports a missing object. The error code stays the same (for example
`serviceStackNotFound` or `projectNotFound`), but the HTTP status changes from
400 to 404 and the error metadata gains the object's ID. v0.2.0–v0.4.7 treat
every HTTP 404 as "this service or app version no longer exists" and every
other 4xx answer as a rejected request. Deleted services, missing app versions
and expired platform Processes are still recognized after the Zerops change.
But once Zerops sends the new answers, those versions read any other missing
object as a missing service or app version:

- A create or import that names a service type or version Zerops does not
  offer, or a resource change Zerops refuses because of a missing type
  version or autoscaling profile, is no longer reported as rejected. MySQL,
  PXC and Weaviate resource changes that would stop at a rejection keep
  retrying instead. When Zerops refuses to delete a PostgreSQL or MySQL
  member's volume for such a reason, the volume is not moved to the
  [orphaned volume inventory](operations.md#orphaned-platform-volumes) and
  retirement keeps retrying.
- A project-level answer such as `projectNotFound` (for example, a wrong
  project ID or a deleted project) reads as the cluster's services being
  absent. Keycloak, Supabase and Temporal component checks report missing
  components, Temporal deletion can be reported complete, and MySQL
  formation skips such members, instead of failing with a clear rejection.
- A 404 from a gateway or an unknown API route reads as a deleted service.

v0.4.8 decides by the error code, so it behaves the same before and after the
Zerops change. Only a missing service or app version counts as absence, and a
missing Process as expiry. Another object's not-found code is a rejected
request, as before. An HTTP 404 without a known Zerops error code is handled
like a temporary platform outage: it is retried, a write answered this way has
an unknown outcome, and it is never evidence that a service was deleted. See
[Zerops operations and limits](zerops.md#operations-and-limits).

**Who is affected:** every installation that manages clusters on Zerops, from
the day Zerops deploys the change. Zerops has not announced a date, and the
switch has no compatibility period. Nomad and Dokploy installations are not
affected; upgrading changes nothing for them.

### Required rollout

Back up state, then upgrade the control plane. Use the
[consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup), preserve
the original `KARAKORAM_MASTER_KEY` separately, and retain the old executable
with its pre-upgrade state. Deploy v0.4.8 through your provider's documented
singleton control-plane deployment path. This step applies the fix. Member
agents never call the Zerops API: v0.4.7 agents keep working with the v0.4.8
control plane and need no rollout. No database restart, engine upgrade, backup
or configuration change is needed. Upgrade before Zerops deploys its change if
you can; upgrading afterwards is also safe.

### Known issues in v0.4.8

v0.4.8 changes nothing else. The
[continuing limitations](limitations.md#continuing-limitations) and all
[known issues in v0.4.7](#known-issues-in-v047) remain present, and their
workarounds still apply.

- **After a lost MySQL Group Replication member is replaced, commits can stall
  for several seconds at a time for about five minutes**. When
  `karactl clusters replace-member` replaces a member whose volume was lost, the
  remaining members keep trying to reach the removed member for about five
  minutes after its name is withdrawn. During that time a single commit can
  take 4–5 seconds roughly every 14 seconds; writes continue between the stalls
  and no acknowledged write is lost. A client with a short read timeout can get
  error 2013 (`Lost connection to MySQL server`) at COMMIT even though the
  transaction was committed. Replacing a healthy member is not affected.
  Workaround: treat a timeout or error 2013 at COMMIT as an unknown outcome —
  check whether the write was applied before retrying, or make writes
  idempotent; a longer client read timeout (for example 10 seconds) can turn
  these errors into slower commits; expect slower commits for about five
  minutes after a lost-member replacement; keep automatic replacement of lost
  MySQL volumes (`KARAKORAM_MYSQL_AUTO_REPLACE_LOST_VOLUMES`) disabled. A fix is
  planned for v0.4.10.

## Upgrading to v0.4.7

v0.4.7 includes the [v0.4.6 security hotfix](#upgrading-to-v046)
(`github.com/klauspost/compress` v1.20.2), the
[v0.4.5 MySQL hotfix](#upgrading-to-v045) and the
[v0.4.4 PITR hotfix](#upgrading-to-v044). When upgrading directly
from v0.4.3, v0.4.4, v0.4.5 or v0.4.6, use the v0.4.7 agent rollout below instead
of an intermediate resource-change rollout; then re-enable PITR if needed,
take a successful full and verify the advancing window as described in the
hotfix recovery steps. The two v0.4.5 MySQL fixes are inherited, not new in v0.4.7.

MySQL/PXC automatic confirmed-loss replacement is disabled by default.
Verify healthy survivors and run `karactl clusters replace-member CLUSTER MEMBER --wait`
when storage is confirmed lost. The experimental automatic path requires
`KARAKORAM_MYSQL_AUTO_REPLACE_LOST_VOLUMES=true` and a control-plane restart;
testing observed GR quorum loss after automatic replacement, with cause still
unknown. Explicit replacement is unchanged; see [handling lost volumes](mysql/operations.md#confirmed-volume-loss).

PSMDB adds security patches `7.0.45-24` and `8.0.34-15` (the new default).
Existing clusters are not automatically patched. Upgrade every older offered
7.0/8.0 patch to the latest in its line using the [MongoDB rolling upgrade
procedure](mongodb/operations.md); adjacent 7.0→8.0 upgrades still require
observed FCV 7.0. Server/router and database-tools package versions now differ.
PSMDB 7.0/8.0 member deployments also replace Percona mongosh 2.10.0 with
official MongoDB mongosh 2.14.1, fixing compromised bundled Node TLS roots
(CVE-2026-115930). A control-plane-only upgrade does not rerun member
preparation; `upgrade-agents` does, as do engine deployments. Verify
`mongosh --version` after rollout. The control plane uses the MongoDB Go driver, not shell-output
parsing; 2.11–2.14 cursor formatting/telemetry changes do not affect its calls.
Legacy 6.0 members retain mongosh 2.8.3 and its unresolved TLS-root exposure.
Native-artifact mirrors must add `mongosh-2.14.1-linux-x64.tgz` with the
exact checksum in the [native artifact inventory](native-artifacts.md).
Shell preparation honors `KARAKORAM_ARTIFACT_MIRROR`; without a mirror it
downloads the locked official standalone archive. Percona's shell package remains
installed to satisfy the server dependency; a local dpkg diversion makes
`/usr/bin/mongosh` resolve to the verified 2.14.1 archive across package upgrades.
PSMDB 6.0 is now recovery/migration-only after its 2025-07-31 EOL: fresh
creates return `spec.version` / `unsupported`; existing updates and adjacent
6.0→7.0→8.0 upgrade paths remain available. Restore-to-new through API/CLI
remains admitted only with a validated same-major/topology backup or PITR
`spec.dataSource`; the wizard disables the deprecated line. This preserves
recovery admission, not a promise of PBM support for EOL data. Validate old
backups before upgrading and take new backups after each binary/FCV transition.

### PXC formation behind pending proxy serving health

The control plane no longer makes native formation checkpoints wait for a proxy
deployment whose serving process is deliberately fenced by birth certification.
This dependency cycle affects v0.4.3–v0.4.6 and can block fresh PXC formation on
Nomad, where deployment completion requires the proxy TCP listener. A Synced
joiner could remain uncheckpointed until the admission watchdog expired, then
stop when failure cleanup removed its donor/fence authority.

This correction is control-plane-only: native Synced progress is checkpointed
before the watchdog and terminal cleanup, without weakening all-member GTID
certification or platform serving-health checks. No additional agent rollout is
required for this correction. An already terminally blocked formation is not
automatically retried or certified; preserve evidence and valuable data, then
follow its recovery guidance. Delete/re-create only disposable failed births.
Fresh three-member PXC creation with ProxySQL was live-verified on Nomad.
Automated regressions also cover Zerops-like and Dokploy-like cold deployment
completion; those paths are verified by tests only.

### Upgrade procedure

1. Back up control-plane state using the
   [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup), preserve
   the original `KARAKORAM_MASTER_KEY` separately, and retain the previous
   executable with its pre-upgrade state. Review backup capacity and maintenance
   windows before proceeding.
2. Upgrade the control plane to v0.4.7. Engine versions and deployed member
   agents are separate from this upgrade; rollback does not undo engine or
   infrastructure changes.
3. Run `karactl -n NS clusters upgrade-agents NAME --wait --timeout 30m` for each
   cluster, observing its native safety gates and maintenance window. New agents
   are required for Temporal on Zerops private IPv6, PostgreSQL `postgres`
   rotation, GR lost-volume admission and the proxy serving probe. Follow the
   [rollout guidance](#agent-rollout-now-covers-every-member-agent-engine) below;
   native process restart does not install an agent.
4. Verify every member's build, native readiness/replication and client
   reconnection. Update PITR API consumers and PXC condition-reason alert filters.
   Resolve any legacy Neo4j expired-credential marker using the explicit
   [maintenance procedure](#neo4j-legacy-credential-recovery), not repeated restarts.

### OpenBao security maintenance

OpenBao 2.7.2 is the new default; 2.7.1 remains available but deprecated for existing clusters and forward rolling upgrades. Update the cluster's `spec.version` to `2.7.2` separately from the control-plane upgrade. Follow [OpenBao security maintenance](openbao/README.md#security-maintenance), especially the changed SSH wildcard-principal default. Member replacement installs AWS KMS plugin 0.1.1 for `awskms` seals; retain the external key/IAM access and verify unseal, healthy Raft voters and lease lifecycle after the rollout.

During an explicit 2.7.1→2.7.2 engine upgrade, each member retains its installed
server version and executable until its prepared deployment completes. A desired
version change, seal recovery, configuration reload or ordinary configuration
pull cannot activate the new binary early. Pending or failed preparation keeps
the installed version; activation requires a fresh configuration acknowledgement.

### Weaviate timeout classification

Response-header timeouts after connection establishment now retain
`context.DeadlineExceeded`, even when the request context's separate deadline
has not expired. The client does not replay an established mutation on another
endpoint. Dial-local timeouts before connection establishment remain eligible
for endpoint failover while the request context is live. These paths are verified
by automated tests.

### PostgreSQL backup encryption maintenance

The current pgBackRest 2.59.1 package remains affected by the upstream
[weak-encryption RNG issue](https://pgbackrest.org/news.html#weak-encryption).
The requested 2.59.3 security patch is not present in the Percona Noble package
indexes as of 2026-11-05; a control-plane or agent upgrade does not claim to fix it.
Even after a fixed binary is deployed, **existing weak stanza subkeys persist**.
Inspect encrypted repositories privately using upstream's subkey check, create a
stanza in a new repository if weak, and take a fresh full backup with a fixed
binary. Test restore/WAL recovery and retain the old repository only while needed.
Do not rewrite historical backup metadata or assume a fresh incremental repairs
old keys. Follow the [operator procedure](postgres/backups.md#encryption-rng-security-notice).

### PostgreSQL dependency preparation and etcd quorum

The requested etcd 3.5.35 server/client packages are unavailable in Percona's
Noble PG16/17/18 indexes as of 2026-11-05; current pins remain 3.5.33. Once
qualified dependency pins are released, install them through the existing
one-member-at-a-time `upgrade-agents` deployment, not a native process restart.
Use `--include-current` when preparation must be rerun for the current build.
Take an etcd snapshot first, preserve quorum throughout, and verify endpoint
health/version plus Patroni leadership and replay after each member. See the
[PostgreSQL rollout procedure](postgres/README.md#dependency-package-rollouts).

### PostgreSQL vector extension maintenance

v0.4.7 replaces Percona's pgvector 0.8.6 APT payload with locked upstream 0.8.7
source bundles for PostgreSQL 16/17/18 and Supabase PG17, fixing CVE-2026-103484.
The upstream tag is pinned to an immutable commit and each ABI is built and
qualified independently; Percona's Noble package indexes still lack this patch.
Existing members retain the old package identity and dependents; preparation
diverts package payloads to `.distrib` without renaming live libraries, then
atomically publishes the verified bundle. Database objects and data are retained.
New members omit the old APT package and establish the same diversions.
Complete the rolling `upgrade-agents` deployment (use `--include-current` when
needed); native process restart alone does not replace the files. After all
members acknowledge the fixed binaries, the existing `ALTER EXTENSION UPDATE`
path runs in every connectable non-template database where installed. SQL failure
keeps extension completion pending and retries without repeating completed member
deployments. Verify each database's extension version and follow the
[upstream-based index checks](postgres/profiles.md#pgvector-security-maintenance).

### Supabase set_user compatibility

The requested security-fix transition from set_user 4.2.0 to 4.3.0 is blocked on
Percona Noble packaging as of 2026-11-05; the old pin is retained, not declared
fixed. Managed bootstrap renders no set_user GUCs or token calls. Application
allowlists and role-transition users must nevertheless review the new
case-sensitive/group matching and reset-token behavior before rollout.
See [set_user maintenance](postgres/profiles.md#set_user-security-maintenance)
for the role/GUC audit, per-database SQL update and live-smoke requirements.

### Supabase component activation boundary

Upgrading only the control plane does **not** activate new release-locked
Supabase application versions. Existing gateway (Envoy), Auth, REST, Realtime,
Storage, imgproxy, postgres-meta, Studio, Edge Runtime, Supavisor, Logflare and
Vector members keep their persisted installed version and executable directory
during ordinary configuration reconciliation. This also applies to legacy
members created before v0.4.7; a new control-plane lock is not an installation
receipt. Newly created components use the current release lock.
If an incumbent's persisted version field is absent, its sealed applied
configuration supplies the pin. Missing or unusable applied installation
identity blocks ordinary reconciliation instead of selecting the current lock.

Activate the qualified artifacts with
`karactl -n NS clusters upgrade-agents NAME --wait --timeout 30m`.
Use `--include-current` to rerun preparation when agents already report the
current build, including same-version rebuilt bundles. The rollout retains the
PostgreSQL engine version, prepares the release-locked component artifacts in
each deployment, and activates their matching application configuration through
the existing component readiness and maintenance-window gates. A component
re-creation or other deployment that reruns preparation likewise uses the
current lock; a native process restart or config reload does not install it.
Optional singleton services can be unavailable during their deployment.
Target configuration is published only after successful deployment submission.
A rejected or lost submission response retains the incumbent configuration;
the durable rollout intent can retry preparation without first requiring an
unprepared replacement to become ready.
Supabase also persists a four-failure component deployment budget for each
native artifact/agent build. Failed preparation cannot turn into an unlimited
control-plane redeployment loop: exhaustion publishes `DeploymentRetriesExhausted`
with `clusters retry-deployments` guidance and stops submissions until re-armed.
Verify each enabled component's reported version and health after rollout,
including gateway routes, Storage access and the analytics/logging pipeline.

### Supabase image transformations

Imgproxy 3.31.4 → 4.0.21 fixes SVG/source-address and ICO vulnerabilities.
v4 changes ETag generation to origin-based validators and SVG rasterization
from 72 to 96 DPI; expect transient cache churn and different transformed SVG
bytes, not persistent object changes. The renderer explicitly disables the new
Last-Modified default to preserve v3 behavior, retaining SVG sanitization,
loopback/link-local guards and private S3 source access.
See [configuration parity](supabase/components.md#imgproxy-v4-configuration-parity).
Roll the qualified artifact through the
[member software procedure](supabase/operations.md#cluster-restart-and-member-software);
a process restart alone does not replace an installed bundle.


### Behaviour changes

- Temporal adds security patches **1.31.5** and **1.32.2** (default), retaining
  deprecated 1.31.4 and 1.32.1 for existing clusters and forward upgrades.
  **`ValidateWorkerDeploymentVersionComputeConfig` now requires namespace Write
  permission**, not just Read; clients relying on the old authorization can
  receive permission-denied errors. Review their policy before upgrading.
  Release-locked UI **2.58.0** replaces 2.56.0 without a rendered-config migration.
  Existing UI members retain their installed 2.56.0 version and executable path
  on a control-plane-only upgrade. Activate the current UI artifact through
  `upgrade-agents` (use `--include-current` if needed), which prepares it before
  the replacement member starts; the Temporal server version remains unchanged.
  Failed preparations have a durable four-attempt component budget, with a
  `DeploymentRetriesExhausted` condition and `clusters retry-deployments`
  re-arm instead of an indefinitely retained failed receipt. Cancelling before
  submission (or after rejection) leaves a healthy old UI untouched.
  Mirror `temporal_1.31.5_linux_amd64.tar.gz`,
  `temporal_1.32.2_linux_amd64.tar.gz` and `ui-server_2.58.0_linux_amd64.tar.gz`
  with their [locked checksums](../internal/engine/nativeartifacts/lock.json).
  See [Temporal upgrade notes](temporal/operations.md#v047-compatibility-notes).
- `upgrade-agents` now covers every member-agent engine; availability still
  depends on engine topology and backend rollout semantics.
- Weaviate offers `1.39.15` and `1.40.3` (default); `1.39.12` and `1.40.1`
  are deprecated but retained for existing clusters. Upgrade engine versions
  explicitly after agent rollout, then qualify full S3 backup/restore and
  queries; see [patch maintenance considerations](weaviate/operations.md#patch-maintenance-considerations)
  for startup rebuild I/O and internal backup authentication changes.
- Neo4j defaults to 2026.10.1; 2026.09.0 and 2026.10.0 remain offered with
  explicit backup-required forward paths. All three use Temurin JRE 21.0.13+9.
  Existing members need [agent rollout](neo4j/operations.md#agent-upgrades)
  to install the JRE; control-plane-only updates retain the installed Java home.
- PostgreSQL, Citus and Supabase support restart-free `postgres` rotation after
  agent upgrades. Peer-auth activation may need one configuration restart first.
- MySQL GR and PXC automatic confirmed-loss replacement is experimental and
  disabled by default; explicit replacement retains native survivor/donor proof.
  MySQL single still requires storage recovery or restore.
- Percona Server/PXC 8.0 entries are deprecated after community EOL on
  2026-04-01. Fresh empty creates/import targets must use 8.4; rejection identifies
  `spec.version` with reason `unsupported`. Existing 8.0 updates, backups/restores
  and offered upgrades to 8.4 remain available. Recovery-only `spec.dataSource`
  creates retain same-driver, same-major validation: recover 8.0 backup/PITR data
  into 8.0, verify it, then upgrade to 8.4. XtraBackup 8.0.35-36 and all historical
  artifact/backup metadata pins remain unchanged. See [MySQL lifecycle policy](mysql/README.md).
- MySQL/PXC automatic PITR re-anchoring is **ON by default**. Opt out with
  `spec.backup.pitr.autoReanchor: false`; additional fulls consume storage and IO.
- PITR responses add `intervals`; legacy summaries remain unchanged.
- PXC adds `GTIDHistoryPersistenceUnavailable`; update reason-based alerts.
- Eligible ProxySQL count/resource changes preserve serving on Zerops and
  Dokploy under [ADR 0069](adr/0069-proxysql-serving-scale-and-resource-changes.md).
  PostgreSQL/Supabase proxy count increases now wait for maintenance when
  serving cannot be preserved; legacy runtimes keep their maintenance-gated path.
- Temporal supports Zerops private IPv6 with upgraded agents. MySQL/PXC mutation
  isolation is per cluster, and Neo4j verifies credential recovery before marking
  bootstrap complete; existing false-completion markers need explicit repair.
- Exhausted initial member and component deployments now apply corrected desired
  resource bounds before a re-armed deployment, including on Zerops. Update the
  resources, then use `clusters retry-deployments`; a resource-only change does
  not re-arm deployment by itself. Existing service and volume identities are
  retained; data-bearing members must not be disabled or deleted as a workaround.
- Data MCP reconnects after a companion connection closes or its session is
  missing. Tool listing retries once; tool execution is never replayed. See
  [tool-list troubleshooting](data-mcp.md#tool-list-troubleshooting-v047).
- **MCP error results:** failed MCP tool calls return `isError` with the REST
  error body as text content and no longer include `structuredContent`, whether
  or not output schemas are enabled. Clients that read structured error details
  must read the text content instead. Successful calls keep `structuredContent`.
- **Data MCP companion refresh:** Data MCP companions (MongoDB and PostgreSQL) are redeployed once after the upgrade, honoring each cluster's maintenance window; Data MCP is briefly unavailable for that cluster. A failed companion preparation stops after four attempts, keeps the previous companion serving where possible, and reports `DeploymentRetriesExhausted` with the retry command. Automatic replacements emit `DataMcpCompanionRollout`. Only the private stateless companion is replaced, not database members or volumes. Replacement preparation downloads and checksum-verifies the locked bytes before the start-first backend promotes the replacement and retires the old companion. See [companion operations](data-mcp.md#companion-operations-and-status).

### Agent rollout now covers every member-agent engine

After upgrading the control plane to v0.4.7, run the existing rollout command for
MySQL (single or Group Replication), PXC, OpenBao (Raft or PostgreSQL storage),
Keycloak, Temporal, Neo4j Community and Weaviate:

```sh
karactl -n NS clusters upgrade-agents NAME --wait --timeout 30m
```

PostgreSQL (including Citus), Supabase, MongoDB, RabbitMQ and Elasticsearch retain
their existing support. Existing v0.4.3 agents become upgradable once the control
plane is v0.4.7; unknown legacy identities are also eligible. This installs the
frozen control-plane agent build without changing applied engine versions,
resources, bindings or persistent runtime/volume identities. Supabase's separately
release-locked application components and Temporal's separately locked UI
activate through deployment preparation, as described in their activation and
upgrade notes; these are distinct from the database/Temporal server version.
It replaces earlier resource-change/re-creation workarounds for installing agent
fixes; a native process restart still does not replace its agent.

- **MySQL/PXC:** ProxySQL first, with every live instance acknowledging build,
  configuration/credentials and writer health; non-writer data members next and
  the initially observed writer last. GR checks ONLINE membership, surviving
  majority and GTID-safe election with proxy reader/writer admission. PXC checks
  Primary/Synced/ready/connected membership, full component size and history
  continuity, then excludes the routed writer only after a safe successor is
  available and waits for proxy handoff before deployment. ProxySQL uses serial,
  native-serving-gated start-first replacement on Dokploy only when the running,
  deployed, volume-less incumbent and incoming build both carry the serving-probe
  marker; spare capacity is required. A legacy/unmarked first upgrade uses
  ordinary whole-service deployment and installs the marker for subsequent
  rollouts. Cold/stopped proxies also use ordinary deployment. Zerops uses
  ordinary whole-service deployment; its count/resource preservation contract
  does not cover new builds. Nomad uses a canary followed by potentially
  destructive replacements with TCP-only admission. Those ordinary paths can
  interrupt the whole proxy endpoint even with replicas. MySQL/PXC persist the
  chosen policy before submission and retain it on receipt-missing replay,
  including `--include-current`. MySQL single has database downtime. A planned
  GR primary handoff can fail in-flight or new statements while routing
  converges; see [MySQL agent rollout](mysql/operations.md#agent-rollout).
  If runtime recovery or confirmed loss appears mid-rollout, the serialized
  operation settles its current member/routing drain without a new deployment,
  then retires with a recovery diagnostic so lost-volume replacement can proceed.
  Confirmed-lost members remain excluded from routing, never readmitted as healthy.
- **OpenBao:** standbys/inactive members before the initial active; native
  step-down must establish another active before its deployment. Raft requires
  healthy unsealed voters and surviving quorum/autopilot gates. PostgreSQL
  storage uses native HA semantics without Raft APIs. Native recovery and
  Shamir/auto-unseal finish before the next member. A singleton is unavailable
  during deployment and until unseal completes.
- **Keycloak:** the single runtime is redeployed and must acknowledge native
  readiness, configuration and the exact build. Authentication has downtime
  even when its source PostgreSQL is HA; unexpected multi-instance services
  are refused.
- **Temporal:** history → matching → frontend → worker → server → UI, skipping
  disabled components. Every expected instance must acknowledge native
  readiness, applied version/configuration and the exact build; history/server
  settle for one minute. Deployment addresses logical component services, not
  individual instances. Singleton components have downtime, and platform
  replacement policy controls availability with replicas.
- **Neo4j Community:** its singleton redeployment interrupts all database access
  until native readiness and exact-build acknowledgement.
- **Weaviate:** non-voters, then non-leader voters, then the observed Raft
  leader. Healthy synchronized peers, surviving voting majority, applied minimum
  and collection replication factors of at least two, and native proof of no
  sole physical shard replica gate clustered rollout. Unknown shard evidence
  refuses disruption. Singletons bypass impossible redundancy checks but have
  database downtime.

Use an appropriate [maintenance window](operations.md#maintenance-windows) and
verify native replication/readiness, every member's build and client reconnects.
Each new member/component disruption and deferred submission rechecks the window
and native safety; already-started handoff/deployment recovery finishes after
window closure. Current agents are skipped unless `--include-current` is set,
and completed checkpoint entries are not repeated. Cancellation stops new work,
not completed deployments: known receipts require native readiness and exact
target-build acknowledgement. An intent with a missing receipt may have reached
the backend; cancellation settles native readiness on the old or target build
without resubmitting.

Receipt-missing replay adopts a ready target deployment where persisted build
transition and instance evidence prove completion. An explicit same-build
`--include-current` refresh cannot prove acceptance from unchanged build identity:
there is no backend deployment lookup by member/request identity, so replay can
cause another window/native-gated deployment and interruption. A changed
control-plane executable never silently retargets an existing rollout; follow
its diagnostic and re-run for the new target after recovery. See
[member-agent rollout](#rolling-out-member-agents) and
[ADR 0065](adr/0065-agent-upgrade-all-member-engines.md).

### PostgreSQL administrator password rotation

PostgreSQL single/Patroni HA, Citus and Supabase now support in-place rotation of
the revealed `postgres` password. Upgrade the control plane, then complete
`upgrade-agents` on every data member first. The new agents use exact-owner
Unix-socket peer authentication for local management, with an agent-managed
identity map outside PGDATA so rebuilds/restores cannot remove it. Warm activation
stages and observes a reload of the currently active map before publishing peer
HBA rules. Adopting the external `ident_file` startup path may require the ordinary
pending configuration restart during the preceding agent rollout; rotation itself
is restart-free. Admission remains closed until every data member proves
successful password-independent local socket access. Older or unobservable data
agents receive HTTP 409 with `upgrade-agents` guidance; remote SCRAM/TLS
authentication is unchanged.

```sh
karactl -n NS system-users rotate NAME postgres --yes
```

Finish structural/rolling work first and follow the returned operation through
all member, proxy and component acknowledgements before revealing/distributing
the replacement credential. A sealed transition journal and native receipts
recover partially committed rotations forward; do not repair one with manual
`ALTER ROLE`, secret edits or another rotation. Rotation itself does not restart
PostgreSQL, including a singleton. Supabase credential consumers restart and may
be briefly unavailable; Citus can have transient cross-group authentication
failures while local role changes and outbound authentication converge.

This is not a dual-password grace period. Administrator clients must use the
replacement on reconnect; existing sessions are not terminated. Rotation is not
session revocation or remediation of arbitrary superuser compromise. Bindings
and Data MCP use separate identities and are unchanged; other engine-owned
PostgreSQL identities remain unsupported on this route. See
[administrator rotation and containment](postgres/access-and-tls.md#administrator-credential-containment).

### Automatic MySQL/PXC lost-volume replacement

Confirmed loss of initialized storage leaves MySQL Group Replication and PXC
Degraded by default, with `VolumeReplacementSafe=False` and reason
`AutomaticReplacementDisabled`. Verify healthy survivors, then run
`karactl clusters replace-member CLUSTER MEMBER --wait`.
To enable experimental automatic add-before-remove replacement, set
`KARAKORAM_MYSQL_AUTO_REPLACE_LOST_VOLUMES=true` and restart the control plane.
Testing observed GR quorum loss after an automatic confirmed-loss replacement;
the cause is not yet known. Explicit replacement is unchanged.
When opted in, `VolumeReplacementSafe=False` explains native admission refusal.
An unreachable agent, identity conflict, unknown mount, stale observation or
storage API error is not proof of loss and never authorizes initialization of
empty old storage.

GR requires fresh ONLINE survivors in the configured group, a writable primary,
a configured-member majority and a donor containing the lost member's last
persisted GTIDs when known. **Upgrade GR agents before relying on automatic
recovery:** the new native `GroupName` observation is required for same-group
proof. Old agents continue ordinary lifecycle behavior but this new admission
fails closed without that evidence. Upgrade deployed agents for the pre-start
sentinel gate as well.

PXC requires fresh Primary/Synced/ready/connected survivors in one state UUID,
a configured-member majority, compatible GTID histories, no active
history-continuity latch and a qualified SST donor. A lost formation seed or
pinned donor moves to a qualified survivor; existing join certification still
owns SST and final routing fences.
Outstanding rollout drains must settle before replacement admission; a withdrawn
survivor is not an eligible donor or writer. An active rollout yields through its
recovery cleanup rather than being preempted by automatic replacement.

The new member and volume join before the old identity retires. Proxy exclusion
must be acknowledged, the exact old runtime stopped, and native membership
removal proved before resource cleanup; retirement does not contact lost mysqld
or initialize an empty old volume. One deterministic operation is used per lost
incarnation. Terminal replacements are not automatically retried: inspect and
resolve blockers, then request managed replacement if needed.

MySQL single cannot automatically recover lost data. Without a safe surviving
copy, recover original storage or restore a verified backup into a new cluster.
There is no forced quorum/bootstrap, row-equivalence proof, zero-loss or RTO
guarantee. See [MySQL operations](mysql/operations.md).

### MySQL and PXC automatic PITR re-anchoring

With PITR and backup storage configured, the control plane now automatically
takes a full backup after a physical archive-writer change (including failover,
rolling maintenance and resource relaunch). This is **ON by default** when
`spec.backup.pitr.autoReanchor` is omitted. Extra full backups consume storage
and IO and can increase the writer's peak memory; review capacity before upgrade.
Set `spec.backup.pitr.autoReanchor: false` to retain manual baseline management.

The writer must remain stable for five minutes after non-backup cluster
operations finish. A pending/running backup blocks the automatic full, and a
one-hour cooldown limits re-anchor fulls. Returning to the newest full's source
does not queue another full. Coverage stays frozen until a new full succeeds
and verifies; the gap between the old safe endpoint and new baseline cannot be
restored. Foreign-writer binary logs remain ineligible for replay.

Successful re-anchor fulls count toward the full schedule with the largest
positive `retainCount` (schedule name breaks ties), or retain the newest two
when no full schedule supplies a count. Normal deletion preserves backup
chains, the newest full anchoring the live/extending interval, and active restore/
clone references, so protected backups can temporarily exceed retention. Older
recovery ranges expire with the fulls removed by retention. Post-restore full
behavior does not change. `PITRReanchorTriggered` and reason-specific
`PITRReanchorSkipped…` cluster events explain enqueue and deferral; skips are
emitted on reason change rather than every reconcile.
See [MySQL backups](mysql/backups.md#automatic-writer-re-anchoring).

### All restorable PITR intervals

`GET /clusters/{cluster}/pitr` adds optional `intervals`, with required `start`
and `end` and optional `backup` (the anchoring full backup name). Legacy
`available`, `start`, `end`, and `message` retain their previous values.
Clients that reject unknown response properties must accept the additive field.
The CLI and restore UI show all ranges and allow earlier covered points;
uncovered gaps remain rejected. MySQL/PXC lists preserve each full's provenance;
remembered PXC coverage and other engines omit provenance when unavailable.
No node-agent protocol or backup artifact migration is required.

### PXC GTID history safety observation

PXC `GTIDHistorySafe=Unknown` now separates comparison unavailability
(`QuorumComparisonUnavailable`) from journal/state/audit persistence failures
(`GTIDHistoryPersistenceUnavailable`). Update condition-reason alert filters.
Comparison messages include secret-safe member outcome classes and distinguish
majority shortage, observation budget and observation-lock contention; do not
parse the message as a stable wire schema. No agent protocol change is required.
PXC Status starts quorum checking concurrently with required readiness reads,
inside the existing five-second status sweep. Sampling has a separate four-second
pass reservation matching the agent read timeout, after a bounded lock wait.
Subsequent same-pass Sync checks reuse completed samples. Parent cancellation
during journal work is classified as observation-budget exhaustion, not a
persistence outage. Synchronous backup/PITR requests can spend up to six seconds
on comparison lock waits and sampling, plus journal/audit I/O and other endpoint
work, subject to parent deadlines; this is not an HTTP latency guarantee.
Configured-majority, complete-observation safe-watermark requirements and durable
divergence latches are unchanged.
Cancellation before journal loading/allowance evaluation completes does not
reclassify an acknowledged frozen-history difference as new divergence.
Full backups remain admitted on typed comparison unavailability, but persistence
failures still fail closed; incrementals/PITR cannot gain unproven continuity.

### Proxy count and resource changes

Serving preservation is an explicit backend, request and deployed-runtime
contract, not a general zero-downtime guarantee. Every requested live proxy must
acknowledge configuration and native writer readiness before count/resources
commit. All resource changes and count reductions still wait for maintenance;
only serving-preserving count increases bypass the window. PostgreSQL and the
Supabase core now also defer increases when capability or runtime approval is absent.

- **Zerops:** count-only changes retain incumbent Linux containers. Resource-only
  CPU/RAM changes qualify only with unchanged CPU mode and disk bounds.
  Unexpected incumbent replacement fails with `INCUMBENTS_REPLACED`; inspect
  platform processes and readiness before retrying. Unresolved autoscaling
  submissions block superseding changes and Stop.
- **Dokploy:** marked, successfully deployed runtimes use serial start-first
  replacement with native serving admission and spare capacity. The new probe
  requires configuration application, a running proxy and a writable database
  before initial task admission; PostgreSQL additionally verifies frontend
  availability and active managed users. Old agents fail the probe closed.
  Legacy unmarked runtimes retain the disruptive/maintenance-gated path until
  an intended deployment installs the supporting agent and marker.
- **Nomad:** count-only scaling retains incumbents; resource rollouts do not
  provide the serving-preservation contract. MySQL/PXC resource changes retain
  window-gated stop/redeploy; PostgreSQL/Supabase use maintenance-gated provider
  resource rollouts.

For MySQL/PXC, combined count/resource, version and startup/runtime changes keep
the disruptive path. Upgrading the control plane alone does not redeploy a proxy;
the new marker is excluded from its acknowledged runtime hash, preserving
existing Nomad count-only eligibility. Replacing or removing an instance can
disconnect existing sessions even on an eligible serving-preserving path.
Applications must reconnect and handle uncertain transaction outcomes.
Live-provider acceptance of these paths remains pending. See
[ADR 0069](adr/0069-proxysql-serving-scale-and-resource-changes.md),
[Zerops](zerops.md#resources-and-autoscaling) and
[Dokploy](dokploy.md#proxysql-count-and-resource-updates).

### Temporal on Zerops private IPv6

Temporal now uses private IPv6 on Zerops; Dokploy and Nomad retain private IPv4.
Upgrade deployed agents with `upgrade-agents` before expecting IPv6 operation.
Older agents receive HTTP 409 `AgentUpgradeRequired` with `temporal-ipv6` guidance,
not a partially compatible configuration. Every replica must reach its peers and
the bound PostgreSQL writer over private IPv6; application workers must reach
the private frontend. Public IPv4 is not a substitute. No database, artifact or
backup migration is required. Bounded live checks on Zerops covered
combined-layout workflows over private IPv6, an agent refresh and PostgreSQL
writer failover; the split layout, optional UI OIDC and complete lifecycle
acceptance remain unverified. See [Temporal](temporal/README.md#limits) and
[ADR 0068](adr/0068-temporal-zerops.md).

### MySQL/PXC per-cluster mutation isolation

Slow recovery now holds only the normalized namespace/cluster mutation gate, so
unrelated cluster access and maintenance can proceed. Backup-object deletion
and sealed-snapshot cleanup still coordinate separately with active control-plane
backup/restore steps, including across clusters. This does not lock asynchronous
agent uploads or external object-storage clients. No agent upgrade is required
for the control-plane lock change. See
[recovery coordination](mysql/operations.md#full-outage-recovery).

### Neo4j legacy credential recovery

New agents independently clear and verify `passwordChangeRequired=false` before
publishing the bootstrap marker, including after an unchanged-password rejection.
Upgrade both the control plane and member agent. A v0.4.3 false-completion marker
can survive an upgrade: the server may run while managed queries report
`Neo.ClientError.Security.CredentialsExpired`. Ordinary reload, restart or
system-user rotation does not repair that marker.

Arrange a maintenance outage and support-assisted private agent API/volume access.
Preserve the volume, sealed password, marker copy and redacted diagnostics;
finish all active/recovery-pending jobs and prevent new scheduled backups and
lifecycle submissions. Explicitly stop through the agent process API and prove
the server and recovery JVM are absent. Only for this confirmed legacy condition,
remove `karakoram/neo4j/credentials-initialized.json`, leaving stores, applied
state, controls and receipts intact. Explicitly start through the same API,
releasing only the operator hold just taken; allow the recovery request budget.
Verify authenticated readiness, matching configuration/restart token and application
queries. Stop retrying starts on failure and repair the underlying diagnostic.
Follow the full [stopped-member procedure](neo4j/operations.md#legacy-bootstrap-marker-with-expired-credentials);
marker deletion is not a general readiness or lost-password workaround.

### Known issues in v0.4.7

These descriptions and workarounds are historical v0.4.7 guidance. The fixed
dispositions below describe v0.4.8 rather than changing the historical failures.

- **On Dokploy, a ProxySQL count or resource change can leave a cluster stuck**
  (v0.4.7 only; ProxySQL in front of MySQL, PXC, PostgreSQL and Supabase). If the
  new proxy task does not become healthy (for example, the node lacks CPU or
  memory), Docker Swarm pauses the update. The existing proxies keep serving,
  but the operation stays Running for about 15 minutes before failing, cannot
  be cancelled, and afterwards the cluster stays Degraded: changing the proxy
  settings back is accepted but not applied, and agent upgrades are blocked.
  There is no supported way to recover in v0.4.7. Before changing proxy count or
  resources, make sure the nodes have spare capacity for the new proxy tasks.
  If an update pauses, keep the existing proxies running and do not edit the
  Swarm service by hand.
  **Fixed in v0.4.8:** paused updates fail promptly; after repairing the cause,
  submit a different proxy count/resource target to clear the failed receipt.
  See [proxy recovery](#proxy-count-and-resource-changes).

- **Cancelling the replacement of a lost MySQL Group Replication member can
  leave the cluster stuck** (also present in v0.4.0–v0.4.6). When replacing a
  member whose volume is lost (the member and its data are gone), cancelling
  the `karactl clusters replace-member CLUSTER MEMBER` operation with
  `karactl operations cancel ID` after the new member has joined the group makes
  the rollback wait for the lost member to come back, which never happens.
  The cluster keeps serving, but new replacements and agent upgrades are
  blocked and there is no supported way to recover in this release. Do not
  cancel these replacements once they have started; let them finish. If a
  cluster is already stuck on v0.4.7, keep it serving and take a full backup.
  **Fixed in v0.4.8:** joined confirmed-loss replacement completes forward on
  cancellation; unjoined work rolls back its allocation and retains loss for a
  fresh replacement. Existing rollback owners settle after upgrade; cancellation
  acknowledgement does not mean recovery is finished. See
  [GR capability and legacy recovery](#gr-capability-and-legacy-recovery).
- **On Dokploy, a Neo4j cluster created with 1 GiB of memory never becomes ready**
  (v0.4.7). At the documented minimum of 1 GiB, a new single-member Neo4j cluster
  on Dokploy runs out of memory and CPU while starting: it never reports Ready
  and the create operation fails after about 40 minutes. Workaround: create
  Neo4j clusters on Dokploy with at least 2 GiB of memory. Existing clusters are
  not affected. **Fixed in v0.4.8:** new clusters/resource edits require 2 GiB
  on all backends; unchanged legacy shapes are grandfathered, not automatically
  resized. See [Neo4j sizing guidance](#neo4j-minimum-memory).
- **On Dokploy, replacing a data member can put two members on the same node**
  (MySQL Group Replication and PXC also in v0.4.0–v0.4.6; PostgreSQL HA and
  Supabase also in v0.2.0–v0.4.6). After `karactl clusters replace-member` — for
  a healthy member or one whose volume was lost — the new member can be placed
  on a Swarm node that already runs another data member of the same cluster,
  even when another node has room. The operation still reports success, but
  losing that node then loses quorum. Check placement after every replacement
  with `karactl clusters members CLUSTER` (the `PLACEMENTGROUP` column shows the
  node). If two data members share a node, there is no reliable way to move one
  in this release: replacing again or draining nodes can make it worse. Keep
  the cluster as it is on v0.4.7 and take a fresh full backup.
  **Fixed in v0.4.8:** new Dokploy/Nomad imports preserve predecessor identity and
  strictly exclude survivor nodes; existing co-location needs inspection and
  repair. Healthy GR replacement requires upgraded full-view survivor agents.
  See [placement repair](#check-database-placement-after-v047-replacements).
- **Karakoram's status checks cause heavy disk writes on the Dokploy manager**
  (also present in v0.2.0–v0.4.6). Karakoram reads Dokploy's API several times per
  second for each cluster, and Dokploy records every API-key use in its own
  database, so each request writes to Dokploy's database. With a few clusters
  this is around 1 MB/s and several hundred write operations per second on the
  Dokploy manager — more with Group Replication clusters and several proxies —
  which can slow down Dokploy on small virtual machines with slow disks and can
  make clusters briefly report `Degraded`. Your data is not affected.
  Workaround: run the Dokploy manager on fast SSD storage and give it enough
  disk throughput for the number of clusters you manage; avoid co-locating
  other disk-heavy workloads on the manager.
  **Fixed in v0.4.8:** control-plane snapshots and parallel observation reduce
  request amplification, not Dokploy accounting writes. Infrastructure sizing
  remains relevant; historical I/O figures are not a fixed-version guarantee.
  See [observation caveats](#companion-refresh-and-dokploy-observation).
- **On Dokploy, a ProxySQL rollout whose new proxy instance fails to start can
  take down the existing proxies** (v0.4.7). For proxies using the
  serving-preserving rollout, when changing the proxy count or resources, or
  running `karactl clusters upgrade-agents CLUSTER --include-current`, Karakoram
  replaces proxy instances one at a time and should stop if a new instance
  never becomes healthy. If the new instance fails its health check, Docker
  Swarm can still treat it as finished and shut down an existing proxy instance,
  causing a proxy outage until the change is undone. Workaround: before such a
  change, make sure the Dokploy nodes have enough free CPU and memory for one
  extra proxy instance and that the cluster is healthy; run the change in a
  maintenance window; if a proxy rollout stops or fails, don't resume the Docker
  Swarm service update by hand.
  **Fixed in v0.4.8:** fixed agents plus the fixed control plane provide opted-in
  task/revision-bound admission. Upgrade old marked proxies before count/resource
  changes; surge capacity, no manual resume and daemon/executor-restart caveats
  remain. See [proxy recovery](#proxysql-admission-startup-and-recovery).
- **MySQL Group Replication clusters on Dokploy can briefly report Degraded, and
  adding proxy instances can be reported as failed** (also present in
  v0.4.3–v0.4.6). The Dokploy provider reads the state of each member and proxy
  one at a time, so on clusters with several ProxySQL instances a status check
  can run out of time: the cluster occasionally reports `Degraded` for a moment,
  and changing the proxy count (for example from 2 to 3) can end as failed after
  about 20 minutes even though Dokploy finished the change and all proxy
  instances are healthy. Your data and client traffic are not affected.
  Workaround: keep two proxy instances on Dokploy; if a proxy-count change
  fails, check the cluster's status and the Dokploy service before retrying,
  because the change has usually been applied.
  **Fixed in v0.4.8:** concurrent bounded observations and snapshots correct the
  serialized-read mechanism, not all latency/network causes. The two-proxy
  workaround is not current v0.4.8 advice. See
  [observation changes](#companion-refresh-and-dokploy-observation).

## Upgrading to v0.4.6

v0.4.6 is a security hotfix of v0.4.5 with one change: the
`github.com/klauspost/compress` library, used by the MongoDB driver, is updated
from v1.18.7 to v1.20.2. It does not include the features planned for the next
feature release, v0.4.7.

### Fixed in v0.4.6

**Control-plane crash from a crafted MongoDB response
([GO-2026-6865](https://pkg.go.dev/vuln/GO-2026-6865), also
[GHSA-fpmh-frq8-7rj8](https://github.com/klauspost/compress/security/advisories/GHSA-fpmh-frq8-7rj8)):**
in v0.4.5 and earlier, a crafted zstd-compressed MongoDB wire response could
make the decoder panic instead of returning an error. The control plane
administers MongoDB clusters from background reconciliation, so such a panic
stops the whole control-plane process: the API, UI and management of every
cluster it serves are interrupted until it restarts, and a server that keeps
sending crafted responses can repeat the crash. The impact is denial of
service; no code execution or data corruption is known. A control-plane crash
does not stop database processes directly, but management and in-flight
operations are interrupted until the control plane is back.

**Who is affected:** installations running v0.4.5 or earlier that manage
MongoDB (PSMDB) clusters. An attacker must get crafted responses onto a
control-plane connection to a managed `mongod` or `mongos`, either by
compromising or controlling that server, or by impersonating it or injecting
into its connection. With the default `requireTLS` mode, certificate
verification stops ordinary network attackers, but a compromised server can
still send crafted responses over its valid TLS connection. In `disabled`,
`allowTLS` and `preferTLS` modes the control plane can use plaintext
connections, so an attacker who can alter that network path can also inject
responses. Storing data, ordinary database credentials or Karakoram API access
alone are not sufficient. Disabling MongoDB network compression is not a
mitigation: the driver decodes a compressed response from the server even
when compression was not negotiated. Installations without MongoDB clusters
never decode MongoDB responses and are not exposed; upgrading still removes the
vulnerable library.

### Required rollout

1. **Back up state, then upgrade the control plane.** Use the
   [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup), preserve
   the original `KARAKORAM_MASTER_KEY` separately, and retain the old executable
   with its pre-upgrade state. Deploy v0.4.6 through your provider's documented
   singleton control-plane deployment path. This step applies the fix. No
   database restart, engine upgrade, backup or configuration change is needed,
   and v0.4.5 member agents keep working with the v0.4.6 control plane.
2. **Optionally roll MongoDB member agents.** Member agents run the same
   executable and also contain the library, but they only connect to the
   `mongod` or `mongos` on their own member over loopback. Exploiting this
   path requires control of that local endpoint or the ability to inject its
   replies, subject to the connection's TLS verification. The process that
   crashes is that member's agent, not the shared control plane; the effect on
   the cluster depends on the member's role and quorum. To remove the
   vulnerable library from MongoDB members as well, run
   `karactl clusters upgrade-agents NAME --wait` for each
   MongoDB cluster in a maintenance window; see
   [rolling out member agents](#rolling-out-member-agents). Agents for other
   engines never open MongoDB connections and need no relaunch. `karactl` does
   not contain the library.

After the control-plane upgrade, reachable agents reporting the older build
appear in `status.agents` as outdated (unreachable or unreported builds as
unknown) until they are rolled. The `AgentsCurrent` condition is
informational and does not make clusters unready or Degraded. If you enabled
automatic agent rollout (`karactl settings agent-rollout`), it rolls outdated
agents of supported engines after the upgrade, as for any release.

### Known issues in v0.4.6

v0.4.6 changes nothing else. The
[continuing limitations](limitations.md#continuing-limitations) and all
[known issues in v0.4.5](#known-issues-in-v045) remain present, including Data
MCP failures after a companion connection closes, Zerops failed-deployment
resource recovery and false PXC GTID-history incidents. Their workarounds still
apply. Those three issues are **fixed in v0.4.7**, including failed-deployment
resource recovery on Zerops, Dokploy and Nomad; the other continuing limitations
keep their individually documented status.

- **Creating a Percona XtraDB Cluster on Nomad fails** (also present in
  v0.4.3–v0.4.5; Zerops and Dokploy are not affected). A new three-member PXC
  cluster on the Nomad provider fails after about 5–9 minutes with
  `GTIDMappingCertificationBlocked`: the last member is never started and the
  failed create shuts down another member. No existing data is affected; this
  happens only when creating a cluster. On v0.4.3–v0.4.6, use MySQL Group
  Replication on Nomad, or create PXC clusters on Zerops or Dokploy.
  **Fixed in v0.4.7:** formation no longer blocks on a fenced ProxySQL
  deployment. Fresh three-member PXC creation with ProxySQL was live-verified
  on Nomad; see [formation upgrade guidance](#pxc-formation-behind-pending-proxy-serving-health).
- **A cluster can report Degraded while its Data MCP companion is unresponsive**
  (also present in v0.4.1–v0.4.5). If a cluster's Data MCP companion accepts
  connections but stops answering — for example while it restarts or when it
  hangs — the control plane waits too long for it, the cluster's health
  observations go stale, and the cluster reports `Degraded` (`ObservationStale`)
  for as long as the companion stays unresponsive, even though the database
  itself is healthy. It recovers by itself once the companion answers again.
  If it persists, disable Data MCP for that cluster
  (`spec.dataMcp.enabled: false`, applied with
  `karactl clusters patch CLUSTER -f change.yaml`) and re-enable it later.
  **Fixed in v0.4.7:** companion readiness observation is bounded by the
  observation timeout, so native availability stays fresh and Data MCP is
  reported unavailable while the companion is unresponsive.

## Upgrading to v0.4.5

v0.4.5 is a hotfix of v0.4.4 with only two MySQL fixes. It does not include
the features released in v0.4.7.

### Fixed in v0.4.5

**MySQL PITR coverage after copied-data provisioning:** in v0.4.0–v0.4.4,
the recovery-window end could stop advancing while binlogs kept uploading and
`BinlogArchive` stayed healthy. This commonly appears after a writer change in
Group Replication or PXC to a member provisioned by native clone or state
snapshot transfer (SST), including during ordinary cluster formation. A
single-instance writer restored from a physical backup, or the initial writer
of a restored/cloned cluster, can also be affected without a writer change.
The trigger is executed GTID history imported without corresponding local
binlog history, not log expiration alone. Repeated full backups on that writer
did not repair forward coverage: each could stop at its own snapshot file.

Coverage and restore selection now verify the exact physical snapshot
file/position and distinguish imported baseline GTIDs from logged dependencies.
Missing anchors or post-snapshot dependencies still fail closed. Verified
same-source chains that cannot extend now report `PITR=False/CoverageBlocked`
with the reason and verified endpoint, plus a deduplicated Warning event.
The safe prefix remains usable; `BinlogArchive` independently reports upload
health and can remain `True`. Idle time alone is not a coverage failure.
This does not remove the [physical-writer boundary](mysql/backups.md#binary-log-pitr):
a new writer's uploads cannot extend a different writer's full baseline.

**Fresh-cluster first full backup:** in v0.4.0–v0.4.4, a full could fail with
`metadata_invalid` when the source had no executed GTIDs. XtraBackup legitimately
writes only the binlog filename and position in that case, but the parser
required a third GTID column. This is shared by MySQL single-instance, Group
Replication and PXC, on all providers, whenever executed GTIDs are empty; not
every fresh clustered source is empty. Managed bootstrap account creation is
not binary-logged, so it does not ensure a nonempty set. Before the fix, a
committed binary-logged application write followed by a new full could avoid
this parser error. Corrected agents require no such write: filename and position
remain mandatory and validated, while an absent/empty GTID column is accepted.

### Required rollout and recovery

1. **Back up state, then upgrade the control plane first.** Use the
   [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup), preserve
   the original `KARAKORAM_MASTER_KEY` separately, and retain the old executable
   with its pre-upgrade state. Deploy v0.4.5 through your provider's documented
   singleton control-plane deployment path. Control-plane rollback does not
   undo database or infrastructure changes.
2. **Relaunch every affected MySQL/PXC data-member agent.** In a maintenance
   window, apply a genuinely changed CPU or memory resource target; rolling
   maintenance stops and redeploys members with the current executable. An
   unchanged patch, control-plane restart or `karactl clusters restart` does
   not replace agents. MySQL/PXC have no supported `upgrade-agents` operation
   in v0.4.5, although the shared CLI exposes that command for other engines.
   Check provider capacity, keep a validated backup and independent recovery
   path, and expect writer-handoff interruptions (downtime for a singleton).
   Do not attempt the roll without a healthy majority. Verify each member's
   build, replication and client writes. See the
   [resource-change relaunch guidance](#existing-clusters-and-member-artifacts).
   These fixes need data-member agents, not a PXC proxy rollout.
3. **Re-enable PITR if disabled** (`spec.backup.pitr.enabled: true`) only after
   all affected data agents run the corrected code. Retain native binlogs and
   archived objects until coverage checks finish.
4. **Take a successful new full backup from the current writer.** Failed first
   fulls stay failed; the upgrade neither repairs them nor requires an application
   write. Request a new full and observe its completion. For PITR, the corrected
   agent versions its coverage-cache identity and rebuilds existing fulls' proofs.
   If the physical writer/source stays the same and the exact anchor and
   contiguous archived dependencies remain available, an existing full blocked
   only by imported baseline history can resume extending across the formerly
   stuck period. This is conditional, not retroactive recovery of missing logs.
   On Group Replication and PXC the rolling relaunch moves the writer at least
   once, so establish coverage from the new full; do not assume PITR beyond the
   old verified endpoint into the old-to-new-baseline gap is recovered.
   Preserve earlier backups and proven windows. A new full cannot reconstruct
   purged unarchived logs or bridge different physical sources.
5. **Verify the window advances** with `karactl backups pitr-window CLUSTER`
   after post-backup writes and completed archive uploads/coverage checks.
   Confirm its end passes the previously frozen endpoint. Inspect `PITR`,
   `BinlogArchive`, Warning events and writer-agent diagnostics; green upload
   health or a zero-width full-backup snapshot is not continuous PITR coverage.

Do not downgrade the control plane after rolling agents; keep the upgraded
control plane with the corrected agents. In particular, versions below v0.4.3
cannot negotiate the pending/freshness coverage contract.

### Known issues in v0.4.5

The [continuing limitations](#continuing-limitations) from v0.4.3
still apply in v0.4.5, including physical-writer PITR boundaries, the single-interval
window display, PostgreSQL administrator-password rotation, Temporal on Zerops
and incomplete Nomad live coverage. The Zerops failed-deployment resource
recovery and false PXC GTID-history incident issues documented
[below](#known-issues-in-v044) also remain unfixed in v0.4.5; their workarounds
still apply in v0.4.5. The PXC issue is **fixed in v0.4.7**, not included in
the v0.4.5 or v0.4.6 hotfixes. Failed-first-deployment resource recovery on
Zerops, Dokploy and Nomad is **fixed in v0.4.7**; it is not included in the
v0.4.5 or v0.4.6 hotfixes. The documented workarounds remain necessary on
those releases.

- **Data MCP can keep failing with `tool listing unavailable` after its
  connection to a cluster's companion drops** (also present in v0.3.0–v0.4.4).
  If the connection between the control plane and a cluster's Data MCP
  companion closes — for example after the companion restarts or a network
  interruption — the gateway keeps reusing the closed connection, and Data MCP
  requests for that cluster keep failing; the stale connection is only dropped
  after about 10 minutes with no Data MCP requests for that cluster, or when
  the control plane restarts. Your databases are not affected. Workaround: stop
  sending Data MCP requests to that cluster for about 10 minutes, or restart
  the Karakoram control plane (clusters keep running while it restarts).
  **Fixed in v0.4.7:** the gateway reconnects after the companion connection
  closes, so no control-plane restart is needed.

## Upgrading to v0.4.4

v0.4.4 is a hotfix of v0.4.3 with only two MySQL/PXC point-in-time recovery
corrections. It does not include the features released in v0.4.7.

### Fixed in v0.4.4

**MySQL PITR archiving after schema/account statements:** the parser now accepts
native `mysqlbinlog` space/tab Query headers, including standalone queries
without Xid. In v0.4.3, such events could silently stop archiving while the
`BinlogArchive` condition still appeared healthy. MySQL single-instance, Group
Replication and PXC clusters with PITR enabled are affected; full backups are
not affected. Successful schema/account changes alone do not prove archive
health: some statements carry Xid and could already continue archiving.

Failed decode, rotation or upload attempts now report
`BinlogArchive=False/ArchiveFailed`, a Warning event with reason `ArchiveFailed`,
and an agent log diagnostic.
The condition has a safe error category; the writer agent log adds sanitized
native or client causes (for example, a MySQL error number or an S3 status).
Only a completed successful pass clears the attempt failure; skipped passes
keep it. Only uploaded, contiguous metadata can extend the recovery window.

**MySQL point-in-time restore and clone admission across clock skew:** in
v0.4.3, a point-in-time restore or clone could be rejected with "agent returned
PITR verification predating restore admission" whenever the writer node's clock
was even milliseconds behind the control plane's. The control plane now trusts
the agent's per-request freshness attestation instead of comparing timestamps
from different clocks. This part is control-plane-only: it works with v0.4.3
agents and needs no agent relaunch. Earlier releases lack the check.

### Required rollout and recovery

1. **Back up state, then upgrade the control plane first.** Use the
   [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup), preserve
   the original `KARAKORAM_MASTER_KEY` separately, and retain the old executable
   with its pre-upgrade state. Deploy v0.4.4 through your provider's documented
   singleton control-plane deployment path. Control-plane rollback does not
   undo database or infrastructure changes.
2. **Relaunch every affected MySQL/PXC data-member agent.** In a maintenance
   window, apply a genuinely changed CPU or memory resource target; rolling
   maintenance stops and redeploys members with the current executable. An
   unchanged patch, control-plane restart or `karactl clusters restart` does
   not replace agents. MySQL/PXC have no `upgrade-agents` command in v0.4.4.
   Check provider capacity, keep a validated backup and independent recovery
   path, and expect writer-handoff interruptions (downtime for a singleton).
   Do not attempt the roll without a healthy majority. Verify each member's
   build, replication and client writes. See the
   [resource-change relaunch guidance](#existing-clusters-and-member-artifacts).
   This fix is in data-member agents; it does not require a PXC proxy rollout.
   Earlier proxy certification/TLS rollout requirements remain separate.
3. **Re-enable PITR if disabled**, setting `spec.backup.pitr.enabled: true`
   only after all affected data agents run the corrected code. Retain native
   binlogs while catch-up finishes: the next scheduled archive pass retries
   still-present unarchived closed binlogs in native order, preserving existing
   receipts. No manual skip or index edit is needed to repair a parser stall.
4. **Take a successful full backup from the current writer** before relying on
   PITR. On single-member clusters (and any cluster whose writer did not change
   during the relaunch), retained binlogs from the stuck period are archived and
   point-in-time recovery covers that period again, starting from the original
   full backup — verified by automated tests, not live. On Group Replication
   and PXC clusters the relaunch moves the writer (one writer handoff), so
   coverage restarts from the next full backup and the stuck period cannot be
   restored to a point in time; restore from a full backup instead. Keep
   earlier backups and proven windows.
5. **Verify the window advances** with `karactl backups pitr-window CLUSTER`
   after post-backup writes and completed archive uploads/coverage checks.
   Inspect `BinlogArchive` and any Warning event or writer-agent diagnostic.
   A zero-width full-backup snapshot is not continuous PITR coverage.

**Purged binlogs cannot be recovered by upgrading.** Retained backlog can be
archived automatically, but purged unarchived logs leave a permanent PITR gap.
A missing pending file reports `unarchived_binlog_purged`; do not force coverage
across the gap. Preserve earlier coverage and establish a fresh full baseline.

**Do not downgrade the control plane after rolling agents.** In particular,
a control plane below v0.4.3 cannot negotiate the agents' pending/freshness
coverage contract. Keep the upgraded control plane with the corrected agents.

### Known issues in v0.4.4

The [continuing limitations](#continuing-limitations) from v0.4.3 still apply:
PostgreSQL administrator-password rotation is unavailable, Temporal is unsupported
on Zerops, Nomad live coverage is incomplete, and PITR displays only the interval
with the latest end. See [all current limitations](limitations.md).

- **MySQL point-in-time recovery can silently stop extending after the writer
  changes in Group Replication and PXC clusters — fixed in v0.4.5** (also
  present in v0.4.0–v0.4.3). When the writer moves — automatic failover, switchover, or
  rolling maintenance — to a member whose data was copied in rather than
  replayed from its own binlogs, binlogs keep uploading and `BinlogArchive`
  stays healthy, but the end of the point-in-time window stops advancing.
  This applies to most Group Replication and PXC clusters, because members that
  join when the cluster forms are copied by Group Replication clone or PXC state
  transfer, and to any cluster created from a backup, cloned or restored in
  place. Restores to points before the freeze still work, and full backups
  are not affected. A new full backup does not restart coverage. To check,
  compare the window end from `karactl backups pitr-window CLUSTER` with the
  archived transaction times. Install the corrected agents using the
  [v0.4.5 upgrade procedure](#upgrading-to-v045); see its conditional
  same-source proof rebuild and new-writer full-backup recovery guidance.
- **Fresh-cluster first full backup fails with `metadata_invalid`
  — fixed in v0.4.5** (v0.4.0–v0.4.4; MySQL single-instance, Group Replication
  and PXC when executed GTIDs are empty). Valid two-column XtraBackup metadata
  was rejected until a committed binary-logged write made the GTID set nonempty.
  Install corrected data-member agents and take a new full; no application
  write is needed. Existing failed fulls stay failed. Follow the
  [v0.4.5 upgrade procedure](#upgrading-to-v045).
- **On Zerops, raising the resources of a member that failed its first deployment
  doesn't take effect** (also present in v0.4.3). If a newly added member, extra
  component or proxy exhausts its deployment retries — for example because it
  was given too little memory to start — a later resource increase is recorded
  but never deployed, and retrying deployments redeploys it at the old size.
  These members never started, so they hold no data. Recovery: for optional
  extras and proxies, disable the component and enable it again with the larger
  resources; for a data member added by scaling out, scale back in to remove it,
  then scale out again with the new resources; for a brand-new cluster that
  never became Ready, delete it and create it again. If a replacement member
  is stuck in a cluster that already holds data, never delete the cluster;
  instead: for PostgreSQL and Supabase, cancel the replace operation
  (`karactl operations cancel OPERATION`), which removes only the new member
  and keeps the original; for MySQL Group Replication and PXC, cancel the
  replace operation before the old member starts retiring; for MongoDB, wait
  for the operation to time out (about 2 hours), which removes the new member;
  for RabbitMQ, Elasticsearch, Weaviate and OpenBao, leave the cluster as it
  is and upgrade once the fix is released. After cancelling or a timeout,
  check that the new member is actually gone before making further changes.
  Neo4j and the Data MCP companion are not affected.
  Failed-first-deployment resource recovery on Zerops, Dokploy and Nomad is
  **fixed in v0.4.7**; it is not included in the v0.4.5 or v0.4.6 hotfixes.
  The documented workarounds remain necessary on those releases.
- **PXC clusters can be wrongly flagged as having diverged GTID history after a
  member restart** (also present in earlier v0.4 releases). After any member
  restarts and rejoins while writes continue — a rolling CPU, memory or
  parameter change, an engine upgrade, a `mysqld` crash or agent restart, TLS
  rotation, suspend/resume or a restore — the cluster can report Degraded with
  a GTID history incident even though all members have the same data.
  Incremental backups are blocked and point-in-time recovery coverage stops
  advancing until a full backup succeeds. Workaround: take a full backup; the
  flag clears about 20 seconds after it completes. If a cluster change (for
  example a resource change or proxy count change) was still running when the
  flag appeared, the cluster can get stuck: the manual full backup and
  cancelling the change are both refused. To recover, add an enabled
  full-backup schedule a few minutes ahead next to your existing schedules
  (`spec.backup.schedules`: name, cron, `type: full`, `retainCount`,
  `enabled: true`) with `karactl clusters patch CLUSTER -f change.yaml`; change
  nothing else in that patch; this works when the change itself had already
  been applied and the cluster was only waiting to report healthy (for example
  a finished proxy count change): the scheduled full clears the flag and the
  pending change completes. Afterwards restore your original schedules.
  If the change was still rolling through members when the flag appeared, the
  scheduled full can stay Pending and the cluster stays stuck; the cluster keeps
  serving reads and writes, so leave it as it is and upgrade once the fix is
  released. This needs configured backup storage and healthy members.
  **Fixed in v0.4.7**; not fixed by the v0.4.5 or v0.4.6 hotfixes.

## Upgrading to v0.4.3

Back up control-plane state using the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup),
preserve the original `KARAKORAM_MASTER_KEY` separately, and retain the previous
executable with its pre-upgrade state before deploying v0.4.3.

```sh
git fetch origin --tags && git checkout v0.4.3
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Control-plane rollback does not undo engine or infrastructure changes.

There are **no new SQLite migrations since v0.4.2**; the inventory still ends
at `017_backup_drills.sql`. Native artifact locks do change: Supavisor has a
rebuilt IPv6-capable SQL listener artifact.

### Fixed in v0.4.3

- **MySQL member roles — FIXED:** the API and UI now report primary/secondary
  roles for single-instance MySQL, Group Replication and PXC members, instead
  of showing `unknown` as in v0.4.0–v0.4.2.
- **Group Replication whole-group recovery — FIXED:** a previously formed
  group can re-form automatically after all members go down together, or after
  retained-volume suspend/resume, when all members' GTID histories are equal
  and complete stopped-member identity evidence is available. The correction is
  control-plane-side and affects v0.4.0–v0.4.2. It was live-verified on Nomad
  with three GR members; the PXC/Galera path is verified by tests only.
  See [whole-group recovery](#group-replication-whole-group-recovery).
- **MySQL PITR recovery without a writer — FIXED:** single-instance and Group
  Replication restore/clone admission now validates the archived recovery
  evidence without requiring a live `ONLINE` source writer. v0.4.0–v0.4.2
  could return HTTP 503 precisely when the cluster had no available writer.
  This does not remove live-source-spec requirements for cloning into a new
  cluster or bypass archived coverage and recovery safety checks. An in-place
  point-in-time restore of a Group Replication cluster left without a writer by
  a whole-group outage was verified live on Nomad; single-instance and clone
  paths are verified by tests only.
  A separate in-place PITR restore after writer changes was also live-verified
  on Nomad with exact recovered data and post-target absence checks. This does
  not relax the physical-source replay-lineage requirements.
- **MySQL rolling handoffs — FIXED:** maintenance now waits for the rejoined
  member and selected writer to be admitted by every serving ProxySQL instance,
  rather than relying only on native membership. Updated proxy agents also
  shorten the monitor DNS-cache lifetime and refresh interval. These issues
  affected v0.4.0–v0.4.2; the DNS-cache correction requires new proxy agents.
- **Nomad ProxySQL replica-count changes — FIXED:** count-only scaling no
  longer stops the existing proxy. Configuration acknowledgement and healthy
  writer admission gate new replicas; scale-in respects maintenance windows.
  Zerops and Dokploy still stop and redeploy the whole proxy for count changes.
  Proxy resource, version and runtime changes remain disruptive. Verified by
  tests only.
- **PITR archiver memory — FIXED:** binlog decoding now streams into a compact
  index instead of retaining the decoded log in memory. v0.4.2 was affected
  with PITR enabled at the default interval. v0.4.0/v0.4.1 share the affected
  archiver: their normal configuration refreshes usually starved the default
  timer, but archiving could run with intervals below 60 seconds or when
  refreshes were delayed. The corrected archiver requires updated data-member
  agents.
- **Idle PITR observation memory — FIXED:** object-store listing producers
  could be left behind when a consumer stopped early: a blocking channel send
  in the minio-go S3 library could outlive cancellation, retaining one listing page
  per abandoned producer. The control plane polls PITR coverage every five
  seconds and cancels slow polls, so this accumulated even on idle members;
  an observed 2 GiB GR member grew by approximately 1 GB per hour.
  Per-pass S3 transports were also left open, and every coverage poll reread
  all sidecars. The correction stops listing producers, closes repository
  transports and caches unchanged coverage with streaming rebuilds and
  verified append handling. The same listing-lifecycle pattern is corrected
  in Elasticsearch, PostgreSQL/Supabase and Weaviate control-plane checks.
  Deploy the corrected control plane and MySQL/PXC data-member agents.
  Existing MySQL/PXC clusters retain the leak until their member agents
  relaunch with the corrected executable; upgrading only the control plane
  does not replace an already running agent.
- **Small clustered-member memory headroom:** GR/PXC members at or below
  2 GiB now default to a 768 MiB InnoDB buffer pool; the GR message cache is
  128 MiB. Existing clusters receive dynamic default buffer-pool shrink after
  a control-plane upgrade; the bounded GR cache requires updated data-member
  agents. Until agents are replaced, the older GR rendering retains pool/4:
  192 MiB with a 768 MiB pool, not the new 128 MiB cap; a normal config reload
  does not install that cache change. Explicit operator pool overrides retain
  precedence. 2 GiB remains
  the clustered minimum; at least 3 GiB per member is recommended, plus
  workload headroom. See [memory sizing](mysql/configuration.md#memory-sizing).

**Measured live member-agent memory with PITR enabled:** on Nomad with three
2 GiB GR members, archive intervals of 1 and 300 seconds, at least 1,000 archived
files, idle periods and about 1 MiB/s writes, agent RSS stayed **at or below
48 MiB in steady state**, peaking at **54.9 MiB during a 3.8 GB full backup**.
The idle cell used 30 ms injected storage latency; 30-minute memory ranges
were **at most 5.2 MiB**, with **no out-of-memory kills**. This is a scoped
acceptance measurement, not a workload capacity guarantee. Released v0.4.2
code reproduced listing-page leakage in tests; earlier live runs of affected
code reached about 1 GB per idle agent and an agent was OOM-killed.

- **PITR coverage convergence — FIXED:** after an agent relaunch, failover or
  a large archive batch, long histories could keep recovery coverage pending
  indefinitely in v0.4.0–v0.4.2. One agent-owned background refresh now persists
  progress across short polls and restarts. Polls read compact state rather than
  rescanning history. The updated control plane negotiates explicit pending
  responses with progress and no unverified recovery window. A pending baseline
  does not hide completed windows from other full backups. A restore target
  already covered by a completed baseline can be admitted; an uncovered target
  remains pending while an eligible baseline is verifying or queued, rather than
  being prematurely declared out of range. Restore admission with a reachable
  source agent requires verification started after that admission request,
  not just a cached status result.
  Memory retains at most 32 baseline entries; compact proofs and resumable
  checkpoints remain on disk. Cleanup expires baselines unrequested for 24 hours
  and retains up to 256 inactive baselines in addition to the protected current
  working set; these are metadata limits, not backup-retention settings.
  This requires updated agents **and control-plane pending/freshness handling**.
  Coverage convergence and agent relaunch on long history were live-verified
  on Nomad without injected latency; convergence under storage latency is
  verified by tests. See [coverage behavior](mysql/backups.md).

- **PITR GTID coverage — FIXED:** binlog `Previous-GTIDs` entries spanning
  multiple server UUIDs are preserved during streaming decoding, so coverage
  decisions retain the complete history rather than a truncated namespace.
- **PXC GTID history after restore:** restored inverse-SID histories no longer
  cause a false divergence latch. Acknowledged differences are bounded and
  retired when consumed, with member-incarnation fencing. Restore certification
  verifies replicated GTID mapping before releasing access, with bounded,
  seed-pinned follower SST remedies when safe. Explicit new restores remain
  available after terminal certification failure; each stop boundary is
  checkpointed before changing runtime state.
- **PXC availability observations:** the complete reconcile observation pass
  has a shared read budget. Concurrent quorum sampling preserves historical
  health and the oldest supporting proof on retries; missing or timed-out
  evidence is not proof of safe history. Backup completion checks current GTID
  evidence.
- **Condition transition timestamps:** unchanged observed conditions keep their
  original transition time across provider synchronization. Status or reason
  changes record a new transition; message-only refreshes preserve
  `lastTransitionTime`.
- **PXC create-from-backup — FIXED:** fresh, stopped, empty restore followers
  receive durable SST authorization before seed startup. Existing-data identity
  checks and serialized joins remain enforced. This is agent-side: new clusters
  created on v0.4.3 receive it; delete and re-create failed v0.4.1/v0.4.2 clones.
  Three-member PXC creation from a full backup has been verified on Zerops
  with v0.4.3 code. Point-in-time cloning into a new three-member PXC cluster
  has also been verified on v0.4.3 on Dokploy: data matched exactly on every
  member, and rows after the target time were absent. PITR into a new cluster
  still requires the source cluster specification, including on Zerops; this
  is distinct from requiring an online source writer.
  See [backup recovery requirements](backups.md).
- **ProxySQL monitor TLS — FIXED:** GR discovery/revival retains encrypted
  monitor connections; managed GR and Galera hostgroups carry explicit TLS
  defaults. Existing GR clusters need the proxy-agent redeployment described below.
  PXC and single-instance clusters are not affected by this discovery defect:
  PXC retains explicit server rows with `use_ssl=1`, and singleton proxies
  have no discovery path. Fresh GR clusters created with v0.4.3 code on Zerops
  were verified with zero monitor denials and zero SSL errors on every member.
- **Frozen PITR windows after archive-writer changes — FIXED reporting:**
  treat any GR primary change with PITR enabled as a fresh-full-backup boundary,
  including automatic failover and rolling maintenance such as a resource
  change. A different physical archive source cannot extend the previous
  baseline's window; it freezes at the last provable endpoint until a full
  backup anchors the current writer's history. PXC has the same restriction
  when its archive writer/source changes. Restores within the proven old
  window, up to the frozen end, remain available.
  v0.4.0–v0.4.2 reported PITR available and `lag N s` without exposing the freeze.
  v0.4.3 reports `WindowFrozenWriterChanged` when newer foreign-writer uploads
  are excluded and separates upload lag from the recoverable endpoint.
  **Take a new full backup** before relying on newer targets. Schedule regular
  full backups so re-anchoring normally happens within one schedule period;
  failed backups or further writer changes can delay it. A new full does not
  fill the intervening recovery gap. Existing valid current-writer baselines
  or a returning original writer with contiguous history are exceptions;
  verify the window rather than assuming coverage survives a primary change.
- **PXC certification agent rollout:** restore certification requires corrected
  agents on **every data member and every deployed proxy**, as well as the
  control plane. Updating data agents alone leaves an outdated proxy unable to
  participate. See [existing-member rollout](#existing-clusters-and-member-artifacts).
- **Weaviate on Zerops — FIXED:** memberlist and Raft advertise the member's
  private address, including IPv6-only backends. DNS-dependent starts retry
  without blocking explicit stop intent, and held processes are prepared before
  start admission. This resolves the startup issue published for v0.4.0–v0.4.2.
- **Native listener address families:** Neo4j, Keycloak, Elasticsearch, RabbitMQ
  and Supabase use listeners compatible with the backend's native IP family,
  preserving IPv4 backends. **Supavisor IPv6 SQL listeners — FIXED:** the native
  artifact has been rebuilt and its digest and size updated in the release
  lock. The rebuilt artifact is published with the v0.4.3 release;
  control-plane deployment alone does not supply it to existing members.
- **MySQL singleton streaming-backup memory — FIXED:** default InnoDB buffer
  pool size is `min(RAM / 2, RAM - 768 MiB)`, with a floor of `128 MiB`.
  XtraBackup streaming uses `--parallel=1 --read-buffer-size=1M`.
  Single-member admission remains at 1 GiB; these changes are not a workload
  capacity guarantee. Custom sizing and workload concurrency still require
  headroom. Backup failures retain reason codes and native exit statuses/signals
  with secret-free summaries; SIGKILL indicates possible OOM, not proof of it.

Measured **single-member** full backups with PITR off and a 1 GiB memory limit peaked at
**84.4% (906.7 MB)** on fresh v0.4.3 agents: mysqld 739 MiB RSS,
XtraBackup 129 MiB and xbcloud 40 MiB, with no OOM. With pre-v0.4.3 agents
and only the v0.4.3 control plane, the peak was **96% (984.9 MB)** with no OOM:
a thin margin; re-create the agents. These observations are not workload
capacity guarantees. In a measured PXC full backup with PITR off and three 2 GiB data
members, the backup member's sampled peak was **85.0% (1.826 GB)**,
with XtraBackup RSS **132 MB**, no OOM kills and no restarts. This is the
sampled backup-window high-water mark, not a new lifetime memory peak or a
workload capacity guarantee.

MySQL Group Replication full backup measured with PITR off on v0.4.3 on Dokploy with three
2 GiB members: the member taking the backup peaked at **90.7%** of its 2 GiB
limit (mysqld approximately 1.71 GiB, XtraBackup approximately 130 MiB and
xbcloud approximately 35 MiB); the other members were at 85–86%, with no OOM.
The thin measured margin supports retaining the 3 GiB recommendation for GR
members taking backups, plus workload headroom; it is not a capacity guarantee.

### Group Replication whole-group recovery

**FIXED in v0.4.3:** after a whole-group outage, such as power loss or all nodes
rebooting, MySQL Group Replication could stay Degraded without a writer
(`GroupReformationBlocked` / `UnsafeOfflineGTID`) even with equal GTID histories.
The same issue affected suspending and resuming a GR cluster that keeps its
volumes (`karactl clusters delete --keep-volumes` + `karactl clusters resume`).
The offline comparison was limited by a four-second observation timeout.

Offline recovery now uses dedicated per-member probe budgets: 60 seconds total
for GR and the existing 180 seconds for Galera/PXC re-formation. Up to three
members are probed concurrently after every member passes deployed/stopped
prechecks. Blocked sweeps retry with backoff from 15 seconds to five minutes.
These are probe budgets, not guaranteed recovery times. Missing identities,
unavailable members or incompatible GTID histories still fail closed; do not
force bootstrap or reset GTIDs. See [full-outage recovery](mysql/operations.md#full-outage-recovery).

This affects **v0.4.0–v0.4.2** and is **control-plane-side**, with no agent
change required. **Live-verified on Nomad with three GR members:** after all
three `mysqld` processes were killed together, the group re-formed automatically
without manual recovery; a writer was observed about 252 seconds after the
kill, all acknowledged rows matched, and there were no OOM kills. Offline
probes took 4.2–4.5 seconds each, exceeding the old four-second limit.
Retained-volume suspend/resume also passed: a writer was available after about
115 seconds and the cluster was Ready after about 181 seconds, with exact data.
These are scoped observations, not recovery-time guarantees. PXC/Galera
re-formation remains **verified by tests only**.

### PXC member retirement

**FIXED in v0.4.3:** replace-member and scale-in could stall when the retiring
member's agent rejected reload with HTTP 400, `Galera local peer and port required`.
The control-plane-only fix resumes an already-stuck retirement with existing
agents. The retiring member retains its own Galera identity only in its own
configuration; survivor configurations exclude it. SST donor and seed selection
move to a healthy admitted survivor. Once retirement is irreversible it completes
even if a joiner certification owner is blocked or expired. Preserve a validated
backup and leave surviving members alone while the upgraded control plane
finishes the retirement.

### PXC formation and restore certification

PXC joins are serialized: a preceding admitted joiner must become `Synced`
before another empty follower starts. Replicated probe certification checks
GTID mapping at formation and restore, not just matching rows or Galera health.
Inspect `GTIDMappingCertified` and `RestoreGTIDCertified` in cluster conditions.
Missing or inconsistent evidence fails closed with a `Blocked` certification
state rather than declaring the formation safe. Only lifecycle-proven empty
joiners without unique frozen history qualify for automatic SST remedies.

Follow the condition's operator guidance: for failed birth certification,
delete and re-create the never-formed cluster; for scale or replacement,
scale back or cancel the affected admission; for restore, submit a new restore
from a verified recovery source. Preserve valuable data and recovery evidence.
Do not clear safety state, force bootstrap, or overwrite an unproven member.
Existing clusters with a latched GTID incident receive a diagnostic; certification
does not silently clear the incident or rewrite historical mappings.

A history-incident latch on an already-serving cluster never stops ProxySQL or
refuses writes. Its documented exits are replace-member, a fresh full backup,
restore, or delete. A full-backup acknowledgement requires a current mixed-SID
mapping check; mixed mapping is never silently acknowledged.

**Limit:** `GTIDHistorySafe` describes GTID-history compatibility, not row
equivalence. Privileged unlogged writes (`SESSION wsrep_on=OFF`,
`sql_log_bin=0`, or `gtid_next` manipulation) can create row drift invisible to
this condition. Restrict those privileges and verify data independently.

**Limit:** while a history latch is active, the advertised PITR window remains
at the last remembered window. Point-in-time restore targets outside provable
coverage are refused with `outside conservative contiguous binlog coverage`. Use a
full-backup restore or clear the latch through a documented exit first.

### ProxySQL monitor TLS

After upgrading the control plane with the corrected proxy agent artifact,
redeploy the proxy of an existing Group Replication cluster. PXC and
single-instance clusters are not affected by this discovery defect; Galera
hostgroup defaults are defensive policy, not evidence of the same defect.
A native process restart, an unchanged spec, or a control-plane-only upgrade
does not replace its agent. v0.4.3 MySQL/PXC do not support `upgrade-agents`.

**v0.4.7 supersedes this workaround:** after upgrading the control plane, use
[agent rollout](#agent-rollout-now-covers-every-member-agent-engine) to replace
data and proxy agents without a resource change. The procedure below describes
v0.4.3 only.

Use a genuinely changed, valid `spec.components.extra.proxy.resources` target.
For example, if the proxy is currently fixed at 1 GiB, save this as
`proxy-maintenance.yaml` after checking capacity, cost and the maintenance window:

```yaml
spec:
  components:
    extra:
      proxy:
        resources:
          minRam: 2
          maxRam: 2
```

```sh
karactl -n NS clusters patch NAME -f proxy-maintenance.yaml --wait
```

**This is not online:** the whole proxy is stopped and redeployed, even with
multiple proxy replicas. Client connections through it fail during the operation;
plan an interruption and reconnect handling. On Zerops, this exact fixed-release
procedure with proxy minRam/maxRam changed from 1 to 2 GiB bumped the generation
and completed in 211.6 seconds (about 3.5 minutes). The concurrent writer saw
`OperationalError`; the precise outage duration was not measured. Afterward,
monitor samples showed no `kk_monitor` denials; follow-up primary-switch testing
reported zero SSL errors and successful proxy reads and writes to the formerly
offline member. This is a measured Zerops result, not a timing guarantee for
other deployments.

Unchanged data members are skipped and their identities are retained. Returning
to the original resource size is another disruptive deployment. Verify the new
proxy build, native TLS/monitor state and application reads and writes afterward.
Do not disable TLS or rotate monitor credentials as a substitute for installing
the correction.

### Existing clusters and member artifacts

**MySQL/PXC agents are not automatically upgraded.** A control-plane upgrade
does not replace running data-member or proxy-agent executables. On v0.4.3,
MySQL/PXC do not support `upgrade-agents`, and `karactl clusters restart`
restarts native database/proxy processes, **not their agents**.

For v0.4.7, use [agent rollout](#agent-rollout-now-covers-every-member-agent-engine)
instead of the resource-change/new-target procedures below; their v0.4.3
compatibility and live-verification statements remain historical.

**Upgrade order is required: upgrade the control plane first, then relaunch
MySQL/PXC agents. Do not downgrade the control plane below v0.4.3 while
v0.4.3 agents run.** A v0.4.2 control plane with v0.4.3 agents is unsupported:
it cannot negotiate pending coverage or enforce the new fresh-verification
contract. The new agent returns an observation error to an old control plane
while coverage is pending or expired; the old control plane retains its
released PXC fallback to remembered windows on observation errors rather than
the new pending/freshness handling. Completed windows remain wire-compatible,
but that does not make the old-control-plane/new-agent combination supported.
Upgrading the control plane first supports legacy agents temporarily; it does
not give those agents the memory or background-convergence corrections.

The code-supported in-place data-agent relaunch path is a **genuinely changed
CPU or memory resource target**: rolling maintenance stops and redeploys each
member with the current executable. An unchanged patch does not do this.
The rolling resource change relaunching v0.4.3 agents was **verified live on
Nomad (Group Replication)**; upgrading in place from v0.4.2 agents is
**verified by tests only**.
Use a maintenance window, check provider capacity, preserve a validated backup
and independent recovery path, and expect write interruptions during writer
handoff (downtime for single-member clusters). Do not attempt a rolling change
on a cluster that has already lost its majority. Verify every member's build,
native replication, recovery coverage and client writes after relaunch.

Alternatively, restore or clone a verified backup into a new v0.4.3 cluster,
verify member builds and recovered data, then switch clients before retiring
the original cluster. New-target recovery has live evidence for MySQL and PXC;
PITR cloning still requires the source cluster specification. Do not copy
executables into running members.

Data-agent fixes include PITR streaming, listing cancellation, bounded coverage
and background convergence, XtraBackup streaming flags, and GTID certification.
The proxy TLS fix needs the disruptive proxy PATCH described above; the
rolling-handoff DNS policy also requires corrected proxy agents.

**PXC certified restores require every data member and every deployed proxy
to advertise certification support.** A data-member CPU/memory change does not
replace proxy agents. After relaunching data agents, redeploy outdated PXC
proxies using a genuinely changed
`spec.components.extra.proxy.resources` target. The whole proxy stops and
redeploys, so plan a client interruption even with multiple replicas. Verify
certification support on all participants before restoring.
**This existing-cluster certification rollout is not verified live for
v0.4.3.** It is distinct from the GR-only monitor-TLS defect and its tested
proxy redeployment above; PXC's proxy update here installs certification
support, not a fix for that GR discovery defect.

**After relaunching MySQL/PXC data-member agents with a resource change,
re-enable PITR if it was disabled, then take a successful full backup before
relying on point-in-time recovery.** Re-enable with
`spec.backup.pitr.enabled: true` only after all affected data agents run
v0.4.3 code. The relaunch roll can move the GR primary, or PXC archive source,
freezing the previous baseline's window. A fresh full from the current writer
establishes the post-roll replay anchor; verify coverage convergence and the
reported window after subsequent archives complete. Keep the previous backup
chain and proven window: the new full does not fill an intervening gap.
A control-plane-only upgrade is not sufficient.

The **control-plane-only** portions include member-role reporting, retirement
completion, whole-group recovery, rolling-handoff admission, provider-aware
proxy scaling, condition timestamps, frozen-window reporting and no-writer
restore admission. Default buffer-pool sizing applies through `SET GLOBAL`;
observation budgets and backup-completion checks also use the new logic. Legacy
agents report
certification unavailability rather than gaining the agent-side certificate.

**Weaviate clusters that never formed on Zerops must be re-created.** Preserve
the create request, confirm there is no valuable imported or external state,
then deploy with current agents. v0.4.3 Weaviate does not support `upgrade-agents`;
v0.4.7 rollout requires a formed cluster. Preserve data-bearing clusters and use
the v0.4.7 [agent rollout](#agent-rollout-now-covers-every-member-agent-engine)
only when its native safety gates can be satisfied.

**Supabase poolers on IPv6-only backends require the rebuilt Supavisor native
artifact**, not just a control-plane upgrade. The rebuilt artifact is published
with the v0.4.3 release; mirrors must serve bytes matching the updated release
lock. Use the supported [member-agent rollout](#rolling-out-member-agents)
and verify native SQL pooler connectivity over the private address family.
For v0.4.3, other affected engines use supported agent rollout where available;
Neo4j and Keycloak need reviewed, data-preserving re-creation for agent-side
listener fixes. On v0.4.7, use the
[agent rollout](#agent-rollout-now-covers-every-member-agent-engine) instead.

### Continuing limitations

The following limitations describe v0.4.3. v0.4.7 adds PostgreSQL administrator
rotation, Temporal on Zerops and full PITR interval lists as described
[above](#upgrading-to-v047). Incomplete Nomad live coverage still does not change
EXPERIMENTAL engine status. See current [limitations](limitations.md).

**MySQL/PXC PITR window display:** `karactl ... pitr-window` and cluster status
show only the restorable interval with the latest end, not every valid interval.
Immediately after a new full backup, an earlier restorable range can therefore
be hidden while the newest interval is only a single snapshot point. It is not
shown together with that newer interval unless they merge. Restore admission
still checks all proven intervals and accepts a target in the hidden earlier
range if its backup chain and archived coverage remain valid; the display does
not prove those earlier recovery points are gone. A display fix is planned for
a later release.

### Known issues in v0.4.3

The archiving issue is fixed in v0.4.4; the PXC false-history issue is fixed
in v0.4.7, not by the hotfix.

- **MySQL PITR archiving can silently stop after a schema or account change
  — fixed in v0.4.4** (v0.4.3 only; single, Group Replication and PXC).
  Native `mysqlbinlog` Query headers use a tab; the old parser expected a
  space. A standalone Query event without Xid, including a no-op schema or
  account statement, can stop uploads while `BinlogArchive` still looks healthy.
  Statements such as `CREATE DATABASE`, `CREATE TABLE`, `ALTER`, `CREATE USER`
  and `GRANT`, including Karakoram-managed changes, can encounter this path;
  successful changes alone do not prove archive health. Full backups are
  unaffected. Compare the endpoint from `karactl backups pitr-window CLUSTER`
  with current time and check that it advances after writes. A new full,
  toggling PITR or restarting native processes does not repair the old parser.
  Install the corrected data-member agents using the
  [v0.4.4 upgrade procedure](#upgrading-to-v044). Retained unarchived binlogs
  are retried automatically; purged unarchived logs leave a permanent gap.
  On Group Replication and PXC the relaunch moves the writer, so point-in-time
  recovery into the stalled period itself is not available.

- **PXC clusters can be wrongly flagged as having diverged GTID history after a
  member restart** (also present in earlier v0.4 releases). After any member
  restarts and rejoins while writes continue — a rolling CPU, memory or
  parameter change, an engine upgrade, a `mysqld` crash or agent restart, TLS
  rotation, suspend/resume or a restore — the cluster can report Degraded with
  a GTID history incident even though all members have the same data.
  Incremental backups are blocked and point-in-time recovery coverage stops
  advancing until a full backup succeeds. Workaround: take a full backup; the
  flag clears about 20 seconds after it completes. If a cluster change (for
  example a resource change or proxy count change) was still running when the
  flag appeared, the cluster can get stuck: the manual full backup and
  cancelling the change are both refused. To recover, add an enabled
  full-backup schedule a few minutes ahead next to your existing schedules
  (`spec.backup.schedules`: name, cron, `type: full`, `retainCount`,
  `enabled: true`) with `karactl clusters patch CLUSTER -f change.yaml`; change
  nothing else in that patch; this works when the change itself had already
  been applied and the cluster was only waiting to report healthy (for example
  a finished proxy count change): the scheduled full clears the flag and the
  pending change completes. Afterwards restore your original schedules.
  If the change was still rolling through members when the flag appeared, the
  scheduled full can stay Pending and the cluster stays stuck; the cluster keeps
  serving reads and writes, so leave it as it is and upgrade once the fix is
  released. This needs configured backup storage and healthy members.
  **Fixed in v0.4.7**; not fixed by v0.4.4.

## Upgrading to v0.4.2

Back up control-plane state using the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup),
preserve the original `KARAKORAM_MASTER_KEY` separately, and retain the previous
executable with its pre-upgrade state before deploying v0.4.2.

```sh
git fetch origin --tags && git checkout v0.4.2
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

There are **no new SQLite migrations** from v0.4.1 to v0.4.2; the inventory
in `internal/store/migrations` still ends at `017_backup_drills.sql`.
There are no new engine resource minimums, Go dependency changes or native
artifact lock changes in this release. The v0.4.1 memory floors still apply.
Control-plane rollback does not undo engine or infrastructure changes.

### Changed behavior

- Dokploy retains a maximum of four build attempts per rendered deployment
  content/retry epoch across control-plane restarts. Exhausting this budget
  reports `DeploymentRetriesExhausted`; inspect deployment logs and correct
  the failed deployment rather than expecting indefinite retries.
- Temporal rejects an initial data source at admission because it does not
  support create-from-backup. This does not remove its separate restore support.
- Keycloak fills each omitted resource field independently from catalog defaults,
  including disk-only requests; it does not resize existing clusters automatically.
- Resume and recovery restore only previously admitted exposure during a closed
  maintenance window. Deferred exposure changes remain deferred and unacknowledged.

### Fixed in v0.4.2

Unless noted otherwise, the following issues affected both v0.4.0 and v0.4.1:

- **MySQL Group Replication formation:** generated recovery credentials now fit
  MySQL's limit, with sanitized startup diagnostics. Delete and re-create
  never-formed legacy groups; do not force bootstrap or edit coordinator state.
  Existing single-member clusters receive compatible recovery credentials before
  their first adoption into a group.
- **PXC formation on Zerops:** startup persists the required files and directory
  chain without a filesystem-wide sync that the platform rejects.
- **OpenBao rolling status:** Updating or Upgrading remains visible between
  member replacements until convergence completes. This was a v0.4.1 regression;
  v0.4.0 was not affected by the intermediate Ready status.
- **Clone readiness:** OpenBao, MySQL, PXC, Neo4j and Weaviate now acknowledge
  completed initial restores so clone creation can finish and report Ready.
  Temporal now rejects unsupported initial data sources rather than accepting
  a clone request it cannot complete.
- **Keycloak resource defaults:** omitted CPU, memory and disk fields receive
  their catalog defaults independently, including requests supplying only disk.
- **Dokploy deployment retries:** failed initial builds remain observable and
  retryable, with a bounded build-attempt budget retained across control-plane
  restarts, rather than leaving creation blocked behind a failed build.
- **MySQL Group Replication primary handoff:** restart, replacement and rolling
  maintenance can select a new primary using the native MySQL function.
- **Resume and exposure recovery:** already-admitted TCP and HTTPS exposure can
  recover during a closed maintenance window without admitting deferred exposure
  changes. This covers the affected database and service engines, including
  OpenBao resume and Supabase gateway recovery. This was a v0.4.1 regression;
  v0.4.0 was not affected.
- **MySQL and PXC full backups:** the control plane recognizes XtraBackup's
  native full-backup checkpoint type, allowing full backups and their dependent
  restore, clone and point-in-time recovery baselines. This fix needs only a
  control-plane upgrade, not a member-agent update. Take and verify a new full
  backup; a previously failed backup is not a recovery baseline.
- **MySQL and PXC in-place restore:** the restore operation now releases the
  server stop holds during re-formation so the restored seed and followers can
  start and restore can complete. This affected v0.4.0 and v0.4.1 but was
  previously masked by the full-backup metadata defect described above.
- **MySQL Group Replication restore follower re-clone:** in-place recovery can
  re-clone followers after restoring the seed. Clone recipients fence clients
  while preparing old data for replacement, and rejected native starts can be
  retried only when copying never began. PXC uses its own Galera recovery path.
- **MySQL and PXC point-in-time recovery:** binlog archiving now preserves its
  deadline across configuration refreshes instead of being postponed
  indefinitely. Archive lag and recovery-window status now reflect the
  completed archive rather than configuration refresh timing.
- **ProxySQL configuration reloads:** the ProxySQL agent no longer reloads
  unchanged configuration every minute, which briefly routed writes to
  replicas and caused occasional failed writes.
- **MySQL clone failure diagnostics:** operation events now report the clone
  stage and, when available, the MySQL error number without exposing SQL,
  credentials or raw server error messages.

### Existing MySQL and PXC agents

**For v0.4.7:** [agent rollout](#agent-rollout-now-covers-every-member-agent-engine)
replaces the re-creation requirement described here for v0.4.2.

The MySQL Group Replication clustered-restart/primary-handoff, in-place restore
follower re-clone, MySQL/PXC PITR binlog archiving and ProxySQL reload fixes are
**agent-side**, as are the new clone diagnostics. Existing MySQL and PXC
clusters receive these fixes **only by being re-created with v0.4.2 agents**.
For example, take and verify a backup, clone or restore it into a new cluster
on v0.4.2, verify the new member builds and recovered data, then switch clients.
Back up and verify an independent recovery path before deleting any cluster;
preserve application data when planning re-creation.

`upgrade-agents` covers **PostgreSQL, Supabase, MongoDB, RabbitMQ and
Elasticsearch only**; it does not cover MySQL or PXC. Upgrading only the
control plane or restarting the native database process does not replace
agent code. The separate in-place restore **stop-hold release** fix is
control-plane-side and does not itself need an agent upgrade; successful
follower re-clone still requires the updated agents described above.

### Other engine agents

For an upgrade **from v0.4.1**, the v0.4.2 fixes for **OpenBao** (rolling status,
clone readiness and exposure recovery), **Neo4j** and **Weaviate** (clone readiness
and exposure recovery), **Keycloak** (resource defaults and exposure recovery),
and **Temporal** (initial-data-source rejection) are **control-plane-side**.
Upgrading the control plane is enough for these fixes; these engines do not
need re-creation for a v0.4.1 → v0.4.2 upgrade. Dokploy initial-build retry and
shared exposure-recovery fixes also run in the control plane.
The v0.4.2 exposure-recovery fixes for **PostgreSQL (including Citus), Supabase,
MongoDB, RabbitMQ and Elasticsearch** are also control-plane-side; these fixes
do not themselves require an agent rollout.

OpenBao, Neo4j, Weaviate, Keycloak and Temporal still do **not** support `upgrade-agents`. This statement
does not install agent-side fixes from older releases: when upgrading from
v0.4.0, also follow the separate v0.4.1 agent guidance below. MySQL/PXC formation
durability and sanitized startup/clone diagnostics include agent-side changes;
use new v0.4.2 members, not merely a database-process restart.

For v0.4.7, all of these engines support the
[agent rollout command](#agent-rollout-now-covers-every-member-agent-engine);
the unsupported statement above describes v0.4.2 only.

### Known issues in v0.4.2

- **PostgreSQL administrator password rotation remains unavailable.** The
  revealed `postgres` password cannot be rotated in place. Follow
  [administrator credential containment](#postgresql-administrator-password-cannot-be-rotated-yet).
- **ProxySQL can treat a healthy MySQL Group Replication member as offline**
  (also present in v0.4.0 and v0.4.1). The proxy's health monitor can connect to
  a member without TLS, which the member rejects; ProxySQL then logs
  `Access denied for user 'kk_monitor'` and `Assuming offline` for that member
  and sends it no traffic, although the member is ONLINE in the group.
  If Group Replication elects that member primary, writes through the proxy
  can fail. Seen on Zerops; other providers may be affected. Don't disable TLS
  or change the monitor account to work around it. **Fixed in v0.4.3**;
  existing clusters need the disruptive [proxy redeployment](#proxysql-monitor-tls).
- **MySQL point-in-time recovery makes member agents use more and more memory**
  (v0.4.2; in v0.4.0 and v0.4.1 the normal trigger is blocked because MySQL full
  backups fail there). The binlog archiver decodes each interval's binlogs in
  memory, so busy members need a lot of memory per archive pass. Separately,
  once a cluster has about a day or more of archived binlogs, the control
  plane's routine recovery-coverage checks start timing out, and each one leaves
  memory behind in the member agent — up to about 0.5 GB per hour, even when the
  cluster is idle and whatever the archive interval. Eventually `mysqld` is
  killed for running out of memory, restarts repeatedly and refuses writes; a
  Group Replication cluster can lose its majority and stay without a writer
  until an operator intervenes. Acknowledged writes are preserved.
  Workaround: disable point-in-time recovery on all MySQL, Group Replication
  and PXC clusters until the fix (`spec.backup.pitr.enabled: false`, applied with
  `karactl clusters patch CLUSTER -f change.yaml`); full backups keep working.
  Disabling it stops further growth but doesn't free memory already used:
  `karactl clusters restart` doesn't help, because it restarts only the database
  and proxy processes. To free it, make a resource change (for example CPU or
  memory), which restarts members one at a time; do this in a maintenance window,
  expect a short write interruption while the writer moves, expect downtime on
  single-member clusters, and don't do it on a cluster that has already lost
  its majority. Don't lower binlog retention to work around it. A fix is planned
  for v0.4.3.
  **Fixed in v0.4.3:** streaming binlog decoding and a compact index bound
  archiver memory; updated data-member agents are required. The workarounds
  above remain guidance for users still on v0.4.0–v0.4.2.
  v0.4.3 also fixes cancelled listing producers, unclosed repository transports
  and repeated sidecar scans that could grow idle PITR-agent memory; deploy
  the corrected control plane as well as data-member agents.
- **MySQL Group Replication doesn't recover by itself after all members go down
  at once** (also present in v0.4.0 and v0.4.1). After a whole-group outage —
  for example a power loss or all nodes rebooting — or after suspending and
  resuming a Group Replication cluster that keeps its volumes, the cluster can
  stay Degraded with no writer, reporting `GroupReformationBlocked`
  (`UnsafeOfflineGTID`), even though every member has the same data. The check
  that compares members before re-forming the group times out. Recovery: don't
  force a bootstrap or reset GTIDs; restore the cluster in place from its
  latest full backup
  (`karactl restores create CLUSTER NAME --backup FULL_BACKUP --yes`).
  Data written after that backup is lost, because point-in-time restore isn't
  available on a cluster without a writer in this release. Until the fix, take
  full backups frequently and avoid suspending Group Replication clusters.
  PXC is not affected by this GR timeout defect. **Fixed in v0.4.3**; see
  [whole-group recovery](#group-replication-whole-group-recovery).
- **PXC after an in-place restore on Zerops:** after either a full or PITR
  restore, one member can be flagged `GTIDHistoryDiverged` and the cluster
  reports Degraded even though the checked data and writes are correct.
  This issue is **fixed in v0.4.3** through bounded history handling and
  [formation and restore certification](#pxc-formation-and-restore-certification).
  Verify recovered data and replication natively. Existing latched incidents
  are diagnosed, not silently cleared; use a documented operator exit.
- **Despite the clone fix above, creating a multi-member Percona XtraDB Cluster
  from a backup never completes** (also present in v0.4.1). This applies to
  clones from full, incremental and point-in-time backups on every provider.
  The create operation fails after about 20 minutes; the new cluster stays not
  Ready with all members unreachable and no proxy. Your source cluster and its
  backups are not affected. **Fixed in v0.4.3**; delete the failed target and
  re-create it with corrected agents as described [above](#fixed-in-v043).
- **Replacing a Percona XtraDB Cluster member or scaling PXC in never completes**
  (also present in v0.4.0 and v0.4.1). Automatic healing of PXC members
  (restart or rejoin of the same member) is not affected. The new member joins, but the old
  member's retirement is rejected repeatedly; the operation stays Updating or
  Scaling and fails after about 20 retries, the old member stays in Removing,
  and further operations on the cluster are blocked. The remaining members keep
  serving reads and writes with reduced redundancy. Cancelling only helps
  before the old member starts retiring. Workaround: don't replace PXC members
  or scale PXC in until the fix. If an operation is already stuck, leave the
  remaining members alone (don't retry, restart, scale back or remove volumes),
  keep a validated backup, and upgrade the control plane once the fix is
  released — it is control-plane-only and completes the stuck retirement
  without new member agents. Group Replication and other engines are not
  affected. **Fixed in v0.4.3**; the control-plane upgrade resumes an
  already-stuck retirement with existing agents. See [resolution](#pxc-member-retirement).
- **Full backups can run out of memory on single-member MySQL clusters with
  1 GiB of memory** (also present in v0.4.0 and v0.4.1). The kernel can kill
  `mysqld`, causing the member to restart and the backup to fail.
  Workaround: give MySQL members at least 2 GiB of memory before taking full
  backups (3 GiB recommended for Group Replication members), plus headroom for
  your workload. **Fixed in v0.4.3** as described
  [above](#fixed-in-v043); this workaround remains guidance for v0.4.2 agents.
- **Nomad live coverage of the new engines is incomplete.** Implemented
  capabilities do not establish full lifecycle/recovery acceptance. OpenBao,
  Temporal, MySQL/PXC, Neo4j, Weaviate, Keycloak and Citus remain EXPERIMENTAL,
  as does Data MCP; see [engine availability](providers.md#engine-availability).
- **Temporal is not supported on Zerops:** membership requires private IPv4
  networking, while Zerops private networking is IPv6. Use Dokploy or Nomad
  within their documented capability limits.
- **Weaviate cannot start on Zerops** (also present in v0.4.0 and v0.4.1):
  members never become Ready because Zerops private networking is IPv6-only
  and Weaviate's cluster address is not advertised for IPv6. Nomad and Dokploy
  are not affected. This v0.4.2 issue is **fixed in v0.4.3**; see
  [current agent and re-creation guidance](#existing-clusters-and-member-artifacts).

Other continuing limitations include [empty Docker volume objects after
Dokploy deletion](dokploy.md) and PostgreSQL's configuration acknowledgement
wait after binding/access changes; backups still require readiness.

## Upgrading from v0.4.0 to v0.4.1

Back up control-plane state using the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup)
and preserve the original `KARAKORAM_MASTER_KEY` separately before deploying.
Retain the v0.4.0 executable with its pre-upgrade state. Then deploy v0.4.1:

```sh
git fetch origin --tags && git checkout v0.4.1
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

There are **no new SQLite migrations** in v0.4.1; the migration inventory still
ends at 017. This does not make rollback of subsequent engine or infrastructure
changes automatic. Engine packages and member agents are separate from the
control-plane executable.

### Member-agent rollout and changed minimums

Before any PostgreSQL member redeployment, follow the
[version-drift inventory](#postgresql-exact-versions-and-existing-version-drift)
below. Recover an access-held restore before requesting an agent rollout.
On a healthy cluster, `karactl -n NS clusters upgrade-agents NAME --wait --timeout 30m`
supports **PostgreSQL (including Citus), Supabase, MongoDB, RabbitMQ and
Elasticsearch**. It retains engine versions and honors maintenance windows and
health/quorum gates. Follow [member-agent rollout](#rolling-out-member-agents)
and verify every member's reported build afterward.

**OpenBao, Temporal, MySQL, PXC, Neo4j, Weaviate and Keycloak do not support
`upgrade-agents`.** Existing executables are not replaced by restarting the
control plane or native process. Agent-side fixes reach these engines only
through control-plane-managed member re-creation; use deletion/recreation for
disposable clusters, or the engine's documented backup/new-target recovery
where supported. Preserve and verify valuable data first. There is no generic
data-preserving agent-only upgrade for these engines; consumer deletion is
especially destructive, as explained below. Do not use volume retention or
copying binaries as a substitute.

**For v0.4.7:** all of these engines support
[agent-only rollout](#agent-rollout-now-covers-every-member-agent-engine).
The re-creation requirement above applies to v0.4.1, not the current rollout path.

Agent-side changes include Keycloak JDBC and Temporal SQL driver settings;
MySQL Group Replication pre-bootstrap observation and message-cache budgeting;
MySQL/PXC address-family-aware SQL listeners; PXC numeric Galera binding,
managed access serialization and archive eligibility; and Neo4j idempotent
restore credential recovery and its reload/error handling. PostgreSQL agents
carry the listener/proxy, TimescaleDB and coordinated Citus recovery fixes.
Control-plane-only fixes, including idle resource-update suppression, status
publication and maintenance admission, apply without replacing agents.

MySQL Group Replication and PXC now require **`minRam >= 2` GiB per data
member**, even with custom buffer settings; raise `maxRam` with it when necessary.
MySQL single-member deployments retain their 1 GiB floor. Both providers retain
the 5 GB data-disk floor; proxy resources are separate. Create/update and dry-run
admission reject undersized specifications, including unrelated updates to an
existing undersized cluster. Existing clusters are **not automatically resized**
by deployment of the control plane. Correct their requested RAM through
supported resource maintenance, or delete/re-create an unsuccessful fresh
deployment. A RAM-only resize is not proof that a new agent was installed.
The Group Replication message-cache fix specifically needs new data-member
agents; the PXC admission floor itself is control-plane-only. See
[MySQL resource guidance](mysql/configuration.md#mysql-group-replication-only).
The higher floor is not a bootstrap workaround: MySQL Group Replication cannot
form on any provider in this release, and PXC cannot form on Zerops. See
[known issues in v0.4.1](#known-issues-in-v041).

Explicit 1 GB disks are now admitted for Keycloak, Neo4j and Weaviate, matching
their engine minimums instead of the old shared 5 GB request floor. This does
not shrink existing volumes or add disk growth to Dokploy/Nomad.

There are **no new native release-asset bundles**. The four Percona Server lock
entries add Ubuntu 24.04's `libtirpc3t64` runtime dependency; preparation installs
it through APT. Exact-byte native artifact mirrors need no additional bundle,
but restricted/offline installations must provide this package and its
dependencies through their Ubuntu package source. Source-built control planes
must use a published artifact release explicitly when needed; see
[native artifacts](native-artifacts.md).

Disruptive changes for the new engines, including runtime-replacing HTTPS,
hostname and public TCP route changes, now wait for maintenance admission.
Use a generation-scoped explicit override only when intended; safety checks
still apply. Unchanged updates no longer cancel in-flight work. Status now
retains observed members on failed reads, distinguishes version drift from
configuration acknowledgement waits, and reports build inventory independently
of rollout support. These corrections do not turn missing evidence into health.

### Keycloak and Temporal PostgreSQL proxy compatibility

v0.4.0 Keycloak can fail before readiness when its PostgreSQL binding uses
ProxySQL: the proxy rejects mixed text/binary result-column formats and named
cursor portals. Disabling binary transfer alone is insufficient: Keycloak's
Hibernate fetch size of 64 still creates named portals in transactions.
New consumer deployments use Keycloak JDBC `preferQueryMode=simple`, covering
both restrictions even with explicit fetch sizes. JDBC parameter encoding and
TLS hostname verification remain enabled, but server-side preparation and
incremental cursor fetching are disabled; large results are buffered in memory.
Temporal uses the `postgres12` (lib/pq) plugin with
`disable_prepared_binary_result=yes`, including for explicitly prepared
visibility queries; lib/pq binds the unnamed portal. The writer proxy remains
in place. OpenBao 2.7.1 PG storage uses pgx but only single-column reads, so the
mixed-format rejection does not affect it.

**Upgrading the control plane alone does not install these agent-rendered
settings. Neither provider supports `upgrade-agents`; process restart/reload
also retains the old executable.** Do not copy binaries into running members.

For a failed, disposable consumer with **no valuable application state**, save
its API create request (not response-only fields), record the source/binding and
database names, then after deploying the fixed control plane and configured
member artifact, recreate **only the consumer**:

```sh
karactl -n NS clusters delete CONSUMER --wait --yes
karactl -n NS clusters create -f consumer.json --wait --timeout 30m
karactl -n NS clusters status CONSUMER -o json
karactl -n NS clusters members CONSUMER -o json
```

This is full deletion: it **drops the consumer-owned bound databases and login**.
Never target the source PostgreSQL cluster. Verify the new member's build
identity and native readiness, not merely a Ready binding. A Keycloak deployment
that never passed initial Liquibase startup may have only incomplete bootstrap
state, but confirm there is no imported/pre-existing data or external writes
before deleting it. Do not generalize this to an already-used Keycloak cluster.

Temporal can accept writes before a later visibility lookup fails. Readiness
failure or the proxy error does **not** prove it is empty: deletion can destroy
workflow histories, running executions/timers/tasks, namespace metadata and SQL
visibility. For any valuable Temporal or Keycloak data, stop application writes,
take and verify a source PostgreSQL backup, retain the current consumer and
binding, and arrange an explicitly reviewed data-preserving migration or
deployment procedure. The disposable recreation sequence is not that procedure;
this release does not add a generic in-place agent rollout for these providers.

**Do not use `--keep-volumes` suspend/resume as an agent-upgrade shortcut.**
Binding release with retention disables the login and removes its binding
record; a subsequent new binding incarnation can reject the retained database
owner rather than safely adopt it. Retention is not a transparent redeployment
or a backup. Do not edit internal state, role ownership or platform logical
records to bypass this fence.


### PostgreSQL exact versions and existing version drift

In v0.4.0, installing PostgreSQL profiles could upgrade a previously exact-pinned
server as an APT dependency. For example, a cluster assigned Percona PostgreSQL
17.10.1 with PostGIS could actually run 17.11.1 while member status still reported
17.10.1. This affects the shared PostgreSQL/Supabase core on all providers.
Native-artifact mirrors do not freeze the separate APT repositories.

**Before refreshing existing member agents, changing profiles/resources, or
replacing a data runtime, inventory every member with `SELECT version()` over a
verified connection.** Old agents echo assigned versions, so v0.4.0 status is not
proof of the installed release. Record SQL results and take a backup. If a
member runs a newer catalog release than requested, explicitly update the
cluster's desired release to that observed release (or a supported newer one)
before any runtime preparation. Do not install older packages over the existing
data directory to make the binary match the old specification.

For PostgreSQL 16, SQL and older agents may report only the upstream release
`16.15`, with no Percona package revision. Select catalog version `16.15.1`
for that release (or a newer supported same-major catalog version). Missing
product revisions are not treated as downgrades or unresolved drift; an explicitly
reported newer product revision still blocks an older pin. The corrective engine
upgrade rolls members normally, including when they still run v0.4.0 agents.
Wait for its generation to converge before `upgrade-agents`. An agent-only rollout
before correcting the stale database pin is rejected rather than reinstalling
older database packages. Public member versions retain the actual SQL release,
which can remain `16.15` after alignment to catalog `16.15.1`.

v0.4.1 keeps the requested server package as the APT candidate throughout profile
installation. Updated member agents report the running Percona product release,
not configuration intent. An unavailable SQL version observation is empty.
An unexpected observed data-member release raises `VersionDrift=True` with the
member, observed/desired releases and recovery guidance, and the cluster is not
Ready. Expected source releases during a forward rollout remain Updating
instead of producing drift incidents. A confirmed mismatch survives missing
observations as `VersionDrift=Unknown` until fresh SQL confirms the target;
missing observations alone do not create drift on unaffected members. The
specification is never rewritten automatically. Preparing a runtime below its
observed server release is rejected, including agent-only redeployment.

For an affected 17.10.1-assigned/17.11.1-running cluster, explicitly select
17.11.1, complete the same-major reconciliation and agent refresh, verify SQL and
member status agree on every member, then take a new backup. Only then request an
adjacent major upgrade. Major-upgrade admission reports unresolved assignment/
observation drift explicitly rather than rejecting an actual-release backup as
though no current backup existed. Review [PostgreSQL version-drift recovery](postgres/README.md#existing-version-drift)
before proceeding. This upgrade neither downgrades engines nor recreates clusters.

### PostgreSQL restores stuck at access

v0.4.0 accidentally sent every managed-role recovery password request through
the binding mutation scope. The agent rejected it before SQL execution because
normal binding mutations require an OID-fenced journal. This affects every
provider, including single-member and HA PostgreSQL/Supabase; it requires a
managed login role present in the recovered catalog. It is not an IPv6, TLS,
extension-profile or PostgreSQL minor-version error. v0.2.0 and v0.3.0 do not
contain this regression.

An affected restore is Failed and the cluster Degraded at `access`, with client
HBA rules and proxy users intentionally held closed. **Do not submit another
restore or edit HBA files.** Preserve the control-plane database/master key,
backup objects and sealed recovery credentials.

For a stuck cluster with only ordinary managed roles (no `kb_` binding roles),
deploy the fixed control plane first, preserving its state and master key.
The fixed control plane uses the original password-only protocol accepted by
v0.4.0 member agents: the existing journal automatically resumes, rekeys roles,
publishes access and releases client holds. No new API action is required.
Wait for `RestoreRecovery` to clear, restore-resource success, working verified
SQL and fresh cluster readiness before refreshing agents. The original Failed
operation remains historical evidence. If version drift also exists, resolve
the actual/desired release mismatch as described above **after recovery and
before any member redeployment**; this recovery does not downgrade PostgreSQL.

New control-plane/member-agent pairs also support password-only recovery of
binding identities, gated by the member's client-held state. **An already stuck
v0.4.0 source containing recovered `kb_` roles cannot use `upgrade-agents` as an
escape:** its restore journal owns the operation queue. Skipping binding rekey
is not safe: normal binding ownership checks do not reapply a password rotated
since the backup, and could leave a nominally repaired consumer unable to log
in. There is no in-place legacy-agent bypass. Preserve the backup and use the
supported delete/new-target recovery in
[failed restore and client holds](postgres/backups.md#failed-restore-and-client-holds).
Remove dependent consumers/bindings through their supported lifecycle first if
they prevent source deletion. Do not copy executables into running containers.

Before another restore, complete a supported member-agent rollout on a healthy
cluster. A failed destructive restore never opens incomplete data merely
because a retry budget expires: repair and automatic continuation, or explicit
target abandonment and new-target recovery, are the supported outcomes.

### PostgreSQL administrator password cannot be rotated yet

**v0.4.1 does not support in-place rotation of the `postgres` administrator
credential returned by the connection endpoint.** `system-users rotate` returns
HTTP 409 with an explanation; the other internal PostgreSQL system roles are
also not rotatable through this command. Native application-role password
rotation and managed binding rotation do not rotate the administrator.
For v0.4.7, see [PostgreSQL administrator password rotation](#postgresql-administrator-password-rotation).

If this credential is exposed, restrict access at both private and public
network boundaries and terminate existing privileged sessions; removing public
exposure alone does not disconnect them. Treat the exposure as a superuser
compromise and assess data, schema and role changes. If retaining data, quiesce
writers and restore or clone trusted data into a **fresh cluster**, which gets
new system credentials. Verify the recovered data, new authentication and
rejection of the exposed password on every new client route. Physical recovery
also carries application identities from the source catalog; inspect them for
compromise.

Migrate applications and bindings deliberately: binding sources are immutable,
so bound consumers require recreation and an explicit data migration. Resolve
deletion guards, retire the compromised source through the normal lifecycle,
verify its endpoints/runtimes are gone, and securely retire exposed connection
files. In-place restore, suspension/resume and TLS rotation are not password
revocation; manual SQL password changes or private rekey calls are not supported
workarounds. Follow the detailed
[administrator credential containment procedure](postgres/access-and-tls.md#administrator-credential-containment)
and [CLI rotation limitations](cli.md#command-groups).

### Citus restores require updated member agents

v0.4.0 Citus restores could stall at credential recovery because system-role
changes were implicitly propagated to workers whose client access was
intentionally fenced. v0.4.1 performs node-local, coordinated credential
recovery on every group primary before releasing access; it does not relax
client fences to make recovery succeed.

**Both the updated control plane and updated member agents are required.**
Older agents reject the new local password-recovery request before SQL
execution; the restore fails closed with recovery holds retained rather than
downgrading to an unsafe request. Updating the control plane alone does not
replace member executables. Complete the supported agent rollout on a healthy
formation before starting a restore.

An already failed v0.4.0 Citus restore keeps its recovery journal and holds:
neither another in-place restore nor `upgrade-agents` can take ownership.
Do not cancel recovery, edit HBA rules, clear journals or copy executables into
running members. Preserve control-plane state, the master key, sealed recovery
credentials, the retained backup inventory and all group repositories/WAL.
Recover by abandoning the failed target with backups retained:

```sh
karactl -n NS clusters delete FAILED_TARGET --keep-backups --yes --wait
```

Do **not** add `--keep-volumes`. Wait for deletion and verify the original
runtimes and volumes are gone. Confirm the retained coordinated backup and
archives still exist, then prepare a complete cluster-create request named
`citus-recovery.json` for a **new** target with fixed member artifacts,
source-compatible PostgreSQL version, distributed topology, matching per-group
replica count, worker-group count, Citus database and profile requirements.
Configure the target's registered backup storage and resources. Within `spec`,
select the retained backup by name:

```json
"dataSource": {
  "namespace": "NS",
  "cluster": "FAILED_TARGET",
  "backup": "RETAINED_BACKUP"
}
```

Use backup-name selection, not a timestamp or live-source PITR request: the
retained aggregate resolves its own coordinated named point after source
deletion.

The retained PostgreSQL source is identified by its recorded pgBackRest
repository and backup label, with the aggregate's group repositories, labels,
named restore point and replay bounds for Citus. It does not require a
`location` field or a snapshot-time native-binary URL. v0.4.0 incorrectly
required `location` after source deletion even though coordinated backups do
not populate it; v0.4.1 accepts their native provenance instead. Existing
complete retained records need no metadata rewrite or new backup. Incomplete
records still fail closed; a display location cannot replace missing native
provenance or sealed recovery credentials.

This admission fix requires only a control-plane update. The **new target**
must still use the fixed member agents described above; the control-plane fix
does not make old member executables safe for Citus credential recovery.

Submit the new target once:

```sh
karactl -n NS clusters create -f citus-recovery.json --wait --timeout 30m
karactl -n NS clusters status NEW_TARGET -o json
karactl -n NS clusters members NEW_TARGET -o json
```

Verify every member's fixed build, successful recovery, current Ready status,
generation convergence, Citus metadata and archive health. Before reopening
writes, verify recovered application data and authentication through the
coordinator endpoint. This is **new-target recovery**, not success of the
original failed restore. See [coordinated Citus recovery](postgres/distributed.md#coordinated-backup-and-pitr)
and [failed restore and client holds](postgres/backups.md#failed-restore-and-client-holds).

### Known issues in v0.4.1

The following issues published for v0.4.1 are **fixed in v0.4.2**: MySQL Group
Replication formation on every provider; PXC formation on Zerops; MySQL/PXC
full-backup failures; OpenBao briefly reporting Ready during rolling updates;
OpenBao/MySQL/PXC/Neo4j/Weaviate clones never reporting Ready; and delayed
resume/public-exposure recovery under a closed maintenance window. See the
[v0.4.2 upgrade guidance](#upgrading-to-v042) for affected versions and required
re-creation of existing MySQL/PXC clusters for agent-side fixes.

The PostgreSQL `postgres` administrator password also cannot be rotated yet; see
[its containment procedure](#postgresql-administrator-password-cannot-be-rotated-yet).
Temporal remains unsupported on Zerops.

## Upgrading from v0.3.0 to v0.4.0

**Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY` before starting v0.4.0.** Use the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup); retain the old binary and state together for recovery. Existing namespaces, clusters, credentials and policies are retained; upgrading does not migrate their provider or opt them into new engine topologies.

### Automatic state changes

Opening the store at startup applies these migrations in filename order, skipping recorded migrations. Each SQL migration and its completion record commit in one transaction; no separate migration command is needed.

| Migration | Purpose |
| --- | --- |
| `013_postgres_bindings.sql` | PostgreSQL consumer bindings, source identity and dependency metadata |
| `014_ops_cluster_fields.sql` | Adds managed-cluster ownership and maintenance-override generation fields |
| `015_notifications.sql` | Durable notification delivery outbox and retry indexes |
| `016_control_plane_backups.sql` | Control-plane backup run records and single-active-run index |
| `017_backup_drills.sql` | Backup drill reports and cluster/active-run indexes |

Migrations are **forward-only**. Downgrading an upgraded store to v0.3.0 is not supported; an older executable is not a schema rollback. To roll back, stop the new control plane and restore the consistent pre-upgrade state with its original master key and matching old binary. This does not roll back engine data or infrastructure changes made after the snapshot.

### PostgreSQL binding database access

[PostgreSQL](https://www.postgresql.org/) bindings are new in v0.4.0. Databases Karakoram creates for [PostgreSQL bindings](postgres/bindings.md) do not keep PostgreSQL's default PUBLIC `CONNECT` and `TEMPORARY` privileges: Karakoram revokes both from PUBLIC and grants them only to the binding's owning role; explicit grants Karakoram manages, such as Data [MCP](https://modelcontextprotocol.io/) readers scoped to that database, are kept. Another binding's role cannot connect to a consumer's database.

Karakoram re-checks these privileges on reconcile, not through the [SQLite](https://sqlite.org/) migration: if PUBLIC access reappears (for example, granted by hand or restored from older data), it is revoked again and sessions of roles without `CONNECT` are disconnected. Applications must use the owning binding identity or deliberately managed grants. This concerns binding databases only, not a blanket revocation on unrelated PostgreSQL databases.

### Source-build prerequisites

[`go.mod`](../go.mod) now declares **`go 1.26.0`**, with no separate `toolchain` directive. Source builders need [Go](https://go.dev/) 1.26 or newer; the [README build prerequisites](../README.md#build-from-source) also require [Node](https://nodejs.org/) 22.6+, [npm](https://docs.npmjs.com/) and [Make](https://cgit.git.savannah.gnu.org/cgit/make.git/). `make build-all` builds the UI before both executables; `make build` alone does not build UI assets. Prebuilt installations need no Go or Node toolchain.

### New runtime environment variables

`internal/server/config.go` adds no settings in this release. These new variables are read by the operational controllers/transports or offline recovery command:

| Variable | Default | Effect |
| --- | --- | --- |
| `KARAKORAM_BACKUP_DRILL_CONCURRENCY` | `1` | Installation-wide concurrent drill limit, integer 1–4; invalid values fail startup |
| `KARAKORAM_NOTIFY_ENDPOINT_ALLOWLIST` | unset | Comma-separated exact `host` or `host:port` exceptions for notification destinations; see [outbound safety](notifications.md#delivery-and-outbound-safety) |
| `KARAKORAM_RESTORE_S3_ACCESS_KEY_ID` | unset | S3 access key for offline `karakoram admin restore-state` |
| `KARAKORAM_RESTORE_S3_SECRET_ACCESS_KEY` | unset | S3 secret key for offline `karakoram admin restore-state` |

Restore credentials are needed only for [S3](https://aws.amazon.com/s3/) recovery, not ordinary startup. `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` and `KARAKORAM_ARTIFACT_MIRROR` already existed; they are not new settings. Native-test binary paths and development hooks are not installation requirements. See [notifications](notifications.md), [backup drills](backup-drills.md) and [offline state restore](control-plane-backup.md#local-export-and-offline-restore).

### New native artifacts

Installations mirroring native artifacts must add the following exact filenames from [`lock.json`](../internal/engine/nativeartifacts/lock.json), preserving their locked SHA-256 and size. Artifacts are selected by engine/version/profile, not all downloaded at control-plane startup.

| Feature | New files |
| --- | --- |
| OpenBao | `openbao_2.7.1_linux_amd64.tar.gz`, `openbao-plugin-database-mongodb_linux_amd64_v1.tar.gz`, `openbao-plugin-kms-aws_linux_amd64_v1.tar.gz` |
| Keycloak | `keycloak-26.8.1.tar.gz` |
| TimescaleDB | `timescaledb-pg16-2.31.0-ubuntu24.04-amd64.tar.gz`, `timescaledb-pg17-2.31.0-ubuntu24.04-amd64.tar.gz`, `timescaledb-pg18-2.31.0-ubuntu24.04-amd64.tar.gz` |
| Citus | `citus-pg16-14.2.0-ubuntu24.04-amd64.tar.gz`, `citus-pg17-14.2.0-ubuntu24.04-amd64.tar.gz`, `citus-pg18-14.2.0-ubuntu24.04-amd64.tar.gz` |
| Temporal | `temporal_1.31.4_linux_amd64.tar.gz`, `temporal_1.32.1_linux_amd64.tar.gz`, `ui-server_2.56.0_linux_amd64.tar.gz` |
| Percona Server | `Percona-Server-8.4.11-11-Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-Server-8.4.10-10-Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-Server-8.0.46-37-Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-Server-8.0.45-36-Linux.x86_64.glibc2.35-minimal.tar.gz` |
| PXC | `Percona-XtraDB-Cluster_8.4.11-11.1_Linux.x86_64.glibc2.35-minimal.tar.gz`, `Percona-XtraDB-Cluster_8.0.46-38.1_Linux.x86_64.glibc2.35-minimal.tar.gz` |
| XtraBackup | `percona-xtrabackup-8.4.0-7-Linux-x86_64.glibc2.35-minimal.tar.gz`, `percona-xtrabackup-8.0.35-36-Linux-x86_64.glibc2.35-minimal.tar.gz` |
| Neo4j / Java | `neo4j-community-2026.09.0-unix.tar.gz`, `neo4j-community-2026.10.0-unix.tar.gz`, `neo4j-community-2026.10.1-unix.tar.gz`, `OpenJDK21U-jre_x64_linux_hotspot_21.0.13_9.tar.gz`; retain `OpenJDK21U-jre_x64_linux_hotspot_21.0.12.1_1.tar.gz` for transitional replacement preparation |
| Weaviate | `weaviate-v1.40.1-linux-amd64.tar.gz`, `weaviate-v1.39.12-linux-amd64.tar.gz` |

[TimescaleDB](https://www.timescale.com/) and [Citus](https://www.citusdata.com/) bundles are Karakoram-built; development builds need a mirror or explicit `KARAKORAM_ARTIFACT_RELEASE` containing their exact locked bytes. Other additions have official upstream URLs. [Weaviate](https://weaviate.io/) is upstream-only, mixed-license and **not redistributed as a Karakoram release payload**. A private mirror is not permission to redistribute upstream software. See [native artifacts](native-artifacts.md) for resolution order, provenance and licensing.

### Deploy on Zerops

After the state/key backup, deploy the published tag from the checkout root:

```sh
git fetch origin --tags
git checkout v0.4.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Set `PROJECT_ID` as below, then follow [deployment and readiness](#deployment-and-readiness) and [member-agent rollout](#rolling-out-member-agents). Review [engine/provider status](providers.md#engine-availability) and the [verified-offline Ops features](operations.md) before enabling new functionality.

[Temporal](https://temporal.io/) is available on [Dokploy](https://dokploy.com/) and [Nomad](https://developer.hashicorp.com/nomad) only in v0.4.0. [Zerops](https://zerops.io/) support is pending IPv6 private networking: the current Temporal membership runtime requires a private IPv4 route to the bound PostgreSQL writer.

## Upgrading from v0.2.0 to v0.3.0

**Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY` before starting v0.3.0**, as for every upgrade ([consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup)). Namespaces, clusters, secrets, policies and API tokens are retained; no cluster recreation is required.

### Automatic state changes

No [SQLite](https://sqlite.org/) migrations are added after `012_sanitize_diagnostics.sql`. [OAuth](https://oauth.net/2/) grants and API-token restrictions use existing tables, and dry-run previews run in read-only transactions. Migrations remain forward-only; keep the pre-upgrade backup for rollback.

### Agent access is on, read-only

After the upgrade the control plane serves the [MCP server](mcp.md) at `/mcp` because `KARAKORAM_MCP` defaults to `readonly`: holders of valid API tokens, including existing unrestricted ones, can call read-only tools through the same RBAC and audit path as the REST API. Nothing becomes readable that the token could not already read. To disable the endpoint, set `KARAKORAM_MCP=off` in the control plane's service environment ([Zerops](https://zerops.io/): the service's environment variables, or `envVariables` in the import YAML) and restart; set `readwrite` only deliberately. New variables, all documented in [Install](install.md#required-secrets-and-generated-settings):

| Variable | Default | Effect |
| --- | --- | --- |
| `KARAKORAM_MCP` | `readonly` | `off`, `readonly` or `readwrite`; invalid values fail startup |
| `KARAKORAM_MCP_OUTPUT_SCHEMAS` | `false` | Set `true` in the server environment and restart to advertise output schemas; off keeps discovery/context smaller without changing successful structured results or text content |
| `KARAKORAM_MYSQL_AUTO_REPLACE_LOST_VOLUMES` | `false` | Experimental automatic confirmed-loss replacement for MySQL GR and PXC; off leaves Degraded with explicit `replace-member` guidance. `true`/`false`; invalid non-empty values fail startup. Control-plane restart required; explicit replacement and other engines unchanged |
| `KARAKORAM_OAUTH` | `off` | Embedded OAuth authorization server for MCP connectors |
| `KARAKORAM_OAUTH_DCR` | `off` | Dynamic client registration, only with OAuth on |
| `KARAKORAM_OAUTH_CLIENT_HOSTS` | unset | Optional allowlist of client-metadata hosts |

Existing API tokens stay unrestricted; mint [restricted tokens](access-control.md#restricted-api-tokens) for agents with `karactl mcp config`. Stored RBAC policies are kept. The new `cluster-data` resource is covered by the administrator wildcard but not by the built-in viewer or operator roles; grant it explicitly per `<namespace>/<cluster>`. [Data MCP](data-mcp.md) stays off for every cluster until `spec.dataMcp.enabled: true` is set on that cluster.

### New native artifacts

Data MCP companions for [MongoDB](https://www.mongodb.com/), [PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) use two new entries in `lock.json`, downloaded only when a cluster enables data MCP: `mongodb-mcp-server-3.1.0-ubuntu24.04-amd64.tar.gz` is a Karakoram-built release asset with no upstream URL, and `pgedge-postgres-mcp-server_1.1.0_linux_x86_64.tar.gz` comes from the official [pgEdge](https://www.pgedge.com/) release. Operators running a `KARAKORAM_ARTIFACT_MIRROR` must add both files with their exact locked bytes; development builds need a mirror or explicit `KARAKORAM_ARTIFACT_RELEASE` containing the locked MongoDB bundle. See [data MCP companion artifacts](native-artifacts.md#data-mcp-companion-artifacts).

### Deploy on Zerops

After the state/key backup, deploy the published tag from the checkout root:

```sh
git fetch origin --tags
git checkout v0.3.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Set `PROJECT_ID` as below, then follow [deployment and readiness](#deployment-and-readiness) and [member-agent updates](#rolling-out-member-agents). Data MCP companions are new private services created only when a cluster opts in.

## Upgrading from v0.1.x to v0.2.0

**Back up control-plane state and preserve the original `KARAKORAM_MASTER_KEY` before starting v0.2.0.** Use the [consistent pre-upgrade backup](#consistent-pre-upgrade-state-backup) below, not a live copy of the [SQLite](https://sqlite.org/) main file. Let engine operations settle and retain the old revision and backup for recovery.

Existing [Zerops](https://zerops.io/) namespaces and clusters retain their provider, resource identities, secrets and desired specifications; no cluster recreation or provider migration is required. Stored namespaces without a provider kind normalize to `zerops`, and omitted namespace-create kinds still default to Zerops. [PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/) are new engines; upgrading the control plane does not change an existing cluster's engine driver or engine version.

### Automatic state changes

Startup applies the following migrations added after v0.1.2, in order, without a separate migration command:

| Migration | Purpose |
| --- | --- |
| `010_lifecycle_recovery.sql` | Marks failed delete tracking successful when the matching cluster incarnation is already absent; releases backup inventory stuck in `Deleting` after terminal failed/cancelled deletion so deletion can be retried. |
| `011_metrics_retention.sql` | Adds the `metrics(timestamp,id)` index for bounded retention scans. |
| `012_sanitize_diagnostics.sql` | Rewrites legacy diagnostic JSON and event details using the current sanitizer in bounded, resumable batches; leaves secret values and row metadata intact. |

Migration 011 builds an index over existing metrics; migration 012 rewrites legacy diagnostics. These can extend first startup on large stores. Measure on an isolated copy and allow enough readiness time; see [Monitoring](monitoring.md#upgrading-an-existing-metrics-database). Migrations are forward-only: an older binary is not a schema rollback.

### API and specification compatibility

The `/api/v1` API and existing Zerops cluster spec shapes remain. The [OpenAPI](https://www.openapis.org/) changes are principally additive, but clients must account for these response and admission changes:

- Namespace responses now require `backend` and `capabilities` and may include non-secret `config`; namespace creation accepts optional `backend` and `config`. `projectId` remains the field name for the provider target ID. Strict clients that reject new response properties need updated models.
- `GET /backends` lists compiled-in provider metadata and requires `namespaces/read` on global `*`.
- Cluster create/PUT/PATCH reject missing provider capabilities with `400 invalid_request`. A newly selected public IP family that fresh provider information proves unavailable is rejected; unchanged legacy exposure is preserved and unknown availability does not reject admission.
- `POST /clusters/{cluster}/retry-deployments` explicitly re-arms exhausted deployment budgets. `spec.retryEpoch` is server-managed/read-only, not an ordinary spec-edit control.
- Backup or source-cluster deletion can return `409 invalid_state` while an in-flight restore/create depends on it; PITR dependencies also protect the source/history. Wait for completion or safe cancellation rather than bypassing the guard.
- PostgreSQL role/grant and Supabase credentials, SMTP, functions and JWT-rotation endpoints are added. Topology, backup-type, member-role and credential enums gain engine-specific values; member responses may include `componentVersion`, and agent status may include `message`. Some engine-administration reads now document `503` for unavailable dependencies.
- Restore readiness is explicitly false during recovery and remains false with `RestoreFinished` until ordinary reconciliation observes database health. Do not infer database readiness from operation completion alone.

Use the matching v0.2.0 CLI/SDK models; see the [API reference](api.md) and [OpenAPI contract](../api/openapi.yaml). No existing path or request property was removed in the v0.1.2-to-v0.2.0 contract diff; expanded enums, stricter admission and deletion guards are notable behavior changes, not a blanket promise that every old client handles new responses.

### Deploy on Zerops

After the state/key backup, deploy the published tag from the checkout root:

```sh
git fetch origin --tags
git checkout v0.2.0
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Set `PROJECT_ID` as below. Preserve the existing volume, master key, secrets, public origin and single-writer deployment settings. Then follow [deployment and readiness](#deployment-and-readiness) and [member-agent updates](#rolling-out-member-agents); upgrading the control-plane binary alone does not replace running member binaries.

### New optional providers

Zerops remains the reference provider. [Dokploy](https://dokploy.com/) and [Nomad](https://developer.hashicorp.com/nomad) are additional options, not an automatic migration for existing installations. Review [Providers](providers.md), then the [Dokploy installation](dokploy.md#install-the-control-plane-on-dokploy) or [Nomad installation](nomad.md#install-the-control-plane-on-nomad). Keep the existing default namespace on its original target; additional targets use explicit provider selection and credentials through [Namespaces](namespaces.md).

## Prerequisites

For new installations use [Install](install.md). The concrete deployment, maintenance-container and ownership commands below are for the **[Zerops](https://zerops.io/) provider**. [Dokploy](dokploy.md#install-the-control-plane-on-dokploy) and [Nomad](nomad.md#install-the-control-plane-on-nomad) use their guides' singleton/stop-first deployment paths with the same consistent-state and master-key backup requirements. Zerops upgrades need the target checkout, authenticated [zcli](https://docs.zerops.io/references/cli) with deployment access, runtime secrets and Local Storage maintenance-container access. Use `zcli login` with a Zerops token; avoid shared terminals/shell history.

From the checkout root, set:

```sh
PROJECT_ID='<project-id>'
```

Commands assume default service names; substitute customized names consistently.

## Before deploying

1. Read target-release compatibility notes. Retain the previous source revision/application version.
2. Let provisioning, backup, restore, scaling and upgrade operations settle. Avoid desired-state changes during maintenance. Engine processes continue independently, but reconciliation, new operations, schedules and monitoring pause while the control plane is down; choose a window that will not miss required backups.
3. Take the consistent state backup below; preserve `KARAKORAM_MASTER_KEY` separately. State without its original key cannot decrypt credentials.
4. Keep the same volume, secrets, project ID and public origin. Never rerun the installer or regenerate the master key.

## Consistent pre-upgrade state backup

**Warning:** A live copy of [SQLite](https://sqlite.org/)'s main file or a live volume tarball can omit committed WAL changes. There is no `karakoram admin backup-db` command.

1. Stop the runtime:

   ```sh
   zcli service stop karakoram -P "$PROJECT_ID"
   ```

   Wait until the stop finishes and the runtime is `STOPPED`; no control-plane writer may remain.
2. Open **karakoramdata → Web shell**. The maintenance container mounts state at `/data` (runtime: `/mnt/karakoramdata`). Archive outside it:

   ```sh
   umask 077
   tar -czf /tmp/karakoram-state-before-upgrade.tar.gz -C /data .
   ```

   Include the whole directory: database, remaining WAL/SHM sidecars and persisted identities. Never attach another running writer.
3. Download through the maintenance container's **File browser**. Store encrypted off-platform, record version/time and restrict access: the archive contains authentication state and encrypted infrastructure secrets. `/tmp` and same-volume copies are not durable protection against host/volume loss.
4. Leave stopped for immediate deployment; otherwise restart the old version:

   ```sh
   zcli service start karakoram -P "$PROJECT_ID"
   ```

See [Zerops Local Storage maintenance](https://docs.zerops.io/local-storage/how-to/manage). Automated consistent backups can use SQLite's native backup API/tooling; do not assume the runtime includes `sqlite3`.

## Deploy

### Prebuilt release (default)

After state/key backup, select the published release:

```sh
git fetch origin --tags
git checkout v0.4.1
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --workspace-state clean --disable-logs
```

Choose your target tag, or update `main`: [`deploy/zerops/RELEASE`](../deploy/zerops/RELEASE) pins the latest published release, not newer source. Tags pin themselves. **Never edit the pin by hand**; only the [release tool](../CONTRIBUTING.md#releasing) changes it.

The default setup verifies the manifest's SSH signature, archive SHA-256 and binary version; allow seconds, with no [Go](https://go.dev/)/[Node](https://nodejs.org/) toolchain. `--workspace-state clean` uploads only committed `HEAD`; do not add `--no-git`, which uploads the whole checkout.

For [Git](https://git-scm.com/)-connected services, redeploy the chosen tag or `main` through [Zerops](https://zerops.io/) controls; its ref determines the pin. Installer `buildFromGit` is a one-time import, not webhook configuration. Never rerun the helper or reimport to upgrade.

Download, signature/checksum or build failure never reaches deployment and leaves the running version untouched. A service left stopped after backup remains stopped until started or successfully deployed.

### Source builds and provenance

Use `--setup karakoram-source` for the exact checked-out commit, including UI and static Go binary (minutes): forks, development or unreleased fixes. Git imports select `zeropsSetup: karakoram-source`.

Before source pushes without Git metadata, merge literal `git describe --tags --always` / `git rev-parse HEAD` output into the source setup's `build.envVariables`; see [source provenance](install.md#build-provenance-for-local-pushes). Explicit commit overrides discovery; the pipeline records UTC build time. These are build inputs, not runtime settings. Prebuilt binaries already carry provenance; missing source metadata appears as `unknown (built without git metadata)` in the API/UI.

Prefer `--workspace-state clean` and commit intended source/pipeline changes before pushing. `--no-git` uploads the whole checkout: keep private imports outside it and exclude other sensitive files first. The [source helper](install.md#one-command-helper) uses this no-Git path.

### Deployment and readiness

Both setups replace the executable in the existing runtime. Keep `deploy.temporaryShutdown: true` and one-container maximum: stop the old writer before starting the new one. Brief UI/API downtime is intentional; do not use overlapping deployments. Automatic state migrations need no separate command. Deployment probes `/healthz` on port 8080 (120-second failure timeout, 10-second retries). Wait for deployment completion and `ACTIVE`, then check:

```sh
curl --fail --show-error https://example.com/healthz
karactl --context production health
karactl --context production version
```

Check clusters, operations, backup inventory, member agents and logs without exposing secrets. Healthy control-plane readiness does not prove engine recovery works.

## Existing single-project installations

Existing clusters, events and metrics enter namespace `default`; existing secrets and master key remain valid. Stored policy is normalized: `*` becomes `**`, while bare cluster patterns such as `orders*` become `default/orders*`, retaining their old project scope. Namespace rules keep bare names. Review [access control](access-control.md).

Upgrades retain stored policy rather than adding fresh-install namespace-read defaults. If operators/viewers should enumerate namespaces, merge these rules into the **complete existing policy**, preserving administrator access; use narrower grants where appropriate:

```csv
p, role:operator, namespaces, read, **
p, role:viewer, namespaces, read, **
```

Without namespace-read grants the UI shows only `default`, even with remote-cluster access. Remote relays update automatically; expect brief management interruptions and check desired/reported versions and connected state in [Namespaces](namespaces.md#relay-health).

## Rolling out member agents

Upgrading or restarting the control plane does **not** replace existing member agents. A member receives the current executable on its next control-plane-managed deployment. Do not copy binaries into running containers or add another supervisor.

Inspect `karactl clusters get NAME` or `clusters status NAME`: `status.agents` reports `targetBuild`, `targetVersion`, and the `outdated` and `unknown` member names. Member output includes `agent.version`, `agent.commit` and `agent.build`. The `AgentsCurrent` condition is informational: `True/Current`, `False/Outdated`, or `Unknown/Unreported`; it never makes a cluster unready or Degraded. An `AgentsOutdated` event signals outdated member agents.

Build inventory is independent of rollout execution: v0.4.7 supports
`upgrade-agents` for PostgreSQL (including Citus), Supabase, MongoDB/PSMDB,
RabbitMQ, Elasticsearch, MySQL/PXC (including ProxySQL), OpenBao, Keycloak,
Temporal, Neo4j and Weaviate. A healthy native engine is not evidence of a current
agent executable. Legacy agents without build metadata remain unknown.
PostgreSQL proxy, Supabase, Temporal and Keycloak instance observations
conservatively report a non-current or unknown identity when replicas disagree,
rather than letting a current replica hide an incomplete rollout.

PostgreSQL proxy members report the identities observed on their live instances,
independently of native proxy health. If instances disagree, member inventory
selects a non-current build (or an unknown identity for a legacy instance), so
one current replica cannot hide an incomplete rollout. Older control planes can
omit proxy build metadata even when the agent reports it; upgrading the control
plane corrects this inventory without redeploying the proxy. Missing inventory
metadata alone does not explain a Running rollout: completion reads the agent's
reported executable build directly, after platform deployment and health gates.

```sh
karactl clusters upgrade-agents NAME --wait --timeout 30m
# Include agents already reporting the current build:
karactl clusters upgrade-agents NAME --include-current --wait
```

The durable `upgrade-agents` operation redeploys the current executable at the **same engine version**, using each engine's safe, one-member-at-a-time rolling sequence and health/quorum gates. Unknown/legacy agents are eligible for a manual run. If every agent is current, the API returns HTTP 409 with code `AgentsCurrent` unless `--include-current` is set. Cancel with `karactl operations cancel ID`; cancellation is cooperative between members, not a rollback of completed deployments.

Rollouts respect [maintenance windows](operations.md#maintenance-windows). Use `--ignore-maintenance-window` for an explicit override; this does not bypass engine safety gates. Timing out or interrupting `--wait` does not cancel the rollout; retain the operation ID and use `operations wait ID`.

An agent rollout can be claimed even if ordinary desired-state synchronization
fails (for example, because the old agent contains the bug the rollout repairs).
This exception applies only to `upgrade-agents`: it does not start unrelated
queued actions or displace an already Running operation. Provider claim policy,
maintenance windows, native health checks and bounded operation retries still
apply. Delete and keep-volumes suspend already use a separate cleanup path and
do not require successful ordinary synchronization.

The enqueue-frozen rollout target is not silently retargeted after another
control-plane upgrade. A Pending PostgreSQL rollout targeting a previous
control-plane build is claimed and fails explicitly before deployment, with a
message to re-run `upgrade-agents`. Keep that receipt for evidence and submit a
new rollout targeting the current control plane. An already Running rollout
retains its provider-specific recovery/checkpoint safeguards.

PostgreSQL resumes an in-flight member after a control-plane restart. If a
failover changed the WAL timeline while a replica was being redeployed, it
persists a fresh barrier from the current primary of the same replication
group and system identity. Completion requires the replica to report streaming,
the current leader and timeline, no lag, and replay at or beyond that barrier;
WAL positions from different timelines are never compared. A different system
identity fails explicitly rather than treating a foreign replica as caught up.
Barrier refresh requires successful observations of every group member and a
unique self-reported primary with healthy DCS. Local replica status alone is not
proof of its WAL source: the control plane also reads the selected primary's
replication view to confirm exactly one streaming sender for that member and
acknowledged replay past the barrier, on the expected primary system/timeline.
This bounded read-only query uses the existing `kk_monitor` identity,
namespace-aware native connection and cluster-CA hostname verification. It
requires no member-agent upgrade. Missing SQL access or replication proof never
acknowledges the member; do not weaken TLS or grant arbitrary SQL access to
bypass a failed proof.
Unproven catch-up remains subject to the existing retry/deadline budgets; restore
replication and follow the failure's `re-run upgrade-agents` guidance.

A changed control-plane build can finish acknowledging the final already
deployed member against the original frozen target and retire that operation.
It cannot deploy remaining members for the old target: that requires a new
rollout. Success of the old operation does not imply agents match the new
control plane; inspect inventory before requesting the new rollout.

Agent rollout requires an advertised engine driver operation backed by a one-member disruption journal. From v0.4.7 every member-agent engine implements that contract, including [Keycloak](https://www.keycloak.org/), [OpenBao](https://openbao.org/), [Temporal](https://temporal.io/), [MySQL](https://www.oracle.com/mysql/), [PXC](https://www.percona.com/mysql/software/percona-xtradb-cluster), [Neo4j](https://neo4j.com/) and [Weaviate](https://weaviate.io/); see [agent rollout now covers every member-agent engine](#agent-rollout-now-covers-every-member-agent-engine) for per-engine ordering and availability. On v0.4.5 and earlier control planes those engines report unsupported agents during automatic rollout without scheduling any disruption, and manual rollout is rejected. Enabling automatic rollout never opts any engine into a generic restart or redeployment fallback.

Automatic rollout is **off by default**:

```sh
karactl settings agent-rollout get
karactl settings agent-rollout set --automatic=true --max-concurrent-clusters 2
karactl settings agent-rollout set --automatic=false --max-concurrent-clusters 1
```

These installation-wide settings include a read-only `currentBuild` (`version`, `commit`, `build`). `set` replaces the settings; omitted flags default to `automatic=false` and `maxConcurrentClusters=1` (allowed range **1–5**).

When enabled, the controller checks every **60 seconds**, enqueuing rollouts only for clusters with **outdated** agents, bounded by `maxConcurrentClusters` across the installation and respecting maintenance windows. Unknown/legacy agents alone do not trigger automatic rollout: run manually. A failed rollout for a target build is **not retried automatically** for that build; fix the cause and re-run manually.

## Rollback

- A pre-deployment build failure leaves the current application unchanged. After temporary shutdown, inspect service/process state; start/redeploy the previous version if necessary.
- Redeploy an old binary only if it supports the current schema. Migrations are forward-only; a pre-namespace binary requires pre-upgrade state.
- Otherwise stop **every writer**, preserve failed-version state, and restore the complete pre-upgrade archive into an **emptied** state directory through the maintenance container. Never merge stale WAL/SHM or identity files. Restore `zerops` ownership, restrictive permissions and the matching master key; deploy the old binary before starting it.
- State rollback rewinds specifications, operations, tokens, sessions and inventory—not external services or engine data. Review changes and in-flight operations before resuming reconciliation. It does not undo engine-level upgrades or compatibility changes; see [MongoDB](mongodb/operations.md), [RabbitMQ](rabbitmq/operations.md) and [Elasticsearch operations](elasticsearch/operations.md), and [Backups and storage](backups.md) for data recovery.

For [Weaviate](https://weaviate.io/), follow [native upgrade operations](weaviate/operations.md#restart-and-upgrade): only cataloged patch-forward or one-minor-forward changes, never downgrades or skipped minors. Members roll with quorum/health gates; restoring the control-plane archive does not reverse native data changes.

Retain the archive, master key and previous revision until the upgrade and agent transition are accepted.

## Version notes

### v0.4.0: fleet operations and recovery

- Member build identity and explicit rolling agent upgrades keep executable rollout separate from database-version upgrades. Automatic rollout is opt-in; existing and legacy agents are not redeployed merely because the control plane restarts.
- Optional cluster maintenance windows defer new disruptive work. Emergency healing and near-expiry certificate recovery remain exempt; explicit overrides never bypass engine safety gates.
- Placement reports distinguish authoritative anti-affinity from best-effort evidence. Member relocation uses native replacement and synchronization, and cannot promise a new host on providers without placement support.
- Control-plane backup policy, durable notification deliveries and backup-drill reports add forward-only [SQLite](https://sqlite.org/) migrations. Preserve a consistent pre-upgrade snapshot and the original master key; an older binary must not open newer state.
- State snapshots do not contain the operator master key or external provider credentials. Rebuild with the original key, endpoint identity and valid credentials; review external changes since the snapshot before resuming reconciliation.
- Import the [Grafana dashboards and Prometheus rules](monitoring.md) and configure [notification channels](notifications.md) deliberately. Existing installations retain their RBAC policy; installation-wide settings still require explicit settings access.
- Native configuration qualification now gates every registered engine/component on IPv6-only member rendering, with explicit local-only socket exceptions. Temporal still rejects IPv6-only backends. Supavisor artifact smoke checks both SQL listeners over IPv6 as well as the existing IPv4 transaction path; the SQL family patch still requires a rebuilt and published artifact.

### v0.1.2

- Default [Zerops](https://zerops.io/) deployments use signed prebuilt releases pinned by the checkout, not unreleased source; forks/unreleased fixes require `karakoram-source`. Preserve the state volume, master key and single-writer deployment settings.
- Signed CLI checksum manifests start with v0.1.2; [verify the signature and checksum](install.md#install-karactl) and match the deployed server version.
- Cluster specification changes record `ClusterSpecChangeRequested` audit events with field-level details for `audit:read`; see [Reviewing audit activity](operations.md#reviewing-audit-activity).
- Platform resource-update rejections expose the hostname, error code and attempted bounds; see [Resources and autoscaling](clusters.md#resources-and-autoscaling).

### v0.1.1

Healthy clusters need no action. Project-scoped Full access tokens work without sudo mode; existing resources gain current ownership evidence during reconciliation.

### Legacy installations (before v0.1.1)

For unverified ownership or repeated `zerops HTTP 400 (processNotFound)`, upgrade to v0.1.1 or newer first. Expired imports have unknown outcomes: reconciliation observes existing services and imports missing ones; other expired actions use bounded retries.

If initial provisioning still reports unverified ownership, choose one recovery path:

- Delete only never-deployed, data-free runtime/volume services from the failed import so reconciliation recreates them. A member volume appends `v` to its hostname (e.g. `<cluster>m0v`). **Never delete healthy members or retained data.**
- Supply a **temporary**, project-scoped Full access sudo-mode token for **one ownership-verification reconcile only**. For `default`, change `KARAKORAM_ZEROPS_TOKEN` and restart. Then restore the non-sudo token, restart again and **revoke the temporary sudo token**. Other namespaces use [token rotation](namespaces.md#add-validate-and-rotate).

Hostnames alone never prove [service ownership](security.md#service-ownership).
