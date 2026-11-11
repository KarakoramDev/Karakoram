<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Limitations

These cross-engine constraints define the operating model; engine-specific limits are linked below.

- **PSMDB 6.0 is EOL (2025-07-31).** New empty clusters must use 7.0 or 8.0; the create wizard disables 6.0. Existing clusters remain manageable and can upgrade 6.0→7.0→8.0 with adjacent FCV gates. Validated API/CLI `spec.dataSource` recovery of existing same-major data remains admitted, not guaranteed by current vendor PBM support. Its retained mongosh 2.8.3 lacks the bundled TLS-root fix shipped for supported lines. See [MongoDB lifecycle and recovery](mongodb/README.md).

## Zerops failure domains and storage

- **No anti-affinity guarantee on [Zerops](https://zerops.io/).** Separate services/volumes and dedicated CPU do not prove independent hosts, racks or zones. One host failure can remove several voters. Placement groups are best-effort observations: missing/changing signals can produce unknown, false or stale groups; distinct groups do not certify future separation. The [Dokploy](dokploy.md) and [Nomad](nomad.md) providers pin each member's volume to a node and keep one replica set or cluster on distinct nodes while enough nodes exist; they report that node as authoritative placement but cannot protect against shared racks or physical hosts below the scheduler.
- **Local Storage is single-host, non-replicated storage.** Each runtime is co-located with its volume. Runtime root disks are wiped on deployment; keep engine data only on assigned per-member volumes. Shared filesystems are unsupported. Replication and backups protect different failure modes.
- **Local Storage minimum is 5 GB.** Mounted capacity follows the volume service's `maxDisk`, not runtime disk or volume `minDisk`. Growth is supported; shrinking is rejected. Resource/autoscaling behavior belongs in [clusters](clusters.md).
- **Live volume archives are not consistent database backups.** Use [engine-native backups](backups.md) and a separately [consistent control-plane snapshot](operations.md#control-plane-state-backup-and-recovery).

## Provider-specific limitations

[Zerops](zerops.md) is the reference provider. [Dokploy](dokploy.md) and [Nomad](nomad.md) are additional providers; their status is documented on the [Providers page](providers.md). See the [provider comparison](providers.md#capability-differences) before selecting one.

- **Dokploy:** node-local [Docker](https://www.docker.com/) volumes have no enforced quota or supported growth. The operator owns Swarm, node disks, registry, DNS and firewall. Admin API keys are not target-scoped; use a dedicated installation. Public TCP has no provider CIDR filtering; public HTTPS needs a domain suffix and wildcard DNS.
- **Nomad:** dynamic `mkdir` host volumes have no enforced quota or supported growth. The operator owns dedicated clients, private-copy rootfs, routed networking, ACLs, disk monitoring and load balancing. Trust every submitter who can place jobs on the clients; namespace metadata is not hostile-tenant isolated. Managed public HTTPS is unavailable.
- **Public TCP on both providers:** requires explicit `allowAllAddresses: true` and an allowlist covering the whole address family. Enforce narrower access externally. Nomad static ports need load-balancer forwarding to the correct member node; they are not a routing mesh.
- **[PostgreSQL](https://www.postgresql.org/) and [Supabase](https://supabase.com/):** both use the shared runtime contract on these providers, but their guides do not claim engine acceptance. Locked native artifacts and registered [S3](https://aws.amazon.com/s3/) destinations must be reachable. Supabase public gateway HTTPS is rejected on Nomad.

## Control-plane and platform operations

- **One control-plane replica; no control-plane HA.** [SQLite](https://sqlite.org/) WAL requires a single writer. Keep `maxContainers: 1` and `temporaryShutdown`; upgrades interrupt management. Lost state cannot be reconstructed by automatically adopting existing services. Preserve consistent state and the original master key; see [recovery](operations.md#control-plane-state-backup-and-recovery).
- **REST polling, not platform watches.** Zerops Processes are asynchronous. Request acceptance is not completion or engine health. Desired resource settings are not current allocation; CPU-mode changes can enter `PlatformCooldown`. See [operations and status](clusters.md#operations-and-status).
- **Repeated reconciliation errors are suppressed.** Consecutive identical `ReconcileError` records are suppressed per worker; see [resources and autoscaling](clusters.md#resources-and-autoscaling) for attribution and reset rules.
- **Platform rejections remain authoritative.** Diagnostic messages are bounded and never include credentials or environment values; see [resource diagnostics](clusters.md#resources-and-autoscaling).
- **Schedules run in the control plane.** Downtime pauses new scheduled requests and retention. Unchanged enabled schedules with an existing durable cursor retry missed admissions after recovery, at most one occurrence per reload/callback; new or edited schedules have no retroactive guarantee. Already-running engine work has separate semantics; see [backups](backups.md).
- **Deletion can leave charged storage.** Platform refusals can move verified owned volumes into [orphan inventory](operations.md#orphaned-platform-volumes); completing cluster deletion does not prove every volume is gone.
- **No container-exec API.** Node work uses a bounded authenticated agent, not arbitrary remote SSH/exec. [Admin recovery](operations.md#recovering-local-admin-access) uses service secrets/restart or a trusted offline maintenance environment.
- **Deployment environments are snapshots.** Secret changes require restart/redeploy; runtime YAML environment values override service secrets. Member provisioning imports infrastructure before artifact deployment. Resource updates send complete vertical/horizontal settings.
- **Existing targets and cataloged engines only.** Karakoram does not create infrastructure projects/clusters; Zerops is the reference provider, with [Dokploy](dokploy.md) and [Nomad](nomad.md) providers. The system `core` service is outside its ownership. Public [Git](https://git-scm.com/) import requires a public repository; private source needs local deployment. Members use prebuilt artifacts.

## Namespaces and relays

- **One namespace per provider target.** Names are namespace-local for clusters/storage. Provider kind, namespace name and target identity are immutable; only descriptions change. The `default` infrastructure credential is environment-managed; API rotation returns 409. See [namespace lifecycle](namespaces.md).
- **One relay per additional target; no relay HA.** All current providers require it. Its loss interrupts management/config pulls/monitoring there, not engine replication. It is not application connectivity. Remote management requires outbound public HTTPS and WebSocket forwarding; the default namespace remains direct.
- **One ordered tunnel shares congestion and failure fate.** EOF closes both directions; TCP half-close is not propagated. The relay is not a general VPN or bulk-backup transport. Keepalive/reconnect behavior does not establish a longevity or throughput SLA.
- **Removal guards retained resources.** Clusters, storages, retained backup records and orphan inventory must be cleared first. Removal deletes the owned relay, not its project or RBAC policies; review grants before name reuse.
- **Bounds do not eliminate DoS.** Bounded handshakes/messages/streams still share listener/network resources. Project DNS/network remain trusted; inner relay TLS does not replace engine TLS. See [relay threat model](security.md#relay-threat-model).

## Access and networking

- On the **Zerops provider**, public TCP requires project IPv6 or dedicated IPv4; shared HTTP/IPv4 routing is not a generic engine TCP gateway. Verify client address-family connectivity and account for dedicated IPv4 cost.
- Zerops public route synchronization is project-wide, including other clusters' changes. `Ready`/`isSynced` do not establish external reachability. Check allowed and denied clients independently; retain TLS/authentication as well as allowlists. Other providers have the [exposure limits above](#provider-specific-limitations).
- Infrastructure administrators remain trusted. On Zerops, **Full access without sudo mode cannot read sensitive environment values**; this does not remove mutation authority. See [Security](security.md) for token authority, public exposure and TLS boundaries; Karakoram is not a sandbox against infrastructure administrators.
- Credential/leaf rotation is not general CA-source or master-key rotation. Use engine access guides for supported TLS transitions and certificate requirements.

### Certificate and member-credential lifecycle

- **Leaf recovery requires valid trust roots and a reachable control plane.** Agent HTTPS leaves are renewed through the member's authenticated configuration pull, independently of database health or rolling restart gates. An expired or future-dated agent leaf therefore does not have to authenticate a control-plane-to-member status request to receive its replacement. CA verification, hostname verification and bearer authentication remain enabled. Database certificates still follow engine-specific reload/rolling safety rules.
- **Renewal needs writable control-plane state.** If persisting a replacement internal HTTPS leaf fails, new handshakes can continue using the active identity only while it is currently valid. Repair the storage fault before that identity expires; expired or future-dated identities are never used as a continuity fallback.
- **Keep clocks synchronized.** New certificates are backdated by five minutes, not indefinitely. A verifier more than five minutes behind the issuing clock can reject a new certificate as not yet valid; a sufficiently advanced clock can see certificates as expired. Correct the affected clock first. With a currently valid CA, reconciliation replaces future-dated leaves after clock correction. It never replaces an established CA merely because the clock makes it invalid.
- **CA rollover is not generally supported.** Internal and relay CAs are long-lived but finite; issuing a new leaf cannot extend CA validity. Most engine drivers reject cluster CA replacement; [Temporal](https://temporal.io/) explicitly supports a [three-phase cluster CA rotation](temporal/access-and-tls.md#revocation-and-rotate-tls) that invalidates all prior client certificates. Monitor CA expiration externally and arrange supported, coordinated migration; do not replace pinned roots, delete PKI state or recreate data automatically as a renewal workaround. Leaf rotation alone is not a CA migration.
- **Member bearer tokens are non-expiring and not hot-rotatable.** They are separate from browser sessions and API access tokens. Changing only the stored member token does not update a running member's startup environment and breaks both management directions with authentication failures. Recovery/rotation requires coordinated delivery of the same token to control-plane state and the affected member environment followed by member redeployment/restart. Existing database processes may continue under cached configuration while management is disconnected.

## Observability, auth and tooling

- Built-in monitoring is short-term and control-plane-dependent, not independent HA alerting; see [collection, retention and sample freshness](monitoring.md).
- Agent log tails are bounded, not a durable archive; see [log limits and retention](operations.md#health-and-logs).
- Proxies may buffer event delivery. Preserve replay cursors and read durable events/status rather than resubmitting work; transport details live in [API events](api.md#pagination-and-events).
- [OIDC](https://openid.net/developers/how-connect-works/) publicly configures scopes/groups claim, not arbitrary internal authentication options. Username uses `preferred_username`; no public `allowedGroups`/`usernameClaim`/`autoCreate` toggles exist. Login captures group subjects rather than continuously querying the issuer; automatic identity creation grants no roles. See [access control](access-control.md).
- CLI authentication uses saved bearer tokens, not browser/device SSO or a standalone contexts manager. See [CLI](cli.md). Generated-contract consistency checks do not establish behavior; see [verification](verification.md).

## Cluster data MCP

- [Data MCP](data-mcp.md) is opt-in, limited to [MongoDB](https://www.mongodb.com/)/PSMDB, PostgreSQL, Supabase, [Elasticsearch](https://www.elastic.co/elasticsearch) and [RabbitMQ](https://www.rabbitmq.com/). MongoDB/PostgreSQL/Supabase add a private companion service; native Elasticsearch/RabbitMQ do not.
- Reviewed manifests deny unknown tools. No upstream resource proxying, connection switching or credential reveal is available. Clients without form elicitation/MRTR cannot execute tools requiring confirmation.
- Result/rate/concurrency bounds are not query-cost guarantees. Stored data can inject prompts; confirmations do not provide rollback. RabbitMQ peeking consumes/requeues and therefore needs write access. Elasticsearch hidden/system/dot indices and streams backed by them are refused.
- PostgreSQL's companion connects to the first scoped database (default `postgres`); existing PUBLIC grants are not revoked and can weaken scope isolation. See [scope and identity caveats](data-mcp.md#managed-identities-and-rotation).

## Known issues in v0.4.8

v0.4.8 is a hotfix that only changes how the Zerops provider classifies Zerops
API errors; see the [v0.4.8 upgrade notes](upgrade.md#upgrading-to-v048). All
[known issues in v0.4.7](#known-issues-in-v047) below remain present, and their
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

## Known issues in v0.4.7

These descriptions and workarounds are historical v0.4.7 guidance, not current
v0.4.8 advice. Fixed dispositions identify the required upgrade actions.

- **Zerops API error change** (v0.2.0–v0.4.7 on Zerops; **fixed in v0.4.8**).
  Once Zerops answers missing objects with HTTP 404 instead of HTTP 400, these
  versions read another object's absence, or a gateway 404, as a missing
  service or app version. Rejected creates and resource changes then keep
  retrying instead of failing as rejected, and a project-level not-found
  answer reads as the cluster's services being absent. Upgrade to v0.4.8; see
  [Fixed in v0.4.8](upgrade.md#fixed-in-v048).

<a id="dokploy-proxy-paused-update"></a>

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
  See [proxy recovery](upgrade.md#proxy-count-and-resource-changes).

<a id="mysql-gr-lost-member-cancellation"></a>

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
  fresh replacement. Existing rollback owners settle after upgrade. Cancellation
  acknowledgement is not finished recovery; see
  [the recovery procedure](upgrade.md#gr-capability-and-legacy-recovery).

<a id="dokploy-neo4j-minimum-memory"></a>

- **On Dokploy, a Neo4j cluster created with 1 GiB of memory never becomes ready**
  (v0.4.7). At the documented minimum of 1 GiB, a new single-member Neo4j cluster
  on Dokploy runs out of memory and CPU while starting: it never reports Ready
  and the create operation fails after about 40 minutes. Workaround: create
  Neo4j clusters on Dokploy with at least 2 GiB of memory. Existing clusters are
  not affected. **Fixed in v0.4.8:** new clusters/resource edits require 2 GiB
  on all backends; unchanged legacy shapes are grandfathered, not automatically
  resized. See [sizing guidance](upgrade.md#neo4j-minimum-memory).

<a id="dokploy-replacement-placement"></a>

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
  **Fixed in v0.4.8:** new Dokploy/Nomad imports preserve predecessor identity
  and strictly exclude survivor nodes. Existing co-location needs inspection and
  repair; healthy GR replacement requires upgraded full-view survivor agents.
  See [placement repair](upgrade.md#check-database-placement-after-v047-replacements).
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
  **Fixed in v0.4.8:** control-plane-only snapshots and parallel observation reduce
  request amplification, not Dokploy accounting writes. Infrastructure sizing
  remains relevant; historical I/O figures are not a fixed-version guarantee.
  See [observation caveats](upgrade.md#companion-refresh-and-dokploy-observation).
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
  task/revision-bound admission. Upgrade old marked agents before count/resource
  changes; surge capacity, no manual resume and daemon/executor-restart caveats
  remain. See [proxy recovery](upgrade.md#proxysql-admission-startup-and-recovery).
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
  serialized-read mechanism, not all latency/network causes. Two proxies is not
  a v0.4.8 maximum recommendation. See
  [observation changes](upgrade.md#companion-refresh-and-dokploy-observation).
- **MySQL/PXC automatic lost-volume replacement is experimental and off by
  default.** Testing observed Group Replication quorum loss after an automatic
  confirmed-loss replacement; its cause is not yet known. Leave
  `KARAKORAM_MYSQL_AUTO_REPLACE_LOST_VOLUMES=false` unless deliberately opting in.
  With it off, confirmed loss leaves the cluster Degraded with guidance to
  verify healthy survivors and run `karactl clusters replace-member CLUSTER MEMBER --wait`.
  Explicit replacement is unchanged. Setting the switch to `true` requires a
  control-plane restart and applies to both GR and PXC; other engines are
  unchanged. See [lost-volume handling](mysql/operations.md#confirmed-volume-loss).

## Continuing limitations

- **MySQL/PXC mutation isolation:** slow offline recovery serializes mutations
  only for its own namespace/cluster, not unrelated clusters. Backup-object
  deletion remains separately serialized against other deletions and active
  control-plane backup/restore steps; it can wait for those steps even when
  their target clusters differ. This does not lock asynchronous agent uploads
  or external storage clients. See [recovery coordination](mysql/operations.md#full-outage-recovery).
- **PostgreSQL administrator rotation requires upgraded agents.** v0.4.7 supports
  in-place `postgres` rotation after `upgrade-agents`; existing sessions are not
  revoked, and other engine-owned identities remain unsupported. See
  [administrator rotation and containment](postgres/access-and-tls.md#administrator-credential-containment).
- **PXC GTID-history observation:** recurrent `GTIDHistorySafe=Unknown` can
  persist while fewer than a configured-member majority are observable and
  compatible. PXC Status now starts quorum checking alongside required reads,
  inside the existing five-second status sweep. Sampling has a separate
  four-second reservation after a bounded lock wait. Synchronous backup/PITR
  requests can spend up to six seconds on comparison lock waits and sampling,
  plus journal/audit I/O and other endpoint work, subject to parent deadlines.
  Parent cancellation during journal work is an observation-budget failure.
  This addresses a
  code-proven failure mechanism, not a confirmed cause of earlier live episodes.
  `QuorumComparisonUnavailable` includes secret-safe per-member outcome classes
  and distinguishes comparison, budget and observation-lock failures;
  `GTIDHistoryPersistenceUnavailable` identifies journal/state/audit failures.
  Neither has a guaranteed recovery time. Incrementals and new PITR continuity
  certification remain gated; journal persistence failures can also block full
  backup admission. If Unknown persists or recurs, preserve the condition message
  and contemporaneous agent Galera outcomes, then contact support or open an issue.
- **GTID-history safety is not row equivalence:** `GTIDHistorySafe` cannot
  detect row drift caused by privileged unlogged writes (`SESSION wsrep_on=OFF`,
  `sql_log_bin=0`, or `gtid_next` manipulation). Restrict those privileges and
  verify data independently.
- **PITR while a history latch is active:** the advertised window stays at
  the last remembered window; targets outside provable coverage are refused with
  `outside conservative contiguous binlog coverage`. Use a full-backup restore
  or a [documented latch exit](upgrade.md#pxc-formation-and-restore-certification).
- **PITR summaries are not the full range list.** Cluster status and the legacy
  `start`/`end` fields still show only the latest-ending merged interval.
  The PITR endpoint's additive `intervals` field, CLI, and UI expose all proven
  ranges; different full backups are not merged in that list. Remembered PXC
  ranges omit backup attribution. Gaps between ranges are not restorable.
- **PITR does not bridge archive-writer changes.** A different physical writer
  freezes the old MySQL/PXC baseline at its last proven endpoint; newer-writer
  uploads alone cannot extend it. `WindowFrozenWriterChanged` identifies foreign
  uploads beyond that endpoint. In v0.4.7, automatic full re-anchoring is enabled
  by default: with PITR/storage configured, the writer must remain stable for
  five minutes, with no non-backup operation active and five minutes since the
  last finished. Pending backups and a one-hour cooldown defer the full.
  Set `spec.backup.pitr.autoReanchor: false` to opt out and take full backups
  manually. Coverage remains frozen until the new full succeeds and its coverage
  is verified. **The gap between the old endpoint and the new snapshot remains
  unrestorable.** Full backups add storage, IO and memory peaks; failed backups
  or further writer changes can extend the wait. Returning to the newest full's
  original writer may resume its trusted continuous history without a new full.
  Automatic backups follow count retention with chain, newest-full anchor and
  in-use protection. Older recovery ranges expire with their retained fulls;
  see [automatic writer re-anchoring](mysql/backups.md#automatic-writer-re-anchoring).
- **Agent deployment is not necessarily zero-downtime.** v0.4.7 supports
  `upgrade-agents` for every member-agent engine, including MySQL/PXC and
  ProxySQL; a control-plane-only upgrade or native process restart still does
  not replace deployed agents. Singleton databases/components have downtime.
  ProxySQL agent rollout preserves serving only on Dokploy with a running,
  deployed, volume-less, serving-probe-marked incumbent and marked incoming build,
  using serial start-first authenticated control-plane/task/revision admission
  for opted-in fixed agents, with spare capacity. Legacy/unmarked first upgrades
  retain old behavior; cold/stopped deployments do not wait for CP admission.
  Zerops new-build deployment and Nomad's TCP-gated canary/destructive rollout
  do not inherit count/resource serving guarantees and can interrupt the whole
  endpoint even with replicas. PostgreSQL/Supabase proxy deployments share the
  eligible Dokploy path. Temporal component availability depends on platform
  instance replacement behavior.
  With `--include-current`, receipt loss for an already-current member can
  cause another gated same-build deployment and interruption because the
  backend has no deployment lookup by member/request identity. See
  [agent rollout](upgrade.md#agent-rollout-now-covers-every-member-agent-engine).
- **Memory minimums are not workload guarantees.** Single-member MySQL keeps
  its 1 GiB admission floor; GR/PXC keep 2 GiB. At least **3 GiB per GR/PXC
  member** is recommended for backups, plus workload headroom. With PITR off,
  measured full-backup peaks were 84.4% of 1 GiB for single MySQL, 85.0% of
  2 GiB for PXC and 90.7% of 2 GiB for GR. These are scoped measurements, not
  guaranteed safe limits; see [upgrade memory guidance](upgrade.md#fixed-in-v043).
- **ProxySQL serving preservation is request- and backend-specific.** Nomad
  retains count-only incumbent-preserving scaling, but does not guarantee
  serving-preserving resource rollouts: MySQL/PXC retain stop/redeploy, while
  PostgreSQL/Supabase use maintenance-gated provider resource rollouts.
  Zerops count changes and same-CPU-mode CPU/RAM resizes retain incumbent
  Linux containers; CPU-mode and disk-bound changes are excluded, and unexpected
  incumbent replacement fails with `INCUMBENTS_REPLACED`. Observed terminal
  failures release the submission fence; unresolved submissions still block
  supersession and Stop. A caller crash before checkpointing a returned failure
  can permit one resubmission. Dokploy count/resource updates require fixed-agent
  capability preflight, a deployed serving-probe marker and serial start-first
  replacement with spare capacity. Opted-in tasks require authenticated
  control-plane admission bound to the exact task and applied configuration
  revision, after native readiness. The authenticated task/revision-bound marker
  in container `/tmp` (never the engine volume) survives agent-only restart/re-exec;
  new task/container identities need fresh admission. Persistent failure fences
  cannot be cleared merely by losing an engine receipt; see
  [Dokploy admission and recovery](dokploy.md#proxysql-count-and-resource-updates).
  Later Swarm checks use lock-free agent/supervisor liveness only; dependency
  failures and intentional holds remain engine-managed, avoiding replacement
  cascades. Unmarked legacy proxies retain their
  disruptive/maintenance-gated path until redeployed. All proxy resource changes
  and count reductions remain maintenance-window gated; only serving-preserving
  count scale-out bypasses the window. Replacing or removing an instance can
  disconnect its existing sessions.
  Version/startup-runtime changes do not acquire a serving guarantee from these
  capabilities. Live-provider acceptance remains pending; see
  [ADR 0069](adr/0069-proxysql-serving-scale-and-resource-changes.md),
  [Zerops resources](zerops.md#resources-and-autoscaling),
  [Dokploy proxy updates](dokploy.md#proxysql-count-and-resource-updates) and
  [Nomad resource limitations](nomad.md#proxy-resource-changes-are-not-a-zero-outage-contract).
  Existing GR clusters still need the
  [proxy TLS redeployment](upgrade.md#proxysql-monitor-tls).
- **Nomad live coverage remains incomplete:** PXC, Neo4j, Weaviate, Citus,
  Temporal, OpenBao, Keycloak/bindings, Supabase and fleet operations have only
  partial live coverage for v0.4.7: bounded checks of selected paths, not a
  complete lifecycle/recovery acceptance pass. Dokploy was not live-tested for
  v0.4.7. EXPERIMENTAL status is unchanged.
- **Temporal on Zerops requires IPv6-capable agents and private IPv6 connectivity** to peers and the bound PostgreSQL writer. Bounded live checks covered combined-layout workflows over private IPv6, an agent refresh and PostgreSQL writer failover; the split layout, optional UI OIDC and complete lifecycle acceptance remain unverified.

The v0.4.2 PITR memory, singleton backup-memory, PXC retirement and clone,
PXC post-restore history, ProxySQL TLS and Zerops Weaviate issues are resolved
in v0.4.3 with the [required rollout procedures](upgrade.md#upgrading-to-v043).
Their old workarounds remain [historical guidance](upgrade.md#known-issues-in-v042),
not current v0.4.7 limitations. Existing history latches are not silently cleared.

## Known issues in v0.4.6

v0.4.6 is a security hotfix that only updates the MongoDB driver's compression
library to fix [GO-2026-6865](https://pkg.go.dev/vuln/GO-2026-6865); follow the
[v0.4.6 upgrade procedure](upgrade.md#upgrading-to-v046). The historical
limitations and all known issues in v0.4.5 below remain present. The Data MCP
closed-connection, failed-deployment resource recovery (Zerops/Dokploy/Nomad)
and false PXC GTID-history incident issues are **fixed in v0.4.7**; the other
limitations keep their individually documented status.

- **Creating a Percona XtraDB Cluster on Nomad fails** (also present in
  v0.4.3–v0.4.5; Zerops and Dokploy are not affected). A new three-member PXC
  cluster on the Nomad provider fails after about 5–9 minutes with
  `GTIDMappingCertificationBlocked`: the last member is never started and the
  failed create shuts down another member. No existing data is affected; this
  happens only when creating a cluster. On v0.4.3–v0.4.6, use MySQL Group
  Replication on Nomad, or create PXC clusters on Zerops or Dokploy.
  **Fixed in v0.4.7:** formation no longer blocks on a fenced ProxySQL
  deployment. Fresh three-member PXC creation with ProxySQL was live-verified
  on Nomad; see [formation upgrade guidance](upgrade.md#pxc-formation-behind-pending-proxy-serving-health).
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

## Known issues in v0.4.5

The historical limitations described for [v0.4.5](upgrade.md#known-issues-in-v045)
and the Zerops failed-deployment resource recovery and false PXC GTID-history
incident issues below remain present in v0.4.5; the PXC issue is **fixed in v0.4.7**.
Failed-first-deployment resource recovery on Zerops, Dokploy and Nomad is
**fixed in v0.4.7**; it is not included in the v0.4.5 or v0.4.6 hotfixes.
The documented workarounds remain necessary on those releases.
v0.4.5 fixes only the imported-baseline PITR coverage freeze and empty-GTID backup metadata rejection.
Follow the [v0.4.5 upgrade procedure](upgrade.md#upgrading-to-v045), including
data-member agent relaunch and a new full backup.

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

## Known issues in v0.4.4

The two MySQL issues marked fixed below require the
[v0.4.5 upgrade procedure](upgrade.md#upgrading-to-v045). The Zerops and PXC
issues remain unfixed in v0.4.5.

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
  archived transaction times. Corrected agents rebuild existing coverage proofs:
  an existing same-source full can resume if its exact anchor and contiguous
  archived dependencies remain available. A rolling GR/PXC agent relaunch moves
  the writer; take a new full and do not assume the old-to-new-baseline gap is
  recovered. See the [upgrade procedure](upgrade.md#upgrading-to-v045).
- **Fresh-cluster first full backup fails with `metadata_invalid`
  — fixed in v0.4.5** (v0.4.0–v0.4.4; MySQL single-instance, Group Replication
  and PXC whenever executed GTIDs are empty). The parser rejected valid
  two-column XtraBackup metadata until a committed binary-logged write made
  the set nonempty. Corrected data-member agents need no application write;
  failed backups stay failed, so request a new full after the
  [upgrade](upgrade.md#upgrading-to-v045).
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

## Known issues in v0.4.3

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
  [v0.4.4 upgrade procedure](upgrade.md#upgrading-to-v044). Retained unarchived
  binlogs are retried automatically; purged unarchived logs leave a permanent gap.
  On Group Replication and PXC the relaunch moves the writer, so point-in-time
  recovery into the stalled period itself is not available.

## Engine limits

- [MongoDB limits](mongodb/README.md#limits)
- [RabbitMQ limits](rabbitmq/README.md#limits)
- [Elasticsearch limits](elasticsearch/README.md#limits)
- [Weaviate limits](weaviate/README.md#limits): fixed three-voter clusters; only non-voters can drain and scale down; scale-out does not rebalance shards. Internal transports are plaintext and require isolated private networking. Full S3 recovery preserves member count; no incremental backups or PITR. Official binaries are mixed-license; only community features are enabled.
- [PostgreSQL limits](postgres/README.md#limits): [ProxySQL](https://proxysql.com/) has protocol/driver restrictions; SQL-created users are not automatically proxy-routable. Strict synchronous HA can block writes, and major upgrades are offline and backup-gated.
- [Supabase limits](supabase/README.md#options-and-limits): PostgreSQL 17 only; missing locked component/extension artifacts are rejected. Standard/full preset definitions do not imply available binaries. Database backups omit Storage objects and control-plane-held function/service secrets.
- [OpenBao limits](openbao/README.md#options-and-limits): 1/3/5 members, TLS-only, no seal migration, PITR, performance/DR replication or automated lost-quorum recovery. Shamir re-unseal depends on the control plane and its original master key; auto-unseal depends on the external transit/KMS key. PostgreSQL storage uses the control plane's shared PostgreSQL binding service; a server built without that service rejects it and does not advertise the choice. RabbitMQ connection roles must share a default/maximum TTL pair per connection.
- [Temporal limits](temporal/README.md#limits): Zerops uses private IPv6; Dokploy and Nomad use private IPv4. Requires a PostgreSQL ≥12 source through the control plane's shared PostgreSQL binding service; no multi-cluster replication or namespace authorization. Namespace descriptions cannot be cleared. Restores require recorded manifests in Temporal cluster state, the original source/database names and shared-source acknowledgement when applicable; deleting Temporal backup records does not remove PostgreSQL archives.
- [Neo4j Community limits](neo4j/README.md#access-and-limits): one member and one user database; every user is an administrator. No clustering, online backup, RBAC, native metrics endpoint, PITR or incremental backups. Dumps, restores, restarts and upgrades interrupt all service. [Interrupted first bootstrap](neo4j/operations.md#availability-and-lifecycle) replays isolated credential recovery before normal startup; persistent store, user-metadata or transport failures require operator repair, not marker deletion or store recreation. A v0.4.3 false-completion marker can survive an agent upgrade with `passwordChangeRequired=true`, leaving a running server's managed queries failing with `Neo.ClientError.Security.CredentialsExpired`; reload/restart and ordinary system-user rotation do not automatically repair this case. Follow the [legacy-marker maintenance procedure](neo4j/operations.md#legacy-bootstrap-marker-with-expired-credentials): after upgrading the member and excluding active/pending offline jobs, explicitly stop and verify the JVM is gone, preserve the volume, remove only the credential completion marker, then explicitly start and verify authenticated readiness. Never remove a marker on a live store or release another operation's hold.
- [MySQL and PXC limits](mysql/README.md): engine drivers cannot be switched in place; MySQL single-primary Group Replication and PXC [Galera](https://galeracluster.com/) have distinct recovery/provenance rules. TLS is required; private replication ports need operator-enforced isolation.
- **MySQL/PXC 8.0 is EOL:** fresh empty clusters (including import targets) must use 8.4. Existing 8.0 clusters retain updates, backups/restores and upgrades to 8.4. Recovery-only `spec.dataSource` creates remain possible with validated same-driver, same-major 8.0 backup/PITR sources and independent target-local storage; restore first, then upgrade. Retained XtraBackup 8.0 locks do not provide ongoing community security maintenance. See [MySQL lifecycle policy](mysql/README.md).
- [Keycloak limits](keycloak/README.md): exactly one runtime, maintenance downtime, PostgreSQL binding required; recovery protects source database data separately from the coordinator's sealed administrator state.
- [Native artifacts](native-artifacts.md) currently target [Linux](https://www.kernel.org/) amd64/[Ubuntu](https://ubuntu.com/) 24.04. Development use of source-built locked artifacts needs a configured mirror; mirrors cannot add absent capabilities.

New-engine and [Citus](https://www.citusdata.com/) release status is centralized in [engine availability](providers.md#engine-availability). [Fleet operations](operations.md), [notifications](notifications.md), [control-plane recovery](control-plane-backup.md) and [backup drills](backup-drills.md) are verified offline, not live-provider acceptance or recovery-time guarantees.
