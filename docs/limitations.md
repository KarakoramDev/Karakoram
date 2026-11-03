<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Limitations

These cross-engine constraints define the operating model; engine-specific limits are linked below.

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

## Continuing limitations

- **PostgreSQL administrator password rotation remains unavailable.** The
  `postgres` administrator password cannot be rotated in place; follow
  [credential containment](upgrade.md#postgresql-administrator-password-cannot-be-rotated-yet).
- **PXC GTID-history observation after scale-out:** episodes of
  `GTIDHistorySafe=Unknown` with reason `QuorumComparisonUnavailable` have
  cleared on their own after approximately 16 and 30 minutes on idle PXC
  clusters. The cause remains under investigation; these observations do not
  guarantee recovery. The cluster reports Degraded while this condition is
  Unknown, and history-sensitive backups can be blocked. Avoid history-sensitive
  maintenance, including restore and member replacement, until it returns True.
  If it persists or recurs, capture the condition message and each member's
  Galera status, then contact support or open an issue.
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
  uploads beyond that endpoint. In v0.4.4, automatic full re-anchoring is enabled
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
- **Existing MySQL/PXC agents are not automatically replaced.** The v0.4.3
  PITR memory and coverage-convergence corrections require corrected member
  agents, not just a control-plane upgrade. A real resource change stops and
  redeploys members; relaunching v0.4.3 agents this way was verified live on
  Nomad (Group Replication), while upgrading in place from v0.4.2 agents is
  verified by tests only. `clusters restart` does not relaunch agents, and MySQL/PXC have no
  `upgrade-agents` command. Follow [agent rollout and PITR re-enablement](upgrade.md#existing-clusters-and-member-artifacts).
- **Memory minimums are not workload guarantees.** Single-member MySQL keeps
  its 1 GiB admission floor; GR/PXC keep 2 GiB. At least **3 GiB per GR/PXC
  member** is recommended for backups, plus workload headroom. With PITR off,
  measured full-backup peaks were 84.4% of 1 GiB for single MySQL, 85.0% of
  2 GiB for PXC and 90.7% of 2 GiB for GR. These are scoped measurements, not
  guaranteed safe limits; see [upgrade memory guidance](upgrade.md#fixed-in-v043).
- **ProxySQL changes can interrupt clients.** On Nomad, count-only replica
  changes preserve existing serving replicas. Zerops and Dokploy still stop
  and redeploy the whole proxy for count changes; proxy resource, version and
  runtime changes remain disruptive on all providers. Existing GR clusters need
  the [proxy TLS redeployment](upgrade.md#proxysql-monitor-tls).
- **Nomad live coverage remains incomplete:** PXC, Neo4j, Weaviate, Citus,
  Temporal, OpenBao, Keycloak/bindings, Supabase and fleet operations have not
  been live-verified in the current release campaign. Earlier scoped evidence
  is not a complete lifecycle/recovery acceptance pass. EXPERIMENTAL status is
  unchanged.
- **Temporal remains unsupported on Zerops** because it requires private IPv4.

The v0.4.2 PITR memory, singleton backup-memory, PXC retirement and clone,
PXC post-restore history, ProxySQL TLS and Zerops Weaviate issues are resolved
in v0.4.3 with the [required rollout procedures](upgrade.md#upgrading-to-v043).
Their old workarounds remain [historical guidance](upgrade.md#known-issues-in-v042),
not current v0.4.3 limitations. Existing history latches are not silently cleared.

## Engine limits

- [MongoDB limits](mongodb/README.md#limits)
- [RabbitMQ limits](rabbitmq/README.md#limits)
- [Elasticsearch limits](elasticsearch/README.md#limits)
- [Weaviate limits](weaviate/README.md#limits): fixed three-voter clusters; only non-voters can drain and scale down; scale-out does not rebalance shards. Internal transports are plaintext and require isolated private networking. Full S3 recovery preserves member count; no incremental backups or PITR. Official binaries are mixed-license; only community features are enabled.
- [PostgreSQL limits](postgres/README.md#limits): [ProxySQL](https://proxysql.com/) has protocol/driver restrictions; SQL-created users are not automatically proxy-routable. Strict synchronous HA can block writes, and major upgrades are offline and backup-gated.
- [Supabase limits](supabase/README.md#options-and-limits): PostgreSQL 17 only; missing locked component/extension artifacts are rejected. Standard/full preset definitions do not imply available binaries. Database backups omit Storage objects and control-plane-held function/service secrets.
- [OpenBao limits](openbao/README.md#options-and-limits): 1/3/5 members, TLS-only, no seal migration, PITR, performance/DR replication or automated lost-quorum recovery. Shamir re-unseal depends on the control plane and its original master key; auto-unseal depends on the external transit/KMS key. PostgreSQL storage uses the control plane's shared PostgreSQL binding service; a server built without that service rejects it and does not advertise the choice. RabbitMQ connection roles must share a default/maximum TTL pair per connection.
- [Temporal limits](temporal/README.md#limits): Zerops is not supported yet because membership requires private IPv4; use Dokploy or Nomad. Requires a PostgreSQL ≥12 source through the control plane's shared PostgreSQL binding service; no multi-cluster replication or namespace authorization. Namespace descriptions cannot be cleared. Restores require recorded manifests in Temporal cluster state, the original source/database names and shared-source acknowledgement when applicable; deleting Temporal backup records does not remove PostgreSQL archives.
- [Neo4j Community limits](neo4j/README.md#access-and-limits): one member and one user database; every user is an administrator. No clustering, online backup, RBAC, native metrics endpoint, PITR or incremental backups. Dumps, restores, restarts and upgrades interrupt all service.
- [MySQL and PXC limits](mysql/README.md): engine drivers cannot be switched in place; MySQL single-primary Group Replication and PXC [Galera](https://galeracluster.com/) have distinct recovery/provenance rules. TLS is required; private replication ports need operator-enforced isolation.
- [Keycloak limits](keycloak/README.md): exactly one runtime, maintenance downtime, PostgreSQL binding required; recovery protects source database data separately from the coordinator's sealed administrator state.
- [Native artifacts](native-artifacts.md) currently target [Linux](https://www.kernel.org/) amd64/[Ubuntu](https://ubuntu.com/) 24.04. Development use of source-built locked artifacts needs a configured mirror; mirrors cannot add absent capabilities.

New-engine and [Citus](https://www.citusdata.com/) release status is centralized in [engine availability](providers.md#engine-availability). [Fleet operations](operations.md), [notifications](notifications.md), [control-plane recovery](control-plane-backup.md) and [backup drills](backup-drills.md) are verified offline, not live-provider acceptance or recovery-time guarantees.
