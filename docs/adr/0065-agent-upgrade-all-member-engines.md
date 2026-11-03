# 0065 — Agent upgrades for every member-agent engine

Status: Accepted; extends [ADR 0057](0057-fleet-operations.md)

## Context

Control-plane upgrades do not replace deployed member executables. The original
fleet rollout supported PostgreSQL/Supabase, PSMDB, RabbitMQ and Elasticsearch,
but other member-agent engines required resource changes or new-target recovery
to install agent fixes. Native process restart does not install a new agent.
Operators need one explicit rollout without changing database versions, applied
configuration, resources, persistent identities or PostgreSQL bindings.

Logical infrastructure services are not necessarily individually deployable
instances. Durable intent also cannot prove whether a deployment was accepted
when its response and opaque receipt were lost.

## Decision

Extend the existing `upgrade-agents` operation to MySQL single and Group
Replication (GR), PXC including ProxySQL, OpenBao with Raft or PostgreSQL storage,
Keycloak, Temporal, Neo4j Community and Weaviate. Use the existing API, CLI,
agent-build inventory and opt-in automatic rollout settings; no public API change
or separate rollout mechanism is introduced. After upgrading the control plane
to v0.4.4, existing v0.4.3 agents are eligible for upgrade. Missing legacy build
identity means eligible/unknown, not current.

Freeze the enqueue target and initial order in durable checkpoints. Journal
in-flight intent, deployment receipts and completed members; completed members
are not selected again even with `includeCurrent`. Retain applied versions,
configuration, runtime/volume identities and resources. Require native readiness,
applied configuration and the exact frozen build before progressing. Pending
structural, import, replacement or conflicting recovery work blocks admission.
A different control-plane executable cannot deploy new bytes under the old
frozen identity; recover the in-flight member according to its journal and follow
the operation diagnostic before submitting a new rollout.

### Engine order and native gates

- **MySQL/PXC:** deploy the ProxySQL logical service first and require every live
  proxy instance to acknowledge the target build, configuration/credentials and
  native writer health. Roll non-writer data members before the initially observed
  writer. GR requires all configured members ONLINE, an ONLINE primary and a
  surviving configured majority. Its writer handoff requires GTID containment,
  successor reader-hostgroup admission, election and exact new-writer routing
  acknowledgement before deployment. PXC requires a full Primary component with
  Synced, ready, connected members, GTID continuity and no active history-divergence
  latch. Select a GTID-containing admitted peer, exclude the routed writer and
  observe replacement writer routing before redeploying it; wait for native rejoin
  and proxy readmission. MySQL single has no writer handoff and has downtime.
  If recovery or confirmed loss appears during rollout, settle the current member
  and routing withdrawal without another executable submission, then retire the
  serialized rollout so replacement can take ownership. Confirmed loss releases
  drain ownership without native readmission and retains the separate loss routing
  fence; a lost member never becomes a healthy rollout peer.
- **OpenBao:** freeze standby/inactive-before-active order. Raft requires healthy,
  unsealed peers, live quorum/autopilot checks and a surviving voter majority.
  Step down the active member and observe another healthy active before its
  deployment. PostgreSQL storage uses native HA active/standby and lock-failover
  semantics, not Raft APIs. Reuse native readiness and serial Shamir/auto-unseal
  recovery, including other established peers that seal during the operation.
  A singleton has downtime and may remain unavailable until unseal completes;
  auto-unseal still depends on its configured external service.
- **Keycloak:** redeploy the singleton only. Require exactly one live instance,
  applied configuration, native Keycloak readiness and the target build. Refuse
  unexpected multi-instance services rather than implying qualified HA.
  Authentication is unavailable during rollout even with HA PostgreSQL storage.
- **Temporal:** history → matching → frontend → worker → server → UI, omitting
  disabled components. Require native readiness, applied version/configuration and
  the exact build on every expected instance before the next component. History
  and server components settle for one minute. There is no database-writer
  handoff; workflow/shard recovery follows native service readiness.
- **Neo4j Community:** redeploy the sole member and require authenticated native
  readiness, applied version/configuration and the exact build. All database
  access is interrupted; there is no leader handoff.
- **Weaviate:** roll non-voters, then non-leader voters, then the observed Raft
  leader. Reuse native leader re-election and recovery; do not force leadership
  or bypass quorum. Require all peers healthy/native-ready, synchronized cluster
  statistics and a live voting majority after removal. Clustered deployments
  require applied minimum and collection replication factors of at least two,
  plus native shard evidence proving the member holds no sole physical replica.
  Unknown evidence blocks rollout. Singletons bypass impossible redundancy gates
  but incur database downtime.

### Disruption, cancellation and replay

Check maintenance admission at each member/component's actual disruption boundary,
including deferred submission retries. Recording an order, selecting a successor
or persisting an undisrupted journal does not consume permission to disrupt after
the window closes. Once a real handoff or accepted deployment starts, finish that
member's native recovery and build acknowledgement even after window closure.

Cancellation is cooperative, not rollback. Stop before new disruption or between
members; recover already-disrupted work. A known deployment receipt requires exact
target-build acknowledgement. An intent without a receipt is ambiguous: during
cancellation, settle native readiness on the previously observed or frozen target
build without submitting another deployment merely to cancel. Ordinary
reconciliation may settle operation-owned recovery, not start another member or
silently retarget the rollout.

On receipt-missing replay, adopt a native-ready target deployment when the
engine's persisted build transition and expected instance inventory prove it
completed; Temporal checks every expected instance and Weaviate also requires
synchronized native statistics. Recheck maintenance, native safety and the frozen
build before any resubmission. For an explicit `includeCurrent` refresh of a
member already on the target build, unchanged build identity cannot prove that
deployment occurred. The backend offers deployment status for a known receipt,
not deployment lookup by member/request identity. Conservatively redeploy that
same build under the normal gates rather than falsely declaring completion.
This can cause an additional interruption after receipt loss.

ProxySQL is one logical service with multiple instances, not independently
addressable deployment members. Use one service-level deployment and wait for all
instances before touching data. Backends that replace all instances can briefly
interrupt the whole proxy endpoint, even with multiple replicas. Temporal also
deploys logical component services; platform instance replacement policy, not the
engine rollout, controls availability inside each component.

## Consequences

Every member-agent engine now has an agent-only upgrade path. A control-plane-only
upgrade and native process restart still do not replace agents. No database
version upgrade, forced quorum recovery, unsafe bootstrap or repair of an
unhealthy cluster is implied. Operators retain backups and an independent recovery
path and verify member builds, native readiness and application reconnects.

Maintenance windows authorize disruption starts, not deadlines for recovery.
Singleton downtime, service-level proxy/component replacement and conservative
same-build replay prevent a zero-downtime guarantee. Replicas alone do not prove
uninterrupted application access at every consistency level. Provider deployment,
real native failover/unseal/rejoin and application continuity require separate
live qualification; checkpoint and fake-backend tests do not establish an
availability or data-loss SLA.
