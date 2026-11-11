# 0067 — MySQL and PXC lost-volume replacement

Status: Accepted; native/provider fault verification remains required.

## v0.4.7 update — 2026-11-08

Automatic confirmed-loss replacement is **disabled by default** for both MySQL
Group Replication and PXC. Testing observed Group Replication quorum loss after
an automatic confirmed-loss member replacement; the cause is not yet known.
Opt in only deliberately with
`KARAKORAM_MYSQL_AUTO_REPLACE_LOST_VOLUMES=true` in the control-plane environment
and restart the control plane. This automatic path is **experimental**.
Invalid non-empty values fail startup.

With the switch off, persist loss evidence, fence the lost identity and leave
the cluster Degraded with `AutomaticReplacementDisabled` and operator guidance.
Verify surviving members, then request
`karactl clusters replace-member CLUSTER MEMBER --wait`.
Explicit replacement remains unchanged and still requires native safety proof.
The automatic queueing decision below applies only when opted in.

## Context

A missing runtime is not lost data. A confirmed lost initialized volume cannot
be repaired by starting the old member on empty storage. MySQL Group Replication
(GR) and PXC already provide add-before-remove member replacement, but admitting
that workflow requires surviving native data, not desired replica counts.

## Decision

Use the shared runtime recovery journal and `ReconcileRuntime`. It restores only
runtime services against retained storage. Only confirmed `VolumeLost` evidence
may enter the deterministic `RecoverLostVolume` queue. Errors, unavailable nodes,
unknown inventory and stale observations do not authorize replacement.
Persist affirmative physical/agent loss separately from the shared journal's
broader identity-conflict status flag. Only that confirmed-loss receipt permits
queueing or retiring a member without local native evidence. Restoring the
original volume after an identity conflict must not authorize replacement of
its retained data. A confirmed-lost predecessor is excluded from disk-growth
preflight and from replacement join-certification capability negotiation;
survivors, the joiner and deployed proxies still negotiate normally.

For GR, freshly observe every survivor ONLINE in the configured native group,
with a writable primary and a survivor majority of configured membership. At
least one donor must contain the lost member's last persisted executed GTID set
when known. Persist successful native GTID observations without overwriting them
on observation failure. Missing/invalid survivor GTIDs or group identity fail
closed. The additive agent group-name observation is required; older agents
continue normal service but cannot establish this new admission proof.

For PXC, every survivor must be Primary, Synced, ready and connected, with one
state UUID and a majority of configured data members. Refuse an active persisted
history-continuity divergence latch and require a donor containing the lost
member's last known GTIDs. If the lost member was the seed or pinned SST donor,
move that reference to a qualified donor. Existing joining-member certification
still owns SST fences and final routing admission; changing the seed is not a
certification bypass. Never invoke `pc.bootstrap` or `recover-quorum` automatically.

Queue the existing serialized `replace-member`, preserving its new member and
volume identities, clone/SST readiness, proxy admission and final-membership
certification. Revalidate admission before starting replacement. A lost retiree
needs no local native observation: exclude it from proxy routing, acknowledge
that configuration on proxies, stop and observe its exact runtime, then prove
native survivor membership no longer contains it before deleting resources.
The old identity is never deployed or initialized on fresh storage.

Full-capability GR retirement uses the complete native UUID/state/role/host/port
membership view, not the count of ONLINE members. Every non-retiring survivor, including the new
member, must be locally ONLINE under its persisted UUID in the configured group
and report the same exact ONLINE survivor set with a PRIMARY and no retiree UUID
in any state. View identifiers agree when available from every observer. Unknown
persisted UUIDs require recovery. New healthy replacement and scale-in refuse
missing full `GroupView` capability before disruption; upgrade surviving agents
first. Unknown/unreachable capability also refuses admission.
The proof is refreshed before service/volume deletion and before credential and
member-record cleanup. Healthy retirement also requires it before backend Stop.
GR peers/seeds retain the predecessor until member-record cleanup, so their
removal is gated by the same proof.

Two explicit exceptions retain the weaker released ONLINE/count proof, not exact
UUID-set convergence: structural work already in flight before the control-plane
upgrade, and explicit/automatic confirmed-loss replacement with positively known
legacy survivors. Confirmed-loss admission still requires the existing majority,
same-group, GTID and donor checks. Both emit Warning `LegacyGRRetirement`; the
in-flight warning identifies pre-upgrade ONLINE/count completion, while the
confirmed-loss warning is emitted at admission and directs agent rollout after
recovery. See [the exact warning/refusal messages and recovery order](../upgrade.md#gr-capability-and-legacy-recovery).
Unknown/unreachable capability never selects legacy mode, and already strict work
does not downgrade its proof. Finish recovery before agent rollout, then require
full capability for new healthy structural work.

Cancellation immediately after replacement allocation can remove the untouched
member record without inventing a native UUID. This exception requires persisted
`planned` state with no resource settings, import intent, deploy receipt, runtime
recovery, initialization, or UUID. Import intent is persisted before submission;
the later `imported` checkpoint permits deployment. Missing receipts after a
control-plane restart do not establish that deployment never happened. Record
cleanup still requires the membership proof selected for that work, including
the explicit legacy exceptions above.

Cancelling confirmed-loss GR replacement after its joiner has joined completes
forward. Before join it rolls back the allocation, retaining the lost predecessor
and loss fence for a fresh replacement after cleanup. Existing rollback owners
settle with these scoped rules; they do not require the physically lost member
to return ONLINE. Cancelled queue state acknowledges cancellation, not completed
native recovery. Observe recovery ownership, native membership and cleanup before
retrying replacement or upgrading agents.

The confirmed-loss backend Stop ordering is unchanged: it fences rescheduling
before membership proof, and can withdraw provider discovery at that point.
Separating runtime fencing from discovery withdrawal is deferred pending native
incident analysis; the identity barrier alone does not establish that provider
discovery remains available during replacement. PXC's analogous path uses eligible
Galera component/cluster-size observations; GR UUID rows are not Galera membership IDs.

Agent rollout and replacement are serialized, not mutually preempting workflows.
If loss/recovery appears during rollout, settle its current member and routing
withdrawal without another deployment, then retire the rollout so replacement can
claim ownership. A withdrawn survivor cannot be a donor or writer; outstanding
drains, including readmission acknowledgement, block replacement admission.
Confirmed loss releases a lost member's drain without native readmission while
retaining its independent loss routing fence.

## Consequences

Single topology is refused: there is no surviving replica, so recover original
storage or restore a backup into a new cluster. A single runtime restart with
retained data still causes downtime. GR/PXC majority and donor failures keep the
cluster Degraded with `VolumeLost` and a specific admission reason. Queue replay
uses one deterministic operation ID; failed, cancelled and completed operations
are not endlessly re-enqueued. Operators inspect terminal failures before
requesting a new managed replacement.

This does not prove row equivalence or zero-loss recovery from arbitrary database
corruption. It preserves native quorum, executed-history and certification
boundaries. No public API or operator acknowledgement semantics change.

## Verification

Fake-backed tests cover GR majority/group/GTID admission, unavailable observations,
PXC component/synchronization/history/donor refusal, donor re-pinning, singleton
refusal, queue replay and retirement without contacting the lost member. Live
verification must separately exercise physical volume loss, partitioned storage,
clone/SST and routing on each supported provider. See [0037](0037-runtime-data-identity.md),
[0052](0052-mysql-engine.md) and [0053](0053-pxc-engine.md).
