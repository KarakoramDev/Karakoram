# 0067 — MySQL and PXC lost-volume replacement

Status: Accepted; native/provider fault verification remains required.

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
