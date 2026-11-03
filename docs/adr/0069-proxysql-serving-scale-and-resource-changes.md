# 0069 — ProxySQL serving-preserving replica and resource changes

Status: Accepted

## Context

In v0.4.3, the ProxySQL count-only serving-preserving fast path was available only on Nomad. Zerops and Dokploy lacked that guarantee; MySQL/PXC stopped and redeployed the whole proxy for count changes on those backends. Resource changes also retained disruptive maintenance paths. PostgreSQL/Supabase delegated count/resource changes to the provider, but lacked a serving-preservation contract for deciding whether an increase or resize could safely bypass maintenance admission. A running platform task or listening TCP port alone does not prove that a new proxy has applied configuration and can route to a writable database.

The requirement is continued service for new connections during eligible proxy changes, not survival of every existing connection, an independent-host guarantee, or a guarantee for arbitrary stateless components. Engine admission after a rollout cannot undo traffic sent too early or retirement of the last usable predecessor.

## Decision

### Explicit capability and runtime contracts

Keep `ReplicaScalePreservesServing` for count-only stateless changes and add `ResourceUpdatePreservesServing` for running-service resource changes. These are internal execution contracts, not user-selectable API capabilities. A backend must retain serving incumbents or replace them start-first, one at a time, with readiness equivalent to engine proxy admission before receiving traffic or retiring predecessors. Operation completion must establish the requested running fleet; engine configuration/native readiness remains mandatory.

`ServingResourceUpdate.ResourceUpdatePreservesServingFor(current, target)` can exclude particular resource transitions. `ServingProxyUpdates.ProxyUpdatesPreserveServing(ctx, name)` can require deployed-runtime readiness integration. Engines require the general capability and applicable request/runtime approvals; a failed runtime check or error does not authorize the serving-preserving path.

### Zerops: resize Linux containers and retain incumbents

Karakoram uses Ubuntu Linux-container runtimes, not Docker VMs. [Zerops scaling](https://docs.zerops.io/features/scaling) adjusts Linux-container CPU/RAM in place and adds/removes containers for horizontal scaling. Both serving-preserving capabilities are enabled. Resource approval requires unchanged CPU mode and unchanged minimum/maximum disk bounds. CPU-mode switches are rate-limited by the platform and deliberately treated as disruptive; disk-bound changes are also excluded from the serving-preserving resource path.

Before submitting autoscaling, persist the requested configuration, incumbent container IDs and process baseline. Recover a lost response by identifying the original autoscaling process rather than repeating the mutation; ambiguous attribution does not authorize resubmission. Completion requires successful platform processing and the requested running count. Scale-out and vertical changes must retain every incumbent; scale-in may retain only a subset of incumbents, not replacement IDs. Unexpected replacement fails with `INCUMBENTS_REPLACED`, requiring inspection of platform processes and proxy readiness before retrying. This detects a violated retention assumption; it cannot prevent a platform replacement that has already occurred.

Terminal platform failures and `INCUMBENTS_REPLACED` are recorded separately from the active submission fence. Replaying the same target before terminal observation returns the original operation. Once failure observation is persisted, the fence is released and a new request may retry the same target; unresolved submissions still block superseding targets, CPU-mode changes and Stop. There is a residual acknowledgement gap: a caller that crashes after receiving failure but before checkpointing it may resubmit once. Engines must retain their own failure checkpoint and apply their documented recovery policy rather than treating provider retry permission as automatic retry.

### Dokploy: native-health-gated Swarm replacement

Dokploy reload increments Swarm `ForceUpdate`, including replica-only updates, so preserving service requires safe replacement rather than merely retaining a replica count. Successfully deployed, volume-less ProxySQL runtimes marked `KARAKORAM_PROXY_SERVING_PROBE` qualify. Running count/resource updates install serial **start-first** updates and rollback policy, zero tolerated failures and **pause** on failure; they never fall back to stop-first to escape insufficient capacity.

The healthcheck invokes `agent readiness --serving-proxy`. Initial task/container admission fails closed unless the latest accepted nonzero configuration revision was successfully applied, the supervised proxy process is running, maintenance/stop holds are absent, and native writer readiness succeeds. PostgreSQL additionally uses its native proxy-serving hook, including frontend availability, an ONLINE writer and active managed users; ordinary readiness is unchanged. Swarm initial health admission must precede service DNS/VIP binding and predecessor retirement. Successful admission persists in a provenance-bound container-local marker across agent-only restart/re-exec. The marker binds the PID namespace, PID 1 start time, boot ID and hostname, and resides in container `/tmp`, never the engine volume. New task/container identities require fresh native admission. Subsequent persistent Swarm healthchecks use lock-free agent/supervisor liveness only; dependency failures, configuration failures and intentional holds remain engine-managed rather than provoking Swarm replacement cascades. Engine observations continue to enforce native/configuration health independently. Fresh stateless agents pull their configuration autonomously while the platform operation is pending.

Only probe-marked agents initialize the local admission state/listener. The
readiness client authenticates its local server using kernel peer credentials
for the same effective UID. Setup or bind failure preserves agent startup for
diagnostics but fails serving admission closed.

A durable receipt binds completion to the exact expected `ForceUpdate`, the requested new Running tasks, retirement of all previous Running tasks and completed Swarm update state. Paused, rolled-back, superseded and timed-out updates fail rather than commit; lost reload replies adopt the original operation. Cold deployment clears this serving healthcheck so bootstrap discovery is not deadlocked on unformed dependencies. See [Dokploy proxy updates](../dokploy.md#proxysql-count-and-resource-updates).

### Nomad: retain count-only scaling, not resource preservation

Keep the count-only incumbent-retention path and leave `ResourceUpdatePreservesServing` false. Count-only registration adds the deficit or removes the surplus without a canary replacement. Resource rollout uses one canary with automatic promotion/revert, followed by serial potentially destructive replacements; it is not universally start-first. Promotion checks TCP listeners, not successful current configuration application and an ONLINE writer. Post-operation engine readiness cannot repair this earlier admission gap.

Consequently MySQL/PXC resource changes retain their window-gated whole-proxy stop/redeploy path on Nomad. PostgreSQL/Supabase resource changes remain maintenance-gated provider rollouts: Nomad's running stateless `SetResources` registers a new job without a provider-issued stop, but this is not a serving-preservation guarantee. Native resource-operation completion also has a fallback based on evaluation completion and running count when no matching deployment is found. See [Nomad resource rollout boundaries](../nomad.md#proxy-resource-changes-are-not-a-zero-outage-contract).

### Engine admission, maintenance and upgrade compatibility

MySQL/PXC no-stop resource changes require unchanged version, replica count and acknowledged startup/runtime hash, plus both backend approvals. Combined count/resource, version and runtime changes retain the disruptive path. Persist intent before mutation, retain the operation receipt across reconciliation/restart, and commit the new count/resources only after operation completion and configuration/writer readiness on every expected live instance. Resource updates still wait for a maintenance window; eligible count increases may bypass it, while count reductions wait.

PostgreSQL and the embedded Supabase core similarly require the exact requested live fleet to acknowledge configuration and run with an ONLINE writer before committing count/resources. Count reductions always wait for maintenance; increases now also wait when serving-preserving count capability or runtime approval is absent. All proxy resource changes remain maintenance-window gated, even when serving-preserving, just as for MySQL/PXC. Only serving-preserving count scale-out bypasses that window. Pending operations are polled first, allowing terminal backend submission fences to be finalized before submitting a superseding count. Existing failure receipts remain authoritative rather than becoming success from observing one healthy task. See [MySQL/PXC operations](../mysql/operations.md#restart-configuration-and-scale), [PostgreSQL maintenance](../postgres/operations.md#agent-rollout-and-maintenance-windows) and [Supabase operations](../supabase/operations.md).

Control-plane upgrade does not automatically redeploy proxies or update their executables. The new serving-probe marker is excluded from MySQL's acknowledged runtime hash, preserving existing Nomad count-only eligibility without manufacturing a rollout. Dokploy independently checks successfully deployed metadata: legacy unmarked runtimes keep the disruptive/maintenance-gated path until an intended redeployment installs the marker and supporting agent. For MySQL/PXC, the next requested count/resource update uses stop/redeploy once; PostgreSQL retains its legacy maintenance-gated provider path. An unsupported old agent fails the new serving probe closed, not healthy by default.

Agent-only ProxySQL upgrades additionally require
`ProxyDeploymentPreservesServing`; count/resource capabilities never imply
new-build preservation. Dokploy enables it for running, deployed, volume-less,
probe-marked proxies when the incoming deployment also carries the probe marker.
Its build deployment uses the same serial start-first healthcheck, pause policy
and exact Swarm receipt as count/resource updates. Build completion is required
before checking rollout completion or starting the task-admission timeout.
MySQL/PXC persist the selected policy before submission and reuse it on replay;
the backend adopts the original digest-bound in-flight deploy after a lost reply.
Legacy unmarked images retain ordinary deployment once, because installing a
serving policy must be safe for the incumbent image as well as the new one.
Zerops and Nomad leave the new-build capability disabled: permitted overlap or a
TCP-gated canary alone does not meet the serving-preservation contract.

## Consequences

Eligible updates avoid a service-wide proxy stop, but replacing or removing an instance can disconnect its existing sessions. PostgreSQL graceful draining is bounded and does not guarantee session survival. Applications must reconnect and handle uncertain transaction outcomes; this is not a zero-error or zero-outage SLA.

Dokploy requires spare CPU, memory and placement capacity for a start-first replacement, even for a singleton. Its advertised surge bound is one instance above the larger old/desired fleet; this is a budgeting bound, not reserved capacity. Nomad's canary also needs spare allocation and possibly host-port capacity despite lacking the resource serving guarantee. PostgreSQL budgets backend connection pools for overlap before allowing the rollout; unknown surge bounds conservatively reserve both old and desired sets.

The provider contract is deliberately narrower than general platform autoscaling. CPU-mode/disk changes on Zerops, legacy Dokploy runtimes, unsupported resource transitions, and other disruptive engine changes retain maintenance precautions. A terminal failure does not commit desired resources or justify blind resubmission; consult the engine's recovery guidance.

Automated regression coverage does not establish live-provider acceptance. Live verification remains pending for Zerops incumbent retention and process attribution; Dokploy/Swarm health-before-traffic admission, surge exhaustion, pause and lost-reply recovery; and client reconnection/session behavior. Nomad resource preservation remains disabled pending a stronger rollout and native-admission contract.

## Alternatives considered

- **Always stop/redeploy the whole proxy:** simple but unnecessarily removes the application endpoint when the backend can retain incumbents or safely start replacements first.
- **Treat generic horizontal scaling or start-first as sufficient:** neither establishes applied configuration, writer routing or native health before traffic admission.
- **Enable Nomad resource preservation from its canary policy:** TCP-only promotion and later destructive replacement do not satisfy the strict contract.
- **Trust operation success without instance inventory/native admission:** can accept missing desired instances, stale tasks or proxies that cannot route writes.
- **Automatically redeploy legacy proxies during control-plane upgrade:** introduces unsolicited disruption and assumes an old runtime contains the new probe. Gate on deployed evidence instead.
- **Switch to stop-first when surge capacity is unavailable:** defeats the availability contract; fail visibly and require capacity or an explicitly planned disruptive change instead.
