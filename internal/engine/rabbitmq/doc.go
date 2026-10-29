// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package rabbitmq manages RabbitMQ 4.3 singleton brokers with one persistent
// volume per broker. Runtime and import specifications use backend-neutral
// infrastructure descriptions and the backend's application and volume layout.
// Public TCP, allowlists, public HTTPS and disk growth require backend support.
// Unsupported requests are surfaced without scheduling platform retries.
//
// Data MCP uses the native-rabbitmq gateway and accepts only vhost scopes.
// Reserved karakoram-mcp-ro/rw identities have the monitoring tag, never
// administrator: configure is denied, read is allowed, and write is allowed
// only for rw. Management API reconciliation repairs credentials and grants,
// removes disabled identities and stale scoped grants, and discovers new vhosts
// when scope is empty. Passwords remain sealed and use system-user rotation.
// Gateway credentials select healthy members' private management HTTP(S) URLs.
// Names use monotonically increasing member
// indexes; hostname matches alone never establish infrastructure ownership.
// Recorded service IDs, IDs returned by the member's own import, non-sensitive
// ownership markers and readable legacy agent-token secrets establish ownership.
// Sensitive variables are masked for tokens without sudo mode; verified legacy
// services receive a non-sensitive marker so later checks need no secret reads.
// An untagged volume can also be adopted when a marker- or token-verified runtime
// binds KARAKORAM_DATA_DIR to that exact volume. Expired imports are reconciled
// against observed services; absent services are re-imported, while masked legacy
// services without other evidence require operator recovery.
// Observed service IDs are checkpointed before retrying import. Cleanup returns
// newly adopted IDs to the reconciler before deleting any runtime in a subsequent
// invocation, preserving volume ownership after the mount evidence disappears.
//
// Sync checkpoints imports, deployments, joining and membership removal.
// Distributed fresh creation submits up to two independent deployments without
// waiting for seed capability. The pre-existing generic supervisor holds every
// peer until seed and peer capability are known, including legacy engine agents.
// Seed election and holds precede provisioning; capability RPCs follow available
// platform submissions so unreachable agents cannot delay filling free slots.
// A completed seed may bootstrap and peers may verify membership while later
// platform deployments run. All receipts and native gates still precede Ready.
// Singleton creation retains the ordinary serial platform retry checkpoint.
// Supported blank peers use the bundled fixed-seed discovery backend with an
// explicit seed and unlimited retries, never classic standalone fallback.
// Mixed-version peers lacking support boot only in their ordered turn after
// seed bootstrap. Backend admission may impose a tighter project-wide bound.
// An initial member still planned/imported (including an exhausted deployment)
// blocks later native turns without preventing independent platform provisioning.
// Native bootstrap and joins remain ordered; platform startup never establishes
// Ready. Every broker must report complete running membership before completion.
// The initial discovery gate survives verified storage and process exec/restart;
// only durable native bootstrap/seed-membership verification completes it.
// Rolling changes gate on online quorum and quorum-critical checks, then drain and stop
// the broker before any disruptive platform action. Durable maintenance holds
// prevent background configuration pulls from starting a stopped broker while a
// platform operation is pending. A member must acknowledge its configuration,
// run, rejoin and revive before another member is changed.
// Restart, TLS rotation and agent rollout recheck the maintenance window before
// each new member drain, including operations claimed in an earlier window.
// A drained member always finishes recovery; the persisted operation override
// permits the entire rolling pass outside windows. All three operations allow
// provisioning Sync until native formation or scale-out completes, but isolate
// ordinary Sync while a member change is active.
// Resource changes are applied through the backend and followed by a gated rolling configuration
// update. Backends whose resource receipt reapplies the stored runtime enter
// disruption gates before RAM/CPU changes and skip a second identical deployment;
// reload, configuration acknowledgement and full membership still gate completion.
// Post-replacement reload connection refusal and native observation HTTP 503
// retain visible convergence diagnostics and poll at the readiness cadence as
// scheduled waits, without consuming the tracked operation's failure budget.
// The window is bounded by the shared runtime readiness timeout (300 seconds)
// from the durable broker-stop disruption timestamp, including platform time.
// Legacy in-flight checkpoints start that window on their first startup refusal.
// After the deadline these observations are ordinary counted convergence
// failures with member/checkpoint and log-inspection guidance. No checkpoint
// advances until the original reload/config/membership gates succeed; other
// errors retain backoff.
// Pending platform failures retain their exact action and target with durable
// exponential backoff; CPU-mode cooldowns do not hot-loop.
//
// Removal grows quorum queue replicas on every remaining broker, migrates stream
// queue replicas (independently of the optional stream protocol plugin), and
// shrinks the leaving broker. Its application is drained and stopped while the
// Erlang VM remains a live Khepri voter; a survivor forgets it before full process
// shutdown and infrastructure deletion. Durable classic queues hosted on that
// member block automatic scale-down and replacement: the reported queues must
// be explicitly migrated or deleted by the operator before retrying.
//
// Replacing a lost node uses healthy survivors to join a new, monotonically named
// member and migrate replicated queues. An unreachable departure skips local
// drain/stop commands and is forgotten only with a Khepri majority among survivors.
// This does not recover classic queue contents or enable recovery without a
// surviving metadata majority. Down remaining brokers prevent automatic removal.
// Whole-cluster suspension stops every reachable broker locally before removing
// any runtime, without drain, forget, shrink or reset. Queue replicas, metadata,
// volumes and credentials are preserved even without a running quorum.
// Admitted TCP and management HTTPS exposure are retained separately from
// runtime route receipts; management mutations checkpoint their effective target.
// Resume and runtime recovery restore that policy without a maintenance wait,
// including private exposure. Deferred policy edits restore only admitted
// routes and cannot acknowledge the desired generation until admission.
// Legacy acknowledged policy or matching retained hashes seed admission;
// otherwise unknown legacy exposure defaults to private until admission.
// Unreachable broker shutdowns are recorded and delegated to terminal platform
// runtime deletion before volume retention. Full deletion skips broker RPCs:
// no leadership transfer is useful when every replica is being discarded.
// Resume starts every retained broker before waiting for individual readiness,
// allowing the retained Khepri majority to recover. Independent durable receipts
// permit at most two retained deployments in flight, additionally subject to
// backend admission. Submission reverses the suspension stop order so the
// last-stopped broker starts first; no force-boot or membership reset is used.
// A freed retained-deployment slot is filled before completed-broker start RPCs,
// so native startup cannot unnecessarily serialize the remaining platform wave.
// When project admission refuses refill, completed brokers can still start;
// the admission deadline and any independent start error remain visible.
// Runtime recovery defers only during retained provisioning/deployment; completed
// receipts restore identity-preserving recovery before start/revive can finish.
// Cleanup supersedes retained receipts only on backends guaranteeing deployment
// supersession under their delete fence; other backends must drain the receipts.
// Running-cluster rolling changes retain their single-disruptor quorum gates.
// Mutation checkpoints requeue after one second; native/platform waits and
// quorum-blocked retries retain their own safety bounds.
// Persistently mounted volumes are handed to orphan-volume cleanup only after
// their runtime is absent and their ownership is verified again.
// Member versions come from live native broker identity, not the agent's
// startup-cached version. Missing observed runtime identity blocks readiness,
// including for a one-member definitions clone. Final Sync readiness probes all
// members concurrently and reuses one native response per member; it does not
// serialize duplicate HTTP probes across the persisted readiness lease.
// Agent connectivity records successful generic-agent observation independently
// of the supervised broker's state. Native readiness still requires a running
// broker; paused or errored children override member health and publish stable
// Process/<member>/<process> conditions without discarding the broker role.
// Paused children require investigation before SIGCONT of the verified process;
// status observation never automatically resumes or kills them.
// Independent process/config and native HTTP probes share one concurrent budget.
// Survivor removal gates and join-seed selection probe peers concurrently while
// retaining deterministic seed preference and requiring every survivor healthy.
// Formation reuses its native status response for the subsequent join check;
// no health result is cached across mutations or reconciliation invocations.
// Planned/imported members remain unavailable and retain platform observations,
// but do not spend agent deadlines before an applied deployment is observed.
// Corrected desired configuration can repair the already-disrupted rolling
// member without waiting for its invalid configuration to become healthy or
// changing the captured binary target. Other members remain gated.
// Disk/memory alarms and observed partitions include affected nodes and safe
// recovery actions in member and cluster status. Native Raft recovery handles
// retained members returning after quorum loss; no force-boot is attempted.
// RabbitMQ 4.3 removed Mnesia partition policies: pause_minority and autoheal
// configuration keys no longer influence Khepri/quorum/stream Raft recovery.
// Reserved karakoram/admin credentials, administrator tags, vhost / and its
// managed permissions are periodically restored through the local control
// channel, independent of Management API credentials. Customer access is not
// reconciled. Recreating a deleted vhost does not restore its deleted messages.
//
// Broker credentials, Erlang cookies, CA keys, leaf certificates and pending
// password rotations exist only in SecretStore. Public AMQP requires an explicit
// address allowlist; optional public management uses backend HTTPS subdomains.
// preferTLS supports encrypted and plaintext clients; requireTLS also encrypts
// Erlang distribution. Distribution transport is fixed at creation: switching
// across the requireTLS boundary requires a new cluster and definitions restore.
// disabled and preferTLS can transition through gated rolling changes. Custom
// rabbitmq.conf overrides cannot change managed listeners, discovery, credentials,
// memory/disk limits or logging.
//
// Backups contain gzip-compressed RabbitMQ definitions in S3-compatible storage.
// They do not contain messages and provide no point-in-time recovery. Restore
// imports merge definitions into the target and preserve its reserved system
// users; a clone imports only after the target brokers are natively healthy.
// Readiness additionally requires the durable CloneImported receipt, independent
// of the desired generation. The initial import atomically claims the create
// operation before native work, refusing conflicting mutations and cancelled
// creates; deletion of the target remains allowed. A cancelled initial restore
// is Failed, never Ready merely because its empty brokers are healthy.
// Historical superseded creates without import evidence remain nonReady without
// replaying definitions; ordinary runtime, access, backup and exposure
// reconciliation continues while that durable unproven-restore hold remains.
package rabbitmq
