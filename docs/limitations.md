<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Known limitations

These constraints are part of the product's operating model, not errors to suppress. Healthy database processes, successful platform requests and replicated data do not alone establish host-level HA or recovery guarantees.

## Zerops failure domains and storage

- **No anti-affinity or placement guarantee.** Independent services/volumes and dedicated CPU are not proof of independent physical machines, racks or zones. One host loss can remove multiple voters.
- **Best-effort detection only.** The agent groups hashed host-level signals (visible ZFS ARC header/optional hostid plus CPU model). Missing/changing signals, collisions and platform virtualization can cause unknown/false/stale groups. Container `boot_id` and `/proc/uptime` are not reliable physical host IDs. Even distinct fingerprints do not certify future placement or zone separation.
- **Local Storage is single-machine.** Each member's runtime is co-located with its volume; the volume is not replicated HA storage. Database replication and PBM backups are separate protection. Runtime root disks are wiped on deployment; data must remain on the assigned volume.
- **Local Storage minimum is 5 GB**, and mounted capacity follows the volume service **maxDisk**, not runtime disk or volume minDisk. Growth is supported; shrinking is rejected. Shared SeaweedFS dbpaths are not supported/safe database storage.
- **Platform volume backups are live archives**, not consistent MongoDB or automatically consistent SQLite backups. Use PBM and a consistent stopped-writer/native SQLite state snapshot respectively.

## Control-plane and platform operations

- **One control-plane replica, no CP HA.** SQLite WAL on Local Storage is deliberately single-writer. Keep `maxContainers: 1` and temporaryShutdown. Control-plane upgrades interrupt management; horizontally scaling it is not supported.
- **No Zerops container exec API.** Node-local work uses Karakoram's bounded authenticated agent, not arbitrary SSH/exec commands. Local admin access can be recovered by [a service-secret update and startup restart](operations.md#recovering-local-admin-access); the offline command still requires a trusted maintenance environment. There is no public unauthenticated remote reset endpoint.
- **REST/polling, not platform watches.** Zerops accepts asynchronous Processes; completion requires observing FINISHED and database state. Request acceptance alone does not establish successful completion.
- **Autoscaling configuration is not allocation.** Member `autoscaling` is effective Zerops service configuration; `resources` is current container allocation. `Ready`/observed generation indicate completed reconciliation, not a hard allocated-memory cap or an allocation-convergence SLA. See [scaling observations](user-guide/scaling-and-upgrades.md).
- **CPU-mode changes may be rate-limited.** Karakoram persists a scheduled retry and surfaces `PlatformCooldown` with its deadline. Scheduled waits do not emit repeated `ReconcileError` events; new failed attempts still do. See [scaling](user-guide/scaling-and-upgrades.md).
- **Orphaned volumes require tracking.** If the platform refuses to delete a volume after runtime removal and ownership verification, Karakoram can complete the removal and track the volume as an orphan. A background janitor retries deletion with backoff. Until deletion succeeds, the volume still consumes disk and may incur charges. See [orphaned platform volumes](operations.md#orphaned-platform-volumes).
- **Import and environment semantics.** Member provisioning imports services first, then deploys an artifact with the generated pipeline. Secret writes do not mutate running process environments. Runtime YAML envVariables override service secrets. Resource updates send complete vertical/horizontal settings.
- **Public Git import requires a public repository.** Use a publicly reachable source repository for Git import, or deploy from a local checkout. Members receive prebuilt artifacts and do not need public source builds.
- **Existing projects only.** Namespaces support cross-project management, not project creation or other infrastructure backends. Only cataloged providers are supported. The system `core` service remains outside Karakoram ownership.

## Namespaces and relays

- **Default token is environment-managed.** API token rotation returns 409 for `default`; update `KARAKORAM_ZEROPS_TOKEN` on the control-plane service and restart.
- **One namespace per project.** Cluster and backup-storage names are unique per namespace, not installation-wide. Namespace rename/project reassignment is not supported; only descriptions can change.
- **No relay HA.** Each additional project has one relay container. Losing it interrupts MongoDB administration, agent commands/config pulls and monitoring for that namespace, not database replication. It is not a workload connectivity gateway.
- **Public HTTPS origin is required.** Remote namespaces depend on outbound access to the control-plane origin and WebSocket forwarding through L7. Default namespace networking remains direct.
- **Half-close is not propagated.** EOF closes both sides of a tunneled stream; the relay is not a general TCP/VPN transport.
- **One ordered tunnel per namespace.** Streams share congestion/failure fate. Backup data goes directly member-to-S3; measured transport throughput is evidence for admin/PBM control, not a bulk-backup SLA.
- **L7 longevity is bounded evidence.** A session measured in a test deployment survived at least 31 minutes 23.882 seconds with keepalives and three approximately ten-minute application-idle intervals. Multi-hour/day behavior, actual idle timeout and message-size limits were not characterized; see [ADR 0018](adr/0018-multi-project-namespaces-relay.md#evidence).
- **Removal guards retained resources.** Clusters (including deleting), storages, retained backup records and orphaned-volume records must be gone first. Deletion removes the owned relay, not the project, and does not remove RBAC policy rules. Review obsolete policies before reusing names.
- **Relay bounds are not a DoS guarantee.** Handshakes, control frames, WebSocket messages and active streams are bounded, but shared public listener/network resources remain exposed to denial of service. Project DNS/network are trusted; inner relay TLS is not a substitute for MongoDB TLS.

See [namespace lifecycle](user-guide/namespaces.md) and [relay threat model](security.md#relay-threat-model).

## Access and networking

- **Public TCP needs project IPv6 or dedicated IPv4.** Managed MongoDB listeners are dual-stack (`bindIpAll` plus explicit `net.ipv6: true`); those settings are not user-overridable. Shared IPv4/HTTP subdomain routing cannot carry the MongoDB wire protocol. IPv6-only clients must have IPv6 connectivity; dedicated IPv4 is a separate platform capability/cost.
- **IP allowlists are defense in depth.** Use TLS and strong credentials; a source allowlist is not a substitute for authentication. Keep public access disabled unless needed and verify allowed and denied client access for your deployment.
- **Public route synchronization is project-wide.** Creating/deleting a route invokes the project's `sync-public-port-routing`, including changes made for other clusters. Cluster `Ready` and route `isSynced` describe control-plane state; verify external reachability separately. Clients should use bounded network reconnects without retrying authentication or certificate failures.
- **External replica-set discovery is unresolved automatically.** Public routes use distinct ports on one project IP, while normal `hello` discovery advertises private member hosts. Karakoram does not configure external DNS/SNI split horizons. A public seed/CA/SAN or DNS-only mapping is insufficient if the discovered ports remain private defaults. DirectConnection to a public member is diagnostic and sacrifices normal discovery/failover; do not label it external RS HA.
- **Sharded exposure publishes mongos only.** Config servers/shards stay private. Multiple router containers share a service name rather than independent per-container public discovery.
- **Project users/platform are trusted.** The Full access project token can read other services' sensitive env. The public API's HTTP hop after Zerops L7 termination uses private infrastructure; agent traffic has separate end-to-end TLS. Karakoram is not an isolation sandbox against project administrators.
- **TLS CA source is immutable.** User-provided PKI needs a signing CA key; optional leaf SANs must cover every relevant member and localhost/public IP. Leaf rotation does not implement general CA or master-key rotation. PreferTLS/disabled permit plaintext database traffic; current default is requireTLS with password clients allowed without client certificates.

## Database topology and lifecycle

- **No shard removal.** Shards can be added; decreasing shard count is rejected. Topology type and cluster name are immutable. A replica set is not transformed into sharding by changing a flag.
- **Uniform shard component specs.** Engine replicas/resources apply to every shard; no per-shard independent specification is exposed.
- **Replica sets use 1/3/5/7 members**, with no configurable arbiter/custom voting layout in the managed desired spec. Config sets share those counts. Validation bounds of 128 shards/32 routers are not scale-test evidence or a project-capacity guarantee.
- **Application sharding is your responsibility.** Provisioning/registering shards does not choose shard keys or automatically shard application collections.
- **Curated PSMDB catalog only.** Only advertised pinned packages/upgrade paths are supported. PSMDB **8.3 is excluded as Technical Preview** despite noble packages existing. 6.0 is marked deprecated. Repository presence is not upstream lifecycle support.
- **No arbitrary downgrade/skipped-major path.** Major transitions require old-major FCV, and raising FCV is explicit. A singleton cannot provide zero-downtime restarts/upgrades. Engine overrides cannot replace provider-managed authentication, networking, dbpath/replication/sharding/cache/lifecycle settings.
- **Agent binary rollout is not automatic on CP restart.** Existing agents receive the current executable on their next provider-driven member deployment. Maintain protocol compatibility through mixed versions.
- **No automatic adoption from lost state.** A blank control plane cannot discover missing secret ownership/checkpoints and safely adopt existing members. Keep consistent state+original key backups. No `admin backup-db` or master-key re-encryption command exists; replacing the master-key env breaks decryption.

## Recovery and scheduling

- **Restore is disruptive and serialized.** Stop writers; physical restore intentionally takes mongod offline and suppresses restarts. Sharded restore coordinates routers/balancer. Failed terminal work still requires cleanup; ambiguous PBM initiation deliberately fails rather than automatically launching a duplicate.
- **Source/target compatibility matters.** Clone requires matching topology/engine major and a successful snapshot, or a live source's continuous PITR window. Modern retained snapshots preserve provenance; physical recovery additionally requires the protected snapshot identity. Legacy retained records lacking either are rejected before work starts. DataSource is creation-time and immutable; it is not a continuous replication link or topology migration.
- **Count retention is not time-based recovery coverage.** Successful scheduled backups are counted per schedule; manual/failed backups are not counted the same way. Base/oplog deletion, gaps, quota and storage changes can shorten PITR windows. Only observed PBM ranges are usable. Verify recovery after retention/config changes.
- **Schedules run in the control plane.** Control-plane unavailability pauses new cron requests and retention, with no promised catch-up for missed executions. Already configured member-side PITR/PBM work is different from scheduler availability. After restore, required fresh base success gates PITR restart.
- **Retained backups still depend on control-plane state.** `keepBackups` preserves snapshot records/provenance and protected recovery identities after full source deletion, enabling list/get/delete by the original cluster name and logical/physical snapshot clones into a compatible new cluster. Direct retained-backup deletion removes only that snapshot's S3 keys and metadata; unrelated PITR objects/namespace markers remain. Preserve the state database, original encryption key and registered storage configuration/credentials; orphan S3 objects alone cannot reconstruct them. `keepVolumes` and `keepBackups` are separate decisions. See [retained backups](user-guide/backups-and-restore.md#retained-backups-after-full-cluster-deletion).
- **0.5-GB sizing is not a production default.** This validation floor is intended for disposable tiny workloads, not a guarantee of recovery or load capacity. Default members use 1 GB with bounded WiredTiger/PBM buffers; size for data, concurrency and recovery headroom.

## Observability, auth and tooling

- **PMM is not shipped.** Monitoring is built-in exporter/UI plus Prometheus; no PMM query analytics/client/server deployment. See [ADR 0014](adr/0014-built-in-monitoring.md).
- **Short-term collector, not independent HA monitoring.** Default scrape is 30 seconds, history 24 hours, and the root endpoint can present its latest successful sample after a failed scrape. Use external availability/retention/alerting and status timestamps. PBM-up is process-running, not registration/backup proof; CPU JSON percentage is clamped and engine RSS excludes PBM/container overhead.
- **Agent log tails are bounded/volatile.** At most 1000 lines per process in memory, lost on restart; no durable all-history agent log archive. Zerops runtime logs supply another diagnostic path.
- **Proxies may buffer small SSE frames.** Karakoram sends approximately 16-KiB random-base64 initial comments and similarly padded fifteen-second heartbeats. Small events can wait until the next heartbeat. Padding costs approximately 16 KiB initially plus 16 KiB every fifteen seconds (~1.1 KiB/s per open stream before wire compression). Configure a nonbuffering proxy path where possible. Preserve cursors and use durable event/status/operation reads rather than treating delivery latency as lost work.
- **OIDC public settings are narrower than internal auth Options.** GroupsClaim/scopes are configurable, username defaults to preferred_username; no public allowedGroups/usernameClaim/autoCreate toggles. Group subjects captured at login do not continuously query the IDP. Enabling OIDC auto-creates identities without automatic roles.
- **CLI uses saved bearer tokens**, not browser/device SSO login or a standalone contexts manager. Full recursive surface is [documented](user-guide/cli.md).
- **Generation is not behavioral verification.** `make openapi-check` checks the alias-free specification and generated Go API/client/RBAC and TypeScript files, not live behavior. UI dependencies must already be installed.

See [operations](operations.md), [security](security.md), the [verification guide](verification.md) and the [README documentation index](../README.md#documentation-index) for deployment and recovery guidance.

## RabbitMQ

- **Definitions only, no message backup or PITR.** S3 backups contain topology, users/password hashes, grants, policies and runtime parameters, not messages. Restore merges objects instead of rewinding the target; it filters reserved users/grants and accepts at most 64 MiB uncompressed definitions. Message recovery/replay is your responsibility.
- **Placement is not guaranteed** ([ADR 0016](adr/0016-placement-anti-affinity.md)). Each node has one runtime and one ≥5 GB single-host Local Storage volume; separate volumes can share a host. Losing a majority can make Khepri and queues unavailable. Retained/orphaned volumes still consume storage and may incur charges.
- **Single-node means no HA.** Restarts/upgrades interrupt service. Cluster metadata replication does not replicate classic-queue messages; use quorum queues (or appropriately replicated streams). New quorum queues default to at most three replicas, even on five/seven brokers; scale-up rebalances leaders but does not automatically grow every existing replica group.
- **Durable classic queues block member removal.** `MemberRemovalBlocked` reports classic queues hosted on a leaving node; scale-down/replacement requires explicit migration or deletion. Lost-node replacement cannot recover classic message stores and requires a surviving Khepri voter majority as well as usable queue/stream majorities.
- **Curated 4.3 and Erlang 27 only.** RabbitMQ 4.3.5/4.3.6 use pinned Erlang 27.3.4.17 from Team RabbitMQ apt. Only 4.3.5 → 4.3.6 is cataloged; series upgrades beyond 4.3, downgrades and arbitrary Erlang versions are unsupported. Mnesia partition-handling overrides are not a recovery policy for Khepri-only 4.3.
- **Inter-node TLS is fixed at creation.** `requireTLS` uses IPv6 TLS distribution; `disabled`/`preferTLS` use IPv6 TCP. Only disabled/preferTLS can transition in place. Crossing the requireTLS boundary requires a new cluster and definitions restore, with separate message migration. CA source is immutable; AMQP client certificates are not required.
- **Management public exposure is separate.** Opt-in public management exposes native RabbitMQ UI/API with RabbitMQ accounts, bypassing Karakoram RBAC, and is not covered by the AMQP allowlist. Zerops HTTPS forwards to private HTTP 15672 even in requireTLS. Private project users/platform remain trusted.
- **Drain is not a message drain.** It closes connections and transfers responsibilities before shutdown; it does not empty queues or preserve message copies. Quorum gates can stall maintenance, and permanently lost majorities are not automatically recovered. Applications need connection recovery and survivor capacity.
- **Watermarks follow rolling restart, not live allocation.** Managed total memory is desired minimum RAM; its high watermark is 60%, and disk-free limit is max(0.5 GB, minimum RAM). Live RAM changes alone do not change the running broker's limits. Disk shrinking is rejected.
- **Access is direct, not declarative topology reconciliation.** Vhosts/users/grants/policies are managed through RabbitMQ-specific API/CLI/UI. Generic MongoDB users/roles do not apply. Queue/exchange/binding CRUD and native stream-protocol public routing are not supplied; allowed plugins do not configure an application or migration.
- **Monitoring is aggregate and bounded.** Broker `up` means loopback Prometheus answered, not queue quorum or successful delivery. Default 30-second scrapes/24-hour history and best-effort peer/placement observations are not independent alerting or a failure-domain guarantee.

See the [RabbitMQ guide](user-guide/rabbitmq.md), [lifecycle ADR](adr/0021-rabbitmq-cluster-lifecycle.md), [access/backup ADR](adr/0022-rabbitmq-access-tls-backups.md) and [operational notes](operations.md#rabbitmq-operations).
