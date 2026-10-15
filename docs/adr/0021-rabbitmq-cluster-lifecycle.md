# 0021 — RabbitMQ uses observed, quorum-gated lifecycle changes and a curated 4.3 catalog

Status: Accepted

## Context

RabbitMQ 4.3 removed Mnesia and uses Khepri for metadata; a cluster needs a majority of metadata voters as well as each quorum queue/stream's own quorum. Broker count alone does not establish queue replication or independent failure domains. Stable node names and persistent per-node data are covered by [ADR 0020](0020-rabbitmq-node-persistence.md).

Zerops provides asynchronous runtime actions, not RabbitMQ-aware lifecycle hooks. Its container shutdown deadline is roughly five seconds after SIGTERM, too short to rely on the broker finishing shutdown. A new runtime can also start as a standalone broker even when a discovery list exists. Desired membership must therefore be confirmed from RabbitMQ, not inferred from import/deployment success.

Research references include the [RabbitMQ Cluster Operator](https://www.rabbitmq.com/kubernetes/operator/operator-overview), which separates cluster reconciliation from the Messaging Topology Operator and uses quorum-aware maintenance hooks, and the [Bitnami chart](https://github.com/bitnami/charts/tree/main/bitnami/rabbitmq), which packages persistent Kubernetes deployments and configurable discovery/TLS. [Amazon MQ](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/rabbitmq-broker-architecture.html) exposes single-instance or three-node, multi-AZ managed deployments with node-at-a-time maintenance. Their Kubernetes scheduling or AWS placement guarantees cannot be transferred to Zerops. Hosted-service and access/backup comparisons are recorded in [ADR 0022](0022-rabbitmq-access-tls-backups.md).

Primary RabbitMQ references: [4.3 release notes](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.3.0), [clustering](https://www.rabbitmq.com/docs/clustering), [peer discovery](https://www.rabbitmq.com/docs/cluster-formation), [quorum queues](https://www.rabbitmq.com/docs/quorum-queues), [upgrade and maintenance](https://www.rabbitmq.com/docs/upgrade), [feature flags](https://www.rabbitmq.com/docs/feature-flags) and [inter-node TLS](https://www.rabbitmq.com/docs/clustering-ssl). The release notes are authoritative for 4.3's Khepri-only change; older examples of Mnesia partition handling are not the managed policy.

## Decision

- Offer only `cluster` topology, the `engine` component and 1/3/5/7 singleton brokers; default three. Use stable `rabbit@<service hostname>` names and monotonically increasing member indexes. Default new queues to quorum with a stable initial group target of three, capped by RabbitMQ to available cluster membership when the queue is declared; never derive it from the seed's startup discovery peers. Do not label classic queues replicated.
- Install exact Team RabbitMQ Ubuntu 24.04 apt packages: RabbitMQ 4.3.5 or 4.3.6 (default), Erlang 27.3.4.17. Advertise only the forward patch edge 4.3.5 → 4.3.6. Reject downgrades, arbitrary package versions and uncataloged series transitions.
  Before the final apt index refresh/install, write `/etc/apt/preferences.d/erlang`
  with `Package: erlang*`, the catalog's exact `Pin: version`, and `Pin-Priority: 1001`.
  Keep the explicit install-time package versions too; the wildcard preference
  prevents transitive Erlang dependencies from selecting a newer incompatible build.
- Bootstrap the first broker's managed users. Render `classic_config` peer discovery for planned members, but observe membership and explicitly `stop_app`, `join_cluster` through a seed, and `start_app` when needed. Discovery is a startup aid, not a substitute for join acknowledgement. Peer-list changes alone do not restart running brokers.
- Scale up by provisioning/joining nodes and rebalancing leaders. Remove highest indexes first, requiring healthy survivors: grow quorum replicas on each survivor, migrate stream queues independently of the optional stream-protocol plugin, and shrink the departing node. Drain/stop its RabbitMQ application while keeping the Erlang VM as a live Khepri voter; forget it through a survivor before full process shutdown and owned infrastructure removal. Durable classic queues hosted on the node block scale-down/replacement until explicitly migrated/deleted. Replacement joins a new identity before replicated-queue migration/removal; unreachable lost nodes skip local drain/stop and are forgotten only with a Khepri voter majority among survivors.
- Serialize disruptive changes. On multi-node clusters, await online quorum plus one and reject a quorum-critical shutdown. Drain, persist a maintenance hold and stop the broker through node control **before** a platform deploy/restart or disruptive resource change. Checkpoint the exact action and observe terminal platform completion. Wait for consumed configuration, running membership, no alarms/partitions, and revive the broker before changing another node. Singletons skip the quorum gates and necessarily incur downtime.
- Apply live RAM/disk updates through the backend and then roll configuration so the broker re-reads memory/disk limits. Complete all binary upgrades before `enable_feature_flag all` and leader rebalance; do not enable new flags midway through mixed-version rollout.
- Fix IPv6 Erlang distribution transport at cluster creation: `inet6_tls` for `requireTLS`, `inet6_tcp` for `disabled`/`preferTLS`. A rolling cluster cannot mix those transports. Reject crossing that boundary; use a new cluster and definitions restore, with a separate message-migration plan.
- Resume retained clusters by starting every retained broker before individual readiness/revive waits, allowing the original Khepri majority to recover rather than deadlocking on the first node.

## Consequences

Routine changes preserve node identity/data and stop one broker at a time. Durable holds and checkpoints prevent background configuration pulls or failed platform retries from starting/changing the wrong broker. Quorum failures deliberately stall work rather than force unsafe progress. Draining closes client connections and transfers responsibilities; clients need recovery and sufficient survivor capacity. It does not empty queues.

Lost-node replacement requires surviving metadata and queue/stream majorities; it cannot recover classic contents, a lost last replica or an unavailable majority automatically. `MemberRemovalBlocked` identifies durable classic queues requiring explicit migration/deletion before scale-down or replacement. Majority/volume loss needs an explicit recovery assessment; definitions backups do not recover messages. Placement remains best-effort ([ADR 0016](0016-placement-anti-affinity.md)). Replacement and removal may temporarily add replicas/storage load. Five/seven nodes do not automatically grow all existing queues on scale-up. Resource watermarks follow desired minimum RAM only after restart, not every live allocation change.

The small version catalog and fixed Erlang/transport reduce mixed-runtime uncertainty but restrict upgrades. Future series paths require a catalog/compatibility change, not a user override. CA source is also immutable; certificate renewal is distinct from changing transport or CA authority.

## Alternatives considered

- **Discovery-only membership:** convenient for initial formation, but cannot prove a standalone/replaced broker joined the intended cluster. Retain discovery plus explicit observed joins.
- **Rely on SIGTERM or parallel deployment:** simpler platform integration, but the observed shutdown deadline and majority-loss risk make it unsafe.
- **Grow-and-shrink for every upgrade:** changes every identity and transfers queue logs unnecessarily. Upstream recommends rolling in-place upgrades and reserves grow/shrink primarily for individual replacement.
- **Latest packages or arbitrary Erlang/series selection:** bypasses tested pins and supported mixed-version/feature-flag paths. Keep an explicit catalog.
- **Change Erlang TLS one node at a time:** encrypted and plaintext distribution peers cannot join; use a clean new-cluster cutover instead.
