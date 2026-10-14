# 0020 — RabbitMQ nodes keep their data on per-node Local Storage

Status: Accepted

## Context

A RabbitMQ node's data directory holds its Khepri metadata replica (RabbitMQ 4.3 has no other metadata store), quorum-queue and stream logs, classic-queue message stores and its cluster membership. Node identity is the pair of node name and data directory. Zerops runtime disks are wiped on every deployment, so a broker could either keep its data directory on a per-node Local Storage volume (as PSMDB members do, ADR 0007) or run ephemerally and resynchronize replicas from peers after every deployment.

We prototyped both on Zerops with RabbitMQ 4.3.6 / Erlang 27.3.4.17 from the Team RabbitMQ apt repositories, three `ubuntu@24.04` services with classic-config peer discovery and 10,000 persistent quorum-queue messages:

- **Persistent node, redeploy and restart:** the node returned with the same name and data; all replicas kept identical log indexes and all messages. A node stopped while 1,000 further confirmed messages were published caught up within about 23 seconds of its start request; all 13,000 messages were then consumed and acknowledged.
- **One ephemeral node wiped:** on 4.3.6 the blank node with the same name was re-admitted automatically and its quorum-queue replica resynchronized from peers. This depends on current Khepri/quorum-queue behaviour, transfers the whole queue backlog again and is not guaranteed for other versions.
- **Two of three ephemeral nodes wiped** (for example a co-located host failure or two deployments overlapping): the blank nodes could not join through the survivor, formed a separate empty two-node cluster, and the original quorum queue was lost (`NOT_FOUND`); the survivor kept a stale three-node view and membership repair timed out. Recovering a healthy cluster discarded the original queue.
- Zerops stops a container about five seconds after `SIGTERM`, regardless of the broker's shutdown progress. Vertical RAM changes on a runtime are applied live, but RabbitMQ only re-reads its memory limit on restart.

## Decision

Run every RabbitMQ node as its own `ubuntu@24.04` runtime service with `maxContainers: 1` and its own `local-storage:single@1` volume; place `RABBITMQ_MNESIA_BASE` on the volume and use the stable node name `rabbit@<service hostname>` for the life of the volume. Deploy with `temporaryShutdown: true`.

Because the platform's stop deadline is too short for a guaranteed clean broker shutdown, Karakoram drains and stops the broker through the node agent *before* any deployment, restart, stop or deletion of a running member, after confirming that the member is not quorum-critical. Memory and disk limits are rendered from the desired resources and applied by a gated rolling restart.

## Consequences

Quorum queues, streams and metadata survive deployments, upgrades and node restarts without replica resynchronization, and a full-cluster restart retains messages. The cost is one Local Storage volume per node (5 GB minimum, billed by usage) and the single-host placement of each volume: a host failure takes its node and data with it, and separate volumes do not guarantee separate hosts (ADR 0016). Losing a volume is a member replacement, not a routine event. Message data is not covered by Karakoram backups (ADR 0022).

## Alternatives considered

- **Ephemeral nodes:** cheaper and simpler to place, but every deployment resynchronizes full queue backlogs, and losing a majority of nodes at once destroys queues and metadata; observed behaviour on a single wipe is version-dependent.
- **One service with several containers:** a load-balanced service hostname cannot identify a node, and containers cannot have separate volumes.
- **Shared/network storage:** not appropriate for broker data directories (ADR 0007).
