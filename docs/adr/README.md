# Karakoram architecture decisions

These accepted records describe the architecture and its design rationale. Karakoram provides self-hosted MongoDB, RabbitMQ, Elasticsearch/Elastic Stack, PostgreSQL, Neo4j Community and modular Supabase orchestration through namespaces, without Kubernetes. Zerops is the reference backend; Dokploy and Nomad are also available. Decisions are not deployment-specific availability or recovery guarantees.

Decision priorities are: **(1) database correctness and recoverability, (2) simple self-hosted operation, (3) maintainable engine/infrastructure separation**. Availability guarantees must be limited by the platform's actual failure domains, not inferred from member count. Each record includes alternatives and accepted failure/operational costs.

## Index

| ADR | Decision |
|---|---|
| [0001](0001-in-project-control-plane.md) | Standalone Zerops-native, in-project control plane |
| [0002](0002-go-react-single-binary.md) | Go + React/TypeScript; server/agent binary and karactl |
| [0003](0003-sqlite-state-store.md) | SQLite on Local Storage; one CP replica, state backups and recovery |
| [0004](0004-declarative-reconciliation.md) | Level-triggered Sync, durable operations, cluster locks and SSE |
| [0005](0005-engine-provider-seam.md) | Engine provider seam inspired by OpenEverest v2, not its K8s interface |
| [0006](0006-zerops-backend-client.md) | Infrastructure Backend and an owned thin Zerops REST client |
| [0007](0007-member-deployment.md) | Runtime + volume per mongod; generated pipelines and executable artifacts |
| [0008](0008-node-agent-protocol.md) | Pinned-CA config pull, member tokens and bounded node supervision |
| [0009](0009-resource-naming.md) | Stable compact resource names and platform hostname validation |
| [0010](0010-psmdb-pbm-version-policy.md) | Curated PSMDB/PBM versions; rolling upgrades and explicit FCV raise |
| [0011](0011-pbm-backups-pitr-clone.md) | PBM/S3, schedules, count retention, PITR, restore and clone |
| [0012](0012-secrets-users-tls.md) | Encrypted secrets, purpose-specific users, keyfile and TLS defaults |
| [0013](0013-auth-casbin-rbac.md) | Casbin tuples; local/OIDC auth, API tokens and cookie/CSRF sessions |
| [0014](0014-built-in-monitoring.md) | Agent exporter, Prometheus endpoint and UI; PMM deferred |
| [0015](0015-openapi-code-generation.md) | OpenAPI-first generated contracts and x-karakoram-rbac mapping |
| [0016](0016-placement-anti-affinity.md) | No anti-affinity guarantee; heuristic detection and honest warnings |
| [0017](0017-public-tcp-exposure.md) | Optional public TCP, IP/allowlist policy and discovery-safe endpoints |
| [0018](0018-multi-project-namespaces-relay.md) | Multi-project namespaces; outbound WebSocket/inner mTLS/smux relays |
| [0019](0019-multi-engine-seams.md) | Additive multi-engine seams: provider metadata, optional interfaces, agent runners |
| [0020](0020-rabbitmq-node-persistence.md) | RabbitMQ runtime + Local Storage per node; stable identity and pre-platform shutdown |
| [0021](0021-rabbitmq-cluster-lifecycle.md) | RabbitMQ Khepri-only 4.3 catalog, observed joins and quorum-gated rolling lifecycle |
| [0022](0022-rabbitmq-access-tls-backups.md) | RabbitMQ Management API access, system users, TLS and definitions-only backups |
| [0025](0025-elasticsearch-engine-runtime-model.md) | Elasticsearch runtime + Local Storage nodes, explicit heap and engine readiness |
| [0026](0026-elasticsearch-cluster-formation-and-topology-changes.md) | One-time formation, quorum/health-gated rolling maintenance and add-then-drain topology changes |
| [0027](0027-elastic-stack-components.md) | Version-locked Kibana, Logstash and APM components owned by the cluster |
| [0028](0028-elasticsearch-security-and-snapshots.md) | TLS/file-realm recovery identity, isolated S3 repositories and destructive snapshot restore |
| [0029](0029-elastic-licensing.md) | Default Elastic distributions, Basic-tier provisioning and ELv2 managed-service obligations |
| [0030](0030-backend-neutral-infrastructure-contract.md) | Backend-neutral services, runtime deployment, capabilities and per-namespace backend registry |
| [0031](0031-engine-component-model.md) | Ordered component definitions, per-component resources and engine-owned lifecycle callbacks |
| [0032](0032-dokploy-backend.md) | Dokploy/Swarm backend: alias DNS, built member images, pinned volumes and host anti-affinity |
| [0033](0033-nomad-backend.md) | Nomad backend: isolated exec rootfs, routed CNI, native service discovery and fenced node-local volumes |
| [0034](0034-postgres-engine.md) | Percona PostgreSQL, Patroni/etcd HA, ProxySQL, native access and pgBackRest recovery |
| [0035](0035-supabase-engine.md) | Modular native Supabase over the PostgreSQL core, Envoy gateway and explicit artifact capabilities |
| [0036](0036-native-artifacts.md) | Embedded native artifact lock, mirror precedence, source provenance and signed-release staging |
| [0037](0037-runtime-data-identity.md) | Shared desired-running runtime convergence, durable volume identity and safe replica replacement |
| [0040](0040-agent-interface-mcp.md) | Agent operation metadata, server-side dry-run and the embedded dual-era MCP server with server-enforced confirmation |
| [0054](0054-weaviate-engine.md) | Native Weaviate lifecycle, recovery, access management and artifact licensing |
| [0041](0041-restricted-api-tokens.md) | Restricted API tokens as a deny overlay intersected with owner RBAC |
| [0042](0042-oauth-authorization-server.md) | Embedded OAuth 2.1 authorization server with per-authorization consent for MCP clients |
| [0044](0044-postgres-profiles.md) | Declarative PostgreSQL profiles (Supabase, PostGIS, pgvector, TimescaleDB Apache-2) over one core |
| [0045](0045-postgres-bindings.md) | Bindings of dependent services to Karakoram PostgreSQL: least-privilege roles, rotation, deletion guard |
| [0046](0046-citus-distributed-topology.md) | Citus distributed topology: coordinator and worker Patroni groups, formation DCS, rebalancing, coordinated restore points |
| [0047](0047-openbao-engine.md) | OpenBao: encrypted initialization receipts, control-plane-held unseal material, Raft membership/snapshots and opt-in credential connections |
| [0056](0056-temporal-service.md) | Temporal application service on bound PostgreSQL with managed TLS and workflow namespaces |
| [0052](0052-mysql-engine.md) | Percona Server for MySQL, Group Replication single-primary, ProxySQL, XtraBackup and binary-log PITR |
| [0053](0053-pxc-engine.md) | Percona XtraDB Cluster as a separate provider over the MySQL core |
| [0055](0055-neo4j-community-engine.md) | Neo4j Community singleton, TLS-only access, administrative native users and offline system/graph dumps |
| [0057](0057-fleet-operations.md) | Build-aware rolling member upgrades, maintenance windows and capability-aware relocation |
| [0058](0058-control-plane-resilience-and-signals.md) | Control-plane snapshots and rebuild, durable notifications, backup drills and operational signals |

## References and verification

Use the [verification guide](../verification.md) for automated test coverage and deployment checks. Recovery, authorization and platform integration should be exercised against your own installation; design rationale alone does not establish those behaviors.

Engine operating guides: [MongoDB](../mongodb/README.md), [replica sets](../mongodb/replica-sets.md)
and [sharding](../mongodb/sharding.md), [RabbitMQ](../rabbitmq/README.md),
and [Elasticsearch and Elastic Stack](../elasticsearch/README.md), [PostgreSQL](../postgres/README.md),
[Supabase](../supabase/README.md), [Temporal](../temporal/README.md), [MySQL and PXC](../mysql/README.md), and [Neo4j Community](../neo4j/README.md).

Primary references: [Zerops API](https://docs.zerops.io/references/api.md), [Local Storage](https://docs.zerops.io/local-storage/overview.md), [public networking](https://docs.zerops.io/references/networking/public-access.md), [Percona system users](https://docs.percona.com/percona-operator-for-mongodb/latest/system-users.html), [PBM](https://docs.percona.com/percona-backup-mongodb/latest/), and [OpenEverest provider runtime](https://github.com/openeverest/openeverest/blob/main/provider-runtime/controller/interface.go).

Changing an accepted decision requires updating its consequences, dependent ADRs and affected implementation/docs; preserve numbering. Karakoram is the product name. OpenEverest, Percona and MongoDB appear only as descriptive references, not product branding or endorsement claims.
