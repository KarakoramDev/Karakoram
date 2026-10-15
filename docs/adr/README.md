# Karakoram architecture decisions

These accepted records describe the architecture and its design rationale. Karakoram provides self-hosted PSMDB/PBM orchestration across existing Zerops projects through namespaces, without Kubernetes. The control plane remains in its own project; remote projects use outbound relays. Decisions are not deployment-specific availability or recovery guarantees.

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

## References and verification

Use the [verification guide](../verification.md) for automated test coverage and deployment checks. Recovery, authorization and platform integration should be exercised against your own installation; design rationale alone does not establish those behaviors.

Primary references: [Zerops API](https://docs.zerops.io/references/api.md), [Local Storage](https://docs.zerops.io/local-storage/overview.md), [public networking](https://docs.zerops.io/references/networking/public-access.md), [Percona system users](https://docs.percona.com/percona-operator-for-mongodb/latest/system-users.html), [PBM](https://docs.percona.com/percona-backup-mongodb/latest/), and [OpenEverest provider runtime](https://github.com/openeverest/openeverest/blob/main/provider-runtime/controller/interface.go).

Changing an accepted decision requires updating its consequences, dependent ADRs and affected implementation/docs; preserve numbering. Karakoram is the product name. OpenEverest, Percona and MongoDB appear only as descriptive references, not product branding or endorsement claims.
