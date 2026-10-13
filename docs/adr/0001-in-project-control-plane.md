# 0001 — Standalone, Zerops-native, in-project control plane

Status: Accepted

## Context

Karakoram is self-hosted orchestration software, not a managed database service. Zerops is its infrastructure backend for replica sets, sharding, backups, upgrades, access management and monitoring. OpenEverest v2 supplies useful product concepts, but its control plane and providers depend on Kubernetes. Zerops API credentials alone do not provide private database connectivity.

## Decision

Deploy Karakoram inside the user's existing Zerops project as a separate runtime service named `karakoram`. It manages clusters in that project using a project-scoped Zerops token and private service hostnames. The control plane owns desired state, REST API, CLI-facing workflows, web UI and reconciliation; PSMDB/PBM provide database mechanisms. Neither Kubernetes nor an external hosted coordinator is required.

Expose the UI/API through Zerops HTTPS routing to port 8080. Keep the agent-facing TLS listener on port 9443 private. Installation does not create projects or modify the platform's `core` service. Karakoram is the sole product name; references to OpenEverest, Percona and MongoDB describe origins or dependencies, not affiliation.

## Consequences

Private administration needs no operator-managed VPN and databases remain private by default. Each installation has a project-level failure and trust boundary: project/network loss stops management, and compromise of the control-plane token can affect that project. Running databases are not dependent on control-plane availability for normal replication; cached agent configuration preserves process startup (ADR 0008). Cross-project management is not part of this deployment model. Apache-2.0 reuse retains required attribution without implying trademark rights.

## Alternatives considered

- **External centralized controller:** separates management failure domains, but requires per-project private connectivity and broader tenancy machinery.
- **OpenEverest plus a Zerops provider:** cannot remove the Kubernetes dependency through the existing provider contract (ADR 0005).
- **Managed database service:** does not provide the same self-hosted ownership and deployment model.
