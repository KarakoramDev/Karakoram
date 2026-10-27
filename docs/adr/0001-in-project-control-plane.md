# 0001 — Standalone, Zerops-native, in-project control plane

Status: Accepted

## Context

Karakoram is self-hosted orchestration software, not a managed database service. [Zerops](https://zerops.io) is its infrastructure backend for replica sets, sharding, backups, upgrades, access management and monitoring. [OpenEverest](https://openeverest.io) v2 supplies useful product concepts, but its control plane and providers depend on [Kubernetes](https://kubernetes.io). Zerops API credentials alone do not provide private database connectivity.

## Decision

Deploy Karakoram inside the user's existing Zerops project as a separate runtime service named `karakoram`. Its own project is namespace `default`, managed using an environment-held project-scoped token and direct private service hostnames. Additional existing projects are managed as namespaces through outbound relays (ADR 0018), each with its own token. The control plane owns desired state, REST API, CLI-facing workflows, web UI and reconciliation; [PSMDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb)/[PBM](https://docs.percona.com/percona-backup-mongodb/) provide database mechanisms. Neither Kubernetes nor an external hosted coordinator is required.

Expose the UI/API through Zerops HTTPS routing to port 8080. Keep the agent-facing TLS listener on port 9443 private. Installation does not create projects or modify the platform's `core` service. Karakoram is the sole product name; references to OpenEverest, [Percona](https://www.percona.com) and [MongoDB](https://www.mongodb.com) describe origins or dependencies, not affiliation.

## Consequences

Private administration needs no operator-managed VPN and databases remain private by default. Project/network loss stops that project's management; control-plane compromise can affect every enrolled namespace. Remote namespace management also depends on the public origin and its relay (ADR 0018). Running databases are not dependent on control-plane availability for normal replication; cached agent configuration preserves process startup (ADR 0008). Apache-2.0 reuse retains required attribution without implying trademark rights.

## Alternatives considered

- **External centralized controller:** separates management failure domains, but requires per-project private connectivity and broader tenancy machinery.
- **OpenEverest plus a Zerops provider:** cannot remove the Kubernetes dependency through the existing provider contract (ADR 0005).
- **Managed database service:** does not provide the same self-hosted ownership and deployment model.
