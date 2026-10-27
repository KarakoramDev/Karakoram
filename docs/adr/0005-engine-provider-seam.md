# 0005 — An engine provider seam inspired by OpenEverest v2

Status: Accepted

## Context

Engine-specific topology, validation, users, upgrades and backups should not spread into generic API/reconciler code. [OpenEverest](https://openeverest.io) v2 separates Provider metadata from Instance intent, but its `ProviderInterface` is [Kubernetes](https://kubernetes.io)-native: `Types` registers a runtime scheme, `Context` exposes a controller-runtime client, and watches, owner references and backup hooks consume Kubernetes objects. Reusing its signatures does not remove those dependencies.

## Decision

Define Karakoram's own [Go](https://go.dev) `engine.Provider` with `Name`, `Describe`, `ApplyDefaults`, `Validate`, `Sync`, `Status`, `RunOperation` and `Cleanup`. Implement only `psmdb` now. `Describe` owns the version catalog/upgrade paths, topologies, component defaults/limits, presets, form-schema hints and supported backup types.

The `psmdb`-only scope above is the original decision; [ADR 0019](0019-multi-engine-seams.md) later extends the seam for [RabbitMQ](https://www.rabbitmq.com) and [Elasticsearch](https://www.elastic.co/elasticsearch).

Pass a narrow reconciliation context containing cluster spec/status/provider-owned persisted internal state, the infrastructure Backend, cluster-scoped secret access, required resource-store access, events, agent dialer, executable artifact, logger and clock. The provider owns engine mechanisms; the reconciler owns durability/serialization; the Backend owns platform translation (ADRs 0004 and 0006). Operations carry re-entrant checkpoints, not opaque in-memory jobs. Cleanup respects explicit volume/backup retention.

Retain the conceptual separation of validation, desired-state application, observation and cleanup without importing CRDs, controller-runtime, [Helm](https://helm.sh) or the upstream provider SDK. Provider registration is in-process; this is not a dynamically installable plugin contract.

## Consequences

The core can evolve toward other engines without disguising [Zerops](https://zerops.io) as Kubernetes. Karakoram must implement database lifecycle logic previously delegated to [Percona](https://www.percona.com)'s Kubernetes operator. Generic metadata is reusable by API/CLI/UI, but engine-specific rules stay in the provider. The seam is intentionally bounded by today's requirements, not a promise of compatibility with OpenEverest providers.

## Alternatives considered

- **Reuse OpenEverest's ProviderInterface:** preserves existing code only by retaining its Kubernetes runtime and object model.
- **Put all behavior in a Zerops adapter:** conflates database semantics with infrastructure and makes either replacement expensive.
- **Separate provider services/plugins:** independent releases, but additional networking, authentication and deployment complexity without a current requirement.

## Multi-engine extension

The in-process registry is assembled centrally, with `psmdb` as the explicit CLI/UI default and required, immutable cluster provider selection in API requests. `Describe` now includes capabilities, supported durable operations and process names so API preflight and clients can avoid presenting [MongoDB](https://www.mongodb.com)-only behavior to other engines. Shared models carry extra components, engine-owned JSON options, connection endpoints and provider/location backup provenance; providers still own defaults and validation. Generic CLI creation omits MongoDB defaults for other providers.

The UI's `EngineUI` registry extends wizard sections, detail tabs, monitoring panels and topology summaries without putting engine-specific components into generic pages. Generic UI actions use provider capability/operation metadata, and provider-declared process names drive logs.

Optional `RelayTargeter`, `MetricsCatalog` and `RetainedBackupDeleter` interfaces keep namespace target policy, monitoring vocabulary and retained-backup deletion engine-neutral. Certificate issuance is shared in `internal/pki`, and the infrastructure Backend exposes Zerops public HTTPS subdomain control. Engine-specific access interfaces and HTTP routes remain engine-owned. At the node boundary, `agent.Engine` and its protected `Node` toolkit share cached configuration, authenticated transport, supervision, metrics and durable safety holds while preserving the built-in [PSMDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) runner. Engines are selected explicitly and compiled into the executable; no plugin service or remote-exec contract is introduced.
