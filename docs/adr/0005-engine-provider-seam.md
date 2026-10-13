# 0005 — An engine provider seam inspired by OpenEverest v2

Status: Accepted

## Context

Engine-specific topology, validation, users, upgrades and backups should not spread into generic API/reconciler code. OpenEverest v2 separates Provider metadata from Instance intent, but its `ProviderInterface` is Kubernetes-native: `Types` registers a runtime scheme, `Context` exposes a controller-runtime client, and watches, owner references and backup hooks consume Kubernetes objects. Reusing its signatures does not remove those dependencies.

## Decision

Define Karakoram's own Go `engine.Provider` with `Name`, `Describe`, `ApplyDefaults`, `Validate`, `Sync`, `Status`, `RunOperation` and `Cleanup`. Implement only `psmdb` now. `Describe` owns the version catalog/upgrade paths, topologies, component defaults/limits, presets, form-schema hints and supported backup types.

Pass a narrow reconciliation context containing cluster spec/status/provider-owned persisted internal state, the infrastructure Backend, cluster-scoped secret access, required resource-store access, events, agent dialer, executable artifact, logger and clock. The provider owns engine mechanisms; the reconciler owns durability/serialization; the Backend owns platform translation (ADRs 0004 and 0006). Operations carry re-entrant checkpoints, not opaque in-memory jobs. Cleanup respects explicit volume/backup retention.

Retain the conceptual separation of validation, desired-state application, observation and cleanup without importing CRDs, controller-runtime, Helm or the upstream provider SDK. Provider registration is in-process; this is not a dynamically installable plugin contract.

## Consequences

The core can evolve toward other engines without disguising Zerops as Kubernetes. Karakoram must implement database lifecycle logic previously delegated to Percona's Kubernetes operator. Generic metadata is reusable by API/CLI/UI, but engine-specific rules stay in the provider. The seam is intentionally bounded by today's requirements, not a promise of compatibility with OpenEverest providers.

## Alternatives considered

- **Reuse OpenEverest's ProviderInterface:** preserves existing code only by retaining its Kubernetes runtime and object model.
- **Put all behavior in a Zerops adapter:** conflates database semantics with infrastructure and makes either replacement expensive.
- **Separate provider services/plugins:** independent releases, but additional networking, authentication and deployment complexity without a current requirement.
