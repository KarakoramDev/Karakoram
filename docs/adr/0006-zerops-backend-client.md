# 0006 — A thin infrastructure provider and owned Zerops REST client

Status: Accepted; infrastructure contract and factory construction generalized by [ADR 0030](0030-backend-neutral-infrastructure-contract.md)

## Context

[Zerops](https://zerops.io) exposes projects, service stacks, Local Storage services, environment secrets, app versions, public routes and asynchronous Processes. Its API is not a database controller. The broad `zerops-go` SDK is useful endpoint/DTO research, but exposing platform DTOs throughout the engine would tightly couple lifecycle code to that API.

## Decision

Define `infra.Backend` around required operations: project information; batch import; inspect/list/delete services; artifact deployment; service secret environment; lifecycle/resource changes; public port routing; containers/logs; and non-blocking operation status. Zerops is the sole implementation. Do not add speculative providers.

Own a thin HTTP REST client in `internal/infra/zerops`, implementing only those endpoints and their wire types. Use `zerops-go` and platform documentation as references, not as the public domain contract or a runtime dependency. Preserve returned Process IDs in reconciliation state and poll `GET /process/{id}`; HTTP acceptance is not completion. `GetService` distinguishes an absent service from an API failure.

Create a provider instance per namespace with its project-scoped token. The default token comes from runtime environment; additional tokens are sealed in the state store (ADR 0018). Send tokens only in authorization headers, never logs, artifacts or browser responses. Rotation validates the replacement against the same project before persisting it and updating the existing client's token, without restarting its relay. Rotate the default token by changing the service secret and restarting. Scope lookups/mutations to that namespace's project and owned resource identities; never manage the system `core` service. The engine requests intent through Provider types instead of constructing Zerops HTTP calls.

ADR 0030 replaces platform-shaped service/deployment intent and direct per-namespace Zerops construction with neutral types and registered factories. The owned Zerops REST client and non-blocking operation polling remain; Zerops is still the only registered production provider.

## Consequences

Wire/API changes are isolated and testable, and artifact streaming need not be forced through an unrelated abstraction. Karakoram owns endpoint coverage, error interpretation and compatibility maintenance. No platform rate-limit number or webhook/watch guarantee is invented; reconciliation uses polling. Logs are diagnostics, not durable controller truth.

## Alternatives considered

- **Use `zerops-go` directly:** broad ready-made endpoint coverage, but larger dependency/API surface and transport/DTO coupling; reconsider if its supported subset demonstrably reduces maintenance.
- **Invoke `zcli`:** familiar operations, but subprocess/configuration dependencies and output parsing are undesirable inside the control plane.
- **No Provider seam:** less initial code, but mixes platform and engine responsibilities.
