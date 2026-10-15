# 0019 — Additive multi-engine seams

Status: Accepted

## Context

ADR 0005 introduced a provider seam, but the first implementation let MongoDB assumptions leak into generic code: a single registered provider, a fixed agent process whitelist and readiness port, PSMDB relay targets imported by namespace routing, a MongoDB-only metric vocabulary, PBM-specific retained-backup handling, fixed backup types and UI tabs that always offered users, FCV, balancer and PITR. Adding message brokers and search engines must not change PSMDB behaviour or force each engine to fork generic code.

## Decision

Extend the existing seams additively instead of introducing a plugin system:

- Register every compiled-in provider centrally (`internal/server/providers.go`); route by the immutable, required `spec.provider`; keep `psmdb` the explicit default in CLI convenience flags and the UI. Missing or changed provider values retain the API's structured field-error contract.
- Publish provider **capabilities**, supported **durable operations** and **process names** in `Describe`. The API rejects unsupported operations, backup types and log processes before persisting work; the UI gates tabs and actions by the same metadata, with a PSMDB-compatible UI fallback for older metadata.
- Extend shared models only with optional fields: topology types `cluster` and `dedicatedMasters`, named extra components, engine-owned JSON `engineOptions`, password-free connection endpoints, and provider/location backup provenance. Providers keep defaults and validation. Create/replace inputs and merge-patch inputs remain distinct, closed request schemas; extra component names and engine option keys are intentionally extensible. Optional components can use zero replicas to disable them, and merge-patch can remove named components with `null`.
- Add optional provider interfaces for relay targets, monitoring vocabulary and retained-backup deletion, and an optional infrastructure interface for Zerops public HTTPS subdomains. Callers assert them; absent interfaces mean "not supported".
- Engine-owned S3 artifacts use `backupstorage.NewClient` with credentials read from the sealed store. The exported constructor shares storage verification's endpoint allowlist, TLS configuration and redirect restrictions; callers close its idle connections with the returned cleanup function.
- Keep engine-specific data-plane APIs (for example RabbitMQ virtual hosts and permissions) in engine-owned interfaces, handlers and OpenAPI paths under `/clusters/{cluster}/<engine>/...`, authorized by the existing `database-users` RBAC resource.
- Move certificate issuance to `internal/pki`.
- In the node agent, add `agent.Engine` runners selected by `KARAKORAM_ENGINE` and the configuration's `engine` field. The common loop keeps configuration pull/cache, pinned TLS, bearer authentication, supervision, durable maintenance/stop holds, logs and host metrics; runners render files, start processes, report readiness/version/metrics and register bounded routes under `/v1/<engine>/`. The PSMDB runner stays on its existing code path.
- Engine change reviews pair their patch builder with consequence copy. The shared Apply dialog first builds the outgoing patch, then reconstructs the reviewed spec with RFC 7386 merge-patch semantics; omitted form defaults cannot become advertised changes. The existing MongoDB review continues to inspect only submitted patch fields.

## Consequences

A new engine adds a provider package, an agent runner package, engine-owned routes/UI and one registration line, without editing PSMDB code. Generic API clients gain metadata to avoid MongoDB-only actions. The agent's process whitelist is now engine-specific and enforced by the agent, while the client only validates name syntax. All engines are compiled into one executable, so a vulnerable engine runner still ships to every member; runners are only activated by explicit configuration. Shared enums grow with each engine that needs a new generic value.

## Alternatives considered

- **Generalize users/roles into one cross-engine principal model:** MongoDB roles, RabbitMQ tags/permissions and Elasticsearch roles/API keys do not share semantics; a lowest-common-denominator model would be misleading.
- **One agent binary per engine:** separate artifacts, versioning and upload paths for little isolation benefit; the bounded runner interface keeps authority narrow.
- **Out-of-process provider plugins:** rejected again for the reasons in ADR 0005.
