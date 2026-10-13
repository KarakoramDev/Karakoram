# 0015 — OpenAPI-first server, clients and authorization metadata

Status: Accepted

## Context

The UI and CLI must expose the same operational surface. Handwritten route/type/permission inventories drift easily, especially around credential reveal and object filtering. OpenEverest demonstrates generated server contracts and route authorization mappings, but Karakoram needs its own non-Kubernetes API.

## Decision

Make `api/openapi.yaml` (OpenAPI 3.0.3) the source of truth for the public REST contract rooted at `/api/v1`. Use oapi-codegen v2 to generate Go server types/strict `net/http` interfaces and the Go client used by `karactl` and end-to-end callers. Generate TypeScript types with openapi-typescript for the React UI. Keep implementation handlers separate from generated files; never patch generated output by hand.

Annotate operations with `x-karakoram-rbac` and generate the operation-to-resource/action/object authorization map from that metadata. Explicitly mark unauthenticated authentication/bootstrap-facing contract operations as appropriate; missing authorization metadata must not silently become public. The separate credential resource from ADR 0013 remains explicit. Collection/stream filtering still requires runtime authorization for each returned object; route metadata alone cannot enforce it.

Describe secret inputs as write-only and omit them from ordinary response models. Model durable asynchronous operations and errors honestly rather than returning completed work for an accepted request. Document SSE in the contract and keep the private agent protocol versioned separately from the public API.

Require deterministic regeneration and an `openapi-check` regenerate/diff check when changing the contract. Update the schema first, regenerate affected server/client/TS artifacts and adapt all callers together.

## Consequences

Endpoint shape, CLI/UI types and coarse authorization share one reviewable source. Provider-specific semantic validation still belongs in the engine; an OpenAPI-valid request need not be a safe MongoDB transition. Generation adds pinned tooling/build steps, and codegen output is not evidence that handlers or per-object checks work. API changes require deliberate compatibility handling across shipped clients.

## Alternatives considered

- **Handwritten server and clients:** flexible, but duplicates schemas and permission inventories.
- **Generate OpenAPI from handlers:** convenient implementation-first flow, but makes the public contract secondary.
- **Separate RBAC route map:** simple initially, but security metadata can diverge from endpoint additions.
