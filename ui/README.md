# Karakoram web UI

React 18 + TypeScript + Vite single-page app. Talks to the control plane at `/api/v1` with the
cookie session (`X-Karakoram-CSRF` on unsafe methods) and keeps views live from the
`/api/v1/events/stream` server-sent events.

## Commands

```sh
npm ci              # install (lockfile committed)
npm run gen:api     # regenerate src/api/schema.d.ts from ../api/openapi.yaml (never hand-edit)
npm run dev         # Vite on :5173, proxies /api -> http://localhost:8080 (override KARAKORAM_API_URL)
npm run typecheck
npm run lint
npm run build       # writes ../internal/ui/dist
```

## Embedding in the Go binary

`internal/ui` serves the SPA with history-API fallback and cache headers. The build output is
embedded only when Go is compiled with `-tags ui`; without the tag the package still builds and
serves a "UI not built" page, so Go builds and tests never need Node:

```sh
(cd ui && npm ci && npm run build) && go build -tags ui ./cmd/karakoram
```

## Layout

- `src/api` – generated OpenAPI types, typed fetch client (`openapi-fetch`), query keys, SSE client.
- `src/auth` – session provider (`/auth/me`, `/permissions`), client-side policy evaluation that
  mirrors the server (deny overrides allow, glob objects), route guard.
- `src/components/ui` – dependency-free component kit (buttons, fields, native `<dialog>`, toasts,
  SVG line chart).
- `src/pages` – clusters list, creation wizard, cluster detail tabs, global events/operations,
  settings.
- `src/styles` – design tokens and component styles (no CSS framework).

Credentials are only fetched on explicit "Reveal credentials" (`GET /clusters/{c}/connection`), which
the server gates on the `cluster-credentials:read` permission; they are never cached in the query
store.
